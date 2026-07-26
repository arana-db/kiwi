# Embedded Redis Hot Tier Native ABI v1

> 状态：设计冻结；未获实现授权
> Redis source baseline：tag `8.8.1` / commit `77b6c308396c9700672390a210143a8496fb4b10`
> 当前 Kiwi 运行模式：Cache OFF

关联决定：`D009`、`D010`。

主要需求：`REQ-STABILITY-001..006`、`REQ-HOT-001..012`、`REQ-LICENSE-002..008`。

## 1. 生效条件

本文冻结未来 Redis 派生动态库与 Kiwi Rust 进程之间的 C ABI 设计，以便当前存储、Raft、兼容和发行工作保持稳定边界。

本文不授权：

- 创建 Redis fork 或导入 Redis 派生源码；
- 编写 C/C++ wrapper、Rust FFI、loader 或 Cache ON 路径；
- 新增 Cargo/native build dependency；
- 编译、链接、打包或发布动态库。

只有 `docs/quality/system-stability-gate.md` 全部通过，并且用户基于门禁证据重新明确批准，才能把本文拆成实现任务。

## 2. 设计原则

1. 只导出版本化 C ABI，不暴露 C++ ABI 或 Redis internal ABI。
2. Rust 只保存 opaque handle，不读取 `robj`、SDS 或集合内部布局。
3. 同一侧分配、同一侧释放；allocator ownership 不能跨边界猜测。
4. 所有错误通过 status 和 caller-owned error buffer 返回；不依赖 `errno` 或 thread-local last error。
5. C panic/abort、C++ exception 和 Rust unwind 不能穿越 FFI。
6. 每个实例有独立配置和可变状态；禁止未审计的共享 Redis server globals。
7. 动态库 hash、pairing manifest 和运行时 identity 必须三方一致。
8. 热层失败只能形成 miss、invalidate 或 Cache OFF 降级，不能改变 RocksDB 权威结果。
9. v1 只设计 binary-safe String whole-value，不承诺其他 Redis 类型。

## 3. 平台产物

```text
Linux:   libkiwi_redis_hot_tier.so
macOS:   libkiwi_redis_hot_tier.dylib
Windows: kiwi_redis_hot_tier.dll
         kiwi_redis_hot_tier.lib   # import library only
```

除单一 ABI discovery symbol 外，其他符号默认隐藏。Windows `.lib` 只作为 import library，不定义独立静态链接发行路径。

## 4. ABI 版本模型

动态库只导出一个稳定入口：

```c
#if defined(_WIN32)
#define KIWI_REDIS_HOT_EXPORT __declspec(dllexport)
#define KIWI_REDIS_HOT_CALL __cdecl
#else
#define KIWI_REDIS_HOT_EXPORT __attribute__((visibility("default")))
#define KIWI_REDIS_HOT_CALL
#endif

KIWI_REDIS_HOT_EXPORT
const kiwi_redis_hot_api_v1 *
KIWI_REDIS_HOT_CALL
kiwi_redis_hot_tier_get_api_v1(uint32_t requested_abi_version);
```

版本编码：

```c
#define KIWI_REDIS_HOT_ABI_V1_MAJOR 1u
#define KIWI_REDIS_HOT_ABI_V1_MINOR 0u
#define KIWI_REDIS_HOT_ABI_PACK(major, minor) (((major) << 16u) | (minor))
#define KIWI_REDIS_HOT_ABI_V1 \
    KIWI_REDIS_HOT_ABI_PACK(KIWI_REDIS_HOT_ABI_V1_MAJOR, KIWI_REDIS_HOT_ABI_V1_MINOR)
```

兼容规则：

- major 不相同：拒绝加载；
- library minor 小于 Kiwi required minor：拒绝加载；
- library minor 大于 Kiwi known minor：只读取 `struct_size` 覆盖的已知前缀，保留字段必须为零；
- function table 和所有可扩展 struct 都携带 `struct_size`；
- 不能通过 reinterpret cast 绕过版本检查。

## 5. 基础类型

规范头文件的概念接口如下：

```c
#include <stddef.h>
#include <stdint.h>

typedef struct kiwi_redis_hot_handle_v1 kiwi_redis_hot_handle_v1;

typedef struct {
    const uint8_t *ptr;
    size_t len;
} kiwi_redis_hot_bytes_v1;

typedef int32_t kiwi_redis_hot_status_v1;

#define KIWI_REDIS_HOT_OK ((kiwi_redis_hot_status_v1)0)
#define KIWI_REDIS_HOT_MISS ((kiwi_redis_hot_status_v1)1)
#define KIWI_REDIS_HOT_INVALID_ARGUMENT ((kiwi_redis_hot_status_v1)2)
#define KIWI_REDIS_HOT_ABI_MISMATCH ((kiwi_redis_hot_status_v1)3)
#define KIWI_REDIS_HOT_IDENTITY_MISMATCH ((kiwi_redis_hot_status_v1)4)
#define KIWI_REDIS_HOT_TYPE_MISMATCH ((kiwi_redis_hot_status_v1)5)
#define KIWI_REDIS_HOT_OUT_OF_MEMORY ((kiwi_redis_hot_status_v1)6)
#define KIWI_REDIS_HOT_BUSY ((kiwi_redis_hot_status_v1)7)
#define KIWI_REDIS_HOT_SHUTTING_DOWN ((kiwi_redis_hot_status_v1)8)
#define KIWI_REDIS_HOT_CORRUPT_ENTRY ((kiwi_redis_hot_status_v1)9)
#define KIWI_REDIS_HOT_INTERNAL ((kiwi_redis_hot_status_v1)10)

typedef struct {
    uint32_t struct_size;
    uint32_t code;
    uint32_t message_len;
    uint8_t message_truncated;
    uint8_t reserved[3];
    char message[256];
} kiwi_redis_hot_error_v1;
```

输入 `bytes` 只在函数调用期间借用。长度为零时 `ptr` 可以为 `NULL`；长度非零时 `ptr` 必须有效。所有长度使用 `size_t`，实现必须在转换到 Redis 内部长度前做溢出检查。

Status 使用固定 `int32_t`，不使用底层宽度由编译器决定的 C enum。所有导出函数和 function pointer 都使用 `KIWI_REDIS_HOT_CALL`；规范 header 必须把该 calling convention 应用到 table 中的每个函数指针。

错误结构由调用方分配，库只写入已声明容量。错误消息用于诊断，不定义 Redis 客户端可见错误语义，也不得包含 key/value、凭据或其他敏感内容。

## 6. Identity

```c
typedef struct {
    uint32_t struct_size;
    uint32_t abi_version;
    char redis_upstream_tag[32];
    char redis_upstream_commit[41];
    char redis_downstream_commit[41];
    char selected_license[32];
    char build_id[65];
    uint8_t reserved[128];
} kiwi_redis_hot_identity_v1;
```

约束：

- commit 使用完整 40 位十六进制；
- `build_id` 是规范化构建输入的 SHA-256 十六进制；
- `selected_license` 必须等于 `AGPL-3.0-only`；
- 字符串必须 NUL terminated，未使用字节清零；
- runtime identity 必须与 pairing manifest 一致。

## 7. 配置和实例

```c
typedef struct {
    uint32_t struct_size;
    uint32_t flags;
    uint64_t instance_id;
    uint64_t max_memory_bytes;
    uint32_t eviction_policy;
    uint32_t shard_count;
    uint64_t generation;
    uint8_t reserved[128];
} kiwi_redis_hot_config_v1;
```

实例合同：

- `create` 成功后，配置被库完整复制，调用方可立即释放原内存；
- `instance_id` 在当前进程生命周期内唯一；
- 每个 handle 拥有独立 keyspace、TTL、eviction、generation 和统计状态；
- 不同 handle 之间不得共享可变 Redis database/server globals；
- 必要的进程级只读表或 allocator runtime 必须由库内部同步和引用计数管理；
- 一个实例 OOM、flush 或 destroy 不能修改其他实例；
- v1 配置创建后不可原地变更，配置变化通过新 handle 和 generation 切换实现。

`shard_count` 和 eviction policy 的具体可选值必须在实现前另行冻结；未知值必须返回 `INVALID_ARGUMENT`，不得静默采用默认值。

## 8. Owned buffer 和 allocator

```c
typedef struct {
    uint32_t struct_size;
    uint8_t *ptr;
    size_t len;
    size_t capacity;
    uint64_t owner_cookie;
    uint8_t reserved[32];
} kiwi_redis_hot_owned_buffer_v1;
```

所有权规则：

- `get` 返回的 value buffer 由 Redis 派生库分配；
- Kiwi 只能读取，不能 `free`、`realloc` 或修改该内存；
- Kiwi 必须调用同一 API table 的 `buffer_release`；
- `buffer_release` 必须接受零初始化 buffer，并在成功后把字段清零；
- foreign、已释放或 owner 不匹配的非零 buffer 必须返回 `INVALID_ARGUMENT`，不能尝试释放；
- buffer 不能跨 library unload、handle destroy 或进程边界保存；
- `owner_cookie` 只供库验证归属，Kiwi 不解释；
- library allocator 和 Rust allocator 不互相释放内存；
- v1 不支持调用方注入 allocator，避免 allocator 生命周期和线程语义不清。

如果 allocation 失败，`get` 返回 `OUT_OF_MEMORY` 且输出 buffer 保持零初始化。上层必须按 miss/fallback 处理，不能返回未初始化或旧数据。

## 9. Entry 模型

```c
typedef struct {
    uint32_t struct_size;
    uint32_t db_id;
    kiwi_redis_hot_bytes_v1 key;
    kiwi_redis_hot_bytes_v1 value;
    int64_t absolute_expire_at_ms; /* -1 means no expiry */
    uint64_t generation;
    uint64_t applied_index;
    uint8_t reserved[64];
} kiwi_redis_hot_put_v1;

typedef struct {
    uint32_t struct_size;
    uint32_t db_id;
    kiwi_redis_hot_bytes_v1 key;
    uint64_t expected_generation;
    uint64_t minimum_applied_index;
    int64_t now_ms;
    uint8_t reserved[64];
} kiwi_redis_hot_get_v1;

typedef struct {
    uint32_t struct_size;
    kiwi_redis_hot_owned_buffer_v1 value;
    int64_t absolute_expire_at_ms;
    uint64_t generation;
    uint64_t applied_index;
    uint8_t reserved[64];
} kiwi_redis_hot_get_result_v1;
```

v1 只保存完整 binary-safe String value。`absolute_expire_at_ms` 使用绝对毫秒；热层过期只产生 miss，不修改 RocksDB 权威 TTL。

`get` 只有在 entry generation 相等且 `applied_index >= minimum_applied_index` 时才能返回 hit。损坏、过期或不满足版本条件的 entry 必须删除或视为 miss。

## 10. API table

```c
typedef struct kiwi_redis_hot_api_v1 {
    uint32_t struct_size;
    uint32_t abi_version;

    kiwi_redis_hot_status_v1 (KIWI_REDIS_HOT_CALL *query_identity)(
        kiwi_redis_hot_identity_v1 *out,
        kiwi_redis_hot_error_v1 *error);

    kiwi_redis_hot_status_v1 (KIWI_REDIS_HOT_CALL *create)(
        const kiwi_redis_hot_config_v1 *config,
        kiwi_redis_hot_handle_v1 **out_handle,
        kiwi_redis_hot_error_v1 *error);

    kiwi_redis_hot_status_v1 (KIWI_REDIS_HOT_CALL *get)(
        kiwi_redis_hot_handle_v1 *handle,
        const kiwi_redis_hot_get_v1 *request,
        kiwi_redis_hot_get_result_v1 *out,
        kiwi_redis_hot_error_v1 *error);

    kiwi_redis_hot_status_v1 (KIWI_REDIS_HOT_CALL *put)(
        kiwi_redis_hot_handle_v1 *handle,
        const kiwi_redis_hot_put_v1 *request,
        kiwi_redis_hot_error_v1 *error);

    kiwi_redis_hot_status_v1 (KIWI_REDIS_HOT_CALL *invalidate)(
        kiwi_redis_hot_handle_v1 *handle,
        uint32_t db_id,
        kiwi_redis_hot_bytes_v1 key,
        uint64_t generation,
        kiwi_redis_hot_error_v1 *error);

    kiwi_redis_hot_status_v1 (KIWI_REDIS_HOT_CALL *flush)(
        kiwi_redis_hot_handle_v1 *handle,
        uint64_t next_generation,
        kiwi_redis_hot_error_v1 *error);

    kiwi_redis_hot_status_v1 (KIWI_REDIS_HOT_CALL *buffer_release)(
        kiwi_redis_hot_owned_buffer_v1 *buffer,
        kiwi_redis_hot_error_v1 *error);

    kiwi_redis_hot_status_v1 (KIWI_REDIS_HOT_CALL *destroy)(
        kiwi_redis_hot_handle_v1 **handle,
        kiwi_redis_hot_error_v1 *error);

    uint8_t reserved[256];
} kiwi_redis_hot_api_v1;
```

所有输出结构在调用前由 Kiwi 零初始化。失败时，除文档明确说明的字段外，输出必须保持零值。

## 11. update-or-invalidate

写入权威状态后的未来热层规则：

```text
RocksDB/Raft apply succeeds
  → hot-tier put
       OK      → entry may remain
       failure → invalidate same db/key
                    OK      → continue with metric
                    failure → mark instance unhealthy and flush/disable
```

不能因为 `put` 失败回滚已经 Commit 的业务写，也不能保留可能陈旧的旧 entry。若无法证明 invalidate 成功，整个实例必须进入不可命中状态，直到 flush、generation 切换或 Cache OFF 重建。

## 12. 线程模型

- `query_identity` 可并发调用；
- 同一 handle 的 `get`、`put` 和 `invalidate` 必须由库实现线程安全；
- 不同 handle 可并行使用；
- `flush` 与普通操作的线性化语义必须由实现测试固定；
- `destroy` 不与其他调用并发，Kiwi 必须先停止新请求并等待在途调用归零；
- API 不保存借用的 input pointer；
- v1 不允许库回调 Rust，避免 callback 生命周期和 unwind 风险；
- 库内部创建的线程必须可停止、可 join，并在 `destroy` 返回前释放；
- signal handler、fork-after-init 和 unload-after-thread-start 默认不支持，除非后续 Decision 和测试明确批准。

## 13. Panic、异常和 fatal 行为

- 导出函数必须捕获所有允许捕获的内部错误并返回 status；
- C++ exception 不得穿越 `extern "C"`；
- Rust wrapper 不得 unwind 进入 C；
- 外部输入、OOM、类型错误或损坏 entry 不得调用 `abort()`；
- Redis 上游可能触发进程级退出的路径必须在 fork 中移除、隔离或证明不可达；
- truly unrecoverable invariant violation 必须在实现前列入审核清单，不能默认接受 Redis server 的 fatal 策略。

## 14. Pairing manifest

规范 manifest：

```json
{
  "schema": "kiwi-redis-hot-tier-pairing/v1",
  "kiwi_version": "<version>",
  "kiwi_commit": "<40-hex>",
  "redis_upstream_tag": "8.8.1",
  "redis_upstream_commit": "77b6c308396c9700672390a210143a8496fb4b10",
  "redis_downstream_commit": "<40-hex>",
  "selected_license": "AGPL-3.0-only",
  "abi_major": 1,
  "abi_minor": 0,
  "platform": "<os-arch-libc>",
  "library_name": "<canonical filename>",
  "library_sha256": "<64-hex>",
  "build_id": "<64-hex>",
  "source_url": "<immutable exact source URL>",
  "sbom_sha256": "<64-hex>"
}
```

Schema 必须拒绝未知字段、短 commit、非法 hash、浮动 source URL、绝对 library path 和包含路径遍历的文件名。

## 15. Loader 校验顺序

未来 loader 必须按以下顺序 fail closed：

```text
read manifest
  → validate schema and exact Kiwi pairing
  → resolve library inside approved distribution directory
  → reject symlink/reparse/path escape according to platform policy
  → hash library bytes
  → compare SHA-256
  → load with restricted search path
  → resolve the single discovery symbol
  → negotiate ABI version and struct size
  → query runtime identity
  → compare upstream/downstream/build/license identity
  → create instance
  → only then mark hot tier available
```

任一步失败都不得尝试搜索系统目录、当前目录或任意环境路径中的同名库。默认结果是保持 Cache OFF，而不是加载身份不明的二进制。

## 16. 多实例和生命周期

生命周期状态机：

```text
UNLOADED
  → VERIFIED
  → LOADED
  → INSTANCE_CREATED
  → SERVING
  → QUIESCING
  → DESTROYED
  → UNLOADED
```

要求：

- 每个 Storage/DB 映射必须有显式 instance/shard 设计，不依赖隐含 Redis DB globals；
- generation 单调推进，不能回绕后继续信任旧 entry；
- Snapshot Install、restore、RocksDB reopen 和格式迁移创建新 generation；
- `destroy` 后所有 handle、buffer 和 API 调用都失效；
- 动态库 unload 只能在全部 handle、buffer 和库线程释放后发生；
- 进程退出可以选择不 unload，但必须证明不会影响刷新、日志或崩溃报告。

## 17. 验证计划

获得实现授权后，ABI 至少需要：

- C header compile test：C11、C++、MSVC、Clang、GCC；
- Rust bindgen/cbindgen layout golden test；
- `sizeof`、alignment、enum width 和 calling convention test；
- major/minor/struct-size negotiation test；
- allocator ownership、double release、foreign buffer 和 leak test；
- concurrent get/put/invalidate/flush/destroy race test；
- multi-instance isolation test；
- OOM、corrupt entry、invalid pointer/length 和 error truncation test；
- ASan、UBSan、TSan 或平台适用替代工具；
- library hash、path escape、symbol missing 和 identity mismatch test；
- Cache OFF fallback 和进程重启 test；
- Cache ON/OFF raw differential 和最终 RocksDB 状态对账。

这些是未来实现验收条件，不是当前要执行的测试。

## 18. ABI 变更规则

以下变化必须提升 major：

- 改变已有 struct 字段含义、顺序、大小或所有权；
- 改变 allocator、buffer release、thread safety 或 destroy 合同；
- 改变 status 数值；
- 暴露 Redis internal object/layout；
- 改变 calling convention；
- 允许 callback 或跨边界 unwind。

仅在结构尾部增加可选字段或在 table 尾部增加函数，可以提升 minor，但必须通过 `struct_size` 保持前缀兼容。

任何 ABI 变更同时要求更新 pairing manifest、兼容测试、发行说明和 Corresponding Source。
