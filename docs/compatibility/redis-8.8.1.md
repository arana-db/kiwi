# Redis 8.8.1 兼容合同

> Oracle tag：`8.8.1`
> Oracle commit：`77b6c308396c9700672390a210143a8496fb4b10`
> Oracle license options：RSALv2、SSPLv1、AGPLv3
> Kiwi 当前运行基线：Cache OFF

关联决定：`D001`、`D009`、`D011`、`D012`、`D018`、`D020`。

主要需求：`REQ-COMPAT-001` 至 `REQ-COMPAT-010`、`REQ-STABILITY-001` 至 `REQ-STABILITY-006`。

## 1. 合同目的

Redis 8.8.1 exact binary/source 是 Kiwi 普通命令、RESP、错误、连接状态和最终数据状态的唯一 Redis 行为 Oracle。

“以 Redis 8.8.1 为基线”不等于一次性承诺该 tag 中所有可选能力都已经实现。每项能力必须在机器可读 manifest 中归入：

- `required`：当前完成定义，必须有可重复证据；
- `known_difference`：差异已确认、具备 owner、Issue 和解除条件；
- `deferred`：接口设计按 Redis 8.8.1 预留，但不属于当前里程碑；
- `unsupported`：明确不提供，并定义客户端可见错误或发现行为。

Redis Core、可选模块、管理接口和分布式限制必须分别分类，禁止用笼统的“Redis compatible”隐藏范围。

当前 main@`cb39927e44b84553f98ffee6ed1daa3f7388cf97` 的机器可读 manifest 只有 12 个命令条目，其中 4 个 `required`、2 个 `deferred`、6 个 `known_difference`；已执行的 Trusted Oracle differential 是 8 个 Vector 命令、40 个固定 node 的 Vector-only gate。它证明现有 Vector required registry，不证明 Redis Core、完整 TCL、集群或哨兵兼容。

## 2. Profile

兼容工具至少识别以下版本化 Profile：

```text
redis_8_8_1_core_resp2
redis_8_8_1_core_resp3
redis_8_8_1_runtime
redis_8_8_1_client_ecosystem
redis_8_8_1_standalone_cache_off
redis_8_8_1_raft_single_group_cache_off
kiwi_rocksdb_authority_v1
kiwi_redisraft_public_v1
```

未来热层启用后才能新增 Cache ON Profile。当前 manifest 可以保留 `cache_mode` 字段，但其可执行值只能是 `off`；不得以文档中存在未来字段为由注册隐藏或实验性 Cache ON 模式。

## 3. Oracle 身份和 provenance

每次 Oracle 构建和运行证据至少包含：

```text
upstream_url
tag
commit
source_tree_clean
compiler_identity
make_identity
build_command
build_environment_allowlist
build_log_sha256
binary_sha256
verification_rebuild_binary_sha256
binary_hash_equal
redis_version_output
host_os_and_arch
```

校验必须同时证明：

1. source checkout 的 `HEAD` 等于 exact commit；
2. tag 正确解析到该 commit；
3. 构建前 tracked、untracked 和 ignored 变更符合受控规则；
4. primary build binary 来自 primary exact checkout；
5. verifier 在自己创建的全新 disposable exact checkout 中按受控 recipe 独立重建；
6. primary binary 与 verification rebuild binary 的 SHA-256 完全一致；
7. 正式运行时身份来自 verification rebuild binary，并与 provenance 一致。

Metadata、build log、文件 identity、版本字符串和 ignored binary 即使完全自洽，也不能单独证明构建来源。版本相同但 fresh independent rebuild、exact hash equality 或 runtime binding 不可证明时，不得作为 required evidence。

详细信任边界和执行顺序见 `docs/superpowers/specs/2026-07-28-redis-8.8.1-trusted-oracle-provenance-design.md`。

## 4. Oracle 优先级

```text
Redis 8.8.1 原始 RESP differential
  > Redis 8.8.1 官方 TCL suite
  > Redis 8.8.1 与 Kiwi 最终数据库状态/TTL 对账
  > 当前维护的 redis-rs 测试客户端
  > 其他主流客户端兼容
  > Kiwi 单元测试
```

客户端 typed conversion 可能合并 Null、空集合、整数和字符串表现，也可能隐藏 Push、Pipeline 中间错误或二进制转换差异，因此不能替代 raw wire evidence。

### 4.1 门禁分层和测试来源职责

| 层级 | Required 内容 | 不得解释为 |
|---|---|---|
| PR fast | 受 changed-path 影响的 manifest/registry 合同、确定性 raw RESP2/RESP3、Kiwi/Redis exact response 与 final-state、单元/集成和静态检查 | 全命令兼容、系统稳定或 release ready |
| nightly/full | 固定 Redis exact commit 的官方 TCL external-server suite、扩大后的完整 differential、property/fuzz、确定性故障与可回放 seed/artifact | M6/release 稳定性通过 |
| M6/release | fresh independent Oracle rebuild、binary hash equality、真实 upgrade/rollback、close/reopen、3/5 节点历史和完整 exact-ref evidence bundle | Cache ON 或 Embedded Redis Hot Tier 解冻 |

职责固定如下：

- Redis 官方 TCL suite 提供固定上游场景和断言；runner 只允许 external-server 适配与机器可读 skip registry，不修改上游断言制造通过。
- Python 负责 raw RESP differential、二进制安全输入、跨语言集成、服务进程编排、final-state/TTL 对账和故障场景驱动；不迁移到一个新通用 harness 才能开始首切片。
- redis-rs 只在 raw/TCL 合同之后验证客户端生态；它是 test-only consumer，不是服务端语义 Oracle，也不进入生产依赖图。
- Cluster、Sentinel 和 Raft single-group 不进入第一个 standalone Core smoke；它们分别等待 WP4、WP6、WP7 的前置门禁和 exact-file plan。

## 5. 机器可读 manifest

命令条目至少包含：

```yaml
schema: kiwi-redis-compat/v2
command: GET
redis:
  tag: 8.8.1
  commit: 77b6c308396c9700672390a210143a8496fb4b10
classification: required
modes:
  standalone_cache_off: required
  raft_single_group_cache_off: required
protocols: [resp2, resp3]
arguments: exact
reply_schema: exact
errors: exact-prefix
ttl_semantics: applicable
tests:
  - wire-differential
  - final-state
known_differences: []
owner: cmd-string
```

`kiwi-redis-compat/v2` 保留 command-level `classification`，并新增可选、机器可读的 `required_cases`：

```yaml
command: SET
classification: known_difference
modes:
  standalone_cache_off: known_difference
  raft_single_group_cache_off: deferred
arguments: exact
required_cases:
  registry_path: tests/compat/redis-8.8.1/core-required-jobs.yaml
  registry_schema: kiwi-core-required-jobs/v1
  case_ids: [set-binary-success, set-wrong-arity]
known_differences:
  - owner: cmd-string
    issue: https://github.com/arana-db/kiwi/issues/325
    reason: required evidence currently covers only the listed SET cases, not the complete Redis 8.8.1 SET option surface
    remove_when: every Redis 8.8.1 SET arity and option family is classified and all required cases have trusted raw and final-state evidence
    introduced: 2026-08-20
    affected: standalone_cache_off; resp2/resp3; command-level coverage
    last_verified_ref: redis-source:77b6c308396c9700672390a210143a8496fb4b10
```

`classification` 始终描述整条命令；`required_cases` 只声明 registry 中列出的 case 是 fail-closed required subset，不能把 subset 提升为 command-level `required`。从 v1 迁移到 v2 时，现有 12 条命令的 classification 和 known-difference 语义必须原样保持。Schema 必须拒绝未知字段、空 owner、空 protocols、空 modes、重复命令名、不支持的 classification、空/重复/未登记 required case、registry/schema/path 漂移和 command/case 双向闭包缺口。命令名按 Redis ASCII 大小写规则规范化，不能使用 locale-sensitive 转换。

### 5.1 首个 Core smoke registry

Issue [#433](https://github.com/arana-db/kiwi/issues/433) 是 WP1 首个可执行切片。它把 6 个已存在的 standalone Cache OFF 命令登记到 v2 manifest；六条 command-level classification 均保持 `known_difference`，同时用 `required_cases` 冻结 RESP2/RESP3 各 15 个 fail-closed case：

| 命令 | 固定 case |
|---|---|
| `PING` | 无参数、binary echo、错误 arity |
| `SET` | binary key/value 成功、错误 arity |
| `GET` | binary existing、missing、错误 arity |
| `DEL` | single-key existing、single-key missing、错误 arity |
| `TYPE` | string、missing |
| `PTTL` | persistent `-1`、missing `-2` |

两个协议合计 30 个 server-backed node。首切片不覆盖 multi-key `DEL`、SET options、expiration mutation、transaction、pipeline、Push、Cluster 或 Sentinel，也不得据此声明 Redis Core 已整体兼容。现有 12 条 manifest 加入这 6 个命令后应严格成为 18 条：4 条 command-level `required`、2 条 `deferred`、12 条 `known_difference`。Core registry、manifest `required_cases` 和 evidence 中的 command/case/node 集合必须双向相等；删除或增加任一 case、把 SET options 或 multi-key DEL 偷换成 covered surface、或把六条命令提前改成 command-level `required` 都必须失败。

## 6. Raw RESP differential

测试必须保存双方原始请求和原始响应，并记录连接级事件顺序。比较范围包括：

- 每个 required node 的 exact request bytes，以及 Kiwi/Redis 的 exact response
  frame bytes；证据使用严格 Base64 保留原文，并同时保存可重算的 SHA-256，
  不能只保留 hash；
- server-backed node 在清理前后保存 raw `TYPE`、`PTTL` 和类型专属观察值；
  持久键的 `PTTL` 必须为 `-1`，清理后的缺失键必须为 `-2`，并验证两次
  `DEL` 的幂等结果；

- RESP2/RESP3 frame 类型及嵌套结构；
- Null Bulk、Null Array、Null、空集合和空字符串；
- Simple Error、Blob Error、错误前缀和稳定文本；
- Integer、Double、Big Number、Boolean、Verbatim String、Map、Set、Attribute 和 Push；
- Binary-safe key、value 和参数；
- Pipeline 中间错误后的剩余响应和连接可用性；
- Pub/Sub、tracking 或其他 Push 与普通 response 的交错；
- partial read/write、半关闭、断连和协议错误；
- 执行后的 RocksDB 权威内容、类型、TTL 和过期结果。

涉及随机、时间、无序集合或服务器生成 ID 的命令，只能使用命令专用 normalization。每个 normalization 必须有原因、适用字段和测试，禁止全局忽略顺序、错误文本或 TTL 差异。

## 7. TCL suite

- 测试源码固定到 exact commit。
- 优先采用 external-server 模式连接独立 Oracle 或 Kiwi。
- Standalone Cache OFF 与 Raft single-group Cache OFF 分开保存结果。
- 测试 runner 必须记录 suite commit、参数、环境、skip manifest 和服务端 exact identity。
- 不得修改上游断言来制造通过。

每条 skip 至少包含：

```yaml
test: unit/type/string
reason: exact technical reason
profile: standalone_cache_off
owner: module-or-person
issue: URL-or-number
introduced_at: YYYY-MM-DD
remove_when: measurable condition
```

没有 owner、Issue 或解除条件的 skip 视为 required failure。

## 8. 测试客户端边界

redis-rs 只用于客户端生态验收，可覆盖：

- sync、async 和 multiplexed connection；
- Pipeline、MULTI/EXEC 和错误恢复；
- Pub/Sub、ACL、Streams 和 scripting；
- RESP3 与 Push；
- 适用的 Cluster/Leader 客户端可见错误。

它只能存在于独立兼容工具或 development dependency。CI 必须证明 Kiwi 生产 server crate dependency graph 不包含 redis-rs。

## 9. Cache OFF 合同

当前 required 测试全部运行在 Cache OFF：

```text
Client
  → RESP parser
  → Redis 8.8.1 command validation and semantics
  → OpenRaft consistency gate when applicable
  → RocksDB authoritative state
  → RESP encoder
```

Cache OFF 下不得存在：

- Redis 派生动态库加载；
- 热层 lookup、fill、update 或 invalidate；
- 依赖热层才能通过的 TTL 或类型语义；
- Cache ON 才能达到的正确性、恢复性或可用性声明。

未来 Cache ON 的唯一兼容标准仍是与 Cache OFF 运行同一 Scenario 并产生相同客户端可见结果和权威最终状态。该测试要求已冻结，但目前不授权实现或执行 Cache ON 路径。

只有 `docs/quality/system-stability-gate.md` 全部通过，并且用户基于门禁证据重新明确批准，才能建立 Cache ON 或 Embedded Redis Hot Tier 实现任务。

## 10. Redis 8.8.1 接口设计规则

新增或修改公开接口时必须先核对 Redis 8.8.1：

- 命令名、arity、flags、key specification 和 ACL categories；
- 参数语法、互斥规则、默认值和错误优先级；
- RESP2/RESP3 reply schema；
- TTL、transaction、Lua、Pub/Sub、blocking 和 connection-state 语义；
- `COMMAND`、`HELLO`、`INFO`、`CONFIG` 等发现接口的一致性；
- 客户端在 unsupported/deferred 能力下能观察到的明确结果。

不能先按 Kiwi 内部实现方便程度设计接口，再通过 normalization 或文档声明把差异合理化。

## 11. 证据保存

每次兼容验收至少保存：

- Kiwi exact commit 和二进制 hash；
- Redis exact commit 和二进制 hash；
- Primary build 和 verifier rebuild 各自独立的 source、toolchain、command、environment、build log 和 binary hash；
- 两个 binary 的 exact SHA-256 equality 结果；
- 正式 `INFO server` evidence 与 verifier rebuild binary 的绑定；
- 受控 toolchain identity、versioned recipe 和 required evidence artifact/schema 的严格校验结果；
- provenance 中全部 cleanup boolean 与完成时间，包括进程回收、runtime/checkout/verification log/temporary root 删除、primary/source/tool-FD/output-parent 最终复核和 fallible evidence handle 关闭结果；
- manifest/schema version；
- OS、架构、编译器和关键配置；
- 原始 request/response transcript；
- final-state/TTL 对账；
- known difference 和 skip 清单；
- 命令、退出码和日志 hash。

正式 differential evidence 使用固定文件 allowlist 和逐文件上限：raw transcript
解码后不超过 16 MiB，final-state 不超过 4 MiB，每份 collection/test/Kiwi
日志不超过 8 MiB。Runner 接收的固定 raw evidence set 的逐文件上限合计为
97.0625 MiB，因此不另设不可达的 raw aggregate 分支；controller 构造的 canonical
evidence（包含 runtime、toolchain 和 Redis log）仍必须低于 128 MiB。缺失、额外、
重复、截断、非有限 JSON 数值、symlink、特殊文件或越界内容均必须 fail closed。

Verifier 必须在 callback、进程和目录清理及输入身份复核全部成功后，先原子发布
`kiwi-vector-differential-evidence/v1`，再发布引用其 exact size/SHA-256 的
`kiwi-redis-oracle-provenance/v4`。Provenance 最后可见；任一 write、fsync、rename
或发布后复核失败时，两份 final 文件都必须回滚。CI 只上传这两个最终文件，
不能上传 live work directory、candidate 或 provenance-only 结果；上传 artifact 的
固定保留期为 7 天。

D020 的首切片不建立第二套 controller。Target 是保留上述 held-FD 工具执行、absolute deadline、输出上限、进程组 cleanup、cleanup-before-publish、原子发布、回滚和 provenance binding 内核，只把当前硬编码的 Vector evidence descriptor、allowlist、collector 与 Rust binding 收窄泛化为两个固定 profile：现有 `vector-v1` 继续绑定 `kiwi-vector-differential-evidence/v1`，新增 `core-smoke-v1` 绑定固定的 Core evidence schema。`kiwi-redis-oracle-provenance/v4` envelope 和 Vector 行为必须保持兼容；若实施证明必须改变 v4 字段、含义或外部 binding，实施 task 必须停止并先形成新的 Decision/schema 迁移计划，不能静默升版。

测试通过但缺少 exact identity 或原始 transcript 时，只能作为辅助结果，不能关闭 required compatibility item。

## 12. 基线变更规则

任何 Redis exact tag 或 commit 变化必须：

1. 新建架构 Decision；
2. 完成许可证和发行边界复核；
3. 生成命令、协议、配置、持久化格式和官方测试差异；
4. 更新所有 Profile、manifest、known difference 和 skip；
5. 重新执行 primary build、verifier fresh-checkout independent rebuild、binary hash equality 和 rebuild runtime identity；
6. 重新运行 Cache OFF 系统稳定性门禁；
7. 单独评估未来热层 fork、patch 和 ABI pairing。

不得通过浮动依赖、Docker `latest` 或系统包更新隐式改变基线。
