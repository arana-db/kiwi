# Redis 8.8.1 兼容合同

> Oracle tag：`8.8.1`
> Oracle commit：`77b6c308396c9700672390a210143a8496fb4b10`
> Oracle license options：RSALv2、SSPLv1、AGPLv3
> Kiwi 当前运行基线：Cache OFF

关联决定：`D001`、`D009`。

主要需求：`REQ-COMPAT-001..008`、`REQ-STABILITY-001..006`。

## 1. 合同目的

Redis 8.8.1 exact binary/source 是 Kiwi 普通命令、RESP、错误、连接状态和最终数据状态的唯一 Redis 行为 Oracle。

“以 Redis 8.8.1 为基线”不等于一次性承诺该 tag 中所有可选能力都已经实现。每项能力必须在机器可读 manifest 中归入：

- `required`：当前完成定义，必须有可重复证据；
- `known_difference`：差异已确认、具备 owner、Issue 和解除条件；
- `deferred`：接口设计按 Redis 8.8.1 预留，但不属于当前里程碑；
- `unsupported`：明确不提供，并定义客户端可见错误或发现行为。

Redis Core、可选模块、管理接口和分布式限制必须分别分类，禁止用笼统的“Redis compatible”隐藏范围。

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
redis_version_output
host_os_and_arch
```

校验必须同时证明：

1. source checkout 的 `HEAD` 等于 exact commit；
2. tag 正确解析到该 commit；
3. 构建前 tracked、untracked 和 ignored 变更符合受控规则；
4. binary 来自该 canonical source tree；
5. 运行时身份和 binary hash 与 provenance 一致。

版本字符串相同但来源、commit 或 binary hash 不可证明时，不得作为 required evidence。

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

## 5. 机器可读 manifest

命令条目至少包含：

```yaml
schema: kiwi-redis-compat/v1
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

Schema 必须拒绝未知字段、空 owner、空 protocols、空 modes、重复命令名和不支持的 classification。命令名按 Redis ASCII 大小写规则规范化，不能使用 locale-sensitive 转换。

## 6. Raw RESP differential

测试必须保存双方原始请求和原始响应，并记录连接级事件顺序。比较范围包括：

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
- manifest/schema version；
- OS、架构、编译器和关键配置；
- 原始 request/response transcript；
- final-state/TTL 对账；
- known difference 和 skip 清单；
- 命令、退出码和日志 hash。

测试通过但缺少 exact identity 或原始 transcript 时，只能作为辅助结果，不能关闭 required compatibility item。

## 12. 基线变更规则

任何 Redis exact tag 或 commit 变化必须：

1. 新建架构 Decision；
2. 完成许可证和发行边界复核；
3. 生成命令、协议、配置、持久化格式和官方测试差异；
4. 更新所有 Profile、manifest、known difference 和 skip；
5. 重建受控 Oracle provenance；
6. 重新运行 Cache OFF 系统稳定性门禁；
7. 单独评估未来热层 fork、patch 和 ABI pairing。

不得通过浮动依赖、Docker `latest` 或系统包更新隐式改变基线。
