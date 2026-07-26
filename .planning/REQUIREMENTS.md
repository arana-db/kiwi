# Kiwi 可验收需求

> 基线：Redis 8.8.1
> 状态：已批准需求集 v2
> 规则：每个实现工作项必须引用至少一个 `REQ-*`，每个 `REQ-*` 必须有可重复证据。

## Redis 兼容

- `REQ-COMPAT-001`：普通 Redis 行为必须以 Redis 8.8.1 tag、commit `77b6c308396c9700672390a210143a8496fb4b10` 为唯一 Oracle。
- `REQ-COMPAT-002`：RESP2 与 RESP3 原始 frame 必须做 differential，不得只依赖客户端 typed conversion。
- `REQ-COMPAT-003`：兼容矩阵必须记录命令、模式、返回类型、错误、known difference 和测试证据。
- `REQ-COMPAT-004`：Redis 官方 TCL suite 必须固定到同一 exact upstream commit；skip 必须有 owner、Issue、理由、引入日期和解除条件。
- `REQ-COMPAT-005`：redis-rs 只能进入独立测试工具或 dev dependency，生产 server crate 不得依赖它。
- `REQ-COMPAT-006`：Pipeline 中间错误、partial I/O、连接关闭、Push 交错和二进制 payload 必须有回归测试。
- `REQ-COMPAT-007`：所有新增公共命令、配置、错误、RESP 类型和客户端可见接口必须先取得 Redis 8.8.1 exact Oracle 证据。
- `REQ-COMPAT-008`：Oracle 构建和运行证据必须记录 exact source commit、构建命令、环境、binary hash 和 `INFO server` 身份，禁止任意源码与任意二进制拼接。

## RocksDB 权威存储与格式

- `REQ-STORAGE-001`：RocksDB 保存全量权威数据；Raft metadata、last_applied 和业务状态必须有明确的原子性合同。
- `REQ-STORAGE-002`：恢复测试必须释放所有 DB handle，再从路径重新打开。
- `REQ-STORAGE-003`：磁盘格式必须带 format version、Comparator 身份和迁移策略。
- `REQ-STORAGE-004`：部分写、尾部损坏、metadata/log 不一致、Snapshot 损坏和磁盘满必须有故障测试。
- `REQ-STORAGE-005`：Kiwi RocksDB key/value encoding 必须 binary-safe、round-trip、order-preserving、prefix-safe、canonical 和 stable；合同覆盖 `format_base_key.rs`、适用的 `format_*` 编码以及 `custom_comparator.rs`。
- `REQ-STORAGE-006`：任何派生状态、索引或未来热层都必须能从 RocksDB 权威数据删除后重建，不能成为成功回复或恢复的唯一依据。

## 系统稳定性门禁

- `REQ-STABILITY-001`：启动未来 Embedded Redis Hot Tier 生产实现前，必须由系统稳定性门禁明确给出通过结论并由用户批准解除冻结。
- `REQ-STABILITY-002`：稳定性门禁至少覆盖 Redis 8.8.1 differential、RocksDB 真正 close/reopen、Raft commit/apply/durability、Snapshot、成员变更、进程级故障注入、资源边界和可观测性。
- `REQ-STABILITY-003`：门禁证据必须绑定 branch、HEAD、平台、工具链、命令、测试结果和未覆盖风险；只通过单元测试不得视为系统稳定。
- `REQ-STABILITY-004`：门禁未通过期间，只允许维护未来热层的架构、许可证、ABI 和验收合同；禁止新增 Redis-derived 生产依赖、动态库 loader、热层数据路径或默认配置。
- `REQ-STABILITY-005`：任何解除冻结的决定必须追加到 `.planning/DECISIONS.md`，并同步更新 Roadmap、State 和 Kanban；不得由实现 PR 隐式解除。
- `REQ-STABILITY-006`：稳定性门禁通过只允许提交新的热层规划与授权请求，不自动授权 Redis fork 改造、动态库构建、loader、发行接入或热层生产实现。

## 未来内嵌 Redis 热数据层合同

本节是延期功能的验收合同，不构成当前实现授权。

- `REQ-HOT-001`：RocksDB 保存全量权威数据，热层可以完全删除并从 RocksDB 重建。
- `REQ-HOT-002`：热层未来采用可追溯到 Redis 8.8.1 exact commit 的 `arana-db/redis` 原生动态库；下游 commit、patch 和构建输入必须固定。
- `REQ-HOT-003`：热层内容不得进入 Raft Log、Snapshot、Backup 或磁盘格式。
- `REQ-HOT-004`：Cache miss、eviction、expire、fill drop 和 reset 不得改变 Redis 可观察结果。
- `REQ-HOT-005`：热层更新失败必须 update-or-invalidate，禁止已知旧值继续可见。
- `REQ-HOT-006`：异步 fill 必须校验 DB、Key、cache generation 和 applied index。
- `REQ-HOT-007`：TTL/PTTL 使用绝对毫秒时间；热层过期只产生 miss。
- `REQ-HOT-008`：Cache ON/OFF 必须运行同一套 Redis 8.8.1 兼容、Raft 一致性和故障测试。
- `REQ-HOT-009`：首期只允许 String 进入资格评估；其他类型必须逐个通过独立门禁后开启。
- `REQ-HOT-010`：动态库必须暴露版本化 C ABI；ABI 不得跨边界传递 Redis 内部对象指针、SDS 所有权或 allocator 私有状态。
- `REQ-HOT-011`：动态加载必须验证受控路径、库 hash、ABI version、Redis upstream commit、Redis downstream commit 和必需符号，任一不匹配必须拒绝加载。
- `REQ-HOT-012`：动态库的 allocator、全局状态、线程安全、多实例、创建/销毁、错误所有权、卸载和崩溃隔离必须在生产实现前通过专项审计。

## Raft

- `REQ-RAFT-001`：写成功回复必须发生在 quorum commit、本地 apply 和所选 durability profile 满足之后。
- `REQ-RAFT-002`：Linearizable Read 即使经过未来热层也必须通过 Leader/ReadIndex/Lease 门禁。
- `REQ-RAFT-003`：实现并冻结 `kiwi_redisraft_public_v1`，公开清单内行为 100% 通过。
- `REQ-RAFT-004`：RedisRaft 内部 `RAFT.AE`、`RAFT.REQUESTVOTE`、`RAFT.SNAPSHOT` 等不是公共兼容要求。
- `REQ-RAFT-005`：成员变更、Leader Transfer、Snapshot、日志回滚和真正 close/reopen 必须进入 required CI 或分层门禁。
- `REQ-RAFT-006`：建立带 seed 的 OpenRaft deterministic simulator，检查 Election Safety、Log Matching 和 State Machine Safety。
- `REQ-RAFT-007`：建立 3/5 节点进程级 kill、pause、partition、restart、disk fault 和 Elle/Jepsen history 测试。
- `REQ-RAFT-008`：非幂等写断线必须标记 `SUBMIT_UNKNOWN`，测试和客户端不得自动重放。

## 许可证与供应链

- `REQ-LICENSE-001`：Kiwi 自有、可独立识别的源码保持 Apache-2.0，并保留文件级版权与 SPDX 声明。
- `REQ-LICENSE-002`：未来 Redis-derived native library 必须源自 Redis 8.8.1 exact commit，并为该 fork 明确选择 `AGPL-3.0-only`。
- `REQ-LICENSE-003`：Redis 派生源码必须保留上游版权、许可证、来源、修改记录、下游 exact commit、补丁和构建选项。
- `REQ-LICENSE-004`：包含 Redis-derived native library 的官方组合发行不得标记为 Apache-2.0-only，必须履行 AGPL-3.0-only 的适用义务。
- `REQ-LICENSE-005`：组合发行必须提供与二进制精确匹配的 Kiwi 源码、Redis fork 源码、全部修改、ABI 定义、绑定生成方式、构建脚本、许可证、第三方通知和 SBOM。
- `REQ-LICENSE-006`：运行时源码身份和对应源码入口必须绑定 exact release/tag/commit，不得只指向浮动分支。
- `REQ-LICENSE-007`：动态链接、独立仓库和运行时加载只能作为工程边界，不得作为免除组合发行许可证义务的依据。
- `REQ-LICENSE-008`：首次公开发布包含 Redis-derived native library 的组合发行物前，必须完成开源许可证专项复核并记录结论。

## 可观测性与性能

- `REQ-OBS-001`：核心数据路径必须暴露请求结果、错误类别、延迟、资源使用和恢复状态；日志不得泄露敏感数据。
- `REQ-OBS-002`：Raft 必须暴露 term、role、leader、commit index、last applied、snapshot、membership、replication 和 fsync 指标。
- `REQ-OBS-003`：未来热层启用后必须暴露 hit、miss、fill、fill-drop、eviction、expire、update-failure、invalidate、generation-reset 和 load latency。
- `REQ-PERF-001`：当前性能基线至少包含 Redis 8.8.1 与相同数据集、协议和持久化声明下的 Kiwi；未来热层获准后再增加 Cache OFF/ON 对照。
- `REQ-PERF-002`：性能报告必须包含 P50、P95、P99、P99.9、吞吐、CPU、峰值内存、写放大和测试环境。
- `REQ-PERF-003`：平均吞吐提升不能掩盖尾延迟、内存失控、数据语义变化或恢复退化。

## 工作连续性

- `REQ-WORK-001`：项目长期事实写入 `.planning/`，不得仅保存在会话记忆。
- `REQ-WORK-002`：当前任务状态写入 `.codex/recovery/ACTIVE.md`，checkpoint 追加式保存。
- `REQ-WORK-003`：恢复状态必须记录 branch、HEAD、授权、dirty 归属、证据、剩余工作和下一条安全动作。
- `REQ-WORK-004`：branch、HEAD 或 dirty 漂移时，新会话必须停止写操作并报告差异。
