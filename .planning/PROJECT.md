# Kiwi 项目宪法

> 状态：已批准
> 生效日期：2026-07-26
> Redis 基线：8.8.1
> Kiwi 自有源码许可证：Apache License 2.0
> 未来官方组合发行：履行 AGPL-3.0-only 适用义务

## 北极星

Kiwi 是一个以 Redis 8.8.1 exact tag 可观察语义兼容为目标的生产级 Rust Redis-compatible 数据库。

Kiwi 使用 RocksDB 保存全量、权威、可恢复的数据，使用 OpenRaft 提供经过故障验证的强一致、高可用、成员变更、快照和恢复能力。内嵌 Redis 8.8.1 原生内存热数据层属于后续性能增强：现在只冻结面向 Redis 8.8.1 的来源、许可证、ABI、正确性和发行接口合同，不进入生产实现；只有整体系统通过稳定性门禁并获得明确批准后，才能启动该实现。

任何 Raft、分片、存储格式、兼容性或未来热层优化，都不得改变 Redis 8.8.1 的可观察语义、已经声明的持久化边界和一致性承诺。

## 固定基线

```text
Redis tag:                    8.8.1
Redis commit:                 77b6c308396c9700672390a210143a8496fb4b10
Redis license choices:        RSALv2 / SSPLv1 / AGPLv3
Selected Redis fork license:  AGPL-3.0-only
Kiwi language:                Rust
Kiwi-authored source:         Apache-2.0
Durable truth:                RocksDB
Consensus:                    OpenRaft
Future hot-tier source:       arana-db/redis planned; downstream exact pin pending
Raft API model:               RedisRaft public compatibility profile
Rust client:                  redis-rs test-only compatibility client
Oracle provenance:            independent rebuild and binary-hash equality
```

Redis 8.8.1 exact commit 是当前唯一兼容、接口设计、行为 Oracle、测试和未来原生热层的上游来源基线。`arana-db/redis` 的下游 exact commit、patch 清单、构建配置和产物 hash 尚未建立前，它不是 Kiwi 构建输入；不得使用浮动 tag、branch、binary 或未校验来源替代上述 exact commit。

## 当前产品主线

### 1. Redis 8.8.1 兼容

- RESP2、RESP3、Pipeline、错误、连接和二进制安全行为必须可对照验证。
- 命令、参数、返回类型、边界、TTL、事务、Lua、Pub/Sub、阻塞命令和客户端行为必须有机器可读兼容矩阵。
- Redis 8.8.1 exact binary/source 是普通 Redis 语义的唯一权威 Oracle。
- Oracle verifier 必须在全新的 disposable exact checkout 中独立重建 Redis，要求 primary build 与 verifier rebuild 的 binary hash 完全一致，并只运行独立重建产物取得正式 `INFO server` 证据；调用者提供的 metadata、build log 和 ignored binary 不能自证来源。
- 任何 skip 都必须有 owner、原因、Issue、引入日期和解除条件。
- 新增公共命令、配置、协议字段和管理接口必须先核对 Redis 8.8.1 的行为，不得凭经验推断。

### 2. RocksDB 权威存储与系统稳定性

- RocksDB 始终保存全量、权威、可恢复的数据。
- OpenRaft 提交、状态机应用、RocksDB durability 和客户端成功回复之间必须有可验证合同。
- 崩溃恢复必须释放全部 `Storage`、`Redis`、`Arc<DB>`、column-family、iterator、snapshot 和后台任务等 RocksDB 所有权，再按路径重新打开；不得复用仍持有数据库的对象代替真实恢复。
- 系统稳定性门禁覆盖持久化、恢复、Raft、安全性、协议兼容、故障注入、可观测性和资源边界。
- 单 Raft Group、核心数据路径和恢复闭环达到生产门禁前，不启动 Multi-Raft 或热层生产实现。

### 3. OpenRaft 强一致与高可用

- RedisRaft 是公开 Raft 管理命令、错误和客户端可见行为的主要参考。
- OpenRaft 是 Kiwi 的共识实现，RedisRaft 不是运行时依赖。
- Raft 正确性必须通过确定性模型、多进程故障测试和 Elle/Jepsen 历史证明。
- Cache hit、未来热层或其他加速路径均不得绕过 Leader、ReadIndex、Lease、Commit 或 Apply 门禁。

## 延期的内嵌 Redis 8.8.1 原生内存热数据层

未来的内嵌 Redis 8.8.1 原生内存热数据层必须以 Redis 8.8.1 为接口与来源基线，并满足以下预先冻结的合同：

- 使用 `arana-db/redis` 中可追溯到 exact upstream commit 的 Redis-derived native library。
- 通过版本化 C ABI 和受控动态加载边界接入，不向 Rust 暴露 Redis 内部对象指针、SDS 所有权或 allocator 私有状态。
- RocksDB 仍是唯一全量权威存储；热层只保存可丢弃、可清空、可重建的热点副本。
- 热层内容不进入 Raft Log、Snapshot、Backup 或磁盘格式。
- 热层失败只能降低性能，不能返回旧值、改变 Redis 可观察语义或改变持久化结果。
- 更新失败必须执行 update-or-invalidate；已知旧值不得继续可见。
- Cache OFF 与 Cache ON 必须运行同一套 Redis 8.8.1 differential 和故障测试。
- 动态库 ABI、allocator、全局状态、线程、多实例、卸载和崩溃边界必须在实现前完成专项审计。

统一术语：

```text
中文：内嵌 Redis 8.8.1 原生内存热数据层
英文：Embedded Redis Hot Tier
```

上述术语是唯一现行口径。上述内容是未来设计合同，不是当前实现授权。只有系统稳定性门禁通过、风险复核完成并由用户明确批准后，相关生产代码、fork 改造、动态库构建和运行时加载工作才能进入执行。

## 许可证与组合发行边界

- Kiwi 自有、可独立识别的源码继续使用 Apache-2.0，并保留现有版权和 SPDX 声明。
- 未来 `arana-db/redis` 派生源码和原生动态库选择 Redis 8.8.1 提供的 AGPLv3 选项，按 `AGPL-3.0-only` 管理，并完整保留 Redis 上游版权、许可证和修改记录。
- 如果官方发行物包含 Redis-derived native library，完整组合发行不得声明为 Apache-2.0-only，必须履行 AGPL-3.0-only 的适用义务。
- 组合发行必须提供与二进制精确对应的 Kiwi 源码、Redis fork 源码、全部修改、ABI 头文件、绑定生成方式、构建脚本、版本清单、许可证、第三方通知和 SBOM。
- 远程用户对应源码入口、发行文案和打包方式必须在公开发布前通过开源许可证专项复核。
- 拆分仓库、动态链接或运行时加载只用于工程隔离，不得被描述为规避组合发行的许可证义务。

## 不可妥协原则

1. 数据正确性高于吞吐和平均延迟。
2. P99/P99.9、崩溃恢复和故障行为与正常路径同等重要。
3. 成功回复必须对应明确的 Raft Commit、State Machine Apply 和 durability profile。
4. 连接断开后的非幂等写入属于 `SUBMIT_UNKNOWN`，不得盲目重试。
5. Linearizable Read 即使经过未来加速层也必须通过一致性门禁。
6. Snapshot Install、RocksDB reopen、格式迁移和配置世代变化必须使所有派生状态失效或提升 generation。
7. 外部输入可触发的生产路径不得使用 `unwrap()`/`expect()` 制造进程崩溃；错误必须可传播、可观测。
8. 所有磁盘格式、协议和公开配置必须有版本、兼容测试和迁移策略。
9. 第三方源码必须固定 exact commit，保留许可证、来源、补丁清单和可复现构建证据。
10. 绿色测试不能替代真实边界验证，尤其是 RocksDB 全 handle 释放后按路径 reopen。
11. 接口文档可以先行，但被延期的生产实现不得以 spike、重构或顺手适配的名义提前进入主线。
12. 规划 task 与实施 task 必须分离；规划批准不构成源码实现、暂存、提交、push 或 PR 更新授权，未接受的实现草稿必须冻结并由后续独立 task 重新审计。

## 当前非目标

- 在系统稳定性门禁批准前实现、集成、打包或启用 Embedded Redis Hot Tier。
- Redis Stack/Search/JSON/TimeSeries/Bloom 的完整实现，除非后续独立需求明确纳入。
- 在单 Group 正确性闭环前建设 Multi-Raft。
- 用 S3 直接替代本地 RocksDB 在线文件系统。
- AI 向量数据库、Agent Memory、语义缓存和推理 KV Cache。
- 将 RedisRaft 内部 RPC 命令暴露为 Kiwi 公共接口。
- 把历史 `pikiwidb/rediscache` 代码未经来源和许可证审计直接复制到 Kiwi。

## 质量定义

“质量最高”不是代码量或命令数量，而是以下结果同时成立：

- 行为可对照：Redis 8.8.1 differential 可重复。
- 故障可证明：分区、kill、pause、部分写、磁盘错误和重启历史可检查。
- 状态可恢复：崩溃后代理和数据库都能从持久证据继续。
- 性能可解释：性能结果包含版本、硬件、配置、数据集和 P99/P99.9。
- 依赖可追溯：来源、许可证、补丁、构建输入和 SBOM 完整。
- Oracle 可证明：primary build 与 verifier 独立重建结果一致，正式运行证据来自独立重建产物，不能由自洽 JSON 和任意 ignored binary 拼接。
- 差异不隐藏：known difference 和 skip 都进入清单。
- 延期可执行：稳定性门禁未批准时，热层相关生产实现保持冻结。
