# Kiwi 已批准决定

本文件只记录当前有效的项目决定。Git 历史负责保留已撤销方案；当前工作入口不得并列展示失效基线。

## D001：Redis 8.8.1 是唯一当前兼容与接口基线

- 日期：2026-07-26
- 状态：accepted
- 决定：普通 Redis 协议、命令、错误、配置、RESP 行为、测试 Oracle、接口设计和未来原生热层来源统一以 Redis 8.8.1 exact tag 为基线。
- Commit：`77b6c308396c9700672390a210143a8496fb4b10`
- 后果：所有当前项目真相、实现计划和验收证据只允许声明 Redis 8.8.1；不得建立双版本当前口径，也不得使用浮动 tag、branch 或未校验 binary 替代 exact baseline。

## D002：接受未来官方组合发行物的 AGPL 义务

- 日期：2026-07-26
- 状态：accepted
- 决定：Kiwi 自有、可独立识别的源码继续采用 Apache-2.0；未来 `arana-db/redis` 派生源码和原生动态库选择 Redis 8.8.1 提供的 AGPLv3 选项，按 `AGPL-3.0-only` 管理。
- 组合发行：如果官方发行物包含 Redis-derived native library，完整组合发行不得声明为 Apache-2.0-only，必须履行 AGPL-3.0-only 的适用义务。
- 必需证据：上游与下游 exact commit、版权和许可证、修改记录、ABI、构建脚本、对应源码、第三方通知、SBOM、binary/source pairing 和远程源码入口。
- 限定：拆分仓库、动态链接和运行时加载只用于工程隔离，不构成规避组合发行许可证义务的依据；首次公开组合发行前必须完成许可证专项复核。

## D003：统一热层术语

- 日期：2026-07-26
- 状态：accepted
- 决定：统一使用“内嵌 Redis 8.8.1 原生内存热数据层”或 `Embedded Redis Hot Tier`。
- 理由：固定术语可以准确表达 Redis 原生内存语义、可丢弃副本属性以及与 RocksDB Block Cache 的边界。

## D004：RocksDB 是唯一全量权威存储

- 日期：2026-07-25
- 状态：accepted
- 决定：热层可以完全丢弃；RocksDB 必须保存重建所需的全量、权威、可恢复状态。
- 后果：热层内容不进入 Raft Log、Snapshot、Backup 或磁盘格式；热层错误不得影响正确性。

## D005：RedisRaft 只定义公开兼容 Profile

- 日期：2026-07-25
- 状态：accepted
- 决定：公开 Raft 管理接口尽可能精确参照 RedisRaft，冻结为 `kiwi_redisraft_public_v1`。
- 范围：`RAFT.CLUSTER`、`RAFT.NODE`、`RAFT.TRANSFER_LEADER`、`INFO raft`、`CONFIG raft.*`、错误和 Leader 行为。
- 排除：`RAFT.AE`、`RAFT.REQUESTVOTE`、`RAFT.SNAPSHOT` 等内部 C libraft transport 命令。
- 许可证：RedisRaft 源码和测试不能直接复制进 Apache-2.0 Kiwi；以 clean-room 行为规范和独立测试实现。

## D006：RedisLabs/raft 只作为测试模型来源

- 日期：2026-07-25
- 状态：accepted
- 决定：借鉴其 BSD-3-Clause `virtraft2` 不变量、seed 和故障场景，在 Rust 中围绕 OpenRaft 重建 simulator。
- 禁止：不得用 C libraft 测试通过代替 Kiwi/OpenRaft 正确性证明。

## D007：redis-rs 只作为客户端测试

- 日期：2026-07-25
- 状态：accepted
- 决定：使用当前维护的 redis-rs exact release/commit 建立 Rust 客户端验收。
- 禁止：不得称其为 Redis Rust 服务端，不得作为普通 Redis 语义 Oracle，不得进入生产 server crate 依赖图。

## D008：建立双层工作恢复状态

- 日期：2026-07-25
- 状态：accepted
- 决定：`.planning/` 保存应版本化的长期事实；`.codex/recovery/` 保存本机当前任务、append-only checkpoint 和 Git dirty snapshot。
- 理由：Kanban 不能替代精确的 branch、HEAD、授权和 dirty 归属；本机 runtime state 也不能替代项目 Roadmap。

## D009：热层生产实现延期到系统稳定性门禁之后

- 日期：2026-07-26
- 状态：accepted
- 决定：当前只保存 Embedded Redis Hot Tier 的架构、许可证、接口和验收合同，不启动 Redis fork 改造、原生动态库、运行时 loader、热层数据路径或默认配置的生产实现。
- 解除条件：Redis 8.8.1 兼容、RocksDB 权威存储与真实恢复、OpenRaft 一致性与故障验证、资源边界和可观测性通过系统稳定性门禁，并由用户明确批准。
- 后果：门禁未解除前，任何 spike、重构或顺手适配都不得向生产依赖图和请求路径引入 Redis-derived native code。

## D010：未来热层接口必须按 Redis 8.8.1 设计

- 日期：2026-07-26
- 状态：accepted
- 决定：未来热层统一称为“内嵌 Redis 8.8.1 原生内存热数据层”或 `Embedded Redis Hot Tier`。
- ABI 边界：使用版本化 C ABI 和受控动态加载；不得跨边界暴露 Redis 内部对象指针、SDS 所有权或 allocator 私有状态。
- 正确性边界：RocksDB 始终权威；热层可丢弃、可重建；cache hit 不得绕过 OpenRaft；更新失败必须 invalidate；Cache OFF/ON 必须产生相同的 Redis 8.8.1 可观察行为。
- 说明：本决定冻结未来接口和验收方向，不解除 D009 的实现延期。

## D011：Redis Oracle provenance 使用 verifier 独立重建

- 日期：2026-07-28
- 状态：accepted
- 决定：采用方案 A。Primary build controller 产生构建产物和审计 metadata；verifier 不把 metadata、build log 或 primary binary 当作构建来源信任根，而是在全新的 disposable exact Redis 8.8.1 checkout 中独立执行同一受控构建。
- 接受条件：primary binary 与 verifier rebuild binary 的 SHA-256 必须完全一致；正式 `INFO server` 证据必须来自 verifier rebuild binary。任一不一致、独立构建失败或 cleanup 失败时不得发布 provenance。
- 工具边界：controller bootstrap 使用固定 Linux 解释器信任边界和 isolated Python 模式；Git、CC、Make 及其他外部工具必须从受控目录解析，记录路径、版本、SHA-256 和文件 identity，并通过 held FD 执行需要防路径替换的调用。
- 理由：同一调用者可以伪造完全自洽的 metadata、build log、hash、identity 和 ignored `src/redis-server`。只有独立重建、hash equality 和运行独立产物才能关闭任意 source/binary 拼接路径。
- 失败处理：如果 Redis 8.8.1 在两个受控独立 checkout 中不能产生一致 binary，实施 task 必须停在可复现性调查门禁；不得把 equality 降级为版本字符串、宽松字段比较或允许列表。

## D012：规划 task 与实施 task 分离

- 日期：2026-07-28
- 状态：accepted
- 决定：项目规划、设计固化和实施计划编写必须与源码实现使用不同的 Codex task。规划 task 不继续源码、测试、构建脚本或 CI 实现，也不把提前产生的草稿提交到实现 PR。
- 当前应用：Redis 8.8.1 Oracle provenance 的六文件实现草稿保持未暂存、未提交、未 push，并在原 recovery worktree 冻结。本 task 只发布方案 A 的项目真相、设计和后续实施计划。
- 后续要求：实施必须另开 task，在新的隔离 worktree 保存独立 recovery checkpoint；可只读参考冻结草稿，但必须逐项对照 `D011` 和 `REQ-COMPAT-008` 至 `REQ-COMPAT-010` 重新审计，不能把既有绿测当作接受证据。
- 理由：Kiwi 底层兼容、存储和 Raft 工作并行且依赖严格。混合规划和实现会隐式改变任务优先级、污染 dirty ownership，并让未批准代码反向定义架构。
