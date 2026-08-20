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

## D013：使用单一 SDD 权威入口

- 日期：2026-08-02
- 状态：accepted
- 决定：.planning/SDD.md 是项目唯一权威入口，统一维护当前架构、M0-M10 路线、M0-M6 当前可执行里程碑范围、WP0-WP8 工作包、当前状态、Issue/PR 追踪和验证门禁。
- 后果：STATE.md、KANBAN.md 和 ROADMAP.md 只保留兼容迁移指针，不维护独立状态。PROJECT、REQUIREMENTS、DECISIONS、OPEN_QUESTIONS 和 REFERENCES 作为 SDD 下属注册表。
- 理由：此前同一状态分散在多份文件中，PR 合并后容易产生状态、优先级和当前计划漂移。

## D014：实施 PR 强制关联工作包、Issue 和 Requirement

- 日期：2026-08-02
- 状态：accepted
- 决定：每个实施 PR 必须声明一个 SDD 工作包、一个 primary GitHub Issue 和适用的 REQ-*。
- 关闭语义：只有完整满足 Issue 全部 required acceptance criteria 时使用 Fixes/Closes；部分实现使用 Refs/Related。
- 后果：没有精确 Issue 的工作包不能进入 ready；宽泛 Epic 必须拆 child Issue；Discussion 只能作为设计来源。

## D015：保持 network/storage 数据面隔离并建立统一生命周期

- 日期：2026-08-02
- 状态：accepted
- 当前事实：进程有 bootstrap、network、storage 三个 Tokio Runtime；双 Runtime 只描述请求数据面。
- 决定：保留 network/storage 数据面隔离，不增加第四 Runtime；通过统一 supervisor、有界队列、absolute deadline、cancel 和 JoinHandle 建立确定性生命周期。
- 退出顺序：停止 admission，drain network、storage 和 Raft，结束后台任务，关闭 RocksDB，最后停止 Runtime。

## D016：未知持久化版本默认 fail closed

- 日期：2026-08-02
- 状态：accepted
- 决定：未知 disk format、CF schema、comparator、StorageManifest 或 Snapshot metadata 版本默认拒绝启动或安装。
- 例外：只有显式列入兼容范围并有真实 upgrade/rollback 与 reopen 证据的版本可以接受。
- 理由：旧二进制无法推断未来版本对 CF、encoding、comparator、topology 或恢复语义的改变。

## D017：当前不支持真正 Multi-Key

- 日期：2026-08-02
- 状态：accepted
- 决定：当前真正 Multi-Key 命令在 storage 和 Raft 前统一拒绝；本阶段不实现跨 Slot 原子性、2PC、跨实例快照或分布式锁协议。
- 重新立项条件：明确 slot map、原子 batch、锁或事务协议、故障恢复和 Redis 8.8.1 可观察语义。
- 来源：Discussion #346。

## D018：兼容性与故障验证使用分层门禁

- 日期：2026-08-02
- 状态：accepted
- 决定：PR fast gate 运行受影响的 raw RESP、单元、集成和静态门禁；nightly/full gate 运行 TCL、完整 differential、fuzz、deterministic Raft 和故障矩阵；M6/release gate 运行 fresh rebuild、真实 upgrade/rollback、close/reopen 和 3/5 节点历史验证。
- 后果：绿色 PR fast gate 不能被解释为系统稳定或生产级一致性已经证明。

## D019：在一个 Draft PR 中聚合 VectorSet 合并后全量闭环

- 日期：2026-08-06
- 状态：accepted
- 决定：用户明确选择方案 C，授权在一个最终 Draft PR 中聚合 WP8 VectorSet 合并后闭环、Issue #415 Trusted Redis Oracle、Issue #418 differential、Storage migration/snapshot、Runtime admission、协议一致性、Cluster fail-closed 和供应链哨兵。
- 任务隔离：本决定不废止 D012。治理设计、实施计划、Oracle、Storage、Runtime/Protocol 和 CI/Security 必须使用独立 Codex 子任务、独立 recovery checkpoint 和隔离 worktree；同一个 GitHub PR 只聚合按顺序审查通过的提交，不能把单 PR 解释为单一未隔离执行上下文。
- 范围例外：PR #356 已合并的 VectorSet Phase 1 允许实施正确性、恢复、兼容性和门禁修复；HNSW、量化扩展、新 Vector 命令、全文索引和其他 AI 主线继续 frozen。
- 验收：单 PR 不降低 #415、#418、#421 或现有 COMPAT/STORAGE/RAFT/STABILITY Requirement；任何 hash equality、upgrade/rollback、raw differential、cluster execution、cleanup 或 exact-Head 证据缺失都会使 PR 保持 Draft。
- 权限：本决定授权设计、实现、测试、commit、push 和创建 Draft PR，不授权 merge、修改 branch protection、关闭 Issue 或 Resolve 历史评论。

## D020：冻结 Redis 8.8.1 兼容门禁与测试来源职责

- 日期：2026-08-20
- 状态：accepted
- 决定：Redis 8.8.1 exact commit `77b6c308396c9700672390a210143a8496fb4b10` 继续作为唯一普通 Redis Oracle；兼容门禁按 D018 分为 PR fast、nightly/full 和 M6/release 三层。PR fast 只运行受改动影响的确定性 raw RESP、manifest、单元、集成和静态合同；nightly/full 扩大到固定上游 commit 的官方 TCL external-server suite、完整 differential、property/fuzz 和故障矩阵；M6/release 在此基础上要求 fresh independent rebuild、真实 upgrade/rollback、close/reopen、3/5 节点历史与完整 exact-ref evidence bundle。低层绿灯不得替代高层验收。
- 测试来源：Redis 官方 TCL suite 是上游兼容场景来源，必须固定 exact commit、以 external-server 模式运行并使用带 owner、Issue 和可测解除条件的 skip registry；它不替代 raw wire 或 Kiwi 权威最终状态证据。Python 保留为 raw RESP differential、跨语言集成与故障编排层。redis-rs 仅在 raw/TCL 合同建立后作为 test-only 客户端生态层，不得进入生产 server crate 依赖图，也不得作为普通 Redis 服务端语义 Oracle。
- Harness 边界：不先建设新的通用 `kiwi-test-harness`。首个 Core slice 复用现有 Trusted Oracle controller 的 held-FD 执行、deadline、输出上限、进程组清理、cleanup-before-publish、原子证据发布和 provenance binding 内核，只把 Vector-only evidence descriptor、allowlist、collector 和 binding 收窄泛化为固定的版本化 evidence profile；现有 Vector `kiwi-vector-differential-evidence/v1` 行为必须保持不变。`OracleProvenance::verify_external_bindings` 必须在 size/SHA binding 后严格解析对应 evidence document，并在进程和临时目录删除后重放 registry、collection、raw frame、final-state、cleanup 和 schema/helper pairing；只校验外层 hash 不构成验收。
- 首个实施切片：Issue #433 只覆盖 standalone Cache OFF 下 `PING`、`SET`、`GET`、single-key `DEL`、`TYPE`、`PTTL` 的 RESP2/RESP3 raw smoke differential，共 15 个固定 case、30 个 server-backed node。Compatibility manifest 升为 `kiwi-redis-compat/v2`：现有 12 条 classification 原样迁移；新增六条命令在 command level 保持 `known_difference`，并通过机器可读 `required_cases` 精确绑定 Core registry/schema/case IDs，直到完整 Redis 8.8.1 命令 surface 有证据后才允许提升为 command-level `required`。命令 registry、manifest subset closure、exact request/response、TTL/type final-state、独立 Redis 8.8.1 Oracle identity、Kiwi identity、清理与 evidence/provenance binding 必须 fail closed；该切片不得声明全命令兼容。
- 判别性证据：GREEN 之外必须有保持 endpoint、collection、summary 和 runtime setup 合法的受控 behavior mutants，至少杀死 Kiwi-only raw response byte 漂移、`SET` 回 `OK` 但未持久化、`PTTL -1/-2` 互换；published evidence mutants 必须在重算外层 size/SHA 后仍因内部 semantic replay 失败。
- 集群与哨兵：当前可执行首切片不新增 Cluster、Sentinel、Multi-Key、Raft 或生产存储行为。真正 Multi-Key 继续受 D017 拒绝；Cluster/Leader、Raft single-group 和 Sentinel/客户端生态场景由 WP4、WP6、WP7 在各自 exact-file plan 和前置门禁就绪后实施。
- 后果：OQ-3 与 OQ-10 已收敛，WP1 可在保持 WP8 为当前 accepted 工作包的同时进入 `ready`。规划 task 仍受 D012 约束，不实施源码、测试 runner、构建脚本或 CI；实施必须另开 task、隔离 worktree 和 recovery checkpoint，并以 Issue #433、WP1、适用 Requirement、设计和逐步计划为唯一首切片授权边界。
