# 待确认问题清单（Open Questions）

> 本文件集中收录当前未闭合、需要 owner / 决策 / 证据才能推进的问题。
> 与 `REQUIREMENTS.md`（已按 P0/P1/P2 标注）互补：REQUIREMENTS 描述"要做什么"，本文件描述"还不确定什么"。
> 任何一项得出结论后，应同步更新 `DECISIONS.md` / `ROADMAP.md` / `KANBAN.md`，并从本清单移除。

## OQ-1 热层（Hot Tier）解冻条件与门禁证据
- **背景**：`REQ-STABILITY-001/002/005` 规定，未来内嵌 Redis 8.8.1 原生内存热层在投入生产前，必须由系统稳定性门禁明确给出"通过"结论并由用户批准解除冻结。当前整体处于冻结态（`REQ-HOT-*` 全部 `P2`）。
- **待确认**：
  - 稳定性门禁的"通过"由谁判定、以何种量化指标（延迟/命中率/故障注入覆盖）为阈值？
  - 验收基准（benchmark host、版本化 `cases.yaml`、回填机制）如何固化？
- **关联**：#351（有界 executor，OPEN）、#347（收敛 RocksDB 架构）、#390（阈值冻结机制已建，真实数值待 Linux 主机回填）。
- **阻塞**：未解冻前，禁止新增 Redis-derived 生产依赖 / 动态库 loader / 热层数据路径（`REQ-STABILITY-004`）。

## OQ-2 许可证边界（Apache-2.0 vs AGPL-3.0）
- **背景**：项目主许可证为 Apache-2.0（`LICENSE`）。`REQ-LICENSE-002..008` 标记为 `P2` 未来派生，涉及动态库（热层 Redis 原生库）、第三方依赖的许可证兼容性。
- **待确认**：
  - 热层引入的 `arana-db/redis` 原生动态库（及其下游 patch/构建输入）许可证是否与 Apache-2.0 兼容？是否存在 AGPL-3.0-only 组件需隔离？
  - 哪些许可证允许进入生产依赖，哪些必须置于独立进程/边界外？
- **关联**：`REQ-HOT-010/011/012`（动态库 ABI、hash 校验、专项审计）。

## OQ-3 Redis 兼容版本矩阵
- **背景**：基线条固定为 Redis 8.8.1（`REQ-COMPAT-001`，exact Oracle commit）。但 #325 提案要求"按版本跑 gate"。
- **待确认**：正式声明兼容哪些 Redis 版本（3.2 / 6.2 / 7.0 / 7.2 / 8.8.1）？每个版本是否单独维护兼容性 gate 与 skip list？
- **关联**：#325（统一测试策略）、`REQ-COMPAT-003/004`。

## OQ-4 TOML 配置文件支持
- **背景**：#247 要求提供 proper TOML 配置文件支持。
- **待确认**：当前配置以何种机制加载（env / CLI / 现有 config crate）？TOML 配置与现有配置层如何衔接，是否需迁移说明（注意 #352 结论：删除任何用户配置字段须附迁移说明）？
- **关联**：#247、#352（脚手架清理，等待 #351 稳定后收口）。

## OQ-5 StreamAppend RPC
- **背景**：#252 提议实现 StreamAppend RPC。
- **待确认**：Raft/log 层是否确实需要该 RPC？与 `REQ-RAFT-*` 的 snapshot / apply 语义如何对齐？
- **关联**：#215（LogStorage）、#220（StateMachine for Raft）、#219（LogIndex）。

## OQ-6 Engine trait 泄漏的最终处置
- **背景**：#341 已结论——删除复制 RocksDB API 的 `Engine` trait，明确使用具体 RocksDB 类型与所有权；`Batch` 等真实多执行路径领域抽象保留。新执行 Epic 为 #347。
- **待确认**：#341 原 Phase 1–4 backend-neutral 方案彻底放弃，以 #347 / #349 验收标准为准。是否有遗留引用需在 #347 收口时一并清理？
- **关联**：#347（收敛 RocksDB 架构）、#349（替代子任务）。

## OQ-7 存储格式版本与迁移门禁
- **背景**：`REQ-STORAGE-003` 要求磁盘格式带 format version、Comparator 身份与迁移策略。
- **待确认**：format version 的演进规则、不兼容时的迁移路径与回滚策略如何落地（与 #342 对齐）？
- **关联**：#342（存储格式版本与迁移门禁）、#343（实例拓扑与安全启动）。

## OQ-8 实例拓扑持久化与安全启动
- **背景**：#343 要求持久化实例拓扑并拒绝不安全启动配置。
- **待确认**：拓扑持久化的存储位置、与 Raft 元数据的一致性边界、何为"不安全启动配置"的判定清单？
- **关联**：#343、#342、`REQ-STORAGE-001`（原子性合同）。

## OQ-9 双 Runtime 架构演进
- **背景**：#368 [RFC] 讨论是否进一步拆分网络/存储双 runtime。
- **待确认**：当前双 Tokio runtime（经 `mpsc + oneshot` 解耦）是否需进一步拆分或合并？拆分带来的确定性收益 vs 复杂度成本？
- **关联**：架构审计报告（网络/存储 runtime 解耦设计 🟢）。

## OQ-10 统一测试策略的落地取舍
- **背景**：#325 提案 `kiwi-test-harness` + Oracle 对比 + property-based + 确定性网络模拟（turmoil）。
- **待确认**：
  - 目标 Redis 版本与 oracle 来源（见 OQ-3）；
  - 是否值得 port Redis 官方 TCL suite，还是以 `resp-compatibility` 为 fast gate；
  - Python 测试保留为多语言 client 验证，还是逐步迁到 Rust harness；
  - 集群/哨兵功能当前可用程度。
- **关联**：#325、`REQ-COMPAT-006`（回归测试覆盖）、#340（crash window 测试）。

## 维护规则
- 新增待确认项：编号 `OQ-N`，标题用方括号标注状态（`OPEN` / `BLOCKED` / `DECIDED`）。
- 得出结论：将结论写入 `DECISIONS.md`，更新 `ROADMAP.md` / `KANBAN.md`，并删除本清单对应条目（或标记为 `DECIDED` 并附决策链接）。
- 本清单不替代 issue 讨论；issue 是讨论场所，本文件是决策前的聚合视图。
