# Kiwi Kanban

> 更新日期：2026-07-31
>
> 当前里程碑：M1 Redis 8.8.1 Cache OFF compatibility foundation
>
> 当前 task：`RESP-LIMITS-001` implementation（`codex/fix-resp-parser-limits`）；基线 `main` at `cbc28958f261ae049d67a8b4a9d904d794b37726`
>
> 当前运行模式：Cache OFF
>
> 规则：每张实现卡必须引用完整 Requirement ID；完成必须附 exact commit、环境和可重复验证证据。

## In Progress

| ID | 工作项 | Requirement / Gate 关系 | 当前状态 |
|---|---|---|---|
| `RESP-LIMITS-001` | 有界 RESP 解析、未认证连接 buffer、parser 历史副本和 optional pipeline 背压 | `REQ-COMPAT-002`、`REQ-COMPAT-006`、`REQ-STABILITY-002`、`REQ-STABILITY-003`、`REQ-WORK-003` | PR `#404` 已创建；本地 Windows/WSL 定向验证完成，待发布并复检最终 Head CI |

## Ready for a separate implementation task

启动下列卡片必须创建新的 Codex task、linked worktree、TaskId 和 recovery checkpoint。Planning branch 和旧草稿 worktree 都不能直接切换成 implementation。

| ID | 工作项 | Requirement / Decision | 前置条件 |
|---|---|---|---|
| `M1-001-T2` | Redis 8.8.1 trusted Oracle provenance：primary build、fresh-checkout independent rebuild、binary hash equality、rebuild runtime evidence | `REQ-COMPAT-001`、`REQ-COMPAT-008`、`REQ-COMPAT-009`、`REQ-COMPAT-010`；`D011`、`D012` | PR `#383` 已合并；仍须新建专用 implementation task/worktree；当前 PR `#388` 不替代该任务；真实双 checkout reproducibility 是卡片内第一道实现门禁 |
| `M1-002` | RESP2/RESP3 持久连接级 raw wire differential harness | `REQ-COMPAT-002`、`REQ-COMPAT-006` | `M1-001-T2` 产生可验证 provenance |
| `M1-003` | Redis 8.8.1 TCL external-server runner | `REQ-COMPAT-004` | `M1-001-T2` |
| `M1-004` | redis-rs test-only compatibility crate | `REQ-COMPAT-005`、`REQ-COMPAT-006` | `M1-001-T2` |
| `M1-005` | partial-I/O property tests、parser fuzz seeds 与连接生命周期测试 | `REQ-COMPAT-002`、`REQ-COMPAT-006` | Task 1 manifest 可用 |
| `M2-001` | RocksDB authority/durability contract | `REQ-STORAGE-001`、`REQ-STORAGE-002` | Task 1 manifest；与 M1 Oracle 保持隔离 |
| `M2-002` | 扩展 close/reopen 至全部 RocksDB handle、TTL/metadata、Snapshot 和故障证据 | `REQ-STORAGE-002`、`REQ-STORAGE-004` | `M2-001` |
| `M2-003` | format version、Comparator 和 Kiwi RocksDB key/value encoding contract | `REQ-STORAGE-003`、`REQ-STORAGE-005` | `M2-001` |
| `M4-001` | `kiwi_redisraft_public_v1` 机器可读 manifest | `REQ-RAFT-003`、`REQ-RAFT-004` | Task 1 manifest schema |

## Accepted implementation

| ID | 工作项 | Evidence | 状态 |
|---|---|---|---|
| `M1-001-T1` | Redis 8.8.1 exact compatibility manifest | PR `#372`；final Head `6a692bc195f96327296296977a100af301deaf01`；merge commit `9e91707d774ad367d682e23677dcef79ecb14338` | 已合并到 `main` |
| `PR388-001` | C/C++ sccache 构建接入、跨平台回归探针、PRD/用户故事及 review 一致性修复 | PR `#388`；final Head `1ee8c916a55d03d02a250ed95af83712fa14a742`；2026-07-30 merged | 已合并到 `main` |

## Frozen unaccepted drafts

| ID | Worktree | 内容 | 规则 |
|---|---|---|---|
| `M1-001-T2-DRAFT` | `D:\test\github\kiwi\.worktrees\redis-8.8.1-stability-foundation` | 六文件未提交 Oracle build/verifier 草稿 | 不继续、不 stage、不 commit、不 push、不清理；后续新 task 只读审计，不把绿测当作方案 A 证据 |

## Frozen by system stability gate

下列卡片在 `docs/quality/system-stability-gate.md` 全部通过、M7 前置设计完成审查且用户明确批准对应的单独 implementation task 前，不得转入 Ready 或 In Progress。Gate PASS、PR 合并或里程碑 Ready 均不自动授权生产实现。

| ID | 工作项 | Requirement | Gate | 冻结范围 |
|---|---|---|---|---|
| `M7-001` | 建立并修改用于 Kiwi 发行的 Redis fork | `REQ-HOT-002`；`REQ-LICENSE-002` 至 `REQ-LICENSE-008` | Gate PASS + 单独 implementation task 明确批准 | fork 代码、patch、生产构建均禁止 |
| `M7-002` | Embedded Redis Hot Tier 动态库 C ABI spike | `REQ-HOT-010`、`REQ-HOT-012`；`REQ-LICENSE-003`、`REQ-LICENSE-005`、`REQ-LICENSE-007` | Gate PASS + 单独 implementation task 明确批准 | `.so`、`.dylib`、`.dll`、import/static library 均禁止 |
| `M7-003` | Kiwi 安全 loader、pairing manifest 和 FFI binding | `REQ-HOT-010`、`REQ-HOT-011`、`REQ-HOT-012` | Gate PASS + 单独 implementation task 明确批准 | 生产 crate 依赖、配置和加载路径均禁止 |
| `M7-004` | String update-or-invalidate MVP | `REQ-HOT-001`、`REQ-HOT-003` 至 `REQ-HOT-007`、`REQ-HOT-009` | Gate PASS + 单独 implementation task 明确批准 | Cache ON 读写路径禁止 |
| `M8-001` | Cache OFF/ON differential 与热层故障注入 | `REQ-HOT-004` 至 `REQ-HOT-008`；`REQ-RAFT-002` | M7 全部资格门禁 | 不得用未实现热层替代当前正确性测试 |
| `M8-002` | Redis/Kiwi OFF/Kiwi ON 性能与资源基线 | `REQ-PERF-001`、`REQ-PERF-002`、`REQ-PERF-003`；`REQ-OBS-003` | M7/M8 正确性门禁 | 性能工作不得先于语义与故障证明 |

## Backlog

| ID | 工作项 | Requirement | Milestone |
|---|---|---|---|
| `M3-001` | Redis 8.8.1 String/Hash/List/Set/ZSet required profile 闭环 | `REQ-COMPAT-003`、`REQ-COMPAT-007` | M3 |
| `M3-002` | Transaction、WATCH、Lua 与错误/TTL 语义闭环 | `REQ-COMPAT-003`、`REQ-COMPAT-006`、`REQ-COMPAT-007` | M3 |
| `M3-003` | Pub/Sub、阻塞命令、ACL、RESP3 与连接行为闭环 | `REQ-COMPAT-002`、`REQ-COMPAT-003`、`REQ-COMPAT-006`、`REQ-COMPAT-007` | M3 |
| `M4-002` | Rust OpenRaft deterministic simulator | `REQ-RAFT-006` | M4 |
| `M4-003` | RAFT.CLUSTER/NODE/TRANSFER_LEADER 与 INFO/CONFIG | `REQ-RAFT-003`、`REQ-RAFT-004` | M4 |
| `M4-004` | Membership、Snapshot、Leadership Transfer 和 LogStore reopen 扩展 | `REQ-RAFT-001`、`REQ-RAFT-002`、`REQ-RAFT-005` | M4 |
| `M5-001` | 3/5 节点 process harness 与网络故障矩阵 | `REQ-RAFT-006`、`REQ-RAFT-007` | M5 |
| `M5-002` | 磁盘故障、Snapshot 中断和 Elle/Jepsen history | `REQ-STORAGE-004`；`REQ-RAFT-005`、`REQ-RAFT-007` | M5 |
| `M5-003` | `SUBMIT_UNKNOWN`、查询和受控去重 | `REQ-RAFT-008` | M5 |
| `M6-001` | System Stability Gate Review | `REQ-STABILITY-001` 至 `REQ-STABILITY-006` | M6 |
| `M9-001` | 跨平台发行、SBOM、升级/回滚与运维门禁 | `REQ-LICENSE-003` 至 `REQ-LICENSE-008`；`REQ-WORK-003` | M9 |
| `M10-001` | 单 Group 瓶颈证据与 Multi-Raft 立项评估 | 新里程碑批准时新增 | M10 |

## Done in planning/documentation

以下状态只表示项目真相、设计合同和实施计划完成，不表示源码实现完成。

| ID | 工作项 | 证据 |
|---|---|---|
| `M0R-001` | 项目宪法、需求、决定、Roadmap、State 与 Kanban 已统一 | Redis 8.8.1 stability foundation 文档基线 |
| `M0R-002` | Redis 8.8.1 兼容、系统边界、许可证与未来 ABI 设计已冻结 | 专题设计和稳定性门禁 |
| `M0R-003` | 系统稳定门禁已建立，热层实现卡已冻结 | `docs/quality/system-stability-gate.md` |
| `M0R-004` | Cache OFF 稳定基础总计划已写入 | `docs/superpowers/plans/2026-07-26-redis-8.8.1-stability-foundation.md` |
| `M0R-005` | Trusted Oracle independent-rebuild 设计和实施计划已冻结 | `D011`；`REQ-COMPAT-008` 至 `REQ-COMPAT-010`；2026-07-28 spec/plan |
| `M0R-006` | 规划 task 与实施 task 分离规则已冻结 | `D012`；`REQ-WORK-005`；`CLAUDE.md` recovery rules |

## WIP 与授权规则

- 同一时间只允许一个 implementation card 处于 In Progress；当前为 `RESP-LIMITS-001`，planning/docs task 不得隐式持有 implementation card。
- 规划批准不授权修改 source、tests、build scripts 或 CI，也不授权 stage/commit/push 实现文件。
- 从 planning 转 implementation 必须创建新 Codex task、TaskId、worktree、dirty allowlist 和 recovery checkpoint；不能只把 recovery mode 从 `planning` 改成 `implementation`。
- 冻结草稿不得覆盖、续写、清理或回退；后续实施只读参考并重新审计。
- `M1-001-T2` 必须先通过真实双 checkout reproducibility；不能把方案 A 降级为 metadata self-attestation。
- 系统稳定门禁通过后，M7 仍需用户明确批准一个单独 implementation task，不能因 Gate PASS、PR 合并或 Ready 状态自动启动。
