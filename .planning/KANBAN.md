# Kiwi Kanban

> 更新日期：2026-07-26
>
> 当前里程碑：M0 文档基线已验证；等待 M1 Cache OFF 实现授权
>
> 当前运行模式：Cache OFF
>
> 规则：每张实现卡必须引用 Requirement；完成必须附 exact commit、环境和可重复验证证据。

## In Progress

无。当前等待用户明确授权下一项 Cache OFF 实现工作；等待期间不得把 Ready 或 Frozen 卡隐式转为 In Progress。

## Ready after implementation authorization

这些卡属于下一轮实现，不在本 thread 的写权限内。启动时必须建立新的隔离工作边界并保存 recovery checkpoint。

| ID | 工作项 | Requirement | 前置条件 |
|---|---|---|---|
| `M1-001` | Redis 8.8.1 exact Oracle、机器可读 manifest 与 provenance | `REQ-COMPAT-001`、`003`、`004` | M0 文档一致性通过；用户授权实现 |
| `M1-002` | RESP2/RESP3 持久连接级 raw wire differential harness | `REQ-COMPAT-002`、`006` | `M1-001` |
| `M1-003` | Redis 8.8.1 TCL external-server runner | `REQ-COMPAT-004` | `M1-001` |
| `M1-004` | redis-rs test-only compatibility crate | `REQ-COMPAT-005`、`006` | `M1-001` |
| `M1-005` | partial-I/O property tests、parser fuzz seeds 与连接生命周期测试 | `REQ-COMPAT-002`、`006` | M0 文档一致性通过 |
| `M2-001` | RocksDB authority/durability contract | `REQ-STORAGE-001`、`002` | `M1-001` 基础 manifest |
| `M2-002` | 审计并扩展现有 close/reopen 回归至全部 RocksDB handle、TTL/metadata、Snapshot 和故障证据 | `REQ-STORAGE-002`、`004` | `M2-001` |
| `M2-003` | format version、Comparator 和 Kiwi RocksDB key/value encoding contract | `REQ-STORAGE-003`、`005` | `M2-001` |
| `M4-001` | `kiwi_redisraft_public_v1` 机器可读 manifest | `REQ-RAFT-003`、`004` | M1 manifest schema |

## Frozen by system stability gate

下列卡片不是普通 Backlog。它们在 `docs/quality/system-stability-gate.md` 全部通过且用户重新明确批准之前不得转入 Ready 或 In Progress。

| ID | 工作项 | Requirement | Gate | 冻结范围 |
|---|---|---|---|---|
| `M7-001` | 建立并修改用于 Kiwi 发行的 Redis fork | `REQ-HOT-002`；`REQ-LICENSE-002` 至 `008` | System Stability Gate + 用户新批准 | fork 代码、patch、生产构建均禁止 |
| `M7-002` | Redis Hot Tier 动态库 C ABI spike | `REQ-HOT-010`、`012`；`REQ-LICENSE-003`、`005`、`007` | System Stability Gate + 用户新批准 | `.so`、`.dylib`、`.dll`、import/static library 均禁止 |
| `M7-003` | Kiwi 安全 loader、pairing manifest 和 FFI binding | `REQ-HOT-010`、`011`、`012` | System Stability Gate + 用户新批准 | 生产 crate 依赖、配置和加载路径均禁止 |
| `M7-004` | String update-or-invalidate MVP | `REQ-HOT-001`、`003` 至 `007`、`009` | System Stability Gate + 用户新批准 | Cache ON 读写路径禁止 |
| `M8-001` | Cache OFF/ON differential 与热层故障注入 | `REQ-HOT-004` 至 `008`；`REQ-RAFT-002` | M7 全部资格门禁 | 不得用未实现热层替代当前正确性测试 |
| `M8-002` | Redis/Kiwi OFF/Kiwi ON 性能与资源基线 | `REQ-PERF-001`、`002`、`003`；`REQ-OBS-003` | M7/M8 正确性门禁 | 性能工作不得先于语义与故障证明 |

允许继续完善与这些卡对应的文档合同、ABI 伪代码、许可证清单和验收标准，但不得产生可被生产构建或运行时加载的实现产物。

## Backlog

| ID | 工作项 | Requirement | Milestone |
|---|---|---|---|
| `M3-001` | Redis 8.8.1 String/Hash/List/Set/ZSet required profile 闭环 | `REQ-COMPAT-003`、`007` | M3 |
| `M3-002` | Transaction、WATCH、Lua 与错误/TTL 语义闭环 | `REQ-COMPAT-003`、`006`、`007` | M3 |
| `M3-003` | Pub/Sub、阻塞命令、ACL、RESP3 与连接行为闭环 | `REQ-COMPAT-002`、`003`、`006`、`007` | M3 |
| `M4-002` | Rust OpenRaft deterministic simulator | `REQ-RAFT-006` | M4 |
| `M4-003` | RAFT.CLUSTER/NODE/TRANSFER_LEADER 与 INFO/CONFIG | `REQ-RAFT-003`、`004` | M4 |
| `M4-004` | Membership、Snapshot、Leadership Transfer，以及现有 LogStore reopen 回归的缺口扩展 | `REQ-RAFT-001`、`002`、`005` | M4 |
| `M5-001` | 3/5 节点 process harness 与网络故障矩阵 | `REQ-RAFT-006`、`007` | M5 |
| `M5-002` | 磁盘故障、Snapshot 中断和 Elle/Jepsen history | `REQ-STORAGE-004`；`REQ-RAFT-005`、`007` | M5 |
| `M5-003` | `SUBMIT_UNKNOWN`、查询和受控去重 | `REQ-RAFT-008` | M5 |
| `M6-001` | System Stability Gate Review | `REQ-STABILITY-001` 至 `006` | M6 |
| `M9-001` | 跨平台发行、SBOM、升级/回滚与运维门禁 | `REQ-LICENSE-003` 至 `008`；`REQ-WORK-003` | M9 |
| `M10-001` | 单 Group 瓶颈证据与 Multi-Raft 立项评估（非实现卡） | 新里程碑批准时新增 | M10 |

## Done in this documentation task

以下状态表示项目真相、设计合同和实施计划已经形成可发布的文档闭环，不表示任何生产实现已经完成。

| ID | 工作项 | 证据 |
|---|---|---|
| `M0R-001` | 项目宪法、需求、决定、Roadmap、State 与 Kanban 已统一 | 21 个当前文档一致性检查；58 个 Requirement ID 唯一；10 个 Decision ID 唯一 |
| `M0R-002` | Redis 8.8.1 兼容、系统边界、许可证与未来 ABI 设计已冻结 | 五份专题设计；本地链接、代码围栏和行尾空白检查通过 |
| `M0R-003` | 系统稳定门禁已建立，热层实现卡已冻结 | `docs/quality/system-stability-gate.md`；`REQ-STABILITY-001` 至 `006` |
| `M0R-004` | Cache OFF 稳定基础实施计划已写入 | 8 个有序任务；文档候选集 `git diff --check` exit 0 |

## WIP 与授权规则

- 同一时间只允许一个实现卡处于 In Progress；文档审查可并行，但路径所有权必须明确。
- M0 文档可以通过独立 PR 发布；必须使用显式文件 allowlist，禁止带入无关 dirty 文件。
- 发布 M0 文档不授权修改生产源码、Rust 测试、构建或 CI，也不自动授权任何后续实现卡。
- 此前冻结实现工作树不得覆盖、续写、清理或回退。
- 发现 branch、HEAD 或 dirty ownership 漂移时立即停止写操作并报告。
- 系统稳定门禁通过后，M7 仍需用户重新明确批准，不能自动启动。
