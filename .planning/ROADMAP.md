# Kiwi 唯一项目路线图

> 基线：Redis 8.8.1
>
> Redis exact commit：`77b6c308396c9700672390a210143a8496fb4b10`
>
> 当前运行模式：Cache OFF
>
> 更新日期：2026-07-28

## 路线原则

当前主线先证明一个不依赖 Embedded Redis Hot Tier 的完整、正确、可恢复、可运维系统：Redis 8.8.1 可观察语义由兼容测试判定，RocksDB 保存全量权威状态，OpenRaft 提供一致性与高可用。

热层相关文档、许可证边界、动态库 ABI、加载合同和失效不变量可以提前设计，但不得据此进入生产实现。只有系统稳定门禁全部通过，并由用户重新明确批准后，才允许建立 Redis fork、实现动态库、接入 loader 或启用 Cache ON。

## 依赖顺序

```text
M0 项目宪法、许可证与恢复机制
  → M1 Redis 8.8.1 Cache OFF 兼容 Oracle
  → M2 RocksDB 权威状态与恢复正确性
  → M3 Redis 8.8.1 Cache OFF 核心语义闭环
  → M4 生产级单 Raft Group
  → M5 分布式故障与一致性证明
  → M6 系统稳定门禁
  → [用户重新明确批准]
  → M7 Embedded Redis Hot Tier 资格验证与实现
  → M8 Cache ON 正确性、故障与性能证明
  → M9 生产发行门禁
  → M10 Multi-Raft 与远期容量路线
```

M1 与 M2 可以在边界清晰的隔离任务中有限并行；M3 依赖二者的基础合同。M4、M5 必须以 Cache OFF 为默认且唯一 required 模式完成。M7 在 M6 通过和用户重新明确批准之前保持冻结。M10 不得在单 Group 的稳定性与故障证明完成前启动主线开发。

## M0：项目宪法、许可证与恢复机制

目标：让项目目标、决定、授权边界、当前状态和下一步脱离会话记忆独立存在。

交付：

- `.planning/PROJECT.md`
- `.planning/REQUIREMENTS.md`
- `.planning/ROADMAP.md`
- `.planning/STATE.md`
- `.planning/KANBAN.md`
- `.planning/DECISIONS.md`
- Redis 8.8.1 兼容、系统边界、组合发行许可证和未来 ABI 设计文档
- `.codex/recovery/ACTIVE.md`
- `scripts/codex-workstate.ps1`

退出门禁：

- 所有权威文档对 Redis 基线、Cache OFF 当前主线、RocksDB 权威性和 OpenRaft 实现边界描述一致。
- 热层“只允许设计、禁止生产实现”的边界可由 Kanban 和系统稳定门禁恢复。
- 恢复脚本能够以 append-only 方式记录 branch、HEAD、dirty ownership 和授权，不修改 Git 状态。

## M1：Redis 8.8.1 Cache OFF 兼容 Oracle

目标：先建立权威判定工具，再扩大命令实现；所有测试首先验证不依赖热层的 Kiwi。

交付：

- Redis 8.8.1 exact binary/source pin。
- Primary build 与 verifier fresh-checkout rebuild 的 binary hash equality；两边受控 toolchain identity、versioned recipe 和 required evidence artifact/schema 全部通过校验；正式 `INFO server` 证据只来自独立重建产物。
- Cleanup-before-publish 全部成功并留下可审计 cleanup 结果；任一进程回收、临时目录删除、evidence handle 关闭或 identity/hash 复核失败都不得发布 provenance。
- 机器可读命令、模式和证据兼容矩阵。
- RESP2/RESP3 raw wire differential harness。
- Redis TCL external-server runner。
- redis-rs test-only 客户端套件。
- partial-I/O property test、parser fuzz corpus 和连接生命周期测试。
- skip/known-difference 治理。

退出门禁：Redis 8.8.1 Oracle provenance 不能由 exact source、自洽 metadata 和任意 ignored binary 拼接；受控 toolchain identity 与 versioned recipe 已验证，required evidence artifact/schema 完整，双构建 hash 一致，正式运行证据来自 verifier rebuild，且 cleanup-before-publish 全部成功。基础 String/Hash/List/Set/ZSet 命令能够在 Redis 8.8.1 与 Kiwi Cache OFF 之间产生可重复 transcript；任何差异都有 owner、Issue、理由和解除条件。

## M2：RocksDB 权威状态与恢复正确性

目标：在 Cache OFF 模式下完成正确、可恢复、可迁移的数据真相层。

交付：

- 数据编码、format version 和 Comparator 身份。
- TTL 绝对时间与时钟边界。
- WriteBatch 原子性。
- Raft metadata、last applied 与业务状态边界。
- 审计并扩展现有 close/reopen 回归，证明全部 DB、column-family、iterator、snapshot、clone 和后台任务 handle 释放后能够按路径真正重开。
- 部分写、尾部损坏、磁盘满、I/O error 和 Snapshot 恢复。
- Kiwi RocksDB key/value encoding、`format_base_key.rs`、适用 `format_*` 文件和 `custom_comparator.rs` 的规范与属性测试。

退出门禁：Cache OFF 满足已选 Redis 8.8.1 Profile；崩溃恢复、格式兼容和故障注入证据证明 RocksDB 可以独立恢复全部权威状态。

## M3：Redis 8.8.1 Cache OFF 核心语义闭环

目标：按 Profile 完成 Redis Core 可观察语义，而不是以“支持大多数命令”结束。

交付：

- String、Hash、List、Set、ZSet。
- Bitmap、HyperLogLog、Geo、Streams。
- Transaction、WATCH、Lua。
- Pub/Sub、阻塞命令、ACL。
- RESP3、Pipeline、错误、TTL 和客户端连接行为。
- required、optional、unsupported 和 known-difference 的机器可读边界。

退出门禁：required profile 在 Cache OFF 下全绿；保留差异均经过显式批准，且不存在由未启用热层解释或掩盖的正确性缺口。

## M4：生产级单 Raft Group

目标：把强一致从代码声明变成公开接口合同和可重复测试。

交付：

- `kiwi_redisraft_public_v1`。
- `RAFT.CLUSTER`、`RAFT.NODE`、`RAFT.TRANSFER_LEADER`。
- `INFO raft` 和 `CONFIG raft.*`。
- Linearizable Read、quorum read、Leader redirect。
- Membership、Snapshot、Leadership Transfer。
- 审计并扩展现有 RocksDB LogStore close/reopen 回归，补齐 state-machine、snapshot metadata 和故障证据。
- deterministic OpenRaft simulator。
- 写成功回复、commit、apply 和 durability profile 的时序合同。

退出门禁：公开 RedisRaft profile 100% 通过；3/5 节点正常路径、成员变更和恢复路径可重复；Cache OFF 下不存在一致性绕行路径。

## M5：分布式故障与一致性证明

目标：证明网络、进程和磁盘故障不会破坏已经承诺的 Redis 状态。

交付：

- partition、drop、duplicate、delay、reorder。
- kill、pause/resume、restart。
- 未 Commit 日志覆盖、部分写、磁盘满和 I/O error。
- Snapshot Install 中断和损坏。
- Membership 与故障并发。
- Elle/Jepsen history。
- `SUBMIT_UNKNOWN` 分类、查询和受控去重机制。
- 固定 seed 回归与 nightly random seed。

退出门禁：Election Safety、Log Matching、State Machine Safety、线性一致性和 durability 承诺均有机器可检查证据；任何失败都可用 seed、日志和 exact binary 重放。

## M6：系统稳定门禁

目标：在引入新的内存状态层之前，确认 Cache OFF 系统已经达到可持续演进的稳定基线。

权威门禁：`docs/quality/system-stability-gate.md`。

退出条件包括但不限于：

- Redis 8.8.1 required compatibility profile 通过。
- RocksDB 权威状态、真实 reopen、损坏与磁盘故障测试通过。
- OpenRaft simulator、3/5 节点进程级故障和历史 checker 通过。
- required CI、长期运行、资源边界、升级/回滚和运维演练通过。
- 所有 P0/P1 清零，保留差异和豁免均有 owner、期限和退出条件。
- Gate Review 绑定 exact commit、测试产物和环境；技术门禁通过后只能向用户提交是否进入 M7 的独立授权请求。

未满足任何一项时，M7 保持冻结。自动化全绿本身不构成进入 M7 的授权。

## M7：Embedded Redis Hot Tier 资格验证与实现

状态：**延期且冻结**。本里程碑在 M6 通过和用户重新明确批准之前不得开始生产实现。

门禁前允许的工作仅限于：

- Redis 8.8.1 来源、许可证、组合发行和 Corresponding Source 方案文档。
- 版本化 C ABI、allocator、线程、多实例、全局状态和生命周期合同设计。
- 动态库 pairing manifest、安全加载、错误码和可观测性接口设计。
- RocksDB 权威性、update-or-invalidate、generation 和 applied-index 不变量设计。
- 不参与生产构建、不会被 Kiwi 加载的文档示例或伪代码。

门禁前禁止：

- 创建或修改用于 Kiwi 生产发行的 Redis fork 代码。
- 编译、随包分发或加载热层 `.so`、`.dylib`、`.dll` 或静态库。
- 在生产 crate 引入热层依赖、FFI binding、loader、配置开关或运行时路径。
- 实现 String 或其他类型的 Cache ON 数据路径。
- 用尚未实现的热层推迟或绕过 Cache OFF 正确性问题。

获批后的交付候选：

- 来源、版权、AGPL 选择、补丁、SBOM 和可复现构建。
- 独立、版本化 C ABI，禁止 Rust 依赖 Redis 内部结构布局。
- allocator、panic/unwind、线程、多实例和全局状态审计。
- 受控路径、hash、ABI 和 source identity 校验的安全 loader。
- String whole-value MVP。
- update-or-invalidate、generation、applied-index 防陈旧机制。

退出门禁：关闭或删除热层不影响正确性；任何热层失败都不会返回旧值；组合发行物与 Corresponding Source 检查通过。

## M8：Cache ON 正确性、故障与性能证明

目标：证明热层只改善性能，不改变 Cache OFF 已建立的任何语义、持久化和一致性承诺。

交付：

- 同一套 Redis 8.8.1 Profile 在 Cache OFF/ON 下执行。
- fill、update、invalidate、eviction、expire 和 generation reset 故障注入。
- Snapshot Install、RocksDB reopen、配置世代变化和动态库加载失败测试。
- Redis 8.8.1 / Kiwi Cache OFF / Kiwi Cache ON 性能基线。
- P50/P95/P99/P99.9、吞吐、CPU、峰值内存和写放大。
- 热层与 RocksDB/Raft 分层指标。

退出门禁：Cache ON 与 Cache OFF 的可观察结果一致；故障只造成性能降级；目标工作负载收益可重复，尾延迟、内存、恢复和写放大不越过批准预算。

## M9：生产发行门禁

目标：形成可部署、可升级、可回滚、可审计的发行物。

交付：

- Linux/macOS/Windows CI。
- sanitizer、Miri、fuzz 和 dependency audit。
- SBOM、第三方许可证、NOTICE 和 Corresponding Source。
- 备份、恢复、升级和回滚演练。
- 运维手册、容量和故障手册。
- release compatibility manifest。
- 组合发行物的 exact Kiwi/Redis source、构建参数、动态库 hash 和 ABI identity。

退出门禁：同一版本可以从空环境部署、验证、备份、恢复、升级和回滚；发行包、源码和运行时身份完全对应。

## M10：Multi-Raft 与远期容量

启动条件：单 Group 的兼容、持久化、一致性、故障与稳定门禁已完成，且有证据证明 Leader 吞吐、容量、隔离或故障域成为瓶颈。

候选交付：

- Redis Cluster slot space。
- Meta Group 和 Slot → Group + Epoch。
- Split、Merge、迁移和热点调度。
- S3 Snapshot/Backup。
- 冷对象归档和本地 NVMe cold cache。

对象存储原生引擎、AI 数据库和其他产品方向必须分别建立项目决定，不自动进入本路线。
