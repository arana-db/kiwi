# RocksDB-only Raft 日志 Review Finding 修复实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。

**目标：** 保证生产 Raft 日志只使用 RocksDB，拒绝旧内存日志节点以空 Raft 状态复用已有主数据，并用真实 close/reopen 测试验证持久化。

**架构：** `create_raft_node()` 仍只创建 `RocksdbLogStore`。旧 `raft_logs` 目录仅作为迁移风险标记；当标记存在且 RocksDB Raft 状态全空时 fail closed。持久化测试销毁所有 DB handle 后从同一路径重新打开。

**技术栈：** Rust、OpenRaft 0.9、RocksDB、Tokio、Cargo test、Clippy。

---

## 文件结构

- 修改：`src/raft/src/node.rs`，增加旧内存日志升级 fail-closed 门禁和回归测试。
- 修改：`src/raft/tests/log_store_rocksdb_test.rs`，增加真实 close/reopen 持久化测试。
- 修改：`docs/cluster.md`，记录旧内存模式节点的安全重新入群流程。
- 修改：`docs/superpowers/plans/2026-07-19-remove-memory-raft-log.md`，修正 reopen 证据和残留扫描命令。

### 任务 1：用失败测试冻结旧内存日志升级行为

**文件：**
- 修改：`src/raft/src/node.rs`

- [ ] **步骤 1：增加负例测试**

在真实临时 `raft-data-dir` 中创建旧 `raft_logs` 目录，调用 `create_raft_node()`，断言返回包含 `cannot safely migrate` 的错误。当前实现会成功创建空 `raft_logs_rocksdb`，因此该测试在修复前必须失败。

- [ ] **步骤 2：运行红灯**

```bash
cargo +1.95 test --package raft legacy_memory_log_without_durable_raft_state_is_rejected -- --nocapture
```

预期：FAIL，`expect_err` 收到 `Ok`。

- [ ] **步骤 3：实现最小门禁**

在 `RocksdbLogStore::open()` 后读取 vote、committed、last log 和 last purged。仅当旧 `raft_logs` 存在且四类 durable state 全空时返回迁移错误。不添加任何内存 LogStore、fallback 或运行时选择字段。

- [ ] **步骤 4：验证绿灯并增加允许场景**

再增加一个用例：旧目录存在，但 `raft_logs_rocksdb` 已持久化 vote 时，`create_raft_node()` 必须成功。

### 任务 2：增加真实 RocksDB close/reopen 持久化测试

**文件：**
- 修改：`src/raft/tests/log_store_rocksdb_test.rs`

- [ ] **步骤 1：通过正式接口写入**

新增 `close_and_reopen_recovers_complete_raft_state`：第一阶段用 `RocksdbLogStore::open()`、`RaftLogStorageExt::blocking_append()`、`save_vote()`、`save_committed()` 和 `purge()` 写入 10 条日志及完整元数据。

- [ ] **步骤 2：证明测试能失败**

临时将第二次 `open()` 指向一个新路径，确认 vote/log/purge 断言失败；随后恢复同一路径，不保留临时改动。

- [ ] **步骤 3：真正 close/reopen**

第一阶段结束后销毁所有 store、reader、engine 和 DB handle，然后对同一路径重新调用 `RocksdbLogStore::open()`。验证完整 vote、committed `LogId`、last log、last purged 和未被 purge 的 5..=10 日志内容。

- [ ] **步骤 4：运行定向测试**

```bash
cargo +1.95 test --package raft --test log_store_rocksdb_test close_and_reopen_recovers_complete_raft_state -- --nocapture
```

预期：PASS。

### 任务 3：修正迁移和审查文档

**文件：**
- 修改：`docs/cluster.md`
- 修改：`docs/superpowers/plans/2026-07-19-remove-memory-raft-log.md`

- [ ] **步骤 1：补充安全升级说明**

说明旧内存日志节点的 vote、membership、committed position 和日志尾部无法安全恢复；必须使用新 node ID 与干净 `data-dir`/`raft-data-dir` 从健康 leader 重新入群。

- [ ] **步骤 2：修正 reopen 表述和扫描命令**

把“复用既有 reopen 测试”改为真实 close/reopen 测试；两处 `rg` 命令增加：

```powershell
--glob '!docs/superpowers/plans/2026-07-19-remove-memory-raft-log.md'
```

### 任务 4：完整验证

- [ ] **步骤 1：格式和 Diff**

```bash
cargo +1.95 fmt --all -- --check
git diff --check origin/main...HEAD
```

- [ ] **步骤 2：受影响测试与编译**

```bash
cargo +1.95 test --package conf
cargo +1.95 test --package raft
cargo +1.95 check --package server
```

- [ ] **步骤 3：全 workspace Clippy**

```bash
cargo +1.95 clippy --all-features --workspace -- -D warnings -D clippy::unwrap_used
```

预期：全部命令退出码为 0，且残留扫描不出现任何可执行内存 Raft LogStore 或运行时选择字段。
