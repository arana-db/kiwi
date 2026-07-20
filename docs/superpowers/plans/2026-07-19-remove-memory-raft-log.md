# 移除生产路径内存 Raft 日志实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。

**目标：** 删除正常运行路径中的内存 Raft LogStore，使旧配置项明确报错，并保证集群文档和节点构造只使用持久化 RocksDB 日志。

**架构：** 保留 OpenRaft 的 `RaftLogStorage` 框架接口，但 Kiwi 正常节点只实例化 `RocksdbLogStore`。配置层对已删除的 `raft-use-memory-log-store` 返回迁移错误，避免旧配置被静默忽略；新增真实 RocksDB close/reopen 测试验证 vote、committed、log 和 purge 状态持久化。

**技术栈：** Rust、OpenRaft、RocksDB、SNAFU、Cargo test、Markdown 文档。

---

## 文件结构

- 修改：`src/conf/src/config.rs`，删除 `use_memory_log_store` 配置状态并拒绝旧配置键。
- 修改：`src/conf/src/lib.rs`，增加旧内存日志配置拒绝和完整示例不暴露该配置的测试。
- 修改：`src/raft/src/node.rs`，删除内存日志字段、import 和运行时分支，固定创建 `RocksdbLogStore`。
- 修改：`src/raft/src/lib.rs`，停止导出内存日志模块。
- 删除：`src/raft/src/log_store.rs`，删除仅用于 POC 的非持久化实现。
- 修改：`src/server/src/main.rs`，不再向 RaftConfig 传递内存日志开关。
- 修改：`docs/cluster.md`，集群示例和配置说明只描述持久化 RocksDB 日志。
- 修改：`src/raft/tests/log_store_rocksdb_test.rs`，新增销毁全部底层句柄后对同一路径执行真实 close/reopen 的持久化测试。

### 任务 1：用失败测试冻结配置迁移行为

**文件：**
- 修改：`src/conf/src/lib.rs`
- 测试：`src/conf/src/lib.rs`

- [ ] **步骤 1：增加旧配置项必须被拒绝的测试**

在 `src/conf/src/lib.rs` 的测试模块增加：

```rust
#[test]
fn test_memory_raft_log_store_config_is_rejected() {
    use std::io::Write;

    let mut config_file = tempfile::NamedTempFile::new()
        .expect("test should create temporary config");
    writeln!(config_file, "raft-node-id 1")
        .expect("test should write raft node id");
    writeln!(config_file, "raft-use-memory-log-store true")
        .expect("test should write removed config key");

    let error = Config::load(
        config_file
            .path()
            .to_str()
            .expect("temporary config path should be UTF-8"),
    )
    .expect_err("removed memory raft log configuration must be rejected");

    assert!(
        error
            .to_string()
            .contains("raft-use-memory-log-store has been removed"),
        "unexpected error: {error}"
    );
}
```

- [ ] **步骤 2：增加完整示例不得暴露内存日志开关的测试**

```rust
#[test]
fn test_full_sample_config_uses_persistent_raft_log_store() {
    let sample = Config::full_sample_config();

    assert!(
        !sample.contains("raft-use-memory-log-store"),
        "full sample config must not advertise a non-persistent Raft log store"
    );
}
```

- [ ] **步骤 3：运行测试确认红灯**

运行：

```powershell
cmd.exe /d /s /c 'call "C:\Program Files (x86)\Microsoft Visual Studio\18\BuildTools\VC\Auxiliary\Build\vcvars64.bat" >nul && cargo +1.95-x86_64-pc-windows-msvc test --package conf memory_raft_log_store -- --nocapture'
```

预期：FAIL。当前实现接受 `raft-use-memory-log-store true`，且完整示例包含该字段。

### 任务 2：从配置模型中移除内存日志开关

**文件：**
- 修改：`src/conf/src/config.rs`
- 测试：`src/conf/src/lib.rs`

- [ ] **步骤 1：删除 RaftClusterConfig 字段和默认值**

从 `RaftClusterConfig` 删除：

```rust
pub use_memory_log_store: bool,
```

并从 `Default` 删除：

```rust
use_memory_log_store: false,
```

- [ ] **步骤 2：将旧配置键改为明确迁移错误**

将原解析分支替换为：

```rust
"raft-use-memory-log-store" => {
    return Err(Error::InvalidConfig {
        source: serde_ini::de::Error::Custom(
            "raft-use-memory-log-store has been removed; Kiwi requires the persistent RocksDB Raft log store"
                .to_string(),
        ),
    });
}
```

- [ ] **步骤 3：运行两个配置测试确认绿灯**

运行：

```powershell
cmd.exe /d /s /c 'call "C:\Program Files (x86)\Microsoft Visual Studio\18\BuildTools\VC\Auxiliary\Build\vcvars64.bat" >nul && cargo +1.95-x86_64-pc-windows-msvc test --package conf memory_raft_log_store -- --nocapture'
```

预期：两个测试 PASS。

- [ ] **步骤 4：运行完整 conf 测试**

运行：

```powershell
cmd.exe /d /s /c 'call "C:\Program Files (x86)\Microsoft Visual Studio\18\BuildTools\VC\Auxiliary\Build\vcvars64.bat" >nul && cargo +1.95-x86_64-pc-windows-msvc test --package conf'
```

预期：全部测试 PASS。

### 任务 3：删除正常节点中的内存 Raft LogStore

**文件：**
- 修改：`src/raft/src/node.rs`
- 修改：`src/raft/src/lib.rs`
- 删除：`src/raft/src/log_store.rs`
- 修改：`src/server/src/main.rs`
- 测试：`src/raft/tests/log_store_rocksdb_test.rs`

- [ ] **步骤 1：删除 RaftConfig 中的内存日志字段**

从 `RaftConfig` 和 `Default` 删除 `use_memory_log_store`。

- [ ] **步骤 2：固定使用 RocksdbLogStore**

删除：

```rust
use crate::log_store::LogStore;
```

将 `create_raft_node()` 的条件分支替换为单一路径：

```rust
let log_store_path = config.data_dir.join("raft_logs_rocksdb");
std::fs::create_dir_all(&log_store_path)?;
let log_store = RocksdbLogStore::open(&log_store_path)?;
let raft = Raft::new(
    config.node_id,
    raft_config,
    network,
    log_store,
    state_machine,
)
.await?;
```

- [ ] **步骤 3：删除模块和服务器字段映射**

- 从 `src/raft/src/lib.rs` 删除 `pub mod log_store;`。
- 删除 `src/raft/src/log_store.rs`。
- 从 `src/server/src/main.rs` 构造 `RaftConfig` 的代码中删除 `use_memory_log_store` 赋值。

- [ ] **步骤 4：执行全仓库残留扫描**

运行：

```powershell
rg -n --glob '!docs/superpowers/plans/2026-07-19-remove-memory-raft-log.md' "use_memory_log_store|raft-use-memory-log-store|crate::log_store::LogStore|pub mod log_store;" src docs
```

预期：只允许命中配置拒绝测试、迁移错误和说明旧模式已删除的升级文档；不得再命中可执行内存日志实现或正常配置字段。

- [ ] **步骤 5：运行 Raft 持久化定向测试**

运行：

```powershell
cmd.exe /d /s /c 'call "C:\Program Files (x86)\Microsoft Visual Studio\18\BuildTools\VC\Auxiliary\Build\vcvars64.bat" >nul && cargo +1.95-x86_64-pc-windows-msvc test --package raft --test log_store_rocksdb_test close_and_reopen_recovers_complete_raft_state -- --nocapture'
```

预期：PASS。第一阶段销毁全部 store、reader、engine 和 DB handle 后，第二阶段从同一路径重新打开，并恢复 vote、committed state、last log、last purged 和未清理日志。

### 任务 4：修正文档和示例

**文件：**
- 修改：`docs/cluster.md`

- [ ] **步骤 1：删除内存日志示例配置**

从节点示例中删除：

```conf
raft-use-memory-log-store true
```

- [ ] **步骤 2：更新 Raft 配置表和持久化说明**

删除 `raft-use-memory-log-store` 表格行，并在 `raft-data-dir` 说明附近明确：

```text
Raft 日志始终持久化到 RocksDB。节点重启时会从该目录恢复 vote、committed state 和日志条目；不得复用一个已存在主数据目录并清空 Raft 日志目录。
```

- [ ] **步骤 3：检查文档残留**

运行：

```powershell
rg -n --glob '!docs/superpowers/plans/2026-07-19-remove-memory-raft-log.md' "memory log|内存日志|raft-use-memory-log-store" README.md docs src
```

预期：只允许迁移错误、拒绝测试和明确说明旧模式已删除及升级操作的文档命中，不再有推荐或配置说明。

### 任务 5：完整验证

**文件：**
- 验证全部受影响文件

- [ ] **步骤 1：格式检查**

运行：

```powershell
cargo +1.95-x86_64-pc-windows-msvc fmt --all -- --check
```

预期：退出码 0。

- [ ] **步骤 2：运行 conf 测试**

运行：

```powershell
cmd.exe /d /s /c 'call "C:\Program Files (x86)\Microsoft Visual Studio\18\BuildTools\VC\Auxiliary\Build\vcvars64.bat" >nul && cargo +1.95-x86_64-pc-windows-msvc test --package conf'
```

预期：全部 PASS。

- [ ] **步骤 3：运行 raft 测试**

运行：

```powershell
cmd.exe /d /s /c 'call "C:\Program Files (x86)\Microsoft Visual Studio\18\BuildTools\VC\Auxiliary\Build\vcvars64.bat" >nul && cargo +1.95-x86_64-pc-windows-msvc test --package raft'
```

预期：全部 PASS。若因本机缺少 `protoc` 无法执行，必须记录完整环境错误，并使用 CI 等价环境或提供本地官方 protoc 后重跑，不能把环境失败描述成测试通过。

- [ ] **步骤 4：运行 Clippy**

运行：

```powershell
cmd.exe /d /s /c 'call "C:\Program Files (x86)\Microsoft Visual Studio\18\BuildTools\VC\Auxiliary\Build\vcvars64.bat" >nul && cargo +1.95-x86_64-pc-windows-msvc clippy --all-features --workspace -- -D warnings -D clippy::unwrap_used'
```

预期：退出码 0，无 warning。

- [ ] **步骤 5：检查 Diff 与需求清单**

运行：

```powershell
git diff --check
git status --short
git diff -- src/conf/src/config.rs src/conf/src/lib.rs src/raft/src/node.rs src/raft/src/lib.rs src/raft/src/log_store.rs src/server/src/main.rs docs/cluster.md
```

逐项确认：旧配置被拒绝、内存实现删除、节点固定 RocksDB、文档修正、持久化测试通过或环境阻塞被准确记录。
