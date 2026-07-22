# 删除 Engine 伪抽象（方案 A）实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。

**目标：** 删除 `Engine`/`RocksdbEngine`/`engine` crate，让 storage 与 Raft 只使用具体 RocksDB 类型，并修正 snapshot 热切换期间旧 `Storage` owner 无法 drain 的生命周期缺口。

**架构：** `Redis` 和 `RocksdbLogStore` 分别持有 `Arc<rocksdb::DB>`，`RocksBatch` 借用 `&rocksdb::DB`；DB 内部 callback 继续只持有 PR #358 已建立的 `Weak<DB>`。所有请求和 RocksDB 后台维护通过同一个 storage access gate 参与 pause/drain，`BackgroundTaskManager` 每轮从 `GlobalStorage` 加载当前实例，不再永久缓存旧 `Arc<Storage>`。

**技术栈：** Rust 1.95、rust-rocksdb fork、Tokio、ArcSwap、OpenRaft、SNAFU、Cargo workspace。

---

## 文件结构

- 修改 `src/common/runtime/storage_server.rs`：引入统一 storage access gate；让 batch、non-batch 和后台任务共同参与 pause/drain；后台管理器跟随 `GlobalStorage` 热切换。
- 修改 `src/storage/src/redis.rs`：把 `Option<Box<dyn Engine>>` 改成不可 Clone 的
  `RocksDbOwner`；owner 内部保存 `Arc<DB>`，Drop 时 cancel/wait 后再释放；显式创建
  checkpoint；删除 `need_close` 和旧的条件式 `Redis::Drop`。
- 修改 `src/storage/src/batch.rs`：让 `RocksBatch` 借用具体 `&DB`，调用原生借用式 batch API。
- 修改 `src/storage/src/redis_multi.rs`：把两处原生 `write_opt` 调用改成借用 `&WriteBatch`。
- 修改 `src/storage/src/storage.rs`：`release_resources()` 只释放 Storage owner；更新注释和 batch 回归测试。
- 修改 `src/storage/src/redis_strings.rs`：删除已失效的 Engine 限制注释。
- 修改 `src/storage/tests/redis_basic_test.rs`：增加具体 DB 类型、Storage close/external owner/reopen 回归。
- 修改 `src/storage/tests/redis_{hash,list,set,string,zset}_test.rs`：删除 `set_need_close()` 测试脚手架。
- 修改 `src/raft/src/log_store_rocksdb.rs`：把 `Arc<dyn Engine>` 改成 `Arc<DB>`，直接调用 RocksDB API。
- 修改 `src/raft/tests/log_store_rocksdb_test.rs`：迁移 concrete DB helper，强化 clone/drop/flush/reopen 持久化回归。
- 修改 `src/raft/tests/snapshot_roundtrip_test.rs`：使用真实打开的 target Storage 验证 snapshot 热切换和同路径 reopen。
- 修改根 `Cargo.toml`、`Cargo.lock`、`src/storage/Cargo.toml`、`src/raft/Cargo.toml`：删除 engine workspace package 与依赖边。
- 修改 `README.md`、`README_CN.md`、`CLAUDE.md`：删除 engine crate 架构描述。
- 删除 `src/engine/Cargo.toml`、`src/engine/src/lib.rs`、`src/engine/src/engine.rs`、`src/engine/src/rocksdb_engine.rs`。

### 任务 1：建立可证明的 storage pause/drain 所有权门禁

**文件：**
- 修改：`src/common/runtime/storage_server.rs`
- 测试：`src/common/runtime/storage_server.rs` 内现有 `#[cfg(test)]` 模块

- [ ] **步骤 1：编写 access gate 的失败测试**

在测试模块增加以下行为测试，测试只使用新 API，不先写实现：

```rust
#[tokio::test]
async fn storage_access_gate_blocks_new_access_until_resume() {
    let gate = StorageAccessGate::new();
    let first = gate.enter().await;

    let pausing = {
        let gate = gate.clone();
        tokio::spawn(async move { gate.request_pause().await })
    };

    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!pausing.is_finished());
    drop(first);
    pausing.await.expect("pause task should complete");

    let mut blocked = {
        let gate = gate.clone();
        tokio::spawn(async move { gate.enter().await })
    };
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut blocked)
            .await
            .is_err()
    );

    gate.resume();
    let _guard = tokio::time::timeout(Duration::from_secs(1), blocked)
        .await
        .expect("storage access should resume")
        .expect("storage access task should not panic");
}
```

根据实际 `enter()` 返回类型调整最后一个 `expect`，但必须保留三个断言：pause 等待 inflight、pause 期间新 access 阻塞、resume 后继续。

- [ ] **步骤 2：运行测试验证红灯**

运行：

```bash
cargo test --package runtime storage_access_gate_blocks_new_access_until_resume -- --nocapture
```

预期：编译失败，提示 `StorageAccessGate` 不存在。

- [ ] **步骤 3：实现 RAII access gate**

在 `storage_server.rs` 增加共享门禁：

```rust
#[derive(Clone)]
struct StorageAccessGate {
    paused: Arc<AtomicBool>,
    active: Arc<AtomicU64>,
    notify: Arc<tokio::sync::Notify>,
}

struct StorageAccessGuard {
    gate: StorageAccessGate,
}
```

`enter()` 必须使用双检协议：等待 `paused == false`，增加 `active`，再次检查 `paused`；若 pause 已发生则立即撤销 active 并重试。`StorageAccessGuard::drop()` 统一递减 active，并在 active 变为零时通知 pause waiter。

`request_pause()` 必须先创建并 enable `Notify::notified()` future，再读取 active，避免最后一个 guard 在 waiter 注册窗口释放导致永久挂起：

```rust
async fn request_pause(&self) {
    self.paused.store(true, Ordering::SeqCst);
    loop {
        let notified = self.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if self.active.load(Ordering::SeqCst) == 0 {
            return;
        }
        notified.await;
    }
}
```

`resume()` 清除 paused 并 `notify_waiters()`。不得使用可丢最后一次通知的“先检查 active，再创建 notified future”顺序。

- [ ] **步骤 4：让 pause controller 与 StorageServer 共享 gate**

将 `StorageServerPauseController` 和 `StorageServer` 当前分散的 `paused`、`pending_count`、`pause_notify` 字段收敛到同一个 `StorageAccessGate`。公共 `request_pause()`/`resume()` 方法签名保持不变，server 的 `with_pause_controller()` 复用 controller 的 gate。

- [ ] **步骤 5：让 batch 和 non-batch 请求都持有 guard**

`run_without_batching()` 在加载 `GlobalStorage` 前 `enter().await`，把 guard 移入请求 task 并持有到 `process_single_request()` 返回。

`process_request_batch()` 在开始处理整个 batch 前取得一个 guard，保持到所有 batch task join 完成。pause 期间已排队 batch 可以留在 `BatchProcessor` 中，但不得开始访问 Storage。

删除旧的手工 `pending_count.fetch_add/fetch_sub` 和 notify 逻辑，避免双重计数。

- [ ] **步骤 6：让 BackgroundTaskManager 跟随 GlobalStorage**

把：

```rust
storage: Arc<Storage>
```

改成：

```rust
global_storage: GlobalStorage,
access_gate: StorageAccessGate,
```

构造点使用 `global_storage.clone()`。compaction、flush、statistics 监控循环不得跨 tick 保存 `Arc<Storage>`；每次实际检查前先取得 gate guard，再 `global_storage.load()`，检查结束即释放两者。不得改动 interval、batch size、worker count 或其他 #350/#351 配置。

- [ ] **步骤 7：补充后台 manager 跟随 swap 与 batching pause 测试**

增加：

```text
background_task_manager_loads_current_storage_after_swap
paused_batch_does_not_start_until_resume
pause_and_storage_enter_do_not_cross
```

测试必须分别证明：manager 在 swap 后观察新 Storage；默认 batching pause 期间请求无响应且不写数据；pause 返回时 active guard 为零且新 access 仍被阻塞。

- [ ] **步骤 8：运行 runtime 定向测试**

运行真实 package 的：

```bash
cargo test --package runtime storage_access_gate -- --nocapture
cargo test --package runtime paused_batch -- --nocapture
cargo test --package runtime background_task_manager -- --nocapture
```

预期：全部 PASS，且没有挂起测试。

- [ ] **步骤 9：提交任务 1**

```bash
git add src/common/runtime/storage_server.rs
git commit -m "fix(runtime): drain RocksDB owners during storage swap"
```

### 任务 2：迁移 storage 到具体 `Arc<DB>` 并明确 close owner

**文件：**
- 修改：`src/storage/src/redis.rs`
- 修改：`src/storage/src/batch.rs`
- 修改：`src/storage/src/redis_multi.rs`
- 修改：`src/storage/src/storage.rs`
- 修改：`src/storage/src/redis_strings.rs`
- 修改：`src/storage/tests/redis_basic_test.rs`
- 修改：`src/storage/tests/redis_hash_test.rs`
- 修改：`src/storage/tests/redis_list_test.rs`
- 修改：`src/storage/tests/redis_set_test.rs`
- 修改：`src/storage/tests/redis_string_test.rs`
- 修改：`src/storage/tests/redis_zset_test.rs`
- 修改：`src/raft/tests/snapshot_logindex_test.rs`

- [ ] **步骤 1：编写 concrete Redis DB 类型红灯**

在 `redis_basic_test.rs` 增加：

```rust
fn assert_concrete_rocksdb(db: &rocksdb::DB) {
    assert!(db.cf_handle("default").is_some());
}

#[test]
fn test_redis_uses_concrete_shared_rocksdb_owner() {
    let path = storage::unique_test_db_path();
    let mut redis = create_redis_instance(&path);
    let db = redis.db().expect("Redis should own an open RocksDB");
    assert_concrete_rocksdb(db);
    drop(redis);
    storage::safe_cleanup_test_db(&path);
}
```

使用测试文件现有 helper 名称调整 setup。最终公开契约必须是借用 `&DB`，不能通过
测试重新暴露可 Clone 的 `Arc<DB>` owner。

- [ ] **步骤 2：运行测试验证红灯**

```bash
cargo test --package storage test_redis_uses_concrete_shared_rocksdb_owner --no-run
```

预期：旧实现没有借用式 `db()` accessor，或底层仍是 `dyn Engine`，因此编译失败。

- [ ] **步骤 3：迁移 Redis DB 所有权并建立唯一 shutdown owner**

在 `redis.rs`：

```rust
pub(crate) db: Option<RocksDbOwner>,
```

`RocksDbOwner` 内部持有 `Arc<DB>`，实现 `Deref<Target = DB>` 但不得实现 `Clone`，
不得返回内部 `Arc`。公开 accessor 只允许返回 `Option<&DB>`。

`open()` 使用：

```rust
let db = RocksDbOwner::new(
    DB::open_cf_descriptors(&db_opts, db_path, column_families).context(RocksSnafu)?,
);
let _ = db_once_cell.set(db.downgrade());
```

handles、`DbAccess` 和 `self.db` 都直接借用 owner 中的具体 `DB`。删除
`use engine::{Engine, RocksdbEngine};`，不得增加替代 Engine 转发层。

- [ ] **步骤 4：显式迁移 checkpoint**

`DB` 没有本项目 wrapper 提供的 `create_checkpoint()` 方法。改为：

```rust
let checkpoint =
    rocksdb::checkpoint::Checkpoint::new(db.as_ref()).context(RocksSnafu)?;
checkpoint.create_checkpoint(path).context(RocksSnafu)
```

保留现有 SNAFU RocksDB error 上下文。

- [ ] **步骤 5：迁移 RocksBatch 到 `&DB`**

`RocksBatch` 字段和构造参数改成 `&'a DB`。`commit()` 调用必须借用 batch：

```rust
self.db
    .write_opt(&self.inner, self.write_options)
    .context(RocksSnafu)
```

`Batch`、`BinlogBatch` 和 append callback 保持不变。

- [ ] **步骤 6：删除 `need_close` 和旧 wrapper Drop，补唯一 owner shutdown**

删除：

```text
Redis.need_close
Redis::set_need_close()
impl Drop for Redis
```

删除 storage 测试中所有 `set_need_close(true)` 调用。`RocksDbOwner::drop()` 必须在仍
持有内部 `Arc<DB>` 时调用 `cancel_all_background_work(true)`，等待 callback 退出后再
释放 DB；该逻辑不得放在任何可 Clone 类型上。`Storage::release_resources()` 中止自身
任务后直接 `self.insts.clear()`，注释必须说明这只释放 Storage 持有的 owner，外部
`Arc<Redis>` 可能继续让 DB 存活。

- [ ] **步骤 7：增加 Storage close 责任边界和真实 RocksBatch 回归**

增加：

```text
storage_close_allows_same_path_reopen_without_external_owners
storage_close_releases_only_storage_owned_redis_references
standalone_rocks_batch_put_commit_and_read_back
dropping_last_owner_waits_for_active_compaction_filter_before_reopen
```

外部 owner 测试必须在 `Storage::close()` 后继续通过该 `Arc<Redis>` 写读，随后释放 owner并真实同路径 reopen。batch 测试必须 put、commit、read back；不能只检查 count。
compaction 生命周期测试必须阻塞一个真实 filter，证明旧实现会在 filter 阻塞时提前
完成 owner drop；修复后 drop 必须等待 filter，释放 filter 后限时完成并真实 reopen。

- [ ] **步骤 8：清理失效 Engine 注释**

把 `storage.rs` 的“through Engine trait”改成直接描述 RocksDB table properties。删除 `redis_strings.rs` 中“multi_get 未暴露于 Engine”的注释，保留是否需要 multi_get 优化的独立事实时必须用 RocksDB 具体表述。

- [ ] **步骤 9：运行 storage 绿灯**

```bash
cargo test --package storage test_redis_uses_concrete_shared_rocksdb_owner -- --nocapture
cargo test --package storage storage_close -- --nocapture
cargo test --package storage standalone_rocks_batch_put_commit_and_read_back -- --nocapture
cargo test --package storage dropping_last_owner_waits_for_active_compaction_filter_before_reopen -- --nocapture
cargo test --package storage
```

预期：storage 全部测试通过。

- [ ] **步骤 10：提交任务 2**

```bash
git add src/storage/src src/storage/tests
git add src/raft/tests/snapshot_logindex_test.rs
git commit -m "refactor(storage): use concrete RocksDB ownership"
```

### 任务 3：迁移 RocksdbLogStore 到具体 `Arc<DB>`

**文件：**
- 修改：`src/raft/src/log_store_rocksdb.rs`
- 修改：`src/raft/tests/log_store_rocksdb_test.rs`

- [ ] **步骤 1：先把测试 helper 改成 concrete DB**

测试 helper 返回：

```rust
fn create_test_db() -> (TempDir, Arc<DB>)
```

直接写 helper 接受 `&DB` 并调用：

```rust
db.write(&batch).expect("Write should succeed");
```

新增 `dropping_one_log_store_clone_keeps_remaining_owner_usable`：drop 一个 store clone 后，剩余 clone 必须继续 append/read/flush；释放全部 owner 后同路径 reopen 并恢复 vote、committed、last log、last purged 和保留日志。

- [ ] **步骤 2：运行测试验证红灯**

```bash
cargo test --package raft --test log_store_rocksdb_test --no-run
```

预期：编译失败，因为 `RocksdbLogStore::new()` 仍要求 `Arc<dyn Engine>`，而测试传入 `Arc<DB>`。

- [ ] **步骤 3：迁移生产 LogStore**

字段与构造函数改为：

```rust
#[derive(Clone)]
pub struct RocksdbLogStore {
    db: Arc<DB>,
}

pub fn new(db: Arc<DB>) -> Result<Self, StorageError<u64>>
```

所有 `self.engine` 改为 `self.db`。所有原生 batch 写调用必须借用：

```rust
self.db.write(&batch)
```

`open()` 创建 `Arc::new(DB::open_cf_descriptors(...)?);` 后调用 `Self::new(db)`。删除全部 engine import。

- [ ] **步骤 4：迁移内嵌测试**

`log_store_rocksdb.rs` 内 `create_test_db_with_cfs()`、`create_test_db_without_cfs()` 和 partial-CF setup 全部改为 `Arc<DB>`。直接写调用使用 `db.write(&batch)`。

- [ ] **步骤 5：运行 raft 绿灯**

在环境已有 `protoc` 或通过 `PROTOC` 环境变量提供可执行文件后运行：

```bash
cargo test --package raft --test log_store_rocksdb_test
cargo test --package raft
```

预期：全部通过。

- [ ] **步骤 6：提交任务 3**

```bash
git add src/raft/src/log_store_rocksdb.rs src/raft/tests/log_store_rocksdb_test.rs
git commit -m "refactor(raft): store logs in concrete RocksDB handles"
```

### 任务 4：验证真实打开 Storage 的 snapshot 热切换

**文件：**
- 修改：`src/raft/tests/snapshot_roundtrip_test.rs`
- 必要时修改：`src/raft/src/state_machine.rs`

- [ ] **步骤 1：编写打开 target Storage 的失败测试**

增加 `install_snapshot_replaces_open_target_storage`：source 创建 snapshot；target 必须在 `restore_db_path` 调用 `Storage::open()` 并写入 stale key；target 通过 `ArcSwap` 和共享 pause controller 接入 state machine；install 后 snapshot key 存在、stale key 不存在；释放 state machine/ArcSwap/server/background owner 后同路径真实 reopen。

- [ ] **步骤 2：运行测试验证红灯**

```bash
cargo test --package raft --test snapshot_roundtrip_test install_snapshot_replaces_open_target_storage -- --nocapture
```

预期：当前实现因旧 Storage owner 或 pause/drain 缺口导致 restore/open 失败，或 manager 仍观察旧 Storage。测试必须在修改 snapshot 生产路径前显示真实失败原因。

- [ ] **步骤 3：只修正测试暴露的所有权/错误语义**

优先复用任务 1 的 gate 和动态 GlobalStorage owner。`state_machine.rs` 的日志只有在旧 owner 已完成 drain 时才能写“RocksDB lock released”。

若 destructive restore 已开始后出现错误，不得 resume 到未打开 placeholder；最小安全语义是保持 paused 并返回带路径上下文的错误（fail closed）。在 unpack/metadata 验证等尚未切换 placeholder 的早期错误上仍可安全 resume。

- [ ] **步骤 4：运行 snapshot 回归**

```bash
cargo test --package raft --test snapshot_roundtrip_test
cargo test --package raft --test snapshot_logindex_test
```

预期：全部通过，真实 open target 用例在 Windows 文件锁语义下也成功。

- [ ] **步骤 5：提交任务 4**

```bash
git add src/raft/src/state_machine.rs src/raft/tests/snapshot_roundtrip_test.rs
git commit -m "fix(raft): drain storage owners before snapshot restore"
```

### 任务 5：删除 engine crate、依赖边和过时架构文档

**文件：**
- 修改：`Cargo.toml`
- 修改：`Cargo.lock`
- 修改：`src/storage/Cargo.toml`
- 修改：`src/raft/Cargo.toml`
- 修改：`README.md`
- 修改：`README_CN.md`
- 修改：`CLAUDE.md`
- 删除：`src/engine/Cargo.toml`
- 删除：`src/engine/src/lib.rs`
- 删除：`src/engine/src/engine.rs`
- 删除：`src/engine/src/rocksdb_engine.rs`

- [ ] **步骤 1：运行残留红灯扫描**

```bash
rg -n --glob '!docs/superpowers/**' --glob '!target/**' \
  'Engine|RocksdbEngine|Box<dyn Engine>|Arc<dyn Engine>|&dyn Engine|engine\.workspace|path = "src/engine"|set_need_close|need_close' \
  Cargo.toml Cargo.lock README.md README_CN.md CLAUDE.md src
```

预期：命中 engine crate、storage、raft、Cargo 依赖和过时文档。

- [ ] **步骤 2：删除依赖和 crate**

从根 workspace members 与 dependencies 删除 engine；从 storage/raft manifests 删除 `engine.workspace = true`；删除 `src/engine` 四个文件。不得新增改名 wrapper。

- [ ] **步骤 3：更新 Cargo.lock**

运行：

```bash
cargo check --workspace
```

让 Cargo 正常重写 lockfile，确认本地 `engine` package 与 storage/raft dependency edge 消失；不要手工编辑 lockfile package checksum 或依赖数组。

- [ ] **步骤 4：更新架构文档**

README/README_CN/CLAUDE 的 crate layout 删除 `engine/`，明确具体 RocksDB owner 位于 `storage` 主数据路径和 `raft` 日志路径。不得声称新增了 backend-neutral abstraction。

- [ ] **步骤 5：运行残留绿灯扫描**

重复步骤 1 命令。预期：可执行源码、manifests、lockfile 和当前架构文档零命中；历史设计/计划被明确排除。

另运行：

```bash
rg -n 'cancel_all_background_work' src --glob '*.rs'
```

预期只命中不可 Clone 的 storage `RocksDbOwner::drop()`；Raft LogStore 和其他共享
owner 不得命中。

- [ ] **步骤 6：提交任务 5**

```bash
git add Cargo.toml Cargo.lock src/storage/Cargo.toml src/raft/Cargo.toml README.md README_CN.md CLAUDE.md
git add -A src/engine
git commit -m "refactor(storage): remove the Engine abstraction crate"
```

### 任务 6：全量验证与独立审查

**文件：**
- 不新增生产文件；仅修复验证发现的本 PR 回归

- [ ] **步骤 1：格式和 Diff**

```bash
cargo fmt --all -- --check
git diff --check
```

- [ ] **步骤 2：受影响 crate**

```bash
cargo test --package storage
cargo test --package raft
cargo test --package raft --test log_store_rocksdb_test
cargo test --package raft --test snapshot_logindex_test
cargo test --package raft --test snapshot_roundtrip_test
cargo test --package runtime
```

- [ ] **步骤 3：构建和 Clippy**

```bash
cargo check --package server
cargo clippy --all-features --workspace -- -D warnings -D clippy::unwrap_used
```

- [ ] **步骤 4：workspace 与集成测试**

```bash
cargo test --workspace
```

可运行时启动 `kiwi` 并执行项目 Python integration 流程。平台或环境失败必须在 exact base 上复现后才能标记为基线；不得删断言、跳过失败或降低一致性校验。

- [ ] **步骤 5：生命周期最终扫描**

确认：

```text
src/engine 不存在
没有 Engine trait/object/wrapper
cancel_all_background_work 仅存在于不可 Clone 的 storage RocksDbOwner::drop
没有 need_close/set_need_close
BackgroundTaskManager 不缓存 Arc<Storage>
batch/non-batch/background 都通过 access gate
真实 Redis、LogStore、Storage close、snapshot target 可以同路径 reopen
```

- [ ] **步骤 6：规格审查、代码质量审查和最终整体审查**

按 `subagent-driven-development` 对每个任务先做规格符合性审查，再做代码质量审查；所有任务结束后启动新的最终审查子代理。任何 Important/P0/P1 未修复时不得 push 或创建 PR。
