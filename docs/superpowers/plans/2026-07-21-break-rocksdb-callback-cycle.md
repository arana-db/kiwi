# 打破 RocksDB Callback 强引用环实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。

**目标：** 将 Redis RocksDB callback/compaction filter 对 DB 的永久强引用改为弱引用，并用真实同路径 reopen 测试证明 Redis owner 释放后文件句柄能够关闭。

**架构：** 保持现有 `Engine`、`RocksdbEngine`、`Redis.db` 和 Batch 类型不变，只把 `Arc<OnceCell<Arc<DB>>>` 改为 `Arc<OnceCell<Weak<DB>>>`。DB 内部 callback/factory 每次使用时临时升级弱引用，升级失败时安全跳过或采用保守 Keep 行为，从而打破 `DB -> callback/factory -> Arc<DB> -> DB` 的永久环。

**技术栈：** Rust、arana-db/rust-rocksdb、OnceCell、Arc/Weak、Cargo test、WSL/Linux。

---

## 文件结构

- 修改：`src/storage/tests/redis_basic_test.rs`，增加真实 drop 后同路径 reopen 的回归测试。
- 修改：`src/storage/src/redis.rs`，用 `Weak<DB>` 初始化 callback/factory DB cell，并在 flush trigger 中临时升级。
- 修改：`src/storage/src/data_compaction_filter.rs`，让 factory 保存弱引用 cell，在创建 filter 时临时升级，并同步测试 helper。
- 不修改：`src/engine/**`、`src/raft/**`、`Redis.db` 类型、`RocksdbEngine::Drop`、`Redis.need_close` 和 Batch 类型；这些属于 #349 第二段。

## 任务 1：用真实同路径 reopen 冻结强引用环问题

**文件：**

- 修改：`src/storage/tests/redis_basic_test.rs`

- [ ] **步骤 1：增加失败的生命周期测试**

在 `redis_basic_test` 模块中增加测试。使用两个独立的 `Redis` owner，第一阶段 drop 后立即在同一路径打开第二个实例：

```rust
#[test]
fn test_redis_drop_releases_rocksdb_for_same_path_reopen() {
    let test_db_path = unique_test_db_path();
    safe_cleanup_test_db(&test_db_path);

    {
        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(
            storage_options,
            1,
            Arc::new(bg_task_handler),
            lock_mgr,
        );

        redis
            .open(
                test_db_path
                    .to_str()
                    .expect("test DB path should be valid UTF-8"),
            )
            .expect("first Redis owner should open RocksDB");
        redis.set_need_close(true);
    }

    {
        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut reopened = Redis::new(
            storage_options,
            1,
            Arc::new(bg_task_handler),
            lock_mgr,
        );

        reopened
            .open(
                test_db_path
                    .to_str()
                    .expect("test DB path should be valid UTF-8"),
            )
            .expect("dropping the first Redis owner should release the RocksDB lock");
        reopened.set_need_close(true);
    }

    safe_cleanup_test_db(&test_db_path);
}
```

该测试使用真实 RocksDB lock，不使用 mock、`Arc::strong_count()` 或仅重建 wrapper。

- [ ] **步骤 2：运行红灯并确认失败原因**

在 WSL/Linux 中运行：

```bash
cargo +1.95 test --package storage \
  test_redis_drop_releases_rocksdb_for_same_path_reopen -- --nocapture
```

预期：FAIL。第二次 `Redis::open()` 返回 RocksDB lock/文件句柄仍被当前进程持有的错误。若测试意外通过，先证明当前强引用环是否被其他路径打断，不得直接修改生产代码。

## 任务 2：把 callback/factory 的永久强引用改为 Weak

**文件：**

- 修改：`src/storage/src/redis.rs`
- 修改：`src/storage/src/data_compaction_filter.rs`
- 测试：`src/storage/src/data_compaction_filter.rs`

- [ ] **步骤 1：修改 Redis DB cell 类型**

在 `redis.rs` 中引入 `Weak`：

```rust
use std::sync::{Arc, Weak};
```

将 callback cell 改为：

```rust
let db_once_cell: Arc<OnceCell<Weak<DB>>> = Arc::new(OnceCell::new());
```

DB 打开并构造 `RocksdbEngine` 后只写入弱引用：

```rust
let db_arc = engine.shared_db();
let _ = db_once_cell.set(Arc::downgrade(&db_arc));
```

同步修改 `create_cf_options()` 参数：

```rust
db_once_cell: Option<&Arc<OnceCell<Weak<DB>>>>,
```

- [ ] **步骤 2：修改 flush trigger 的升级逻辑**

flush trigger 不得直接取得永久 `Arc<DB>`。改为每次调用时升级：

```rust
let Some(db) = flush_db_cell.get().and_then(Weak::upgrade) else {
    log::debug!(
        "flush_trigger ignored because RocksDB is not initialized or is closing; cf_id={cf_id}"
    );
    return;
};
```

保留现有 CF 边界检查、handle 查找和 `flush_cf()` 错误日志。`Weak::upgrade()` 失败是 shutdown 状态，不得 panic。

- [ ] **步骤 3：修改 DataCompactionFilterFactory**

在 `data_compaction_filter.rs` 中引入 `Weak`：

```rust
use std::sync::{Arc, Weak};
```

factory 保存弱引用 cell：

```rust
pub struct DataCompactionFilterFactory {
    db: Arc<OnceCell<Weak<DB>>>,
    data_type: DataType,
}

impl DataCompactionFilterFactory {
    pub fn new(db: Arc<OnceCell<Weak<DB>>>, data_type: DataType) -> Self {
        Self { db, data_type }
    }
}
```

创建 filter 时临时升级：

```rust
let db = self.db.get().and_then(Weak::upgrade);
DataCompactionFilter::new(db, self.data_type)
```

升级失败时 `DataCompactionFilter` 保持 `db=None`，现有 `MetaLookup::Unavailable -> CompactionDecision::Keep` 语义必须保持，避免 shutdown 期间错误删除数据。

- [ ] **步骤 4：同步 compaction filter 测试 helper**

将 helper 返回类型改为：

```rust
fn setup_db_for_filter_test(
    path: &std::path::Path,
) -> (Arc<OnceCell<Weak<DB>>>, Arc<DB>)
```

cell 写入：

```rust
db_cell.set(Arc::downgrade(&db)).expect("DB cell should be set once");
```

现有 compaction filter 测试必须继续验证 meta 缺失、有效、过期和版本变化语义。

- [ ] **步骤 5：增加弱引用失效时的保守行为测试**

增加一个不依赖真实 compaction 后台线程的单元测试：

```rust
#[test]
fn test_factory_keeps_data_when_db_owner_is_gone() {
    let db_cell: Arc<OnceCell<Weak<DB>>> = Arc::new(OnceCell::new());
    let temp_dir = TempDir::new().expect("test should create temp dir");

    {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = Arc::new(
            DB::open(&opts, temp_dir.path()).expect("test should open RocksDB"),
        );
        db_cell
            .set(Arc::downgrade(&db))
            .expect("DB cell should be set once");
    }

    let mut factory = DataCompactionFilterFactory::new(db_cell, DataType::Hash);
    let context = CompactionFilterContext {
        is_full_compaction: false,
        is_manual_compaction: false,
    };
    let mut filter = factory.create(context);
    let data_key = encode_data_key(b"missing-owner", 1);

    assert!(matches!(
        filter.filter(0, &data_key, b""),
        CompactionDecision::Keep
    ));
}
```

如果 `TempDir` 尚未导入，在测试模块中使用 `tempfile::TempDir`。该测试验证 owner 消失时不会把数据误判为可删除。

## 任务 3：验证 Green、范围和残留

**文件：**

- 验证：`src/storage/src/redis.rs`
- 验证：`src/storage/src/data_compaction_filter.rs`
- 验证：`src/storage/tests/redis_basic_test.rs`

- [ ] **步骤 1：运行生命周期定向测试**

```bash
cargo +1.95 test --package storage \
  test_redis_drop_releases_rocksdb_for_same_path_reopen -- --nocapture
```

预期：PASS，1 passed，0 failed。

- [ ] **步骤 2：运行 compaction filter 测试**

```bash
cargo +1.95 test --package storage data_compaction_filter -- --nocapture
```

预期：全部相关测试 PASS，包括 DB owner 消失时 Keep。

- [ ] **步骤 3：运行 storage crate 全量测试**

```bash
cargo +1.95 test --package storage
```

预期：0 failed。若出现 Windows/WSL 文件锁或清理失败，记录具体路径和 owner，不得通过 sleep、忽略测试或降低断言规避。

- [ ] **步骤 4：运行格式、Clippy 和 Diff 检查**

```bash
cargo +1.95 fmt --all -- --check
cargo +1.95 clippy --package storage --all-features -- -D warnings -D clippy::unwrap_used
git diff --check
```

预期：全部 exit 0。

- [ ] **步骤 5：确认没有扩大到 Engine 删除阶段**

```bash
git diff --name-only 758f3e8..HEAD
rg -n "OnceCell<Arc<DB>>|OnceCell<Weak<DB>>" src/storage
git diff -- src/engine src/raft Cargo.toml Cargo.lock
```

预期：生产修改只涉及 storage callback/factory 和生命周期测试；`src/engine`、`src/raft`、根 Cargo 文件无 Diff。`OnceCell<Arc<DB>>` 在本次涉及的生产路径中无残留。

- [ ] **步骤 6：Commit**

```bash
git add src/storage/src/redis.rs \
  src/storage/src/data_compaction_filter.rs \
  src/storage/tests/redis_basic_test.rs
git commit -m "fix(storage): break RocksDB callback ownership cycle"
```

Commit body 必须记录红灯、Green 命令和 `Co-authored-by: OmX <omx@oh-my-codex.dev>`，不得 push。

## 审查门禁

实现 commit 后依次执行：

1. 规格合规审查：确认只处理强引用环，没有提前删除 Engine 或修改 Batch/Raft。
2. 代码质量审查：检查 Weak 升级时机、shutdown 语义、保守 Keep、测试真实性和错误处理。
3. Test Guard：确认 reopen 测试使用真实 RocksDB 文件锁，不测试 mock 或 `Arc::strong_count()`。
4. 主代理独立复跑关键测试和检查 Git Diff。

任一审查发现 Critical/Important 时，必须由实现代理修复并重新审查后才能结束第一段。
