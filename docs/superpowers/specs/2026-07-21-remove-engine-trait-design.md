# 删除 Engine 伪抽象与明确 RocksDB 生命周期设计

## 背景

Issue #349 是 Epic #347 的第二阶段。第一阶段 #348 已通过 PR #355 合并，生产 Raft 日志路径现在只使用 `RocksdbLogStore`。下一步需要删除主数据和 Raft RocksDB 路径中没有实际多后端价值的 `Engine` trait。

当前 `Engine` trait 只有 `RocksdbEngine` 一个实现，并直接复制 RocksDB 的类型与方法，包括 `DB`、Column Family handle、iterator、snapshot、`ReadOptions`、`WriteOptions`、`WriteBatch`、sequence number 和 SST table properties。它没有隔离 RocksDB，反而增加动态分派、转发代码和生命周期歧义。

现有实现还有两个需要与 trait 删除同时解决的生命周期问题：

1. `RocksdbEngine` 可以 Clone，但每个 wrapper 的 `Drop` 都调用 `cancel_all_background_work(true)`。任意 clone 被释放时都可能取消仍被其他 owner 使用的 RocksDB 后台工作。
2. `Redis::open()` 把 `Arc<DB>` 写入 `OnceCell<Arc<DB>>`，而该 cell 又被 DB 自己持有的 EventListener 或 CompactionFilterFactory 捕获，可能形成 `DB -> callback/factory -> Arc<DB> -> DB` 的强引用环，使 close/reopen 无法释放文件句柄。

## 目标

- 删除通用 `Engine` trait 和 `RocksdbEngine` 一对一转发层。
- 从 workspace 删除 `engine` crate。
- `Redis`、`RocksBatch` 和 `RocksdbLogStore` 直接使用具体 RocksDB 类型。
- 以 `Arc<DB>` 表达共享所有权，以 `Weak<DB>` 表达 DB 内部 callback 对 DB 的非拥有访问。
- 删除任意共享 wrapper Drop 时隐式取消 RocksDB 后台工作的行为。
- 明确 `Storage::close()`、snapshot install 热切换和最后一个 DB owner 释放之间的责任边界。
- 保持数据格式、Column Family、snapshot、checkpoint、LogIndex、iterator、dynamic options 和 Batch 行为不变。

## 非目标

- 不实现新的 backend-neutral storage trait。
- 不增加 MemoryEngine、Speedb 或其他存储后端。
- 不创建改名后的全 API 转发 wrapper，例如 `DatabaseEngine`、`Backend` 或 `RocksDbEngineV2`。
- 不删除 `Batch` trait、`RocksBatch`、`BinlogBatch` 或 OpenRaft 存储契约。
- 不修改 RocksDB 数据编码、CF 名称、key encoding、snapshot 格式或 LogIndex 协议。
- 不在本任务中重构双 Tokio runtime、请求队列或 RocksDB executor；这些属于 #350 和 #351。
- 不顺带替换项目使用的 arana-db/rust-rocksdb fork。

## 方案选择

采用方案 A：在一个 #349 PR 中完成具体类型迁移、生命周期修复、engine crate 删除和回归验证，并用独立 commit 分层组织变更。

不采用只做机械类型替换的方案，因为它会保留 DB 强引用环和不准确的 close 语义，无法满足 #349 的 shutdown owner 验收条件。

不拆成两个 PR，因为生命周期修复和具体类型迁移共享同一所有权模型；保留中间态会增加一次 RocksDB 全平台 CI 和额外的临时兼容代码。一个 PR 内使用小步 commit 可以获得同样的审查边界。

## 执行分段

根据用户审查意见，实施顺序调整为先单独完成强引用环修复，再继续删除 `Engine` trait：

### 第一段：打破 DB 强引用环

当前先只修改 callback/compaction filter 的 DB 引用模型：

- 将 `Arc<OnceCell<Arc<DB>>>` 改为 `Arc<OnceCell<Weak<DB>>>`。
- flush trigger 和 compaction filter 创建时临时升级弱引用。
- 增加真实 close/reopen 回归测试，证明释放 Redis owner 后同一路径可以重新打开。
- 保持 `Engine` trait、`RocksdbEngine`、`Redis.db`、`RocksBatch` 和 `RocksdbLogStore` 的类型不变。
- 不在这一段删除 `need_close` 或修改 `RocksdbEngine::Drop`；这些属于第二段具体所有权迁移。

第一段完成后必须形成独立 commit，并通过 storage 定向测试、storage crate 测试、格式检查和代码审查。只有第一段证据充分后才进入第二段。

### 第二段：完成方案 A

继续执行本文其余设计：迁移到具体 `Arc<DB>` / `&DB`、删除错误的 wrapper Drop、删除 `Engine` trait 和 crate，并完成 snapshot hot-swap 与全 workspace 门禁。

这一分段只改变实施顺序，不改变 #349 的最终目标和成功标准。

## 具体类型模型

### Redis 主数据 DB

`Redis` 直接持有具体共享 DB：

```rust
pub struct Redis {
    // ...
    pub db: Option<Arc<rocksdb::DB>>,
    // ...
}
```

`Redis::open()` 直接构造 `Arc<DB>`：

```rust
let db = Arc::new(DB::open_cf_descriptors(
    &db_opts,
    db_path,
    column_families,
)?);
```

所有现有 `self.db` 调用继续使用 RocksDB 原生方法。不得增加同名转发函数来模拟已删除的 trait。

### RocksBatch

保留 `Batch` 领域抽象，因为 standalone 模式使用 `RocksBatch`，cluster 模式使用 `BinlogBatch`。仅把 `RocksBatch` 的底层依赖改为具体借用：

```rust
pub struct RocksBatch<'a> {
    inner: WriteBatch,
    db: &'a rocksdb::DB,
    write_options: &'a WriteOptions,
    cf_handles: CfHandles<'a>,
    count: u32,
}
```

`commit()` 直接调用 `DB::write_opt()`。batch、iterator、snapshot 和 CF handle 的生命周期必须继续受具体 DB 引用约束，不使用 `unsafe`、泄漏引用或扩大生命周期规避编译器。

### RocksdbLogStore

`RocksdbLogStore` 使用具体共享 DB：

```rust
#[derive(Clone)]
pub struct RocksdbLogStore {
    db: Arc<rocksdb::DB>,
}
```

`new()` 接受 `Arc<DB>`，验证 `logs`、`meta` 和 `state` CF 存在；`open()` 创建 `Arc<DB>` 后调用 `new()`。LogStore clone 只增加 `Arc` 引用计数，不执行 shutdown 或 cancel。

## Callback 与 CompactionFilter 的非拥有 DB 访问

EventListener、flush trigger 和 CompactionFilterFactory 是 RocksDB 自身配置或后台执行路径的一部分，不能通过强 `Arc<DB>` 永久反向拥有 DB。

将当前：

```rust
Arc<OnceCell<Arc<DB>>>
```

改为：

```rust
Arc<OnceCell<Weak<DB>>>
```

DB 打开后只写入弱引用：

```rust
let _ = db_cell.set(Arc::downgrade(&db));
```

flush trigger 每次使用前升级：

```rust
let Some(db) = db_cell.get().and_then(Weak::upgrade) else {
    log::debug!("flush trigger ignored because RocksDB is closing");
    return;
};
```

`DataCompactionFilterFactory::create()` 同样只在创建 filter 时尝试升级。升级失败表示 DB 已进入释放过程，应构造一个 DB 不可用的 filter 状态并采用现有保守行为，不 panic、不 unwrap、不重新建立强引用环。

单个正在执行的 compaction filter 可以在其有限生命周期内持有升级得到的 `Arc<DB>`；filter 结束后必须释放该引用。factory 和 listener 不得永久持有强引用。

## Shutdown 与所有权责任

### 删除隐式 cancel

删除 `RocksdbEngine::Drop`，且不得把以下逻辑迁移到任何可 Clone 类型：

```rust
db.cancel_all_background_work(true);
```

普通共享引用释放不应改变其他 owner 可观察到的 DB 行为。

### Redis 与 Storage

删除仅服务于 wrapper Drop 的 `Redis.need_close`、`set_need_close()` 和自定义 `Redis::drop()` 清理逻辑。Rust 字段析构和 `Arc` 引用计数负责释放 DB owner。

`Storage::release_resources()` 的责任是：

1. 停止或中止 Storage 自身后台任务。
2. 清理 manager/handler 引用。
3. 清空 `insts`，释放 Storage 持有的 `Arc<Redis>` owner。

它不能在仍有外部 `Arc<Redis>`、iterator、snapshot、batch 或其他 DB owner 时宣称 RocksDB 已物理关闭。源码注释和日志应使用“释放 Storage owner”而不是无条件使用“数据库已关闭”。

最后一个 `Arc<DB>` 被释放时，由 rust-rocksdb 的 `DB::drop` 完成真实关闭和文件句柄释放。只有在一个未来的显式 shutdown 点能够证明没有其他 owner 时，才可考虑主动 `cancel_all_background_work()`；本任务不新增该调用。

### Snapshot install 与 ArcSwap

保留现有 snapshot pause/drain 和 `ArcSwap<Storage>` 切换协议，不重写状态机。

必须验证以下顺序：

1. pause 阻止新请求进入旧 Storage。
2. 已进入的请求完成或 drain。
3. `ArcSwap` 不再向新请求提供旧 Storage。
4. 旧 Storage、Redis、iterator、snapshot 和 DB owner 全部释放。
5. checkpoint 恢复或新 Storage 可以在同一路径重新打开 RocksDB。

测试应在 Windows 和 Linux CI 上依赖 RocksDB 的真实同路径 reopen 行为来发现残留 handle，不能只检查 `Arc::strong_count()`。

## Engine crate 删除

从根 `Cargo.toml` 删除：

- workspace member `src/engine`；
- workspace dependency `engine = { path = "src/engine" }`。

从 `storage` 和 `raft` 的 `Cargo.toml` 删除 `engine.workspace = true`。两者已经直接依赖 `rocksdb.workspace = true`，不新增替代 crate。

删除：

- `src/engine/Cargo.toml`
- `src/engine/src/lib.rs`
- `src/engine/src/engine.rs`
- `src/engine/src/rocksdb_engine.rs`

通过 Cargo 正常更新 `Cargo.lock`，移除本地 `engine` package 依赖边。

同步更新 `CLAUDE.md` 的 crate layout，不再描述 `engine/`，并明确 RocksDB 具体所有权位于 `storage` 与 `raft` 各自的数据路径。

## 错误处理

- RocksDB 原生错误继续通过现有 SNAFU/OpenRaft error mapping 转换，不在本任务中重写 error enum。
- `Weak::upgrade()` 失败是 shutdown/close 期间的正常状态，callback 应安全返回，不 panic。
- CF 缺失仍由 `RocksdbLogStore::new()` 返回现有 StorageError。
- `Storage::close()` 和 snapshot reopen 失败必须保留路径和 RocksDB error 上下文。
- 不新增外部输入可触发的 `unwrap()` 或 `expect()`；测试遵循项目 lint 规则。

## 文件边界

预计修改：

- `Cargo.toml`
- `Cargo.lock`
- `CLAUDE.md`
- `src/storage/Cargo.toml`
- `src/storage/src/redis.rs`
- `src/storage/src/batch.rs`
- `src/storage/src/data_compaction_filter.rs`
- `src/storage/src/storage.rs`
- `src/storage/src/redis_strings.rs`
- `src/storage` 下与具体 DB 类型编译关联的测试
- `src/raft/Cargo.toml`
- `src/raft/src/log_store_rocksdb.rs`
- `src/raft/tests/log_store_rocksdb_test.rs`
- 必要的 snapshot/reopen 生命周期测试

预计删除：

- `src/engine/Cargo.toml`
- `src/engine/src/lib.rs`
- `src/engine/src/engine.rs`
- `src/engine/src/rocksdb_engine.rs`

不得修改：

- RocksDB 数据格式和 CF 名称
- Redis 命令语义
- Raft wire protocol、state machine entry 或 snapshot archive 格式
- runtime queue、worker、batching/scaling/priority 配置

## 测试设计

### 生命周期回归

新增或强化测试以证明：

1. 创建具体 `Arc<DB>` 和多个 LogStore/DB owner。
2. 释放一个 clone 后，剩余 owner 仍可写入、读取、flush 和获取 CF。
3. 释放全部 owner 后，可以从同一路径真实 reopen。
4. reopen 后数据、vote、committed、last log 和 last purged 保持一致。
5. callback/factory 中的弱引用不会阻止 DB 最终释放。

### Storage close/reopen

- `Storage::close()` 后，在没有外部 owner 时可以 reopen 同一路径。
- 如果测试显式保留外部 `Arc<Redis>`，应证明 DB 仍可安全使用，并在释放该 owner 后再验证 reopen；测试不能假设 `close()` 强制销毁外部引用。
- checkpoint 创建与恢复行为保持不变。

### Snapshot hot-swap

- 复用并强化现有 snapshot roundtrip/install 测试。
- 验证旧 Storage owner 释放后，同一路径或恢复路径不存在 RocksDB lock 泄漏。
- 保持 `ArcSwap<Storage>` 读路径和 pause/drain 语义不变。

### Batch 行为

- standalone 模式仍创建并提交 `RocksBatch`。
- cluster 模式仍创建 `BinlogBatch` 并通过 Raft append callback。
- `on_binlog_write()` apply 路径仍绕过 append callback，直接提交 RocksDB batch 并记录 LogIndex。

### 现有行为回归

至少覆盖：

- storage crate 全部测试；
- raft crate 全部测试；
- RocksDB LogStore close/reopen；
- snapshot logindex 与 snapshot roundtrip；
- iterator、snapshot、checkpoint、dynamic options、CF 操作和 SST table properties；
- server check；
- workspace Clippy；
- 可运行条件下的 workspace 和 Python integration tests。

## 残留门禁

实现完成后以下扫描必须没有可执行命中：

```text
Engine
RocksdbEngine
Box<dyn Engine>
Arc<dyn Engine>
&dyn Engine
engine.workspace
path = "src/engine"
cancel_all_background_work
```

`src/engine` 目录必须不存在。设计文档和历史实施计划中的说明可以保留，但扫描命令必须排除这些文件自身。

## 验证命令

```bash
cargo fmt --all -- --check
cargo test --package storage
cargo test --package raft
cargo test --package raft --test log_store_rocksdb_test
cargo test --package raft --test snapshot_logindex_test
cargo test --package raft --test snapshot_roundtrip_test
cargo check --package server
cargo clippy --all-features --workspace -- -D warnings -D clippy::unwrap_used
cargo test --workspace
git diff --check
```

Python integration 需要启动 server 时，使用项目现有 `make standalone` 与 `make -C tests test-python` 流程，并记录未运行或环境失败的准确原因。

## 成功标准

- workspace 中不存在通用 `Engine` trait 或动态 Engine trait object。
- `engine` crate 和一对一 RocksDB 转发层已删除。
- `Redis`、`RocksBatch`、`RocksdbLogStore` 使用具体 RocksDB 类型。
- DB 内部 callback/factory 不通过强引用永久拥有 DB。
- 任意共享 clone 的释放不会提前取消其他 owner 的 RocksDB 后台工作。
- Storage close、snapshot install 和最终 DB 释放责任有准确注释和真实 reopen 测试。
- Batch、snapshot、checkpoint、LogIndex、iterator、dynamic options 和 CF 行为保持不变。
- 受影响测试和跨任务质量门禁通过，没有通过降低持久化或一致性校验换取绿灯。
