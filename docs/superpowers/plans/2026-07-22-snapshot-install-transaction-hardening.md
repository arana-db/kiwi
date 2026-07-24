# Snapshot Install Transaction Hardening Plan

> 按 `executing-plans`、`test-driven-development`、
> `subagent-driven-development` 和 `verification-before-completion` 执行；每个任务独立
> 提交，最终重新做规格与代码质量审查。

**目标：** 让 snapshot install 在 checkpoint staging、全部 Storage owner drain、目录
切换、current snapshot 持久化和跨重启恢复之间形成可验证的 fail-closed 事务；任何消费
`ArcSwap<Storage>` 的并发读或 snapshot builder 都不能取得旧 owner 或 placeholder。

**背景：** commit `4e9b527` 已验证真实 open target 的成功替换和进程内 fail-closed，
但独立审查发现三个缺口：restore 把可恢复的 staging 错误混入破坏区；Raft gRPC read 与
异步 snapshot builder 未参与 drain；post-restore 错误没有持久化 marker，重启可能以新
DB 配旧 applied boundary。

## 任务 1：checkpoint prepare + commit

**文件：** `src/storage/src/checkpoint.rs`、`src/storage/src/lib.rs`、
`src/storage/tests/checkpoint_test.rs`

新增：

```rust
pub fn prepare_checkpoint_restore(
    checkpoint_root: &Path,
    target_db_path: &Path,
    db_instance_num: usize,
) -> io::Result<PreparedCheckpointRestore>;

impl PreparedCheckpointRestore {
    pub fn commit(self) -> io::Result<()>;
}
```

prepare 只校验和复制到同文件系统 sibling temp；不删除、不 rename target。commit 才执行
target 切换。保留 `restore_checkpoint_layout()` 作为兼容 helper，内部调用 prepare 后
commit。prepare 对象未 commit 即 drop 时清理 temp。

红绿测试：缺实例目录时 target 不变；prepare 成功后 target 仍不变；commit 后才替换；
drop prepared 对象会清理 temp。

提交：`refactor(storage): stage snapshot restore before commit`

## 任务 2：覆盖并发 Storage owner 与 snapshot operation

**文件：** runtime storage gate、server pause wrapper、Raft state machine/node/gRPC client
及相关测试。

`StorageAccessGate::enter()` 以字段私有的 public RAII permit 暴露。扩展 Raft
`PauseController`，增加对象安全 `enter()` 并返回只依赖 Drop 的
`Box<dyn StorageAccessPermit>`。生产 wrapper 擦除 runtime permit。

gRPC read 顺序固定为：leader 快速检查；不持 permit 调 `ensure_linearizable()`；
`enter().await`；再次确认 leader；permit 生命周期内 load Storage 并完成 blocking read。
不得持 permit 等待 linearizable applied barrier，否则会与 install drain 形成环。

`KiwiStateMachine` 增加 snapshot operation mutex：

- `apply()` 持 gate；
- `get_snapshot_builder()` 取得 owned guard 后再捕获 applied state 和固定
  `Arc<Storage>`，builder 持 guard 到 checkpoint/persist/purge 完成；
- `install_snapshot()` 在 request_pause/drain 后取得 gate；
- `get_current_snapshot()` 和 `applied_state()` 读取 current snapshot时持 gate。

install 内部 raw load 不再 enter access permit；它已 paused，重复 enter 会自锁。

确定性测试覆盖：paused load 不观察 placeholder；活跃 direct-read 阻塞 install；builder
阻塞 install；等待 linearizability 时 active 计数仍为零；destructive failure 后新读持续
pending 且取消不泄漏 active。

提交：`fix(raft): gate every hot-swappable storage owner`

## 任务 3：持久化 install marker 与启动拒绝

marker 位于主 DB 的稳定 sibling：

```text
<db-parent>/.<db-name>.snapshot-install-in-progress.json
```

marker 至少记录 version、snapshot id、index、term、DB path、snapshot work dir 和实例数。
marker 存在、损坏或版本未知都 fail closed。

顺序：unpack/validate/prepare；request_pause/drain；取得 operation gate；durable 写 marker；
swap placeholder/drop old；commit prepared restore；open/swap new Storage；恢复 tracker、
collector 和 applied state；durable persist current snapshot；durable 删除 marker；最后 resume。

marker 写失败时旧 Storage 尚在，允许 resume。marker 写入后任一步失败时保留 marker、
保持 paused，并返回带路径和重新入群建议的错误。

server 必须在 Config 加载后、创建 runtime/打开 Storage 前检查 marker；无论当前 Raft
配置是否关闭都不得绕过。`create_raft_node()` 在创建 log store 前重复检查。

测试覆盖：malformed marker 拒绝且 log-store 尚未创建；early error 不留 marker；
post-restore persist 失败留 marker并阻止模拟重启；成功 install 清 marker，同路径 reopen
后 DB、`applied_state()`、`get_current_snapshot()` 全部匹配；旧 builder 不能覆盖安装后的
current snapshot。

提交：`fix(raft): persist snapshot install recovery state`

## 任务 4：全量验证与重新审查

运行：

```bash
cargo fmt --all -- --check
cargo test --package runtime
cargo test --package storage
cargo test --package raft
cargo check --workspace
cargo clippy --package runtime --package storage --package raft --lib --all-features -- -D warnings -D clippy::unwrap_used
cargo test --workspace
git diff --check
```

最终门禁：`src/engine` 不存在；Engine 残留零命中；
`cancel_all_background_work` 只在 `RocksDbOwner::drop()`；所有 raw
`storage_swap.load*()` 均位于 state-machine 串行/paused 内部；marker 存在时 server 和
Raft library 都拒绝启动；prepare 失败不 detach live Storage，marker 写入后的失败不
resume。
