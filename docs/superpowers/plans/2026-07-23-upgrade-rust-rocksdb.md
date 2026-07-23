# Kiwi rust-rocksdb 维护基线升级实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）逐任务实现此计划。步骤使用复选框（`- [ ]`）语法跟踪进度。

**目标：** 将 Kiwi 固定到 `arana-db/rust-rocksdb@dd1ac21a1c7176e5d71e145a0e4b941ec84ccf68`，适配线程安全的 Collector Factory 接口，并证明现有 LogIndex SST 属性格式与恢复行为保持不变。

**架构：** 只替换根 workspace 的 Git revision 和 Factory receiver，不改变 LogIndex property key/value、Raft/Storage 数据路径或 RocksDB features。目标 revision 包含已合并的 EventListener callback safety 工作；Kiwi 只更新依赖 pin 和相关验证，不在 README/Cargo 注释中展开该上游实现。依赖升级后复用现有 Storage LogIndex 测试验证真实 bundled RocksDB 路径，并用独立的旧版本数据库探针验证 RocksDB 10.9.1 到 11.1.2 的 reopen/read 兼容性。

**技术栈：** `rust-rocksdb` 0.51 的 MSRV 为 Rust 1.91；Kiwi 实际按 `rust-toolchain.toml` 固定的 `nightly-2025-08-20` 构建和验证。其余组件为 Cargo workspace、RocksDB 11.1.2、WSL Ubuntu、Protocol Buffers 和 Clang。

---

## 任务 1：升级依赖并适配 Collector Factory 合同

**文件：**
- 修改：`Cargo.toml:44-53`
- 修改：`Cargo.lock`
- 修改：`src/storage/src/logindex/table_properties.rs:118-124`

- [x] **步骤 1：只更新 manifest revision，建立编译红灯**

将根依赖改为精确 revision：

```toml
rocksdb = { package = "rust-rocksdb", git = "https://github.com/arana-db/rust-rocksdb", rev = "dd1ac21a1c7176e5d71e145a0e4b941ec84ccf68", features = ["multi-threaded-cf"] }
```

- [x] **步骤 2：更新锁文件并验证预期接口失败**

运行：

```bash
cargo update -p rust-rocksdb
cargo check -p storage
```

预期：锁文件解析到 `dd1ac21...`，`cargo check -p storage` 因 `TablePropertiesCollectorFactory::create` 需要 `&self`、当前实现仍为 `&mut self` 而失败。失败必须定位到 `src/storage/src/logindex/table_properties.rs`，不能接受网络、工具链或其他基线错误作为红灯。

- [x] **步骤 3：写入最小接口适配**

```rust
fn create(&self, _context: TablePropertiesCollectorContext) -> Self::Collector {
    LogIndexTablePropertiesCollector::new(self.collector.clone())
}
```

不得添加锁、`unsafe impl`、wrapper 或改变 Collector 内部状态。

- [x] **步骤 4：验证定向绿灯和协议不变**

运行：

```bash
cargo check -p storage
cargo test -p storage logindex::table_properties -- --nocapture
rg -n 'LargestLogIndex/LargestSequenceNumber|format!\("\{\}/\{\}"' src/storage/src/logindex/table_properties.rs
```

预期：编译和定向测试通过；property key 仍为 `LargestLogIndex/LargestSequenceNumber`，value 仍由 `<log_index>/<sequence_number>` 两段组成。

- [x] **步骤 5：核对锁文件并提交**

运行：

```bash
rg -n 'git\+https://github.com/arana-db/rust-rocksdb\?rev=dd1ac21a1c7176e5d71e145a0e4b941ec84ccf68#dd1ac21a1c7176e5d71e145a0e4b941ec84ccf68' Cargo.lock
git diff --check
git add Cargo.toml Cargo.lock src/storage/src/logindex/table_properties.rs
git commit -m "upgrade(storage): update rust-rocksdb maintenance baseline"
```

预期：`rust-rocksdb` 和 `rust-librocksdb-sys` 两个 lock entry 都固定到新 SHA。

## 任务 2：更新 fork 维护文档

**文件：**
- 修改：`Cargo.toml:44-52`
- 修改：`README.md:112-118`

- [x] **步骤 1：验证现有文档红灯**

运行：

```bash
rg -n 'addtableproperties|working to merge|switch back|lands in upstream' Cargo.toml README.md
```

预期：命中旧 `addtableproperties` 分支和“未来切回官方 crate”的过期承诺。

- [x] **步骤 2：写入最小维护说明**

Cargo 注释和 README 必须说明：

- Kiwi 使用 Arana 自主维护的 `arana-db/rust-rocksdb`。
- 该 fork 提供 Kiwi 需要的 TableProperties Collector/Factory FFI。
- revision 固定用于可审计、可重复的 native dependency 构建。
- 不承诺向 `zaidoon1/rust-rocksdb` 或 GitHub 标注的 parent 回馈代码，也不再链接旧 `addtableproperties` 分支。

- [x] **步骤 3：验证文档绿灯并提交**

运行：

```bash
rg -n 'addtableproperties|working to merge|switch back|lands in upstream' Cargo.toml README.md
rg -n 'arana-db/rust-rocksdb|TableProperties' Cargo.toml README.md
git diff --check
git add Cargo.toml README.md
git commit -m "docs(storage): describe the maintained RocksDB fork"
```

预期：第一条命令无命中，第二条命中新的维护说明。

## 任务 3：补齐损坏 TableProperties 的 fail-closed 测试

**文件：**
- 修改测试：`src/storage/src/logindex/table_properties.rs:220-278`

- [x] **步骤 1：为未覆盖输入编写失败测试**

在现有 `tests` 模块增加独立断言，覆盖：

- 非数字 log index：`not-a-log/5`
- 非数字 sequence number：`233333/not-a-sequence`
- 缺少 `/`：`233333`
- 空 log index：`/5`
- 空 sequence number：`233333/`
- 非 UTF-8：`[0xff, b'/', b'5']`

每种输入都必须得到 `None`，不得 panic 或产生部分恢复值。

- [x] **步骤 2：执行测试并用临时变异证明断言有效**

运行：

```bash
cargo test -p storage logindex::table_properties::tests::test_read_stats_rejects_invalid_components -- --exact --nocapture
```

预期：当前实现可能已经对这些输入返回 `None`。若新增测试直接通过，临时把数字解析失败改为默认值，确认测试会失败，然后立即恢复该变异；记录这是补齐回归覆盖而非生产 bug 修复。

- [x] **步骤 3：只在红灯证明需要时做最小修复**

只有在真实实现错误接受非 UTF-8 时，才将解析入口改为严格 UTF-8：

```rust
let s = std::str::from_utf8(value).ok()?;
```

不得改变 property key、数字类型、分隔符或合法值格式。

- [x] **步骤 4：验证绿灯并提交**

运行：

```bash
cargo test -p storage logindex::table_properties -- --nocapture
cargo fmt --all -- --check
git diff --check
git add src/storage/src/logindex/table_properties.rs
git commit -m "test(storage): reject malformed log index properties"
```

## 任务 4：验证旧数据库和恢复路径兼容性

**文件：**
- 不修改生产文件
- 临时探针：`D:/test/github/review/kiwi-rust-rocksdb-compat-*`
- 修改测试：`src/raft/tests/logindex_integration.rs`

- [x] **步骤 1：使用 Base `8ad50f6afc5a8885a769b6baa776468090f0bee9` 和旧 pin 创建持久化数据库**

旧 writer 必须在独立 worktree/target 中固定 `rust-rocksdb@f7abb18c64fac810f3c4736aef833c340396449b`（`rust-librocksdb-sys 0.41.0+10.9.1`），以固定数据库目录作为唯一跨进程输入。writer 通过真实 Storage/LogIndex open 路径写入并 flush 含 `LargestLogIndex/LargestSequenceNumber` 的 SST，打印实际 `<log_index>/<seqno>` 后退出。

- [x] **步骤 2：使用升级后的分支重新打开同一数据库**

新 reader 必须在另一个进程中固定 `rust-rocksdb@dd1ac21a1c7176e5d71e145a0e4b941ec84ccf68`（`rust-librocksdb-sys 0.47.1+11.1.2`），只接收 writer 留下的同一数据库路径。reader 调用产品的正式 open/recovery 路径，确认 RocksDB 11.1.2 能读取旧 RocksDB 10.9.1 生成的 SST 和 `LargestLogIndex/LargestSequenceNumber` 属性。

两个阶段之间不得保留进程内对象：writer 在退出前必须释放 TableProperties collection、所有 CF handle、DB、Factory、EventListener 和 Collector 的实际 owner；reader 只能在 writer 进程结束后从同一路径重新 open，不能共享 `Arc<DB>` 或其他 handle。探针的预期输出至少包含：

```text
WRITER_OK rust-rocksdb=f7abb18c64fac810f3c4736aef833c340396449b rocksdb=10.9.1 property=233333/<seqno>
READER_OK rust-rocksdb=dd1ac21a1c7176e5d71e145a0e4b941ec84ccf68 rocksdb=11.1.2 reopened=true property=233333/<same-seqno> applied=233333 flushed=233333
```

探针只保留数据库目录，不提交 SST fixture；失败时保留命令、revision、输出和 RocksDB 错误即可。

- [x] **步骤 3：复核异常属性输入门禁**

运行任务 3 的 Storage LogIndex 测试，并运行 `cargo test -p raft --test logindex_integration test_full_flow_multi_cf_properties -- --nocapture`。后者必须对每个真实 CF 执行 `put_cf`/`flush_cf`，记录各自实际 sequence/property pair，释放旧 DB 生命周期后同路径 reopen，再通过正式 `LogIndexOfColumnFamilies::init`/`DbAccess` 逐 CF 恢复；不得用 default CF 代替命名 CF，也不得假定所有 CF sequence 相同。

- [x] **步骤 4：记录环境或兼容性结果**

如果旧数据库无法由新版本打开，保存准确 RocksDB 错误、DB 路径构造方式和工具链版本，停止提交完成声明；不得通过删除数据库或跳过恢复测试获得绿色结果。

## 任务 5：分层质量门禁

**文件：**
- 不新增生产文件

- [x] **步骤 1：格式与 Diff**

```bash
cargo fmt --all -- --check
git diff --check
```

- [x] **步骤 2：定向测试**

```bash
cargo test -p storage logindex::table_properties -- --nocapture
cargo test -p storage logindex::cf_tracker -- --nocapture
cargo test -p storage logindex::event_listener -- --nocapture
```

- [x] **步骤 3：模块测试**

```bash
cargo test -p storage
cargo test -p raft
```

- [x] **步骤 4：Lint 与 workspace**

```bash
cargo clippy --all-features --workspace -- -D warnings -D clippy::unwrap_used
cargo test --workspace
```

- [x] **步骤 5：WSL/Linux 门禁**

在 WSL 中使用独立 ext4 `CARGO_TARGET_DIR`，确认 `protoc`、`clang`、`pkg-config` 和 CMake 可用后，至少执行：

```bash
cargo check -p storage
cargo test -p storage logindex::table_properties -- --nocapture
cargo test -p raft
```

不得把 Windows 结果替代 Linux native/FFI 验证。

## 任务 6：最终审查与交付边界

- [x] **步骤 1：规格审查**

逐项核对 revision、receiver、lock entries、property 字节格式、文档边界和验证命令，不允许扩大到 CI 缓存、预编译 RocksDB或 Raft 持久化重构。

- [x] **步骤 2：代码质量审查**

检查依赖解析、线程安全、错误处理、测试真实性、文档准确性及是否混入无关 Diff。

- [x] **步骤 3：最终新鲜验证**

重新运行本计划中能够证明完成状态的全部门禁，记录退出码、测试数量和未执行项。

- [x] **步骤 4：停止在本地提交状态**

本轮授权包括本地实现和提交，但不自动包含 push、创建 PR、merge 或关闭 Issue。完成后汇报分支、提交、Diff 和验证证据，等待用户对远端写操作单独授权。
