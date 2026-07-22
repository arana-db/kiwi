# Kiwi rust-rocksdb 维护基线升级实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）逐任务实现此计划。步骤使用复选框（`- [ ]`）语法跟踪进度。

**目标：** 将 Kiwi 固定到 `arana-db/rust-rocksdb@9e0de262bd378c84dcef4a226fed1217051fc5c5`，适配线程安全的 Collector Factory 接口，并证明现有 LogIndex SST 属性格式与恢复行为保持不变。

**架构：** 只替换根 workspace 的 Git revision 和 Factory receiver，不改变 LogIndex property key/value、Raft/Storage 数据路径或 RocksDB features。依赖升级后复用现有 Storage LogIndex 测试验证真实 bundled RocksDB 路径，并用独立的旧版本数据库探针验证 RocksDB 10.9.1 到 11.1.2 的 reopen/read 兼容性。

**技术栈：** Rust 1.91、Cargo workspace、arana-db/rust-rocksdb 0.51、RocksDB 11.1.2、WSL Ubuntu、Protocol Buffers、Clang。

---

### 任务 1：升级依赖并适配 Collector Factory 合同

**文件：**
- 修改：`Cargo.toml:44-53`
- 修改：`Cargo.lock`
- 修改：`src/storage/src/logindex/table_properties.rs:118-124`

- [x] **步骤 1：只更新 manifest revision，建立编译红灯**

将根依赖改为精确 revision：

```toml
rocksdb = { package = "rust-rocksdb", git = "https://github.com/arana-db/rust-rocksdb", rev = "9e0de262bd378c84dcef4a226fed1217051fc5c5", features = ["multi-threaded-cf"] }
```

- [x] **步骤 2：更新锁文件并验证预期接口失败**

运行：

```bash
cargo update -p rust-rocksdb
cargo check -p storage
```

预期：锁文件解析到 `9e0de262...`，`cargo check -p storage` 因 `TablePropertiesCollectorFactory::create` 需要 `&self`、当前实现仍为 `&mut self` 而失败。失败必须定位到 `src/storage/src/logindex/table_properties.rs`，不能接受网络、工具链或其他基线错误作为红灯。

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
rg -n 'git\+https://github.com/arana-db/rust-rocksdb\?rev=9e0de262bd378c84dcef4a226fed1217051fc5c5#9e0de262bd378c84dcef4a226fed1217051fc5c5' Cargo.lock
git diff --check
git add Cargo.toml Cargo.lock src/storage/src/logindex/table_properties.rs
git commit -m "upgrade(storage): update rust-rocksdb maintenance baseline"
```

预期：`rust-rocksdb` 和 `rust-librocksdb-sys` 两个 lock entry 都固定到新 SHA。

### 任务 2：更新 fork 维护文档

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

### 任务 3：补齐损坏 TableProperties 的 fail-closed 测试

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

### 任务 4：验证旧数据库和恢复路径兼容性

**文件：**
- 不修改生产文件
- 临时探针：`D:/test/github/review/kiwi-rust-rocksdb-compat-*`

- [x] **步骤 1：使用 Base `8ad50f6a...` 和旧 pin 创建持久化数据库**

使用独立临时 worktree/target，在旧依赖下运行真实 Storage/LogIndex 写入路径；阶段间只保留数据库目录，释放所有 DB、CF、listener 和 collector handle。

- [x] **步骤 2：使用升级后的分支重新打开同一数据库**

调用产品的正式 open/recovery 路径，确认 RocksDB 11.1.2 能读取旧 RocksDB 10.9.1 生成的 SST 和 `LargestLogIndex/LargestSequenceNumber` 属性。

- [x] **步骤 3：复核异常属性输入门禁**

运行任务 3 的 Storage LogIndex 测试，确认 property 缺失、额外分段、非法数字和异常字节不会产生错误恢复结果。

- [x] **步骤 4：记录环境或兼容性结果**

如果旧数据库无法由新版本打开，保存准确 RocksDB 错误、DB 路径构造方式和工具链版本，停止提交完成声明；不得通过删除数据库或跳过恢复测试获得绿色结果。

### 任务 5：分层质量门禁

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

### 任务 6：最终审查与交付边界

- [x] **步骤 1：规格审查**

逐项核对 revision、receiver、lock entries、property 字节格式、文档边界和验证命令，不允许扩大到 CI 缓存、预编译 RocksDB或 Raft 持久化重构。

- [x] **步骤 2：代码质量审查**

检查依赖解析、线程安全、错误处理、测试真实性、文档准确性及是否混入无关 Diff。

- [x] **步骤 3：最终新鲜验证**

重新运行本计划中能够证明完成状态的全部门禁，记录退出码、测试数量和未执行项。

- [x] **步骤 4：停止在本地提交状态**

本轮授权包括本地实现和提交，但不自动包含 push、创建 PR、merge 或关闭 Issue。完成后汇报分支、提交、Diff 和验证证据，等待用户对远端写操作单独授权。
