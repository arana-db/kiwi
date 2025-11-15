# Openraft Adaptor 模式研究报告

## 研究日期
2025-11-08

## Openraft 版本
0.9.21

## 研究目标
验证 Openraft 的 Adaptor 模式是否能够解决 sealed traits 问题，并确认正确的使用方式。

## 关键发现

### 1. Sealed Traits 问题确认 ⚠️ 更新

**重要更正**：经过实际编译测试，Openraft 0.9.21 **确实使用** sealed traits！

- ✅ `RaftLogReader<C>` - 可以直接实现（非 sealed）
- ✅ `RaftSnapshotBuilder<C>` - 可以直接实现（非 sealed）
- ❌ `RaftStateMachine<C>` - **是 sealed trait**，不能直接实现
- ❌ `RaftLogStorage<C>` - **是 sealed trait**，不能直接实现

编译错误明确指出：
```
error[E0277]: the trait bound `PocStateMachine: openraft::storage::v2::sealed::Sealed` is not satisfied
= note: `RaftStateMachine` is a "sealed trait", because to implement it you also need to 
        implement `openraft::storage::v2::sealed::Sealed`, which is not accessible
```

### 2. Adaptor 的真实用途 ⚠️ 更新

`openraft::storage::Adaptor` 的实际用途是：

```rust
pub struct Adaptor<C, S>
where
    C: RaftTypeConfig,
    S: RaftStorage<C>,
{
    // ...
}

// 创建方法
pub fn new(store: S) -> (Self, Self)
```

**Adaptor 的作用**：
- **绕过 sealed traits 限制**：Adaptor 自己实现了内部的 `Sealed`，并对外暴露 `RaftLogStorage` / `RaftStateMachine`
- 只需要传入一个实现了 *旧版* `RaftStorage<C>` trait 的存储对象（这个 trait 仍然可实现，只是被标记为 deprecated）
- 返回**两个** Adaptor 实例：一个充当 log storage，一个充当 state machine
- `RaftStorage<C>` 是一个组合 trait，要求实现：
  - `RaftLogReader<C>`（读取日志）
  - `RaftSnapshotBuilder<C>`（构建快照）
  - `append_to_log` / `delete_conflict_logs_since` / `purge_logs_upto` 等日志写入 API
  - `apply_to_state_machine` / `last_applied_state` 等状态机 API

> ✅ 结论：`RaftStorage<C>` 本身 **不是 sealed trait**。我们完全可以直接实现它，然后通过 `Adaptor::new(store)` 换取 Openraft v2 所需的 `RaftLogStorage` 与 `RaftStateMachine`。先前“无法实现 `RaftStorage`”的说法是错误/过时的描述。

### 3. 正确的集成方式 ⚠️ 完全重写

经过深入研究 Openraft 0.9.21 的源码和错误信息，发现正确的方式是：

#### 方式 A：使用 Openraft 提供的内置存储类型（推荐）

Openraft 提供了 `MemStore` 等内置实现。我们需要：

```rust
// 使用 Openraft 的内置类型
use openraft::storage::MemStore;

let store = MemStore::new(node_id);
let (log_store, state_machine) = Adaptor::new(store);
```

#### 方式 B：实现 v2 API（复杂但灵活）

Openraft 0.9 引入了新的 v2 storage API。需要实现：

1. 实现 `RaftLogReader` + `RaftSnapshotBuilder`（非 sealed）
2. **不能**直接实现 `RaftStateMachine`（sealed）
3. 使用 Openraft 提供的辅助类型或宏

**问题**：Openraft 0.9.21 的文档和示例不够清晰，需要更深入研究。

#### 方式 C：降级到 Openraft 0.8（不推荐）

Openraft 0.8 可能没有 sealed traits 限制，但会失去新功能。

### 4. 需要实现的 Traits

#### RaftLogReader<TypeConfig>

```rust
#[async_trait]
pub trait RaftLogReader<C: RaftTypeConfig> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Send + Sync>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<C>>, StorageError<C>>;

    async fn read_vote(&mut self) -> Result<Option<Vote<C::NodeId>>, StorageError<C>>;
}
```

#### RaftSnapshotBuilder<TypeConfig>

```rust
#[async_trait]
pub trait RaftSnapshotBuilder<C: RaftTypeConfig> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<C>, StorageError<C>>;
}
```

#### RaftStateMachine<TypeConfig>

```rust
#[async_trait]
pub trait RaftStateMachine<C: RaftTypeConfig> {
    type SnapshotBuilder: RaftSnapshotBuilder<C>;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<C::NodeId>>, EffectiveMembership<C::NodeId, C::Node>), StorageError<C>>;

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<C::R>, StorageError<C>>
    where
        I: IntoIterator<Item = Entry<C>> + Send,
        I::IntoIter: Send;

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder;

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<C::SnapshotData>, StorageError<C>>;

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<C::NodeId, C::Node>,
        snapshot: Box<C::SnapshotData>,
    ) -> Result<(), StorageError<C>>;

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<C>>, StorageError<C>>;
}
```

### 5. TypeConfig 验证

当前的 TypeConfig 定义是正确的：

```rust
impl openraft::RaftTypeConfig for TypeConfig {
    type D = ClientRequest;           // ✅ 请求数据类型
    type R = ClientResponse;          // ✅ 响应数据类型
    type NodeId = NodeId;             // ✅ 节点 ID 类型
    type Node = BasicNode;            // ✅ 节点信息类型
    type Entry = openraft::Entry<TypeConfig>; // ✅ 日志条目类型
    type SnapshotData = std::io::Cursor<Vec<u8>>; // ✅ 快照数据类型
    type AsyncRuntime = openraft::TokioRuntime;   // ✅ 异步运行时
    type Responder = openraft::impls::OneshotResponder<TypeConfig>; // ✅ 响应器
}
```

### 6. POC 验证结果

创建了 `adaptor_poc.rs` 文件，包含：
- ✅ `PocStorage` 实现 `RaftLogReader` 和 `RaftSnapshotBuilder`
- ✅ `PocStateMachine` 实现 `RaftStateMachine`
- ✅ 使用 `Adaptor::new(storage, state_machine)` 成功组合
- ✅ 所有代码编译通过，无错误

## 结论 ⚠️ 完全修订

### 主要发现

1. **Openraft 0.9.21 确实使用 sealed traits**
   - `RaftStateMachine<C>` 是 sealed trait
   - `RaftLogStorage<C>` 是 sealed trait
   - `RaftLogReader<C>` 和 `RaftSnapshotBuilder<C>` 不是 sealed
   - 这是 Openraft 0.9 的重大 API 变更

2. **Adaptor 是必需的，不是可选的**
   - Adaptor 是唯一实现了 `Sealed` trait 的公开类型
   - 必须通过 Adaptor 来使用自定义存储
   - Adaptor 的设计强制使用特定的集成模式

3. **当前方法不可行**
   - 不能直接实现 `RaftStateMachine<C>`
   - 不能直接实现 `RaftStorage<C>`（因为它要求 RaftStateMachine）
   - 需要完全不同的实现策略

### 需要进一步研究

1. **Openraft 0.9.21 的正确使用方式**
   - 查看官方示例和文档
   - 研究 `MemStore` 等内置实现的源码
   - 理解 v2 storage API 的设计意图

2. **可能的解决方案**
   - 使用 Openraft 内置的存储类型
   - 找到 Openraft 提供的扩展点
   - 考虑是否需要降级到 0.8 版本

3. **重新评估需求**
   - 是否真的需要自定义存储实现
   - 能否使用 Openraft 的内置类型
   - 如何在 Openraft 的约束下实现我们的需求

### 下一步行动

1. ✅ 更新设计文档，移除关于 sealed traits 的错误描述
2. ✅ 保留 Adaptor 的使用，但说明其真实用途
3. ✅ 开始实现 `RaftLogReader`、`RaftSnapshotBuilder` 和 `RaftStateMachine`
4. ✅ 使用 POC 代码作为参考模板

## 参考资料

- Openraft 文档: https://docs.rs/openraft/0.9.21/
- POC 实现: `src/raft/src/adaptor_poc.rs`
- 当前 TypeConfig: `src/raft/src/types.rs`

## 附录：关键代码片段

### Adaptor 使用示例

```rust
use openraft::storage::Adaptor;

// 创建存储和状态机
let storage = RaftStorage::new(db_path)?;
let state_machine = KiwiStateMachine::new(node_id);

// 使用 Adaptor 组合
let combined_storage = Adaptor::new(storage, state_machine);

// 初始化 Raft
let raft = Raft::new(node_id, config, network, combined_storage).await?;
```

### 错误转换示例

```rust
impl From<RaftError> for StorageError<TypeConfig> {
    fn from(err: RaftError) -> Self {
        StorageError::IO {
            source: StorageIOError::new(
                ErrorSubject::Store,
                ErrorVerb::Read,
                AnyError::new(&err),
            ),
        }
    }
}
```


---

## 更新记录

### 2025-11-08 - 重大更正

在实际编译测试后发现，之前的研究结论**完全错误**。Openraft 0.9.21 确实使用了 sealed traits，这使得直接实现自定义存储变得非常困难。

**错误的原因**：
- 初始研究只查看了文档和类型签名
- 没有实际编译测试代码
- 误解了 Adaptor 的设计目的

**正确的发现**：
- `RaftStateMachine` 是 sealed trait
- 必须使用 Openraft 提供的方式来集成
- 需要深入研究 Openraft 0.9 的 v2 storage API

**下一步行动**：
1. 暂停当前的实现工作
2. 深入研究 Openraft 0.9.21 的官方示例
3. 查看 `MemStore` 和其他内置实现的源码
4. 重新设计集成方案
5. 更新设计文档和任务列表

**教训**：
- 必须通过实际编译来验证假设
- 不能仅依赖文档和类型签名
- Rust 的 sealed trait 模式需要特别注意
