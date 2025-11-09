# Openraft Sealed Traits 集成修复设计文档

## 概述

本文档描述了如何解决 Kiwi Raft 模块与 Openraft 0.9.21 sealed traits 集成问题的技术设计方案。

### 问题背景

Openraft 0.9.21 使用 sealed traits 来限制核心接口的外部实现：
- `RaftLogStorage` - 日志存储接口  
- `RaftStateMachine` - 状态机接口

这些 sealed traits 防止外部 crate 直接实现，导致 Kiwi 无法直接实现这些接口。

### 解决方案

采用 Openraft 提供的 `Adaptor` 类型来桥接我们的实现与 Openraft 的 sealed traits。

## 架构设计

### 整体架构

```
Openraft Core (Raft, sealed traits)
         ↓
Openraft Adaptor Layer
         ↓  
Kiwi Storage Traits (RaftLogReader, RaftSnapshotBuilder, RaftStateMachine)
         ↓
Kiwi 业务实现层 (RaftStorage, KiwiStateMachine)
```

### 核心组件

#### 1. TypeConfig 定义

```rust
pub struct TypeConfig;

impl openraft::RaftTypeConfig for TypeConfig {
    type NodeId = NodeId;
    type Node = openraft::BasicNode;
    type Entry = openraft::Entry<TypeConfig>;
    type SnapshotData = Cursor<Vec<u8>>;
    type AsyncRuntime = openraft::TokioRuntime;
}
```

#### 2. RaftStorage 实现

`RaftStorage` 需要实现 Openraft 的非 sealed traits：
- `RaftLogReader` - 读取日志
- `RaftSnapshotBuilder` - 构建快照

```rust
pub struct RaftStorage {
    db: Arc<DB>,
    state: Arc<RwLock<RaftState>>,
}

impl RaftLogReader<TypeConfig> for RaftStorage { ... }
impl RaftSnapshotBuilder<TypeConfig> for RaftStorage { ... }
```

#### 3. KiwiStateMachine 实现

```rust
pub struct KiwiStateMachine {
    applied_index: AtomicU64,
    storage_engine: Option<Arc<dyn StorageEngine>>,
    node_id: NodeId,
}

impl RaftStateMachine<TypeConfig> for KiwiStateMachine {
    async fn applied_state(&mut self) -> Result<...> { ... }
    async fn apply(&mut self, entries: Vec<Entry>) -> Result<...> { ... }
    async fn get_snapshot_builder(&mut self) -> ... { ... }
    async fn begin_receiving_snapshot(&mut self) -> ... { ... }
    async fn install_snapshot(&mut self, meta, snapshot) -> ... { ... }
    async fn get_current_snapshot(&mut self) -> ... { ... }
}
```

#### 4. Adaptor 集成

```rust
pub type RaftStore = openraft::storage::Adaptor<TypeConfig, Arc<RaftStorage>>;
```

## 组件和接口

### 存储层接口

#### RaftLogReader

```rust
#[async_trait]
impl RaftLogReader<TypeConfig> for RaftStorage {
    async fn try_get_log_entries<RB>(&mut self, range: RB) 
        -> Result<Vec<Entry<TypeConfig>>, StorageError<TypeConfig>>;
    
    async fn read_vote(&mut self) 
        -> Result<Option<Vote<NodeId>>, StorageError<TypeConfig>>;
}
```

#### RaftSnapshotBuilder

```rust
#[async_trait]
impl RaftSnapshotBuilder<TypeConfig> for RaftStorage {
    async fn build_snapshot(&mut self) 
        -> Result<Snapshot<TypeConfig>, StorageError<TypeConfig>>;
}
```

### 状态机接口

```rust
#[async_trait]
impl RaftStateMachine<TypeConfig> for KiwiStateMachine {
    type SnapshotBuilder = Self;
    
    async fn applied_state(&mut self) 
        -> Result<(Option<LogId<NodeId>>, StoredMembership), StorageError>;
    
    async fn apply<I>(&mut self, entries: I) 
        -> Result<Vec<Response>, StorageError>
    where I: IntoIterator<Item = Entry<TypeConfig>> + Send;
    
    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder;
    
    async fn begin_receiving_snapshot(&mut self) 
        -> Result<Box<SnapshotData>, StorageError>;
    
    async fn install_snapshot(&mut self, meta, snapshot) 
        -> Result<(), StorageError>;
    
    async fn get_current_snapshot(&mut self) 
        -> Result<Option<Snapshot<TypeConfig>>, StorageError>;
}
```

## 数据模型

### 日志条目

```rust
pub struct StoredLogEntry {
    pub index: LogIndex,
    pub term: Term,
    pub payload: Vec<u8>,  // 序列化的 ClientRequest
}
```

### 快照元数据

```rust
pub struct StoredSnapshotMeta {
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
    pub snapshot_id: String,
    pub timestamp: i64,
}
```

### 状态机快照

```rust
pub struct StateMachineSnapshot {
    pub data: HashMap<Vec<u8>, Vec<u8>>,
    pub applied_index: u64,
}
```

## 类型转换

### Openraft Entry → ClientRequest

```rust
fn entry_to_client_request(entry: &Entry<TypeConfig>) 
    -> Result<ClientRequest, RaftError> 
{
    match &entry.payload {
        EntryPayload::Normal(data) => {
            bincode::deserialize(data.as_ref())
                .map_err(|e| RaftError::serialization(e.to_string()))
        }
        _ => Err(RaftError::invalid_request("Unsupported entry type")),
    }
}
```

### ClientResponse → Openraft Response

```rust
fn client_response_to_response(resp: ClientResponse) -> Vec<u8> {
    bincode::serialize(&resp).unwrap_or_default()
}
```

### RaftError → StorageError

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

## 错误处理

### 错误类型映射

| Kiwi Error | Openraft StorageError |
|------------|----------------------|
| StorageError::RocksDb | StorageError::IO |
| StorageError::DataInconsistency | StorageError::IO |
| StorageError::SnapshotCreationFailed | StorageError::IO |
| RaftError::InvalidRequest | StorageError::IO |

### 错误处理策略

1. **存储错误**：所有 RocksDB 错误转换为 `StorageError::IO`
2. **序列化错误**：转换为 `StorageError::IO` 并保留原始错误信息
3. **状态机错误**：在 apply 响应中返回错误，不中断 Raft 流程
4. **快照错误**：记录日志并返回错误，触发重试机制

## 异步处理

### 异步操作模式

所有 Openraft trait 方法都是异步的，我们的实现遵循以下模式：

```rust
async fn operation(&mut self) -> Result<T, StorageError<TypeConfig>> {
    // 1. 获取必要的锁（使用 tokio::sync 的异步锁）
    let state = self.state.read().await;
    
    // 2. 执行 I/O 操作（RocksDB 操作在 tokio::task::spawn_blocking 中）
    let result = tokio::task::spawn_blocking(move || {
        // RocksDB 同步操作
    }).await?;
    
    // 3. 返回结果
    Ok(result)
}
```

### 并发控制

- **读操作**：使用 `RwLock::read()` 允许并发读取
- **写操作**：使用 `RwLock::write()` 保证独占访问
- **日志应用**：使用 `Mutex` 保证顺序应用

## 性能优化

### 1. 批量操作

```rust
async fn apply<I>(&mut self, entries: I) 
    -> Result<Vec<Response>, StorageError<TypeConfig>>
where I: IntoIterator<Item = Entry<TypeConfig>> + Send
{
    let mut batch = WriteBatch::default();
    let mut responses = Vec::new();
    
    for entry in entries {
        let response = self.apply_entry_to_batch(&mut batch, entry).await?;
        responses.push(response);
    }
    
    // 一次性写入
    self.db.write(batch)?;
    Ok(responses)
}
```

### 2. 零拷贝

使用 `Bytes` 类型避免不必要的数据拷贝：

```rust
pub struct ClientRequest {
    pub command: RedisCommand,
}

pub struct RedisCommand {
    pub command: String,
    pub args: Vec<Bytes>,  // 使用 Bytes 而不是 Vec<u8>
}
```

### 3. 缓存优化

- 缓存当前 Raft 状态（term, voted_for）在内存中
- 缓存最后的快照元数据
- 使用 RocksDB 的 block cache

## 测试策略

### 单元测试

1. **存储层测试**
   - 日志条目的读写
   - Raft 状态的持久化
   - 快照的创建和恢复

2. **状态机测试**
   - Redis 命令的应用
   - 快照的创建和恢复
   - 错误处理

3. **类型转换测试**
   - Entry ↔ ClientRequest
   - Response ↔ ClientResponse
   - Error 转换

### 集成测试

1. **Adaptor 集成测试**
   - 验证 Adaptor 正确包装我们的实现
   - 测试 Openraft 能否正确调用我们的方法

2. **Raft 核心流程测试**
   - 日志复制
   - 快照生成和安装
   - 状态机应用

### 性能测试

1. **吞吐量测试**
   - 测量每秒处理的请求数
   - 目标：> 10,000 ops/sec

2. **延迟测试**
   - 测量单个请求的延迟
   - 目标：P99 < 10ms

3. **适配层开销测试**
   - 测量适配层引入的额外延迟
   - 目标：< 1ms

## 实现注意事项

### 1. Openraft 版本兼容性

- 当前使用 Openraft 0.9.21
- 需要关注 Openraft 的 API 变更
- 建议锁定版本避免意外破坏

### 2. 线程安全

- 所有共享状态使用 `Arc` + `RwLock` 或 `Mutex`
- RocksDB 本身是线程安全的
- 注意避免死锁

### 3. 错误传播

- 不要在适配层中 panic
- 所有错误都应该转换为 `StorageError`
- 保留原始错误信息用于调试

### 4. 日志记录

- 记录所有重要操作（日志追加、快照创建等）
- 使用结构化日志便于分析
- 区分不同日志级别（debug, info, warn, error）

## 未来扩展

### 1. 支持更多 Redis 命令

当前实现支持基础命令（SET, GET, DEL），未来可扩展：
- List 操作（LPUSH, RPUSH, LPOP, RPOP）
- Hash 操作（HSET, HGET, HDEL）
- Set 操作（SADD, SREM, SMEMBERS）
- ZSet 操作（ZADD, ZREM, ZRANGE）

### 2. 优化快照机制

- 增量快照
- 快照压缩
- 后台快照生成

### 3. 监控和指标

- 暴露 Prometheus 指标
- 集成分布式追踪
- 性能分析工具

## 参考资料

- [Openraft 官方文档](https://docs.rs/openraft/)
- [Openraft GitHub](https://github.com/datafuselabs/openraft)
- [Raft 论文](https://raft.github.io/raft.pdf)
- [RocksDB 文档](https://github.com/facebook/rocksdb/wiki)
