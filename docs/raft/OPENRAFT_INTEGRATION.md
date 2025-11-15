# Openraft 集成指南

## 概述

本文档说明如何在 Kiwi 项目中使用 Openraft Adaptor 模式集成 Raft 共识算法。由于 Openraft 0.9+ 使用 sealed traits 限制直接实现存储接口，我们采用 Adaptor 模式来桥接自定义存储层和 Openraft。

## Adaptor 模式架构

### 核心组件

```
┌─────────────────────────────────────────────────────────┐
│                    Openraft Raft                        │
│                                                         │
│  ┌──────────────────┐      ┌──────────────────┐       │
│  │  RaftLogReader   │      │ RaftStateMachine │       │
│  │    (Adaptor)     │      │    (Adaptor)     │       │
│  └────────┬─────────┘      └────────┬─────────┘       │
└───────────┼────────────────────────┼─────────────────┘
            │                        │
            │  Adaptor Layer         │
            │                        │
┌───────────▼────────────────────────▼─────────────────┐
│              Custom Storage Layer                     │
│                                                       │
│  ┌──────────────┐  ┌──────────────┐  ┌────────────┐ │
│  │  LogStorage  │  │ StateMachine │  │  Snapshot  │ │
│  │  (RocksDB)   │  │  (RocksDB)   │  │  Builder   │ │
│  └──────────────┘  └──────────────┘  └────────────┘ │
└───────────────────────────────────────────────────────┘
```

### 关键特性

- **类型转换层**: 自动转换 Openraft 类型和自定义类型
- **异步桥接**: 将同步存储操作适配为异步接口
- **错误处理**: 统一的错误转换和传播
- **性能优化**: 批量操作和缓存支持

## 快速开始

### 1. 添加依赖

在 `Cargo.toml` 中添加：

```toml
[dependencies]
openraft = "0.9"
tokio = { version = "1", features = ["full"] }
```

### 2. 定义类型配置

```rust
use openraft::BasicNode;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TypeConfig;

impl openraft::RaftTypeConfig for TypeConfig {
    type NodeId = u64;
    type Node = BasicNode;
    type Entry = openraft::Entry<TypeConfig>;
    type SnapshotData = Vec<u8>;
    type AsyncRuntime = openraft::TokioRuntime;
}
```

### 3. 创建 Adaptor

```rust
use crate::raft::adaptor::{LogReaderAdaptor, StateMachineAdaptor};
use crate::raft::storage::{LogStorage, StateMachine};

// 创建存储层
let log_storage = Arc::new(LogStorage::new(db.clone())?);
let state_machine = Arc::new(StateMachine::new(db.clone())?);

// 创建 Adaptor
let log_reader = LogReaderAdaptor::new(log_storage.clone());
let sm_adaptor = StateMachineAdaptor::new(state_machine.clone());
```

### 4. 初始化 Raft 节点

```rust
use openraft::{Config, Raft};

let config = Config::default();
let raft = Raft::new(
    node_id,
    config,
    network,
    log_reader,
    sm_adaptor,
).await?;
```

## 详细示例

### 完整的 RaftNode 集成

```rust
use std::sync::Arc;
use openraft::{Config, Raft};
use crate::raft::{
    TypeConfig,
    adaptor::{LogReaderAdaptor, StateMachineAdaptor},
    storage::{LogStorage, StateMachine},
    network::NetworkImpl,
};

pub struct RaftNode {
    raft: Raft<TypeConfig>,
    log_storage: Arc<LogStorage>,
    state_machine: Arc<StateMachine>,
}

impl RaftNode {
    pub async fn new(
        node_id: u64,
        db: Arc<rocksdb::DB>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        // 1. 创建存储层
        let log_storage = Arc::new(LogStorage::new(db.clone())?);
        let state_machine = Arc::new(StateMachine::new(db.clone())?);
        
        // 2. 创建 Adaptor
        let log_reader = LogReaderAdaptor::new(log_storage.clone());
        let sm_adaptor = StateMachineAdaptor::new(state_machine.clone());
        
        // 3. 创建网络层
        let network = NetworkImpl::new();
        
        // 4. 配置 Raft
        let config = Config {
            heartbeat_interval: 500,
            election_timeout_min: 1500,
            election_timeout_max: 3000,
            ..Default::default()
        };
        
        // 5. 初始化 Raft
        let raft = Raft::new(
            node_id,
            Arc::new(config),
            Arc::new(network),
            log_reader,
            sm_adaptor,
        ).await?;
        
        Ok(Self {
            raft,
            log_storage,
            state_machine,
        })
    }
    
    pub async fn write(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), Box<dyn std::error::Error>> {
        let request = ClientWriteRequest::new(key, value);
        self.raft.client_write(request).await?;
        Ok(())
    }
    
    pub async fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
        self.state_machine.get(key)
    }
}
```

### 自定义存储实现

```rust
use rocksdb::{DB, ColumnFamily};
use std::sync::Arc;

pub struct LogStorage {
    db: Arc<DB>,
}

impl LogStorage {
    pub fn new(db: Arc<DB>) -> Result<Self, rocksdb::Error> {
        Ok(Self { db })
    }
    
    pub fn append_entries(&self, entries: Vec<Entry>) -> Result<(), StorageError> {
        let cf = self.db.cf_handle("raft_log").unwrap();
        let mut batch = rocksdb::WriteBatch::default();
        
        for entry in entries {
            let key = entry.log_id.index.to_be_bytes();
            let value = bincode::serialize(&entry)?;
            batch.put_cf(cf, key, value);
        }
        
        self.db.write(batch)?;
        Ok(())
    }
    
    pub fn get_log_entries(
        &self,
        start: u64,
        end: u64,
    ) -> Result<Vec<Entry>, StorageError> {
        let cf = self.db.cf_handle("raft_log").unwrap();
        let mut entries = Vec::new();
        
        for index in start..end {
            let key = index.to_be_bytes();
            if let Some(value) = self.db.get_cf(cf, key)? {
                let entry: Entry = bincode::deserialize(&value)?;
                entries.push(entry);
            }
        }
        
        Ok(entries)
    }
}
```

## API 参考

### LogReaderAdaptor

实现 `RaftLogReader` trait，提供日志读取功能。

**方法**:
- `try_get_log_entries(range)` - 读取指定范围的日志条目
- `get_log_state()` - 获取日志状态（最后日志 ID）

### StateMachineAdaptor

实现 `RaftStateMachine` trait，提供状态机功能。

**方法**:
- `applied_state()` - 获取已应用的状态
- `apply(entries)` - 应用日志条目到状态机
- `get_snapshot_builder()` - 获取快照构建器
- `begin_receiving_snapshot()` - 开始接收快照
- `install_snapshot(meta, snapshot)` - 安装快照

### SnapshotBuilderAdaptor

实现 `RaftSnapshotBuilder` trait，提供快照构建功能。

**方法**:
- `build_snapshot()` - 构建快照

## 已知限制

### 1. Sealed Traits 限制

Openraft 0.9+ 使用 sealed traits，无法直接实现以下接口：
- `RaftStateMachine` (sealed)
- `RaftLogStorage` (sealed)
- `RaftStorage` (sealed)

**解决方案**: 使用 Adaptor 模式实现非 sealed 的 traits（`RaftLogReader`, `RaftSnapshotBuilder`）。

### 2. 类型转换开销

Adaptor 需要在 Openraft 类型和自定义类型之间转换，可能带来性能开销。

**缓解措施**:
- 使用批量操作减少转换次数
- 缓存常用的转换结果
- 使用零拷贝技术（如 `Bytes`）

### 3. 异步性能

将同步存储操作包装为异步可能影响性能。

**缓解措施**:
- 使用 `spawn_blocking` 避免阻塞异步运行时
- 实现批量操作接口
- 使用专用的存储线程池

### 4. 快照大小限制

大型快照可能导致内存压力。

**缓解措施**:
- 实现流式快照传输
- 使用增量快照
- 配置合理的快照间隔

## 性能优化

### 1. 批量操作

```rust
impl LogStorage {
    pub fn append_entries_batch(&self, entries: Vec<Entry>) -> Result<(), StorageError> {
        let cf = self.db.cf_handle("raft_log").unwrap();
        let mut batch = rocksdb::WriteBatch::default();
        
        for entry in entries {
            let key = entry.log_id.index.to_be_bytes();
            let value = bincode::serialize(&entry)?;
            batch.put_cf(cf, key, value);
        }
        
        self.db.write(batch)?;
        Ok(())
    }
}
```

### 2. 缓存优化

```rust
use lru::LruCache;

pub struct CachedLogStorage {
    storage: Arc<LogStorage>,
    cache: Mutex<LruCache<u64, Entry>>,
}

impl CachedLogStorage {
    pub fn get_entry(&self, index: u64) -> Result<Option<Entry>, StorageError> {
        let mut cache = self.cache.lock().unwrap();
        
        if let Some(entry) = cache.get(&index) {
            return Ok(Some(entry.clone()));
        }
        
        if let Some(entry) = self.storage.get_entry(index)? {
            cache.put(index, entry.clone());
            Ok(Some(entry))
        } else {
            Ok(None)
        }
    }
}
```

### 3. 异步优化

```rust
impl LogReaderAdaptor {
    async fn get_log_entries_async(
        &self,
        start: u64,
        end: u64,
    ) -> Result<Vec<Entry>, StorageError> {
        let storage = self.storage.clone();
        
        tokio::task::spawn_blocking(move || {
            storage.get_log_entries(start, end)
        })
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?
    }
}
```

## 故障排除

### 问题 1: 编译错误 "trait cannot be implemented"

**原因**: 尝试直接实现 sealed trait。

**解决方案**: 使用 Adaptor 模式，实现非 sealed 的 traits。

```rust
// ❌ 错误
impl RaftStateMachine<TypeConfig> for MyStateMachine { }

// ✅ 正确
impl RaftStateMachine<TypeConfig> for StateMachineAdaptor { }
```

### 问题 2: 类型不匹配错误

**原因**: Openraft 类型和自定义类型不兼容。

**解决方案**: 实现类型转换层。

```rust
impl From<CustomEntry> for openraft::Entry<TypeConfig> {
    fn from(entry: CustomEntry) -> Self {
        // 转换逻辑
    }
}
```

### 问题 3: 异步运行时阻塞

**原因**: 在异步上下文中执行同步 I/O。

**解决方案**: 使用 `spawn_blocking`。

```rust
async fn read_from_storage(&self) -> Result<Data, Error> {
    let storage = self.storage.clone();
    tokio::task::spawn_blocking(move || {
        storage.sync_read()
    })
    .await?
}
```

### 问题 4: 快照传输失败

**原因**: 快照过大或网络超时。

**解决方案**: 
- 增加网络超时配置
- 实现流式传输
- 压缩快照数据

```rust
let config = Config {
    snapshot_max_chunk_size: 1024 * 1024, // 1MB chunks
    install_snapshot_timeout: 30000, // 30 seconds
    ..Default::default()
};
```

### 问题 5: 日志条目丢失

**原因**: 存储层未正确持久化。

**解决方案**: 确保写入操作使用 `sync` 模式。

```rust
let mut write_options = rocksdb::WriteOptions::default();
write_options.set_sync(true);
self.db.write_opt(batch, &write_options)?;
```

## 测试指南

### 单元测试

```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_log_reader_adaptor() {
        let storage = Arc::new(LogStorage::new_in_memory());
        let adaptor = LogReaderAdaptor::new(storage);
        
        // 测试日志读取
        let entries = adaptor.try_get_log_entries(0..10).await.unwrap();
        assert_eq!(entries.len(), 0);
    }
}
```

### 集成测试

```rust
#[tokio::test]
async fn test_raft_integration() {
    let node = RaftNode::new(1, create_test_db()).await.unwrap();
    
    // 测试写入
    node.write(b"key1".to_vec(), b"value1".to_vec()).await.unwrap();
    
    // 测试读取
    let value = node.read(b"key1").await.unwrap();
    assert_eq!(value, Some(b"value1".to_vec()));
}
```

## 最佳实践

1. **错误处理**: 始终正确转换和传播错误
2. **日志记录**: 在关键路径添加 trace 日志
3. **性能监控**: 使用 metrics 跟踪操作延迟
4. **资源管理**: 正确管理文件句柄和内存
5. **测试覆盖**: 编写全面的单元和集成测试

## 参考资料

- [Openraft 官方文档](https://docs.rs/openraft/)
- [Openraft GitHub](https://github.com/datafuselabs/openraft)
- [Raft 论文](https://raft.github.io/raft.pdf)
- [项目设计文档](../../../.kiro/specs/openraft-sealed-traits-fix/design.md)
- [性能优化指南](./performance_optimizations.md)
