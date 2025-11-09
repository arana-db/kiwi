# Openraft Adaptor 快速参考

## 常用代码片段

### 初始化 Raft 节点

```rust
use std::sync::Arc;
use openraft::{Config, Raft};
use crate::raft::{TypeConfig, adaptor::*, storage::*};

// 创建存储
let db = Arc::new(rocksdb::DB::open_default("./data")?);
let log_storage = Arc::new(LogStorage::new(db.clone())?);
let state_machine = Arc::new(StateMachine::new(db.clone())?);

// 创建 Adaptor
let log_reader = LogReaderAdaptor::new(log_storage.clone());
let sm_adaptor = StateMachineAdaptor::new(state_machine.clone());

// 初始化 Raft
let raft = Raft::new(
    node_id,
    Arc::new(Config::default()),
    Arc::new(network),
    log_reader,
    sm_adaptor,
).await?;
```

### 写入数据

```rust
// 客户端写入
let request = ClientWriteRequest {
    key: b"key1".to_vec(),
    value: b"value1".to_vec(),
};

let response = raft.client_write(request).await?;
```

### 读取数据

```rust
// 从状态机读取
let value = state_machine.get(b"key1")?;
```

### 添加节点

```rust
// 添加新节点到集群
let node = BasicNode {
    addr: "127.0.0.1:7380".to_string(),
};

raft.add_learner(node_id, node, true).await?;
raft.change_membership([1, 2, 3], false).await?;
```

### 创建快照

```rust
// 触发快照创建
raft.trigger_snapshot().await?;
```

## 配置参考

### 基本配置

```rust
use openraft::Config;

let config = Config {
    // 心跳间隔（毫秒）
    heartbeat_interval: 500,
    
    // 选举超时范围（毫秒）
    election_timeout_min: 1500,
    election_timeout_max: 3000,
    
    // 快照策略
    snapshot_policy: SnapshotPolicy::LogsSinceLast(10000),
    max_in_snapshot_log_to_keep: 1000,
    
    // 网络配置
    install_snapshot_timeout: 30000,
    snapshot_max_chunk_size: 1024 * 1024,
    
    ..Default::default()
};
```

### RocksDB 配置

```rust
use rocksdb::{Options, DB};

let mut opts = Options::default();
opts.create_if_missing(true);
opts.create_missing_column_families(true);

// 性能优化
opts.set_write_buffer_size(64 * 1024 * 1024); // 64MB
opts.set_max_write_buffer_number(3);
opts.set_target_file_size_base(64 * 1024 * 1024);
opts.set_max_background_jobs(4);

// 持久化保证
opts.set_wal_recovery_mode(DBRecoveryMode::AbsoluteConsistency);

let db = DB::open_cf(&opts, path, vec!["raft_log", "raft_state", "kv"])?;
```

## 错误处理

### 常见错误类型

```rust
use crate::raft::error::StorageError;

match result {
    Err(StorageError::LogNotFound(index)) => {
        // 日志条目不存在
    }
    Err(StorageError::SnapshotNotFound) => {
        // 快照不存在
    }
    Err(StorageError::RocksDB(e)) => {
        // RocksDB 错误
    }
    Err(StorageError::Serialization(e)) => {
        // 序列化错误
    }
    Ok(value) => {
        // 成功
    }
}
```

### 错误转换

```rust
impl From<rocksdb::Error> for StorageError {
    fn from(e: rocksdb::Error) -> Self {
        StorageError::RocksDB(e.to_string())
    }
}

impl From<bincode::Error> for StorageError {
    fn from(e: bincode::Error) -> Self {
        StorageError::Serialization(e.to_string())
    }
}
```

## 类型转换

### Entry 转换

```rust
// 自定义 Entry -> Openraft Entry
impl From<CustomEntry> for openraft::Entry<TypeConfig> {
    fn from(entry: CustomEntry) -> Self {
        openraft::Entry {
            log_id: openraft::LogId {
                leader_id: entry.leader_id.into(),
                index: entry.index,
            },
            payload: openraft::EntryPayload::Normal(entry.data),
        }
    }
}

// Openraft Entry -> 自定义 Entry
impl TryFrom<openraft::Entry<TypeConfig>> for CustomEntry {
    type Error = ConversionError;
    
    fn try_from(entry: openraft::Entry<TypeConfig>) -> Result<Self, Self::Error> {
        match entry.payload {
            openraft::EntryPayload::Normal(data) => {
                Ok(CustomEntry {
                    leader_id: entry.log_id.leader_id.into(),
                    index: entry.log_id.index,
                    data,
                })
            }
            _ => Err(ConversionError::UnsupportedPayload),
        }
    }
}
```

## 测试工具

### 创建测试节点

```rust
#[cfg(test)]
async fn create_test_node(node_id: u64) -> RaftNode {
    let temp_dir = tempfile::tempdir().unwrap();
    let db = Arc::new(DB::open_default(temp_dir.path()).unwrap());
    
    RaftNode::new(node_id, db).await.unwrap()
}
```

### 模拟网络

```rust
#[cfg(test)]
struct MockNetwork {
    nodes: HashMap<u64, mpsc::Sender<RaftMessage>>,
}

impl MockNetwork {
    fn new() -> Self {
        Self {
            nodes: HashMap::new(),
        }
    }
    
    fn add_node(&mut self, id: u64, tx: mpsc::Sender<RaftMessage>) {
        self.nodes.insert(id, tx);
    }
}
```

### 验证日志一致性

```rust
#[cfg(test)]
async fn verify_log_consistency(nodes: &[RaftNode]) -> bool {
    let mut logs = Vec::new();
    
    for node in nodes {
        let log = node.get_all_logs().await.unwrap();
        logs.push(log);
    }
    
    // 检查所有节点的日志是否一致
    logs.windows(2).all(|w| w[0] == w[1])
}
```

## 监控和指标

### 获取 Raft 指标

```rust
let metrics = raft.metrics().borrow().clone();

println!("State: {:?}", metrics.state);
println!("Current term: {}", metrics.current_term);
println!("Current leader: {:?}", metrics.current_leader);
println!("Last log index: {:?}", metrics.last_log_index);
println!("Last applied: {:?}", metrics.last_applied);
println!("Snapshot: {:?}", metrics.snapshot);
```

### 自定义指标

```rust
use prometheus::{Counter, Histogram, Registry};

lazy_static! {
    static ref RAFT_WRITES: Counter = Counter::new(
        "raft_writes_total",
        "Total number of Raft writes"
    ).unwrap();
    
    static ref RAFT_WRITE_DURATION: Histogram = Histogram::new(
        "raft_write_duration_seconds",
        "Raft write duration"
    ).unwrap();
}

// 使用
RAFT_WRITES.inc();
let timer = RAFT_WRITE_DURATION.start_timer();
// ... 执行写入 ...
timer.observe_duration();
```

## 命令行工具

### 检查集群状态

```bash
# 查看节点状态
curl http://localhost:7379/metrics

# 查看日志
tail -f /var/log/kiwi/raft.log

# 检查 RocksDB
ldb --db=/path/to/db dump --column_family=raft_log
```

### 性能测试

```bash
# 运行基准测试
cargo bench --bench performance_benchmark

# 使用 redis-benchmark
redis-benchmark -h localhost -p 7379 -t set,get -n 100000 -c 50
```

## 常用命令

```rust
// 检查是否为 Leader
if raft.metrics().borrow().state == ServerState::Leader {
    // 执行写入操作
}

// 等待节点就绪
raft.wait(None).await?;

// 获取当前 Leader
let leader_id = raft.metrics().borrow().current_leader;

// 检查集群健康
let is_healthy = raft.metrics().borrow().state != ServerState::Shutdown;
```

## 相关文档

- [完整集成指南](./OPENRAFT_INTEGRATION.md)
- [故障排除](./TROUBLESHOOTING.md)
- [性能优化](./performance_optimizations.md)
