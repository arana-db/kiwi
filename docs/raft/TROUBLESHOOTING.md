# Openraft 集成故障排除指南

本文档提供 Openraft 集成过程中常见问题的诊断和解决方案。

## 编译错误

### 错误 1: 使用 Adaptor 模式

**背景**: OpenRaft 0.9.x 版本引入了 `Adaptor` 模式来简化存储层实现。

**正确做法**:
1. 直接实现 `RaftStorage` trait（这是一个非 sealed trait）
2. 使用 `Adaptor::new()` 包装你的存储实现
3. `Adaptor` 会自动提供 `RaftLogStorage` 和 `RaftStateMachine` 的实现

**示例**（参考 `src/raft/src/simple_mem_store.rs`）:
```rust
use openraft::storage::Adaptor;
use openraft::RaftStorage;

// 1. 实现 RaftStorage trait
impl RaftStorage<TypeConfig> for SimpleMemStore {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        // 实现
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        // 实现
    }

    // ... 其他方法
}

// 2. 实现 RaftLogReader
impl RaftLogReader<TypeConfig> for SimpleMemStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        // 实现
    }
}

// 3. 实现 RaftSnapshotBuilder
impl RaftSnapshotBuilder<TypeConfig> for SimpleMemStore {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        // 实现
    }
}

// 4. 使用 Adaptor 包装
pub fn create_mem_store() -> (
    impl openraft::storage::RaftLogStorage<TypeConfig>,
    impl openraft::storage::RaftStateMachine<TypeConfig>,
) {
    let store = SimpleMemStore::new();
    Adaptor::new(store)
}
```

### 错误 2: 类型不匹配

**错误信息**:
```
error[E0308]: mismatched types
  expected `openraft::Entry<TypeConfig>`
     found `CustomEntry`
```

**原因**: Openraft 类型和自定义类型不兼容。

**解决方案**:
1. 使用 Openraft 提供的类型（`Entry<TypeConfig>`、`LogId<NodeId>` 等）
2. 如果需要自定义类型，实现类型转换 trait（`From`、`Into`）
3. 参考 `src/raft/src/conversion.rs` 中的转换实现

**示例**:
```rust
use openraft::{Entry, EntryPayload, LogId};

// 使用 Openraft 的类型
let entry = Entry {
    log_id: LogId::new(leader_id, index),
    payload: EntryPayload::Normal(request),
};
```

### 错误 3: 生命周期问题

**错误信息**:
```
error[E0597]: `store` does not live long enough
```

**原因**: 存储对象的生命周期不够长。

**解决方案**:
使用 `Arc` 包装共享状态：

```rust
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct SimpleMemStore {
    vote: Arc<RwLock<Option<Vote<NodeId>>>>,
    logs: Arc<RwLock<BTreeMap<u64, Entry<TypeConfig>>>>,
    // ... 其他字段
}
```

## 运行时错误

### 错误 1: "Not leader" 错误

**症状**: 客户端请求返回 "Not leader" 错误。

**原因**:
1. 节点还未完成选举
2. 节点失去了 leader 地位
3. 网络分区导致节点无法与集群通信

**诊断步骤**:
```rust
// 检查节点状态
let metrics = node.get_metrics().await?;
println!("Current leader: {:?}", metrics.current_leader);
println!("Node state: {:?}", metrics.state);
println!("Current term: {}", metrics.current_term);
```

**解决方案**:
1. 等待选举完成（使用 `wait_for_election`）
2. 将请求重定向到当前 leader
3. 检查网络连接

```rust
// 等待选举
let leader_id = node.wait_for_election(Duration::from_secs(5)).await?;

// 或者检查并重定向
if !node.is_leader().await {
    let leader_id = node.get_leader_id().await;
    // 重定向到 leader
}
```

### 错误 2: 数据不一致

**症状**: 读取的数据与写入的数据不一致。

**原因**:
1. 状态机未正确应用日志条目
2. 持久化失败但未检测到
3. 快照恢复不正确

**诊断步骤**:
```rust
// 检查应用状态
let metrics = node.get_metrics().await?;
println!("Last applied: {:?}", metrics.last_applied);
println!("Last log index: {:?}", metrics.last_log_index);

// 检查状态机
let state = node.state_machine().get_current_snapshot_data().await?;
println!("State machine data: {:?}", state);
```

**解决方案**:
1. 确保 `apply_to_state_machine` 正确执行命令
2. 添加持久化错误处理
3. 验证快照的序列化/反序列化

参考 `src/raft/src/simple_mem_store.rs` 中的 `apply_to_state_machine` 实现：

```rust
async fn apply_to_state_machine(
    &mut self,
    entries: &[Entry<TypeConfig>],
) -> Result<Vec<ClientResponse>, StorageError<NodeId>> {
    for entry in entries {
        match &entry.payload {
            EntryPayload::Normal(ref request) => {
                // 执行命令并获取实际结果
                let result = self.execute_command(request).await;
                // 返回实际结果，不是固定的 "OK"
            }
            // ... 其他情况
        }
    }
    
    // 持久化状态
    if let Some(ref dir) = self.data_dir {
        // 保存到磁盘
    }
}
```

### 错误 3: 节点重启后无法恢复

**症状**: 节点重启后丢失数据或无法加入集群。

**原因**:
1. 未持久化关键状态（vote、logs、membership）
2. 加载持久化数据失败
3. 集群成员关系未恢复

**解决方案**:
确保持久化以下内容：
- Vote（投票信息）
- Logs（日志条目）
- Applied index（已应用索引）
- Membership（集群成员关系）
- State machine data（状态机数据）

参考 `src/raft/src/simple_mem_store.rs` 中的持久化实现：

```rust
// 持久化 vote
async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
    let mut v = self.vote.write().await;
    *v = Some(*vote);
    
    // 持久化到磁盘
    if let Some(ref dir) = self.data_dir {
        let vote_path = dir.join("vote.bin");
        if let Ok(data) = bincode::serialize(vote) {
            std::fs::write(&vote_path, &data)?;
        }
    }
    Ok(())
}

// 加载持久化数据
pub fn with_data_dir(data_dir: Option<PathBuf>) -> Self {
    let store = Self { /* ... */ };
    
    if let Some(ref dir) = data_dir {
        // 加载 vote
        let vote_path = dir.join("vote.bin");
        if vote_path.exists() {
            if let Ok(data) = std::fs::read(&vote_path) {
                if let Ok(vote) = bincode::deserialize(&data) {
                    // 恢复 vote
                }
            }
        }
        
        // 加载 logs、membership 等
    }
    
    store
}
```

## 性能问题

### 问题 1: 写入性能差

**症状**: 写入操作延迟高。

**原因**:
1. 每次操作都同步持久化
2. 未使用批量操作
3. 锁竞争严重

**解决方案**:
1. 使用异步 I/O（`tokio::fs`）
2. 批量持久化
3. 优化锁粒度

```rust
// 批量持久化
async fn apply_to_state_machine(
    &mut self,
    entries: &[Entry<TypeConfig>],
) -> Result<Vec<ClientResponse>, StorageError<NodeId>> {
    // 处理所有条目
    let responses = process_entries(entries).await?;
    
    // 一次性持久化
    self.persist_state().await?;
    
    Ok(responses)
}
```

### 问题 2: 读取性能差

**症状**: 读取操作延迟高。

**原因**:
1. 每次读取都需要通过 Raft 共识
2. 未使用读缓存
3. 状态机访问有锁竞争

**解决方案**:
1. 对于可以接受最终一致性的读取，直接从状态机读取
2. 使用 `RwLock` 允许并发读取
3. 实现读缓存

```rust
// 直接读取（非线性化）
pub async fn read_local(&self, key: &[u8]) -> Option<Vec<u8>> {
    let kv_store = self.kv_store.read().await;
    kv_store.get(key).cloned()
}

// 线性化读取（通过 Raft）
pub async fn read_linearizable(&self, key: &[u8]) -> RaftResult<Option<Vec<u8>>> {
    let request = ClientRequest {
        command: RedisCommand {
            command: "GET".to_string(),
            args: vec![Bytes::from(key)],
        },
        consistency_level: ConsistencyLevel::Linearizable,
    };
    self.propose(request).await
}
```

## 调试技巧

### 1. 启用详细日志

```rust
// 在 Cargo.toml 中
[dependencies]
env_logger = "0.10"

// 在代码中
env_logger::init();

// 运行时设置日志级别
RUST_LOG=debug cargo test
RUST_LOG=openraft=trace,raft=debug cargo test
```

### 2. 检查 Raft 指标

```rust
let metrics = node.get_metrics().await?;
println!("Metrics: {:#?}", metrics);

// 关键指标：
// - current_term: 当前任期
// - current_leader: 当前 leader
// - last_log_index: 最后日志索引
// - last_applied: 最后应用索引
// - state: 节点状态（Leader/Follower/Candidate）
```

### 3. 使用测试工具

```rust
#[tokio::test]
async fn test_state_machine() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_config(1, &temp_dir);
    
    let node = RaftNode::new(config).await.unwrap();
    node.start(true).await.unwrap();
    
    // 测试写入
    let response = node.propose(set_request).await.unwrap();
    assert!(response.result.is_ok());
    
    // 测试读取
    let response = node.propose(get_request).await.unwrap();
    assert_eq!(response.result.unwrap(), expected_value);
}
```

## 常见问题 FAQ

### Q: 如何实现自定义存储后端？

A: 实现 `RaftStorage`、`RaftLogReader` 和 `RaftSnapshotBuilder` traits，然后使用 `Adaptor::new()` 包装。参考 `src/raft/src/simple_mem_store.rs`。

### Q: 如何处理网络分区？

A: Raft 会自动处理网络分区。确保：
1. 配置合适的心跳和选举超时
2. 实现健康检查
3. 监控节点状态

### Q: 如何优化快照性能？

A: 
1. 增量快照（只保存变更）
2. 压缩快照数据
3. 异步创建快照
4. 调整 `snapshot_threshold` 参数

### Q: 测试失败怎么办？

A: 
1. 检查 `docs/raft/COMPLETE_FIX_SUMMARY.md` 了解已知问题和解决方案
2. 启用详细日志查看错误详情
3. 使用 `cargo test -- --nocapture` 查看测试输出
4. 检查临时目录权限和磁盘空间

## 相关资源

- [OpenRaft 官方文档](https://docs.rs/openraft/)
- [OpenRaft GitHub](https://github.com/datafuselabs/openraft)
- [Raft 论文](https://raft.github.io/raft.pdf)
- [项目集成指南](./OPENRAFT_INTEGRATION.md)
- [快速参考](./QUICK_REFERENCE.md)
- [完整修复总结](./COMPLETE_FIX_SUMMARY.md)

## 获取帮助

如果遇到本文档未涵盖的问题：
1. 查看 [OpenRaft 示例](https://github.com/datafuselabs/openraft/tree/main/examples)
2. 搜索 [OpenRaft Issues](https://github.com/datafuselabs/openraft/issues)
3. 查看项目测试用例 `src/raft/src/tests/`
4. 提交 Issue 到项目仓库
