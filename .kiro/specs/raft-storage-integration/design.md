# Raft 与存储层集成设计文档

## 概述

本文档描述了如何将 Kiwi 的 Raft 共识层与现有存储层真正集成，实现强一致性的分布式 Redis 兼容数据库。核心设计思路是使用 Openraft 的 Adaptor 模式桥接我们的 RaftStorage 和 KiwiStateMachine，并修改请求处理流程使所有写操作经过 Raft 共识。

## 架构设计

### 当前架构（问题）

```
Client → Network Runtime → StorageClient → Storage Runtime → Redis (RocksDB)
                                                                ↓
                                                          直接写入，无 Raft
```

### 目标架构（解决方案）

```
Client → Network Runtime → RaftNode → Raft Consensus → StateMachine → Redis (RocksDB)
                              ↓                              ↓
                        RaftStorage (RocksDB)          StorageEngine
                        持久化 Raft 日志              实际数据存储
```

## 核心组件设计

### 1. RaftStorageAdaptor

**目的**: 使用 Openraft Adaptor 模式包装 RaftStorage

**设计**:
```rust
pub struct RaftStorageAdaptor {
    inner: Arc<RaftStorage>,
}

impl RaftStorageAdaptor {
    pub fn new(storage: Arc<RaftStorage>) -> Self {
        Self { inner: storage }
    }
}

// 使用 openraft 的 Adaptor 宏或手动实现
// 将 RaftStorage 的方法映射到 openraft 要求的接口
```

**关键方法**:
- `save_vote()`: 保存投票信息
- `read_vote()`: 读取投票信息
- `append_to_log()`: 追加日志条目
- `delete_conflict_logs_since()`: 删除冲突日志
- `purge_logs_upto()`: 清理旧日志
- `last_applied_state()`: 获取最后应用状态
- `apply_to_state_machine()`: 应用日志到状态机

### 2. RedisStorageEngine

**目的**: 实现 StorageEngine trait，连接状态机到实际的 Redis 存储

**设计**:
```rust
pub struct RedisStorageEngine {
    redis: Arc<Redis>,
}

#[async_trait]
impl StorageEngine for RedisStorageEngine {
    async fn get(&self, key: &[u8]) -> RaftResult<Option<Vec<u8>>> {
        // 调用 redis.get_binary()
    }
    
    async fn put(&self, key: &[u8], value: &[u8]) -> RaftResult<()> {
        // 调用 redis.set()
    }
    
    async fn delete(&self, key: &[u8]) -> RaftResult<()> {
        // 调用 redis.del()
    }
    
    async fn create_snapshot(&self) -> RaftResult<Vec<u8>> {
        // 创建 RocksDB 快照
    }
    
    async fn restore_from_snapshot(&self, data: &[u8]) -> RaftResult<()> {
        // 从快照恢复
    }
}
```

### 3. RaftNode 修改

**目的**: 使用真实的 RaftStorage 而不是 simple_mem_store

**修改前**:
```rust
let (log_store, sm) = crate::simple_mem_store::create_mem_store_with_dir(store_dir);
```

**修改后**:
```rust
// 创建 RaftStorage
let raft_storage = Arc::new(RaftStorage::new(storage_path)?);

// 创建 RedisStorageEngine
let redis_engine = Arc::new(RedisStorageEngine::new(redis.clone()));

// 创建 KiwiStateMachine 并连接存储引擎
let state_machine = Arc::new(
    KiwiStateMachine::with_storage_engine(node_id, redis_engine)
);

// 使用 Adaptor 包装
let log_store = RaftStorageAdaptor::new(raft_storage.clone());
let sm = StateMachineAdaptor::new(state_machine.clone());

// 创建 Raft 实例
let raft = Raft::new(node_id, raft_config, network, log_store, sm).await?;
```

### 4. 请求路由层

**目的**: 将网络层的请求正确路由到 Raft 或直接存储

**设计**:
```rust
pub struct RequestRouter {
    raft_node: Arc<RaftNode>,
    storage: Arc<Storage>,
    mode: ClusterMode,
}

impl RequestRouter {
    pub async fn route_command(&self, cmd: RedisCommand) -> Result<RedisResponse> {
        match self.mode {
            ClusterMode::Single => {
                // 单机模式：直接访问存储
                self.storage.execute_command(cmd).await
            }
            ClusterMode::Cluster => {
                // 集群模式：根据命令类型路由
                if cmd.is_write() {
                    self.route_write(cmd).await
                } else {
                    self.route_read(cmd).await
                }
            }
        }
    }
    
    async fn route_write(&self, cmd: RedisCommand) -> Result<RedisResponse> {
        // 检查是否是 Leader
        if !self.raft_node.is_leader().await {
            // 转发到 Leader
            return self.forward_to_leader(cmd).await;
        }
        
        // 通过 Raft 提交
        let request = ClientRequest {
            id: RequestId::new(),
            command: cmd,
            consistency_level: ConsistencyLevel::Linearizable,
        };
        
        let response = self.raft_node.raft().client_write(request).await?;
        Ok(response.into())
    }
    
    async fn route_read(&self, cmd: RedisCommand) -> Result<RedisResponse> {
        match cmd.consistency_level {
            ConsistencyLevel::Linearizable => {
                // 强一致性读：通过 Raft
                self.raft_node.raft().ensure_linearizable().await?;
                self.storage.execute_command(cmd).await
            }
            ConsistencyLevel::Eventual => {
                // 最终一致性读：直接读取
                self.storage.execute_command(cmd).await
            }
        }
    }
}
```

### 5. 命令分类

**目的**: 识别命令是读操作还是写操作

**设计**:
```rust
impl RedisCommand {
    pub fn is_write(&self) -> bool {
        matches!(
            self.command.to_uppercase().as_str(),
            "SET" | "DEL" | "SETEX" | "PSETEX" | "SETNX" | "APPEND" |
            "INCR" | "DECR" | "INCRBY" | "DECRBY" |
            "LPUSH" | "RPUSH" | "LPOP" | "RPOP" |
            "HSET" | "HDEL" | "SADD" | "SREM" | "ZADD" | "ZREM"
        )
    }
    
    pub fn is_read(&self) -> bool {
        matches!(
            self.command.to_uppercase().as_str(),
            "GET" | "MGET" | "EXISTS" | "STRLEN" | "GETRANGE" |
            "LLEN" | "LRANGE" | "HGET" | "HGETALL" |
            "SMEMBERS" | "SISMEMBER" | "ZRANGE" | "ZSCORE"
        )
    }
}
```

## 数据流设计

### 写操作流程

```
1. Client 发送 SET key value
   ↓
2. Network Runtime 解析命令
   ↓
3. RequestRouter 识别为写操作
   ↓
4. 检查是否是 Leader
   ↓
5. RaftNode.propose(ClientRequest)
   ↓
6. Raft 复制日志到多数节点
   ↓
7. 日志被提交
   ↓
8. StateMachine.apply() 被调用
   ↓
9. RedisStorageEngine.put() 执行
   ↓
10. Redis.set() 写入 RocksDB
    ↓
11. 返回成功响应给 Client
```

### 读操作流程（强一致性）

```
1. Client 发送 GET key
   ↓
2. Network Runtime 解析命令
   ↓
3. RequestRouter 识别为读操作
   ↓
4. 检查一致性级别 = Linearizable
   ↓
5. Raft.ensure_linearizable() (read_index)
   ↓
6. 等待 applied_index >= read_index
   ↓
7. Redis.get() 读取 RocksDB
   ↓
8. 返回结果给 Client
```

### 读操作流程（最终一致性）

```
1. Client 发送 GET key
   ↓
2. Network Runtime 解析命令
   ↓
3. RequestRouter 识别为读操作
   ↓
4. 检查一致性级别 = Eventual
   ↓
5. 直接 Redis.get() 读取 RocksDB
   ↓
6. 返回结果给 Client
```

## 快照设计

### 快照创建

```rust
impl KiwiStateMachine {
    pub async fn create_snapshot(&self) -> RaftResult<StateMachineSnapshot> {
        // 1. 获取当前 applied_index
        let applied_index = self.applied_index();
        
        // 2. 调用 storage_engine 创建快照
        let snapshot_data = if let Some(engine) = &self.storage_engine {
            engine.create_snapshot().await?
        } else {
            Vec::new()
        };
        
        // 3. 序列化快照
        let snapshot = StateMachineSnapshot {
            data: bincode::deserialize(&snapshot_data)?,
            applied_index,
        };
        
        Ok(snapshot)
    }
}
```

### 快照恢复

```rust
impl KiwiStateMachine {
    pub async fn restore_from_snapshot(&self, snapshot: &StateMachineSnapshot) -> RaftResult<()> {
        // 1. 序列化快照数据
        let snapshot_data = bincode::serialize(&snapshot.data)?;
        
        // 2. 调用 storage_engine 恢复
        if let Some(engine) = &self.storage_engine {
            engine.restore_from_snapshot(&snapshot_data).await?;
        }
        
        // 3. 更新 applied_index
        self.set_applied_index(snapshot.applied_index);
        
        Ok(())
    }
}
```

## 配置变更设计

### 添加节点

```
1. 在 Leader 上调用 add_learner(node_id, endpoint)
   ↓
2. 新节点作为 Learner 加入，开始同步日志
   ↓
3. 等待新节点追上 Leader 的日志
   ↓
4. 调用 change_membership() 将 Learner 提升为 Voter
   ↓
5. Raft 通过配置变更日志复制新配置
   ↓
6. 新节点成为正式成员
```

### 移除节点

```
1. 在 Leader 上调用 remove_node_safely(node_id)
   ↓
2. 检查移除后是否还有多数节点
   ↓
3. 调用 change_membership() 移除节点
   ↓
4. Raft 通过配置变更日志复制新配置
   ↓
5. 节点从集群中移除
```

## 错误处理设计

### Raft 层错误

- **NotLeader**: 返回当前 Leader 地址，客户端重定向
- **Timeout**: 重试或返回错误
- **StorageError**: 记录日志，可能触发快照恢复
- **NetworkError**: 重试或标记节点为不可用

### 存储层错误

- **KeyNotFound**: 返回 nil
- **WrongType**: 返回 WRONGTYPE 错误
- **OutOfMemory**: 返回 OOM 错误
- **IOError**: 记录日志，可能需要人工介入

## 性能优化设计

### 批量写入优化

```rust
impl KiwiStateMachine {
    pub async fn apply_batch(&self, entries: Vec<Entry>) -> RaftResult<Vec<Response>> {
        // 1. 收集所有写操作
        let mut batch_ops = Vec::new();
        for entry in &entries {
            if let Some(op) = self.collect_batch_operation(entry) {
                batch_ops.push(op);
            }
        }
        
        // 2. 批量执行
        if let Some(engine) = &self.storage_engine {
            engine.batch_put(batch_ops).await?;
        }
        
        // 3. 返回响应
        Ok(responses)
    }
}
```

### 读操作优化

- **本地读**: 最终一致性读直接从本地读取
- **Follower 读**: 使用 read_index 机制允许 Follower 提供强一致性读
- **缓存**: 在状态机层面添加缓存（可选）

### 日志复制优化

- **流水线**: 使用 openraft 的流水线复制
- **批量复制**: 一次复制多个日志条目
- **压缩**: 定期创建快照压缩日志

## 测试策略

### 单元测试

- RaftStorageAdaptor 的所有方法
- RedisStorageEngine 的所有方法
- RequestRouter 的路由逻辑
- 命令分类逻辑

### 集成测试

- 单节点写入和读取
- 三节点集群写入和读取
- Leader 故障转移
- 节点添加和移除
- 快照创建和恢复

### 一致性测试

- 并发写入一致性
- 故障后数据一致性
- 网络分区后一致性
- 配置变更期间一致性

### 性能测试

- 单节点性能基准
- 三节点集群性能基准
- 读写混合负载测试
- 大数据量测试

## 实现优先级

### P0 (必须完成)

1. RaftStorageAdaptor 实现
2. RedisStorageEngine 实现
3. RaftNode 修改使用真实存储
4. 基础写操作路由
5. 基础读操作路由

### P1 (重要)

6. 请求路由层完整实现
7. Leader 转发机制
8. 强一致性读实现
9. 快照创建和恢复
10. 基础测试

### P2 (优化)

11. 批量写入优化
12. Follower 读优化
13. 配置变更管理
14. 完整测试套件
15. 性能优化

## 风险和挑战

### 技术风险

1. **Openraft Adaptor 复杂性**: 需要深入理解 openraft 的内部机制
2. **性能开销**: Raft 共识会增加延迟
3. **状态同步**: 确保 Raft 状态和存储状态一致

### 缓解措施

1. 参考 openraft 官方示例和文档
2. 使用批量操作和流水线优化性能
3. 添加完整的测试和监控

## 总结

本设计通过 Openraft Adaptor 模式将现有的 RaftStorage 和 KiwiStateMachine 集成到 openraft 中，并修改请求处理流程使所有写操作经过 Raft 共识。这样可以在保留现有优秀存储层实现的基础上，实现真正的强一致性分布式数据库。
