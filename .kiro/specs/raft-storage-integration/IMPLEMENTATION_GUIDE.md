# Raft 与 Redis 存储集成实现指南

## ✅ 已完成的工作

### Task 1: RedisStorageEngine 实现 ✅

- ✅ 创建了 `RedisStorage` trait 作为抽象接口
- ✅ 实现了 `RedisStorageEngine` 的所有方法
- ✅ 创建了 `RedisStorageAdapter` 使用函数闭包避免循环依赖
- ✅ 定义了 `RedisOperations` trait
- ✅ 编译通过

**文件位置**:
- `src/raft/src/storage_engine/redis_storage_engine.rs`
- `src/raft/src/storage_engine/redis_adapter.rs`

---

## 🔧 待完成的工作

### Task 2: 实现 RedisOperations trait

**目标**: 为 `storage::Redis` 实现 `RedisOperations` trait

**位置**: 创建新文件 `src/storage/src/raft_integration.rs`

```rust
// src/storage/src/raft_integration.rs

use crate::Redis;
use std::sync::Arc;

/// Implement RedisOperations for Redis to enable Raft integration
impl raft::storage_engine::RedisOperations for Redis {
    fn get_binary(&self, key: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        self.get_binary(key)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    }

    fn set(&self, key: &[u8], value: &[u8]) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.set(key, value)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    }

    fn del(&self, keys: &[&[u8]]) -> Result<i32, Box<dyn std::error::Error + Send + Sync>> {
        self.del(keys)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    }

    fn mset(&self, pairs: &[(Vec<u8>, Vec<u8>)]) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.mset(pairs)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    }
}

/// Helper function to create RedisStorageEngine from Redis
pub fn create_raft_storage_engine(redis: Arc<Redis>) -> raft::storage_engine::RedisStorageEngine {
    let adapter = raft::storage_engine::RedisStorageAdapter::new(redis);
    raft::storage_engine::RedisStorageEngine::new(Arc::new(adapter))
}
```

**修改**: 在 `src/storage/src/lib.rs` 中添加：
```rust
#[cfg(feature = "raft")]
pub mod raft_integration;
```

---

### Task 3: 修改 RaftNode 使用真实存储

**目标**: 在 `RaftNode::new()` 中创建真实的 Redis 实例并连接到状态机

**位置**: `src/raft/src/node.rs`

**当前代码** (第145行左右):
```rust
// 使用 simple_mem_store
let (log_store, sm) = crate::simple_mem_store::create_mem_store_with_dir(store_dir);
```

**修改为**:
```rust
// 1. 创建真实的 Redis 实例
let redis_path = PathBuf::from(&cluster_config.data_dir).join("redis_data");
let redis_options = Arc::new(storage::StorageOptions::default());
let lock_mgr = Arc::new(kstd::lock_mgr::LockMgr::new(1000));
let bg_task_handler = Arc::new(storage::BgTaskHandler::new().0);

let mut redis = storage::Redis::new(
    redis_options,
    0, // instance id
    bg_task_handler,
    lock_mgr,
);

redis.open(redis_path.to_str().unwrap())
    .map_err(|e| RaftError::Configuration {
        message: format!("Failed to open Redis storage: {}", e),
        context: "RaftNode::new".to_string(),
    })?;

let redis = Arc::new(redis);

// 2. 创建 RedisStorageEngine
let storage_engine = storage::raft_integration::create_raft_storage_engine(redis.clone());

// 3. 创建 KiwiStateMachine 并连接存储引擎
let state_machine = Arc::new(
    KiwiStateMachine::with_storage_engine(
        cluster_config.node_id,
        Arc::new(storage_engine)
    )
);

// 4. 使用 Adaptor 创建 Raft 存储
let raft_storage = Arc::new(RaftStorage::new(storage_path)?);
let log_store = crate::storage::create_raft_storage_adaptor(raft_storage);

// 5. 使用 Adaptor 创建状态机
let sm = crate::state_machine::create_state_machine_adaptor(state_machine);
```

**需要添加的导入**:
```rust
use storage::{Redis, StorageOptions, BgTaskHandler};
use kstd::lock_mgr::LockMgr;
```

---

### Task 4: 创建状态机 Adaptor

**目标**: 创建状态机的 Adaptor 以满足 openraft 要求

**位置**: `src/raft/src/state_machine/adaptor.rs` (新文件)

```rust
use crate::state_machine::KiwiStateMachine;
use crate::types::TypeConfig;
use openraft::storage::Adaptor;
use std::sync::Arc;

/// Create a state machine adaptor for openraft
pub fn create_state_machine_adaptor(
    state_machine: Arc<KiwiStateMachine>
) -> Adaptor<TypeConfig, Arc<KiwiStateMachine>> {
    Adaptor::new(state_machine)
}
```

**修改**: 在 `src/raft/src/state_machine/mod.rs` 中添加：
```rust
pub mod adaptor;
pub use adaptor::create_state_machine_adaptor;
```

---

### Task 5: 更新 server/main.rs

**目标**: 在服务器启动时创建 Redis 实例并传递给 RaftNode

**位置**: `src/server/src/main.rs`

**当前代码** (第147行左右):
```rust
warn!("Cluster mode temporarily disabled due to Raft module compilation issues");
warn!("Falling back to single-node mode");
```

**修改为**:
```rust
// 创建 Raft 节点
let raft_cluster_config = config.cluster.clone();

info!("Initializing Raft node with configuration: {:?}", raft_cluster_config);
let raft_node = match RaftNode::new(raft_cluster_config).await {
    Ok(node) => {
        info!("Raft node initialized successfully");
        Arc::new(node)
    },
    Err(e) => {
        error!("Failed to initialize Raft node: {}", e);
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Failed to initialize Raft node: {}", e)
        ));
    }
};

// 启动 Raft 节点
info!("Starting Raft node (init_cluster: {})", args.init_cluster);
if let Err(e) = raft_node.start(args.init_cluster).await {
    error!("Failed to start Raft node: {}", e);
    return Err(std::io::Error::new(
        std::io::ErrorKind::Other,
        format!("Failed to start Raft node: {}", e)
    ));
}

info!("Raft node started successfully");

// 使用集群模式启动服务器
match start_server_with_mode(protocol, &addr, &mut runtime_manager, true).await {
    Ok(_) => info!("Server started successfully in cluster mode"),
    Err(e) => {
        error!("Failed to start server: {}", e);
        return Err(e);
    }
}
```

---

## 📋 实施步骤总结

### 第一步: 实现 RedisOperations (30分钟)
1. 创建 `src/storage/src/raft_integration.rs`
2. 为 `Redis` 实现 `RedisOperations` trait
3. 添加 helper 函数
4. 更新 `src/storage/src/lib.rs`

### 第二步: 修改 RaftNode (1小时)
1. 修改 `RaftNode::new()` 创建真实 Redis
2. 连接 RedisStorageEngine 到 KiwiStateMachine
3. 替换 simple_mem_store
4. 测试编译

### 第三步: 创建状态机 Adaptor (30分钟)
1. 创建 `src/raft/src/state_machine/adaptor.rs`
2. 实现 adaptor 函数
3. 更新模块导出

### 第四步: 更新服务器启动 (30分钟)
1. 修改 `src/server/src/main.rs`
2. 启用集群模式
3. 测试启动

### 第五步: 集成测试 (1小时)
1. 编写集成测试
2. 测试单节点启动
3. 测试三节点集群
4. 验证数据持久化

---

## ⚠️ 注意事项

### 1. 依赖关系
- `storage` crate 需要添加 `raft` 作为可选依赖
- 使用 feature flag 避免循环依赖

### 2. 生命周期管理
- Redis 实例需要在 RaftNode 生命周期内保持存活
- 考虑使用 Arc 共享所有权

### 3. 错误处理
- 所有存储操作都需要正确的错误转换
- 使用 `map_err` 转换为 RaftError

### 4. 性能考虑
- 批量操作使用 MSET/DEL
- 考虑添加缓存层
- 监控 RocksDB 性能

---

## 🧪 测试计划

### 单元测试
- ✅ RedisStorageEngine 基础操作
- ⬜ RedisOperations trait 实现
- ⬜ 状态机与存储引擎集成

### 集成测试
- ⬜ 单节点启动和基础操作
- ⬜ 数据持久化和恢复
- ⬜ 三节点集群数据复制
- ⬜ Leader 故障转移

### 性能测试
- ⬜ 写入吞吐量
- ⬜ 读取延迟
- ⬜ 批量操作性能

---

## 📊 预期结果

完成后，系统将：
1. ✅ 使用真实的 RocksDB 存储数据
2. ✅ 通过 Raft 实现强一致性
3. ✅ 支持集群模式
4. ✅ 数据持久化到磁盘
5. ✅ 支持故障转移和恢复

---

## 🔗 相关文档

- [RedisStorageEngine 实现](./redis_storage_engine.rs)
- [Raft 架构设计](./design.md)
- [需求文档](./requirements.md)
- [任务列表](./tasks.md)
