# KiwiNetwork 连接缓存优化设计文档

## 背景

当前 `KiwiNetworkFactory` 实现存在两个潜在的性能优化点：

1. **每个 target 创建 2 个 Network 实例** - openraft 的 `spawn_replication_stream` 为每个目标节点创建 2 个 Network 实例
2. **重建 replication 时不缓存旧连接** - 成员变更导致 replication stream 重建时，之前创建的 Channel 被丢弃

> **注意**：这里需要的是**连接缓存**（Connection Cache），而非传统意义上的**连接池**（Connection Pool）。
> - 连接池：对同一个目标建立多个连接，提高并发
> - 连接缓存：避免对同一个目标重复创建连接，复用已建立的 Channel
>
> Raft 场景下每个目标节点只需要一个连接，因此只需要缓存，不需要池。

## 问题分析

### 问题 1: 双重 Network 实例

openraft 源码 ([raft_core.rs](file:///home/mcig/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/openraft-0.9.21/src/core/raft_core.rs)):

```rust
pub(crate) async fn spawn_replication_stream(
    &mut self,
    target: C::NodeId,
    progress_entry: ProgressEntry<C::NodeId>,
) -> ReplicationHandle<C> {
    let target_node = ...;

    // 为该 target 创建 Network 实例
    let network = self.network.new_client(target.clone(), target_node).await;
    let snapshot_network = self.network.new_client(target.clone(), target_node).await;
    // ...
}
```

这意味着对同一个目标节点，创建了：
- 1 个普通复制用的 Network
- 1 个快照复制用的 Network

每个 Network 内部都包含独立的 gRPC Channel，造成资源浪费。

### 问题 2: 无连接缓存

当前 `KiwiNetworkFactory` 是单元结构，没有任何状态：

```rust
pub struct KiwiNetworkFactory;

impl KiwiNetworkFactory {
    pub fn new() -> Self {
        Self  // 没有任何内部状态
    }
}
```

当成员变更时，openraft 会调用 `spawn_replication_stream` 重建所有 replication streams，此时会再次调用 `new_client`，创建新的 Channel，而旧的 Channel（如果底层连接已建立）被丢弃。

## 优化方案

### 设计思路

使用 `Arc<RwLock<HashMap<NodeId, KiwiNetwork>>>` 在 `KiwiNetworkFactory` 中缓存已创建的 Network 实例：

```rust
use std::sync::Arc;
use tokio::sync::RwLock;
use std::collections::HashMap;

pub struct KiwiNetworkFactory {
    // 缓存每个目标节点的 Network 实例
    // Arc 确保多线程安全共享
    // RwLock 允许并发读、独占写
    networks: Arc<RwLock<HashMap<NodeId, KiwiNetwork>>>,
}

impl KiwiNetworkFactory {
    pub fn new() -> Self {
        Self {
            networks: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl RaftNetworkFactory<KiwiTypeConfig> for KiwiNetworkFactory {
    type Network = Arc<KiwiNetwork>;

    async fn new_client(&mut self, target: NodeId, node: &Node) -> Self::Network {
        // 先尝试获取读锁，从缓存获取
        {
            let networks = self.networks.read().await;
            if let Some(network) = networks.get(&target) {
                return Arc::clone(network);
            }
        }

        // 缓存未命中，获取写锁，创建新的 Network
        let mut networks = self.networks.write().await;

        // 双重检查：可能有其他请求刚刚创建了
        if let Some(network) = networks.get(&target) {
            return Arc::clone(network);
        }

        // 创建新的 Network
        let addr = node.raft_addr.clone();
        let endpoint = ...;
        let client = RaftCoreServiceClient::new(endpoint);
        let network = Arc::new(KiwiNetwork { client });

        // 存入缓存
        networks.insert(target, Arc::clone(&network));

        network
    }
}
```

### 为什么要用 Arc<KiwiNetwork>?

openraft 可能同时持有同一个 Network 的多个引用（例如同时进行日志复制和快照复制），所以返回的 Network 必须是 `Clone` 的。使用 `Arc` 可以：

1. **共享底层 Channel** - 多个引用共享同一个 gRPC Channel，避免重复创建
2. **引用计数** - 最后所有引用销毁时自动清理

### KiwiNetwork 的修改

```rust
// 方案 A: 直接使用 Arc<KiwiNetwork>
pub struct KiwiNetwork {
    client: RaftCoreServiceClient<Channel>,
}

// 方案 B: 如果需要在 Network 内部保持可变引用
// 可以用 Arc<Mutex<KiwiNetworkInner>>
pub struct KiwiNetwork {
    inner: Arc<Mutex<KiwiNetworkInner>>,
}

pub struct KiwiNetworkInner {
    client: RaftCoreServiceClient<Channel>,
}
```

### 连接失效处理

当节点地址变更或连接断开时，需要能够移除缓存：

```rust
impl KiwiNetworkFactory {
    /// 移除指定节点的缓存（当节点被移除或地址变更时调用）
    pub async fn remove_client(&mut self, target: NodeId) {
        let mut networks = self.networks.write().await;
        networks.remove(&target);
    }

    /// 清空所有缓存（用于测试或重置）
    pub async fn clear(&mut self) {
        let mut networks = self.networks.write().await;
        networks.clear();
    }
}
```

## 实现步骤

1. **修改 `KiwiNetworkFactory` 结构体**
   - 添加 `networks: Arc<RwLock<HashMap<NodeId, Arc<KiwiNetwork>>>>` 字段

2. **修改 `new_client` 方法**
   - 先查询缓存，有则返回 Arc 克隆
   - 无则创建新实例并缓存

3. **修改 `KiwiNetwork` 返回类型**
   - 将 `type Network = KiwiNetwork` 改为 `type Network = Arc<KiwiNetwork>`

4. **实现清理方法**
   - 添加 `remove_client` 和 `clear` 方法

5. **集成到 Raft 节点**
   - 在成员变更时调用清理方法

## 性能对比

| 场景 | 优化前 | 优化后 |
|-----|-------|-------|
| 3 节点集群，每个 2 个 replication | 6 个 Channel | 3 个 Channel |
| 成员变更后重建 replication | 丢弃旧 Channel | 复用已有 Channel |
| 高频心跳 (100ms) | 连接复用 | 连接复用 |

## 注意事项

1. **内存泄漏风险**: 如果节点永远不删除，HashMap 会持续增长。建议在节点移除时调用 `remove_client`。

2. **连接健康检查**: 当前实现不检查连接是否失效。可以考虑添加缓存健康检查机制。

3. **地址变更**: 如果节点地址变更，需要先调用 `remove_client` 再创建新连接。

4. **测试**: 需要添加单元测试验证缓存逻辑和并发安全性。
