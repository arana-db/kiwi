# Kiwi Raft 集群管理脚本

本目录包含用于管理 Kiwi Raft 集群的脚本工具。

## 启动流程

```bash
# 1. 启动 3 个节点
./scripts/start_cluster.sh

# 2. 初始化集群
./scripts/init_cluster.sh

# 3. 查看集群状态
./scripts/cluster_status.sh
```

### 停止并清理

```bash
# 停止集群
./scripts/stop_cluster.sh

# 清理所有数据
./scripts/clean_cluster.sh --force
```

### 连接到节点

```bash
# 连接到节点 1
redis-cli -p 7379

# 连接到节点 2
redis-cli -p 7380

# 连接到节点 3
redis-cli -p 7381
```