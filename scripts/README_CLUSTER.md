# Kiwi Raft 集群管理脚本

本目录包含用于管理 Kiwi Raft 集群的脚本工具。

## 脚本列表

### 1. start_cluster.sh - 启动集群

启动指定数量的 Raft 节点。

```bash
# 启动 3 节点集群（默认）
./scripts/start_cluster.sh

# 启动 5 节点集群
./scripts/start_cluster.sh --nodes 5

# 跳过构建直接启动
./scripts/start_cluster.sh --no-build

# 清理旧数据后启动
./scripts/start_cluster.sh --clean
```

**功能：**
- 自动生成节点配置文件（cluster_node*.conf）
- 为每个节点分配独立的端口和数据目录
- 构建并启动所有节点
- 日志输出到 `cluster_logs/` 目录

**端口分配：**
- 节点 1: Redis 7379, Raft 8001
- 节点 2: Redis 7380, Raft 8002
- 节点 3: Redis 7381, Raft 8003
- ...

### 2. init_cluster.sh - 初始化集群

初始化 Raft 集群并触发 leader 选举。

```bash
# 初始化集群
./scripts/init_cluster.sh

# 初始化指定节点数的集群
./scripts/init_cluster.sh 5
```

**注意：** 集群只能初始化一次。如需重新初始化，请先清理数据。

### 3. cluster_status.sh - 查看集群状态

查看集群运行状态、leader 信息和节点指标。

```bash
./scripts/cluster_status.sh
```

**显示信息：**
- 各节点运行状态
- 当前 leader 节点
- 节点 Raft 指标（term、log index、applied index）
- 快速命令提示

### 4. stop_cluster.sh - 停止集群

优雅地停止所有集群节点。

```bash
./scripts/stop_cluster.sh
```

**功能：**
- 读取 PID 文件停止所有节点
- 如果 PID 文件不存在，自动搜索并停止相关进程
- 先发送 SIGTERM，必要时使用 SIGKILL

### 5. clean_cluster.sh - 清理集群数据

清理集群的数据文件和配置文件。

```bash
# 交互式清理（需要确认）
./scripts/clean_cluster.sh

# 强制清理（跳过确认）
./scripts/clean_cluster.sh --force

# 自动停止集群并清理
./scripts/clean_cluster.sh --stop --force
```

**清理内容：**
- 集群配置文件（cluster_node*.conf）
- Raft 数据目录（raft_data_*）
- 数据库目录（db_node*）
- 集群日志（cluster_logs/）

### 6. test_raft_sync.sh - 测试 Raft 同步

测试数据在集群节点间的同步效果。

```bash
# 测试 10 个键值对（默认）
./scripts/test_raft_sync.sh

# 测试 50 个键值对
./scripts/test_raft_sync.sh 50
```

**测试内容：**
1. 向 leader 写入数据
2. 验证数据复制到所有节点
3. 验证数据一致性
4. 从 follower 节点读取数据

**⚠️ 当前限制：** Raft 写入路径尚未完全实现。当前写操作直接在 leader 的存储层执行，不经过 Raft 复制。因此测试会显示数据不一致。

## 完整使用流程

### 启动新集群

```bash
# 1. 启动 3 个节点
./scripts/start_cluster.sh

# 2. 初始化集群
./scripts/init_cluster.sh

# 3. 查看集群状态
./scripts/cluster_status.sh

# 4. 测试数据同步
./scripts/test_raft_sync.sh 10
```

### 查看 Leader

```bash
# 方法 1: 使用状态脚本
./scripts/cluster_status.sh

# 方法 2: 直接查询 API
curl http://127.0.0.1:8001/raft/leader | jq

# 方法 3: 查询任意节点
curl http://127.0.0.1:8002/raft/leader | jq
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

### 停止并清理

```bash
# 停止集群
./scripts/stop_cluster.sh

# 清理所有数据
./scripts/clean_cluster.sh --force
```

## Raft HTTP API

所有节点都提供以下 HTTP API：

### 查询 Leader
```bash
GET http://127.0.0.1:8001/raft/leader
```

### 查询节点指标
```bash
GET http://127.0.0.1:8001/raft/metrics
```

### 初始化集群
```bash
POST http://127.0.0.1:8001/raft/init
Content-Type: application/json

{
  "nodes": [
    [1, {"raft_addr": "127.0.0.1:8001", "resp_addr": "127.0.0.1:7379"}],
    [2, {"raft_addr": "127.0.0.1:8002", "resp_addr": "127.0.0.1:7380"}],
    [3, {"raft_addr": "127.0.0.1:8003", "resp_addr": "127.0.0.1:7381"}]
  ]
}
```

### 添加 Learner
```bash
POST http://127.0.0.1:8001/raft/add_learner
Content-Type: application/json

{
  "node_id": 4,
  "node": {
    "raft_addr": "127.0.0.1:8004",
    "resp_addr": "127.0.0.1:7382"
  }
}
```

### 变更成员
```bash
POST http://127.0.0.1:8001/raft/change_membership
Content-Type: application/json

{
  "members": [1, 2, 3, 4]
}
```

## 配置文件格式

自动生成的节点配置文件示例（cluster_node1.conf）：

```ini
# Kiwi Cluster Node 1 Configuration

# Basic server settings
port 7379
binding 127.0.0.1

# Database path (each node needs its own)
db-path ./db_node1

# Raft configuration
raft-node-id 1
raft-addr 127.0.0.1:8001
raft-resp-addr 127.0.0.1:7379
raft-data-dir ./raft_data_1

# Storage settings
memory 1GB
rocksdb-max-background-jobs 4
rocksdb-max-write-buffer-number 2
rocksdb-write-buffer-size 67108864

# Logging
log-dir ./cluster_logs
```

## 故障排查

### 集群无法启动

1. 检查端口是否被占用：
   ```bash
   lsof -i :7379 -i :7380 -i :7381 -i :8001 -i :8002 -i :8003
   ```

2. 查看节点日志：
   ```bash
   tail -f cluster_logs/node1.log
   ```

3. 清理旧数据重新启动：
   ```bash
   ./scripts/clean_cluster.sh --stop --force
   ./scripts/start_cluster.sh
   ```

### 无法初始化集群

错误：`not allowed to initialize due to current raft state`

**原因：** 集群已经初始化过。

**解决：** 清理数据后重新启动：
```bash
./scripts/clean_cluster.sh --stop --force
./scripts/start_cluster.sh
./scripts/init_cluster.sh
```

### 没有 Leader

1. 检查所有节点是否运行：
   ```bash
   ./scripts/cluster_status.sh
   ```

2. 确认集群已初始化：
   ```bash
   curl http://127.0.0.1:8001/raft/metrics | jq
   ```

3. 等待选举完成（通常 2-5 秒）

### 数据不同步

**当前状态：** Raft 写入路径尚未完全实现。写操作直接在 leader 执行，不经过 Raft 复制。

**预期行为：** 未来实现后，所有写操作将通过 Raft 协议复制到所有节点。

## 开发计划

- [x] 集群启动和管理脚本
- [x] Raft 节点初始化和 leader 选举
- [x] Raft HTTP API
- [x] 状态机框架
- [ ] **Raft 写入路径集成**（待实现）
- [ ] 快照和日志压缩
- [ ] 动态成员变更
- [ ] 故障恢复测试

## 相关文档

- [产品概述](../README.md)
- [开发指南](../.kiro/steering/dev.md)
- [技术栈](../.kiro/steering/tech.md)
- [项目结构](../.kiro/steering/structure.md)
