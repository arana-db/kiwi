# Kiwi 强一致模式集成说明

## 概述

Kiwi 使用 OpenRaft (v0.9.x) 提供强一致性，网络层采用双运行时架构，存储层为 RocksDB。Raft 状态机通过 `RedisStorageEngine` 将 Redis 协议映射到持久化存储。

## 关键组件

- `RaftNode`：封装 OpenRaft，负责领导选举、日志复制、配置变更
- `RequestRouter`：在集群模式下将写入通过 `RaftNode.propose()` 路由，读取按一致性级别处理
- `RedisStorageEngine`：Raft 状态机使用的后端，调用 `storage::Redis` 对 RocksDB 进行读写
- `RaftStorage`：Raft 日志存储（独立于业务数据），支持日志读取、范围检索

## 行为说明

- 写操作（如 SET/DEL）在集群模式下由路由器提交到 Raft 并等待多数确认
- 非主节点写入返回 `MOVED <slot> <host:port>` 或 `CLUSTERDOWN`，与 Redis 协议兼容
- 读操作支持线性一致与最终一致两种模式，可在路由器中扩展

## 启动配置

- `server/src/main.rs` 根据配置启动集群模式并初始化 `RaftNode`
- `raft/src/node.rs` 在集群数据目录下创建 `redis_db` 并打开 RocksDB，引导 `RedisStorageEngine`

## 测试参考

- `src/raft/tests/redis_engine_integration_test.rs` 验证通过 Raft 提交的 `SET` 能持久化并可线性一致读取 `GET`

## 与参考项目对齐

- 可参考 RedisRaft 的测试场景验证领导切换与重定向行为
- 与 braft/kiwi-cpp 架构一致：强一致性写入、客户端重定向、快照与日志压缩