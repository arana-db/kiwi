# Kiwi

## 简介

Kiwi 是用 Rust 实现的基于 RocksDB 和 Raft 协议的 大容量、高性能、强一致性的 兼容 Redis 协议的 KV 数据库。
## 功能特色

- **双运行时架构**: 网络和存储运行时分离，实现性能隔离
- **RocksDB 后端**: 使用 RocksDB 作为后端持久化存储
- **Redis 协议兼容**: 高度兼容 Redis 协议
- **Raft 共识算法**: 集成 Openraft 实现强一致性和高可用
- **Adaptor 模式**: 自定义适配器层桥接存储与 Openraft
- **高性能**: 优化的请求处理和专用线程池
- **异步通信**: 基于消息通道的异步通信
- **故障隔离**: 网络和存储操作在隔离的运行时中运行

## 系统要求

- 操作系统：Linux、macOS、FreeBSD 或 windows
- Rust 编译工具链

## 安装指南

请确保已经安装了 Rust 工具链，可以使用 [rustup](https://rustup.rs/) 进行安装：

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

## 快速开始

- 运行 `kiwi-server`（默认监听 `127.0.0.1:7379`）
- 对于集群模式，请参考仓库根目录下的 `config.example.toml` 或 `cluster.conf`，并在启动第一个节点时使用 `--init-cluster` 参数

## 核心组件

- **Raft 网络处理器**: `src/net/src/raft_network_handle.rs`
- **路由器**: `src/raft/src/router.rs`
- **Raft 节点**: `src/raft/src/node.rs`
- **存储后端**: `src/storage/src` 和 `src/raft/src/storage_engine/redis_storage_engine.rs`

更多详情请参见 `docs/CONSISTENCY_README.md`。

## Raft 共识集成

Kiwi 集成了 **Openraft** 库来提供分布式共识和高可用性：

- **Adaptor 模式**: 自定义适配器层桥接 Kiwi 存储与 Openraft 的 sealed traits
- **基于 RocksDB 的 Raft 日志**: 使用 RocksDB 持久化 Raft 日志
- **状态机复制**: 跨集群节点的一致性状态复制
- **快照支持**: 为新节点或落后节点提供高效的状态传输

详细的集成文档请参见 [docs/raft/OPENRAFT_INTEGRATION.md](docs/raft/OPENRAFT_INTEGRATION.md)。

## 开发计划

- ✅ 双运行时架构实现性能隔离
- ✅ 基于消息通道的异步通信
- ✅ 基本 Redis 命令支持（GET、SET、DEL 等）
- ✅ 使用 Adaptor 模式集成 Openraft
- 🚧 支持大部分 Redis 指令
- 🚧 完成集群模式实现
- 🚧 扩展命令支持并优化命令执行效率
- 🚧 增强模块化扩展的功能并提供示例
- 🚧 完善开发文档和用户指南
- 🚧 添加全面的指标和监控

## 贡献

欢迎对 Kiwi 项目的贡献！如果你有任何建议或发现了问题，请提交 Issue 或创建 Pull Request。

## 许可证

本项目采用 Apache License 2.0 许可证。详情请参见 [LICENSE](LICENSE) 文件。
