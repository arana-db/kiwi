# Kiwi

[English](README.md) | 简体中文

## 简介

Kiwi 是一个用 Rust 构建的 Redis 兼容键值数据库，通过 RocksDB 和 Raft 协议提供高容量、高性能和强一致性。

## 特性

- **双运行时架构**：网络和存储运行时分离，实现性能隔离
- **RocksDB 后端**：使用 RocksDB 作为持久化存储后端
- **Redis 协议兼容**：高度兼容 Redis 协议
- **Raft 共识算法**：集成 OpenRaft 实现强一致性和高可用性
- **适配器模式**：自定义适配器层连接存储与 OpenRaft
- **高性能**：通过专用线程池优化请求处理
- **异步通信**：基于消息通道的异步通信
- **故障隔离**：网络和存储操作在隔离的运行时中运行

## 系统要求

- 操作系统：Linux、macOS、FreeBSD 或 Windows
- Rust 工具链

## 安装

确保已安装 Rust 工具链。可以使用 [rustup](https://rustup.rs/) 安装：

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

## 快速开始

### 从源码构建

```bash
# 克隆仓库
git clone https://github.com/arana-db/kiwi.git
cd kiwi

# 快速检查（快速，推荐开发时使用）
cargo check

# 构建项目
cargo build --release

# 运行服务器（默认监听 127.0.0.1:7379）
cargo run --release
```

### 使用开发脚本

为了更快的开发工作流，使用提供的脚本：

**Linux/macOS:**
```bash
# 首次使用需要添加执行权限
chmod +x scripts/*.sh

# 快速检查（最快）
./scripts/dev.sh check

# 构建并运行
./scripts/dev.sh run

# 自动监视模式（文件保存时自动检查）
./scripts/dev.sh watch
```

**Windows:**
```cmd
# 快速检查（最快）
scripts\dev check

# 构建并运行
scripts\dev run

# 自动监视模式
scripts\dev watch
```

### 集群模式

对于集群模式，参考仓库根目录的 `config.example.toml` 或 `cluster.conf`，并在启动第一个节点时使用 `--init-cluster` 标志：

```bash
cargo run --release -- --config cluster.conf --init-cluster
```

### 推荐的开发工作流

#### 🚀 首次设置（自动提示）

当你第一次运行 `build` 或 `run` 时，脚本会自动提示你安装 sccache 和 cargo-watch：

```bash
⚠️  First-time setup recommended for optimal performance!
Run quick setup now? (y/n)
```

**按 'y'** 自动安装，或手动运行：

```bash
# Linux/macOS:
chmod +x scripts/quick_setup.sh
./scripts/quick_setup.sh

# Windows:
scripts\quick_setup.cmd
```

设置后，后续构建将**快 50-90%**！

#### 📝 日常开发

开发脚本**自动使用 sccache**（如果已安装）：

```bash
# Linux/macOS:
./scripts/dev.sh check   # 快速检查（比构建快 5-10 倍）
./scripts/dev.sh build   # 构建（自动使用 sccache）
./scripts/dev.sh run     # 运行（自动使用 sccache）
./scripts/dev.sh watch   # 文件保存时自动检查

# Windows:
scripts\dev check        # 快速检查
scripts\dev build        # 构建（自动使用 sccache）
scripts\dev run          # 运行（自动使用 sccache）
scripts\dev watch        # 文件保存时自动检查
```

**性能优化建议：**
- 开发时使用 `check` 进行快速语法检查（快 5-10 倍）
- 使用 `watch` 实现文件保存时自动检查（即时反馈）
- 只在需要执行程序时使用 `build` 或 `run`
- 脚本会自动使用 sccache（如果已安装，无需手动配置）

**为什么编译很慢？** 查看 [为什么重新编译？](docs/WHY_RECOMPILING.md) 了解诊断和解决方案。

详细的构建优化指南，请参见 [docs/BUILD_OPTIMIZATION.md](docs/BUILD_OPTIMIZATION.md) 或 [docs/编译加速方案总结.md](docs/编译加速方案总结.md)。

## 核心组件

- **Raft 网络处理器**：`src/net/src/raft_network_handle.rs`
- **路由器**：`src/raft/src/router.rs`
- **Raft 节点**：`src/raft/src/node.rs`
- **存储后端**：`src/storage/src` 和 `src/raft/src/storage_engine/redis_storage_engine.rs`

更多详情，请参见 `docs/CONSISTENCY_README.md`。

## Raft 共识集成

Kiwi 集成了 **OpenRaft** 库以提供分布式共识和高可用性：

- **适配器模式**：自定义适配器层连接 Kiwi 存储与 OpenRaft 的密封特征
- **基于 RocksDB 的 Raft 日志**：使用 RocksDB 持久化 Raft 日志
- **状态机复制**：跨集群节点的一致状态复制
- **快照支持**：为新节点或落后节点提供高效的状态传输

详细的集成文档，请参见 [docs/raft/OPENRAFT_INTEGRATION.md](docs/raft/OPENRAFT_INTEGRATION.md)。

## 开发路线图

- ✅ 双运行时架构实现性能隔离
- ✅ 基于消息通道的异步通信
- ✅ 基本 Redis 命令支持（GET、SET、DEL 等）
- ✅ 使用适配器模式集成 OpenRaft
- 🚧 支持大多数 Redis 命令
- 🚧 完整的集群模式实现
- 🚧 扩展命令支持和命令执行优化
- 🚧 增强模块化扩展能力及示例
- 🚧 完善的开发文档和用户指南
- 🚧 全面的指标和监控

## 文档

- [快速开始指南](docs/QUICK_START.md)
- [构建优化指南](docs/BUILD_OPTIMIZATION.md)
- [编译加速方案总结](docs/编译加速方案总结.md)
- [使用说明](docs/使用说明.txt)
- [脚本使用说明](scripts/README.md)

## 贡献

欢迎为 Kiwi 项目做出贡献！如果您有任何建议或发现问题，请提交 Issue 或创建 Pull Request。

## 许可证

根据 Apache License 2.0 许可。详见 [LICENSE](LICENSE)。
