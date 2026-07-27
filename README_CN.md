# Kiwi

[English](README.md) | 简体中文

## 简介

Kiwi 是一个用 Rust 构建的 Redis 兼容键值数据库，通过 RocksDB 和 Raft 协议提供高容量和强一致性。

## 特性

- **双运行时架构**：网络和存储运行时分离，实现性能隔离
- **RocksDB 后端**：使用 RocksDB 作为持久化存储后端
- **Redis 协议兼容**：高度兼容 Redis 协议
- **Raft 共识**：集成 OpenRaft 实现强一致性和高可用性
- **适配器模式**：自定义适配器层连接存储与 OpenRaft
- **高性能**：通过专用线程池优化请求处理
- **异步通信**：基于消息通道的异步通信
- **故障隔离**：网络和存储操作在隔离的运行时中运行

## 架构

```text
src/server/    → 入口 (main.rs)
src/net/       → TCP 服务、连接管理、集群路由
src/cmd/       → 命令定义：Cmd trait、CmdMeta、命令表
src/executor/  → 命令执行器：tokio 异步任务池
src/storage/   → 多实例 RocksDB 所有权、列族、TTL
src/resp/      → RESP 协议：解析、编码、RespData 类型
src/raft/      → Raft 共识：OpenRaft 集成、RocksDB 日志存储、状态机、路由器
src/conf/      → 配置：加载、校验、集群配置
src/client/    → 客户端上下文：连接状态、参数、响应缓冲
src/common/runtime/ → 运行时管理：网络与存储间的异步通道
src/common/macro/   → 过程宏：#[stack_trace_debug]
src/kstd/      → 工具：LockMgr（分片 key 级锁）
```

### 请求流程

```text
Client → TCP accept [网络运行时] → RESP 解析 → 命令查找
  → CmdExecutor [网络运行时] → Cmd.execute() 调用 StorageClient
    → MessageChannel →
  → StorageServer [存储运行时] → RocksDB
    ← oneshot 响应 ←
  → RESP 编码 [网络运行时] → 写回客户端
```

## 开发路线图

- ✅ 双运行时架构实现性能隔离
- ✅ 基于消息通道的异步通信
- ✅ 基本 Redis 命令支持（GET、SET、DEL 等）
- ✅ 使用适配器模式集成 OpenRaft
- 🚧 大多数 Redis 命令
- 🚧 完整的集群模式
- 🚧 扩展命令支持与执行优化
- 🚧 模块化扩展能力
- 🚧 全面的指标和监控

## 快速开始

### 环境要求

Kiwi 的普通开发、CI 和发布基线是精确固定的 Rust 1.97.1 stable。克隆仓库后，
`rust-toolchain.toml` 会让 rustup 自动选择该工具链。所有 Kiwi workspace crate
均使用 Rust 2024 Edition。

项目还必须安装 `protoc`，以及编译 RocksDB 所需的原生 C/C++ 工具。Windows
使用 Rust MSVC target，并安装 Visual Studio C++ 构建工具；Linux 和 macOS 除
`protoc` 外，还需安装项目在对应平台使用的 C/C++ 构建依赖。完整的平台安装
命令见[开发指南](docs/development.md#prerequisites)。

```bash
# 安装 rustup；进入本仓库后由 rust-toolchain.toml 选择 Rust 1.97.1 stable
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# macOS 原生 C/C++ 工具、protobuf 编译器和构建依赖
xcode-select --install
brew install protobuf cmake

# Debian/Ubuntu Linux 原生 C/C++ 工具、protobuf 编译器和构建依赖
sudo apt install clang cmake libclang-dev llvm-dev pkg-config protobuf-compiler
```

Windows 使用与 CI 一致的官方 Protobuf 27.1。请在 PowerShell 中下载并解压
`protoc-27.1-win64.zip`，将解压目录下的 `bin` 加入 `PATH`，然后运行
`protoc --version`；可直接使用[开发指南中的 PowerShell 命令](docs/development.md#prerequisites)。

固定日期的 nightly 只用于 Sanitizer 等专项检查，不定义普通开发或发布基线。

### 获取代码

```bash
git clone https://github.com/arana-db/kiwi.git
cd kiwi

# 核验当前 checkout 实际选择的编译器
rustup show active-toolchain
rustc --version --verbose
```

### 单机模式

```bash
make standalone                    # 构建（release）并启动 kiwi，监听 127.0.0.1:7379

# 另一个终端：
redis-cli -p 7379 set foo bar     # OK
redis-cli -p 7379 get foo         # "bar"
```

### 集群模式

```bash
make cluster                       # 启动 3 节点 Raft 集群（默认），节点 RESP 端口为 7379/7380/7381
make cluster NODES=5               # 启动 5 节点集群

# 另一个终端（连接节点 1）：
redis-cli -p 7379 set foo bar     # OK
redis-cli -p 7379 get foo         # "bar"
```

手动步骤和 Raft 架构细节见 [docs/cluster.md](docs/cluster.md)。

## 文档

| 文档 | 说明 |
|------|------|
| [docs/development.md](docs/development.md) | 开发环境、构建优化、sccache |
| [docs/cluster.md](docs/cluster.md) | Raft 集群快速入门与写路径验证 |
| [docs/key-encoding.md](docs/key-encoding.md) | Key 编码内部实现 |
| `kiwi --sample-config` | 生成默认配置文件 |
| `kiwi --full-sample-config` | 生成包含所有配置项的完整配置文件 |

## 依赖说明

### RocksDB（Arana 维护的 Fork）

Kiwi 使用 [Arana 维护的 rust-rocksdb fork](https://github.com/arana-db/rust-rocksdb) 提供 Storage LogIndex 所需的 TableProperties Collector/Factory FFI。依赖使用维护发布标签 [`v0.51.0-arana.2`](https://github.com/arana-db/rust-rocksdb/tree/v0.51.0-arana.2)。已发布的标签不得移动，同时由 `Cargo.lock` 记录解析后的精确提交，保证构建可审计、可复现。

## 贡献

欢迎贡献。指南见 [CONTRIBUTING.md](CONTRIBUTING.md)。

## 许可证

Apache License 2.0。详见 [LICENSE](LICENSE)。
