# Kiwi

[English](README.md) | 简体中文

## 简介

Kiwi 是一个以 Redis 8.8.1 可观察行为为兼容目标的 Rust 数据库。RocksDB 保存全量、权威、可恢复的数据，OpenRaft 提供强一致与高可用。

兼容与接口设计基线固定为 Redis tag `8.8.1`、commit `77b6c308396c9700672390a210143a8496fb4b10`。当前 required 工作统一运行在 Cache OFF 模式，重点推进兼容性、权威恢复、Raft 正确性和系统稳定性。内嵌 Redis 8.8.1 原生内存热数据层目前只保留未来设计边界；稳定性门禁通过并获得单独批准前，不进入实现。

## 特性

- **双运行时架构**：网络和存储运行时分离，实现性能隔离
- **RocksDB 权威存储**：保存全量、持久、可恢复的数据
- **Redis 8.8.1 兼容目标**：协议、命令、错误、TTL、事务和客户端行为必须对照 exact upstream 验证
- **稳定优先交付**：兼容性、RocksDB 真正 close/reopen 恢复和 OpenRaft 单 Group 正确性必须先通过系统稳定性门禁，才重新评估延期的加速工作
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

项目北极星、需求、阶段路线、当前状态和 Kanban 统一维护在：

- [项目宪法](.planning/PROJECT.md)
- [可验收需求](.planning/REQUIREMENTS.md)
- [唯一路线图](.planning/ROADMAP.md)
- [当前状态](.planning/STATE.md)
- [Kanban](.planning/KANBAN.md)

README 不再维护第二份容易漂移的路线清单。

## 实现状态

Kiwi 以 Redis 8.8.1 兼容为目标，但兼容目前仍是**进行中的目标**，而非已交付状态。已知的差距与代码审查结论以 [`code-review-findings.md`](code-review-findings.md) 为准。在假定某个 Redis 命令或行为已完整支持之前，请先查阅该文件，并参考上文相关门禁文档确认当前的稳定性门槛。

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
| [Redis 8.8.1 兼容合同](docs/compatibility/redis-8.8.1.md) | Exact Oracle、原始 RESP、TCL 和客户端测试边界 |
| [Redis 8.8.1 系统边界](docs/architecture/redis-8.8.1-system-boundaries.md) | Cache OFF 请求、存储和共识边界 |
| [系统稳定性门禁](docs/quality/system-stability-gate.md) | 重新评估延期热层工作前的 required 证据 |
| [延期的 Native ABI 合同](docs/architecture/redis-hot-tier-native-abi.md) | 未来接口设计，不构成实现授权 |
| [组合发行许可证设计](docs/architecture/combined-distribution-licensing.md) | 未来 Redis 派生库和源码发行义务 |
| [产品需求文档 (PRD)](docs/prd.md) | 目标、范围与 Redis 8.8.1 兼容基线 |
| [工程质量门禁](docs/quality/quality-gates.md) | 代码、测试与发布质量门禁 |
| [人物角色与用户故事](docs/personas-and-user-stories.md) | 目标用户与使用场景 |
| [文档索引](docs/INDEX.md) | 整个 `docs/` 树的地图与建议阅读顺序 |
| [设计计划与规格](docs/superpowers/) | 带日期的设计记录（plans + specs） |
| `kiwi --sample-config` | 生成默认配置文件 |
| `kiwi --full-sample-config` | 生成包含所有配置项的完整配置文件 |

## 依赖说明

### RocksDB（Arana 维护的 Fork）

Kiwi 使用 [Arana 维护的 rust-rocksdb fork](https://github.com/arana-db/rust-rocksdb) 提供 Storage LogIndex 所需的 TableProperties Collector/Factory FFI。依赖使用维护发布标签 [`v0.51.0-arana.2`](https://github.com/arana-db/rust-rocksdb/tree/v0.51.0-arana.2)。已发布的标签不得移动，同时由 `Cargo.lock` 记录解析后的精确提交，保证构建可审计、可复现。

## 贡献

欢迎贡献。指南见 [CONTRIBUTING.md](CONTRIBUTING.md)。

## 许可证

Kiwi 自有源码采用 Apache License 2.0。详见 [LICENSE](LICENSE)。未来包含 Redis 派生原生库的官方组合发行必须履行适用的 AGPL-3.0-only 义务，具体边界记录在架构文档和 [第三方通知](THIRD_PARTY_NOTICES.md)。
