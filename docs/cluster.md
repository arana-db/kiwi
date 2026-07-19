# 集群快速入门与写路径验证

本指南介绍如何在单机和 Raft 集群模式下运行 Kiwi，以及如何手动验证 Raft 写路径（leader 写入复制到 follower）。

> 自动化多节点集成测试在另外的流程中管理。以下步骤是用于验证写路径集成的手动流程。

## 环境要求

- Rust 工具链（stable）和 `protoc`（参见项目 README / `docs/development.md`）。
- `redis-cli` — 用于操作 RESP 端口（`brew install redis` / `apt install redis-tools`）。
- `grpcurl` — 用于调用 Raft admin gRPC API 进行集群初始化（`brew install grpcurl`）。
  Raft gRPC 服务已启用反射，无需 `.proto` 文件。

构建服务端二进制文件：

```bash
cargo build --release --bin kiwi   # 二进制文件位于 target/release/kiwi
```

## 单机模式（默认）

在没有 Raft 配置的情况下，Kiwi 作为单个独立节点运行。写入直接到本地 RocksDB（无共识）。

```bash
cargo run --release --bin kiwi          # 默认监听 127.0.0.1:7379
redis-cli -p 7379 set k1 hello          # OK
redis-cli -p 7379 get k1                # "hello"
```

## 集群模式

当配置文件中包含以下 Raft 配置项时，节点以集群模式运行。配置文件使用 Redis 风格的 `key value` 格式（每行一个）。

### 1. 为每个节点编写配置

`node1.conf`:

```conf
port 7379
binding 127.0.0.1
data-dir /tmp/kiwi/n1/db
raft-node-id 1
raft-addr 127.0.0.1:8501
raft-resp-addr 127.0.0.1:7379
raft-data-dir /tmp/kiwi/n1/raft
```

类似地创建 `node2.conf` 和 `node3.conf`，修改 `port`/`raft-addr`/`raft-resp-addr`/`data-dir`/`raft-data-dir` 和 `raft-node-id`（分别为 2 和 3）。保持 `raft-resp-addr` 等于各节点的 `<binding>:<port>`——这是返回给客户端的重定向地址。

Raft 配置项说明：

| 配置项 | 含义 |
|-----|---------|
| `raft-node-id` | 唯一节点 ID（`u64`） |
| `raft-addr` | Raft 内部通信的 gRPC 地址 |
| `raft-resp-addr` | 对外公布的 RESP 地址（用于 `MOVED` 重定向） |
| `raft-data-dir` | Raft 日志和快照的持久化存储目录 |

Raft 日志始终持久化到 RocksDB。节点重启时会从 `raft-data-dir` 恢复投票状态、已提交位置和日志条目。每个节点必须使用独立且稳定的目录；节点重启或升级时不要删除该目录。

### 2. 启动节点

```bash
RUST_LOG=info ./target/release/kiwi --config node1.conf &
RUST_LOG=info ./target/release/kiwi --config node2.conf &
RUST_LOG=info ./target/release/kiwi --config node3.conf &
```

此时每个节点都在各自的 RESP 端口和 Raft gRPC 端口上监听。在集群初始化之前没有 leader，因此**写入会被拒绝**，返回 `ERR not leader`。

### 3. 初始化集群

在任意节点的 Raft gRPC 端口上调用一次 `Initialize`，列出所有成员：

```bash
grpcurl -plaintext -d '{"nodes":[
  {"node_id":1,"raft_addr":"127.0.0.1:8501","resp_addr":"127.0.0.1:7379"},
  {"node_id":2,"raft_addr":"127.0.0.1:8502","resp_addr":"127.0.0.1:7380"},
  {"node_id":3,"raft_addr":"127.0.0.1:8503","resp_addr":"127.0.0.1:7381"}
]}' 127.0.0.1:8501 kiwi.raft.v1.RaftAdminService/Initialize
# => { "response": { "success": true, "message": "OK" }, "leaderId": "1" }
```

leader 会在选举超时窗口内（一两秒）选出。

## 验证写路径

### 找到 leader

写入操作在 leader 上返回 `OK`，在 follower 上返回 `MOVED <leader-resp-addr>`：

```bash
redis-cli -p 7379 set probe v   # OK            -> node1 是 leader
redis-cli -p 7380 set probe v   # MOVED 127.0.0.1:7379
redis-cli -p 7381 set probe v   # MOVED 127.0.0.1:7379
```

### Leader 写入 → follower 读取（复制）

```bash
# 在 leader 上写入
redis-cli -p 7379 set repltest hello_from_leader   # OK
redis-cli -p 7379 incr counter                     # 1

# 从 follower 读取（最终一致——等待复制完成）
sleep 1
redis-cli -p 7380 get repltest    # "hello_from_leader"
redis-cli -p 7381 get repltest    # "hello_from_leader"
redis-cli -p 7380 get counter     # "1"
```

端到端链路：命令 → leader 网关（在 leader 上通过）→ `BinlogBatch` 捕获编码后的 CF 变更 → Raft `client_write`（共识）→ 各节点的状态机通过 `on_binlog_write` 应用到本地 RocksDB → follower 本地读取可观察到复制后的值。

## 注意事项与当前限制

- **读取为最终一致性。** follower 提供本地读取；值只有在日志条目复制并应用后才可见。线性一致性读（读屏障）尚未实现。
- **`MOVED` 为简化实现。** Kiwi 返回 `MOVED <addr>`（无 hash slot），不同于 Redis Cluster 的 `MOVED <slot> <ip:port>`。现成的集群感知客户端不会自动跟随该重定向；需手动重连到返回的地址。
- **快照安装。** Raft 快照安装后，节点的存储会被替换，当前不会重新设置 Raft 写入钩子——该节点上的写入会绕过共识，直到重启。这是一个已知的待修复问题，需在生产集群使用前解决。
- **仅 leader 可写。** follower 拒绝写入，返回 `MOVED` / `ERR not leader`。任意节点均可读取。
