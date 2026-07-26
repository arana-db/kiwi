# Redis 8.8.1 系统边界

> 状态：当前有效架构合同
> Redis source baseline：tag `8.8.1` / commit `77b6c308396c9700672390a210143a8496fb4b10`
> 当前运行模式：Cache OFF
> 热层状态：仅设计冻结，未获生产实现授权

关联决定：`D001`、`D009`、`D010`。

主要需求：`REQ-STORAGE-001..006`、`REQ-STABILITY-001..006`、`REQ-HOT-001..012`、`REQ-RAFT-001..008`。

## 1. 系统定义

Kiwi 是以 Redis 8.8.1 可观察语义为目标的 Rust 数据库。当前系统由 RESP 网络层、命令执行层、OpenRaft 一致性层和 RocksDB 权威存储层组成。

Redis 8.8.1 在当前阶段承担两种职责：

1. exact binary/source 作为外部兼容 Oracle；
2. 定义未来接口和 Embedded Redis Hot Tier 的来源基线。

Redis 派生源码或动态库当前不在 Kiwi 生产运行时内。

## 2. 当前 Cache OFF 拓扑

```text
                         test-only boundary
                    +-------------------------+
                    | Redis 8.8.1 exact Oracle|
                    +-------------------------+
                              ^
                              | raw differential / TCL
                              |
+--------+  RESP2/3  +-------------+  command  +-------------+
| Client |---------->| net / resp  |---------->| CmdExecutor |
+--------+           +-------------+           +------+------+
                                                        |
                                                        v
                                              +---------+---------+
                                              | StorageClient     |
                                              | async channel     |
                                              +---------+---------+
                                                        |
                                                        v
                                              +---------+---------+
                                              | StorageServer     |
                                              | Storage / RocksDB |
                                              +---------+---------+
                                                        |
                                            cluster: Binlog bridge
                                                        |
                                                        v
                                              +---------+---------+
                                              | RaftNode          |
                                              | quorum / apply    |
                                              +-------------------+
```

Oracle 和 Kiwi 是两个独立测试进程。Oracle 不保存 Kiwi 数据，不参加 Raft，不参与生产请求，也不成为 Kiwi 运行依赖。

## 3. 请求边界

### 3.1 Standalone

```text
Client
  → TCP accept / connection state
  → RESP parse
  → Redis 8.8.1 command lookup and validation
  → CmdExecutor
  → StorageClient
  → async message channel
  → StorageServer
  → Storage / RocksDB authoritative read/write
  → oneshot response
  → RESP encode
  → Client
```

### 3.2 Raft single group

```text
Client write
  → Redis 8.8.1 validation
  → deterministic Binlog / Batch construction
  → storage-runtime append_log_fn bridge
  → channel to network-runtime drain task
  → RaftNode.client_write
  → quorum commit
  → state-machine apply
  → RocksDB durability profile
  → StorageServer / StorageClient response
  → client reply
```

```text
Client linearizable read
  → Redis 8.8.1 validation
  → Leader / ReadIndex / approved lease gate
  → StorageClient / async channel / StorageServer
  → Storage / RocksDB authoritative read
  → reply
```

成功回复与 Commit、Apply、durability 的精确关系必须由公开配置和测试固定。连接在结果未知时断开，非幂等写入属于 `SUBMIT_UNKNOWN`，不能盲目重试。

## 4. 数据所有权

| 状态 | 当前权威来源 | Raft 复制 | Snapshot/Backup | 可重建 |
|---|---|---:|---:|---:|
| Redis 业务数据 | RocksDB state machine | 是 | 是 | 从持久状态恢复 |
| 类型和编码元数据 | RocksDB state machine | 是 | 是 | 从持久状态恢复 |
| TTL/absolute expire-at | RocksDB state machine | 是 | 是 | 从持久状态恢复 |
| Raft log/metadata | OpenRaft storage contract | 按 Raft 协议 | 按恢复合同 | 按日志/快照恢复 |
| Redis Oracle 数据 | 外部测试进程 | 否 | 否 | 测试临时数据 |
| Embedded Redis Hot Tier | 当前不存在 | 否 | 否 | 未来必须可从 RocksDB 重建 |

RocksDB 必须在没有任何 Redis 派生运行时组件时保存完整业务真相。

## 5. 组件责任

### 5.1 `net` / `resp`

- 连接生命周期、partial I/O 和 backpressure；
- RESP2/RESP3 二进制安全解析和编码；
- Push 与普通 response 排序；
- 协议错误后的连接行为。

### 5.2 `cmd` / `executor`

- Redis 8.8.1 命令元数据、arity、flags 和 key specification；
- 参数、类型、ACL 和错误优先级；
- 将非确定性输入转换为可复制的确定性操作；
- 不把未来热层命中与否写进命令语义。

### 5.3 `StorageClient` / `StorageServer` / dual-runtime bridge

- `StorageClient` 在网络运行时提交请求并等待 oneshot 响应；
- async message channel 提供网络运行时与存储运行时之间的有界通信；
- `StorageServer` 在存储运行时执行具体 `Storage` 操作；
- Cluster 写把确定性 `Binlog` 通过 `append_log_fn` 交给网络运行时 drain task，再调用 `RaftNode.client_write`；
- backpressure、取消、关闭和结果未知状态必须有界且可观测。

### 5.4 `raft`

- 写入顺序、Commit、Apply 和成员状态；
- Linearizable Read 门禁；
- Snapshot、恢复和 Leadership Transfer；
- 不复制任何未来热层状态。

### 5.5 `storage` / concrete RocksDB ownership

- RocksDB 全量权威数据；
- binary-safe stable encoding；
- TTL 绝对时间；
- WriteBatch 原子性；
- all-handle close/reopen 和格式迁移。

### 5.6 Compatibility tooling

- 构建并验证 exact Redis 8.8.1 Oracle；
- 保存 raw transcript 和最终状态；
- 管理 required、known difference、deferred、unsupported 和 skip；
- 保持测试依赖与生产依赖隔离。

## 6. 当前禁止的耦合

在热层重新获批前，生产代码不得：

- 链接或运行时加载 Redis 派生 native library；
- 包含 Redis internal header、`robj`、SDS 或内部 struct 布局；
- 以 Redis allocator 管理 Rust 所有权对象；
- 新增 Cache ON 配置、启动参数、隐藏环境变量或 feature flag；
- 在 RocksDB 读取前查询未获批的内存副本；
- 将 cache generation、eviction 或命中状态写入稳定磁盘格式；
- 让 compatibility Oracle 成为生产依赖；
- 因未来热层存在而降低 Cache OFF 正确性、恢复或性能门禁。

## 7. 未来热层接入缝

未来唯一允许评估的接入方式是版本化 native C ABI：

```text
Kiwi Rust process
  → controlled dynamic loader
  → versioned C ABI / opaque handle
  → Redis 8.8.1-derived native library
```

接入缝必须位于一致性门禁之后、RocksDB 权威读写周围，并遵守：

- 读命中不能跳过 OpenRaft gate；
- miss 始终回退 RocksDB；
- 写入以 RocksDB/Raft 结果为准；
- 热层更新失败执行 update-or-invalidate；
- known stale entry 不得返回；
- Snapshot Install、reopen、restore 和 generation change 使旧 entry 失效；
- 删除动态库后系统仍可用 Cache OFF 启动并保持正确性。

详细 ABI 见 `docs/architecture/redis-hot-tier-native-abi.md`。该文档只是设计冻结，不授权创建 loader、FFI 或动态库。

## 8. 未来发行边界

`arana-db/redis` fork、Redis 派生动态库和 Kiwi 官方组合包的许可证、provenance、hash、SBOM、NOTICE 与 Corresponding Source 要求见 `docs/architecture/combined-distribution-licensing.md`。

当前 Cache OFF 发行物不得包含占位、空壳或未启用的 Redis 派生二进制。只有 `docs/quality/system-stability-gate.md` 全部通过、用户重新批准并完成许可证复核后，才能建立组合发行实现任务。

## 9. 故障归属

| 故障 | 当前处理边界 |
|---|---|
| RESP malformed/partial input | net/resp 拒绝或关闭连接，不能崩溃进程 |
| Command validation failure | 返回 Redis 8.8.1 对应错误，不进入状态机 |
| Raft quorum unavailable | 按一致性合同拒绝、重定向或超时 |
| RocksDB write failure | 不返回成功；保留足够错误上下文 |
| RocksDB reopen failure | 启动失败或进入明确不可服务状态 |
| Snapshot corruption | 拒绝安装并保留可诊断证据 |
| Client disconnect during write | 标记 `SUBMIT_UNKNOWN`，查询后再决定 |
| Redis Oracle failure | 兼容测试失败；不影响生产运行 |
| Future hot-tier failure | 当前不存在；未来只能降级性能，不能改变结果 |

## 10. 热层准入条件

以下条件必须全部成立：

1. `docs/quality/system-stability-gate.md` 全部通过，并有 exact HEAD 证据；
2. required Redis 8.8.1 Cache OFF Profile 达到批准范围；
3. RocksDB authority、reopen、故障恢复和格式合同关闭 P0/P1；
4. OpenRaft 单 Group 一致性与故障门禁关闭 P0/P1；
5. ABI 和组合发行许可证完成技术与法律复核；
6. 用户基于上述证据重新明确批准热层实现。

任一条件缺失时，状态保持“设计冻结、实现未授权”。
