# Kiwi RedisRaft Public Compatibility Profile v1

> Profile：`kiwi_redisraft_public_v1`
>
> 行为参考：RedisLabs/redisraft commit `ade4aa8e6aa5c3b21678a1998309825f06567d4f`
>
> 共识实现：OpenRaft

## 1. 原则

RedisRaft 是公开管理接口和客户端可见行为的参考，不是 Kiwi 的运行时依赖。普通 Redis 命令仍以 Redis 8.8.1 exact tag、commit `77b6c308396c9700672390a210143a8496fb4b10` 为 Oracle。

RedisRaft 源码和测试采用 RSALv2/SSPLv1。Kiwi 不复制其实现或测试代码，而是从公开命令和可观察行为形成 clean-room specification，再独立实现。

## 2. Required 命令

| 命令 | 兼容要求 |
|---|---|
| `RAFT.CLUSTER INIT [cluster-id]` | 参数、集群状态、reply schema 和错误 |
| `RAFT.CLUSTER JOIN addr:port [addr:port ...]` | 多地址、joining 状态和错误 |
| `RAFT.NODE ADD id addr:port` | `id=0` 自动分配、两元素回复、成员变更串行化 |
| `RAFT.NODE REMOVE id` | 节点校验、commit/apply 后 `OK` |
| `RAFT.TRANSFER_LEADER [node-id]` | 成功、非法目标、非 Leader、超时、意外 Leader |
| `INFO raft` | 稳定字段 schema 与兼容别名 |
| `CONFIG GET/SET raft.*` | 名称、单位、边界、可变性和错误合同 |

## 3. Deferred 与 Excluded

- `RAFT.SHARDGROUP GET/ADD/REPLACE/LINK`：延后到 Multi-Raft。
- `RAFT.TIMEOUT_NOW`：可作为受控 admin/test 能力，不承诺普通客户端稳定使用。
- `RAFT.DEBUG`：仅测试构建或受控管理接口。
- `RAFT.ENTRY`、`RAFT.AE`、`RAFT.REQUESTVOTE`、`RAFT.SNAPSHOT`、`RAFT.NODESHUTDOWN`：RedisRaft 内部 transport，不进入公共 Profile。
- `RAFT.IMPORT`、`RAFT.SCAN`：不进入 v1。

## 4. Required 错误和状态

至少固定：

```text
NOCLUSTER No Raft Cluster
NOCLUSTER No Raft Cluster (joining now)
LOADING Raft module is loading data
CLUSTERDOWN No raft leader
MOVED 0 host:port
CROSSSLOT Keys in request don't hash to the same slot
TIMEOUT no quorum for read
TIMEOUT not committed yet
ERR Already cluster member
ERR invalid node id
ERR node id does not exist
ERR node id has already been used in this cluster
ERR transfer timed out
ERR different node elected leader
```

Profile 必须区分 exact text、exact prefix 和 Kiwi 有意 deviation。任何 deviation 都进入 manifest，不能靠测试 skip 隐藏。

## 5. 写入和读取合同

成功写回复：

```text
quorum commit
AND local state machine apply
AND configured durability profile
```

当前 required 模式是 Cache OFF，不依赖任何热层状态。未来热层获准后，更新失败必须执行 update-or-invalidate，但不能改变上述成功回复边界。

断线或未收到回复不代表未执行。非幂等写进入 `SUBMIT_UNKNOWN`，查询 exact request/session/index 状态后才能决定是否补偿。

默认强一致读必须证明当前节点仍有有效 Leader/quorum 视图。新 Leader 在当前 term 的安全 apply 门禁满足前不得返回可能倒退的状态。

## 6. INFO 与 CONFIG

`INFO raft` 至少提供：

- implementation、version、OpenRaft version
- cluster/db ID、node ID、role、state、leader ID、term
- voting/member counts
- current/commit/last-applied index
- log bytes/entries/fsync
- snapshot index/term/size/status/duration
- peer connection/error/replication state
- client outstanding/proxy/redirect/timeout

RedisRaft 配置名称可以作为 compatibility alias，但默认值必须由 Kiwi 的 Tokio、OpenRaft、RocksDB 和目标网络测试决定，不能机械照抄 C libraft 默认值。

## 7. 三层测试

### A. Public golden transcript

- 对固定 RedisRaft binary/commit 采集命令、reply type、错误和状态。
- Kiwi 独立重放并对比。
- 不分发 RedisRaft binary，除非许可证审查明确允许。

### B. Deterministic OpenRaft simulator

借鉴 RedisLabs/raft BSD `virtraft2`：

- seed 调度
- drop/duplicate/delay/reorder/partition
- election safety
- log matching
- state machine safety
- membership/snapshot/compaction
- independent reference model
- failure history 最小复现

### C. Process-level distributed tests

- 3/5 节点独立目录和进程。
- graceful stop、kill、pause、restart。
- partition、磁盘错误、Snapshot Install 中断。
- 未 Commit log overwrite。
- RocksDB close/reopen。
- Elle/Jepsen history。

RedisRaft suite 中的大量 skip 和本地 TTL/Lua 局限不能成为 Kiwi 的兼容上限。
