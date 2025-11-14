Kiwi (Rust)

Kiwi 是一个兼容 Redis 协议的存储引擎，采用 OpenRaft 提供强一致性。网络与存储运行时分离，写入在集群模式下通过 Raft 共识，数据持久化到 RocksDB。

快速开始：
- 运行 `kiwi-server`，默认绑定 `127.0.0.1:7379`
- 集群模式参考 `conf/kiwi.conf`，并在 `server/src/main.rs` 启动参数中选择 `--init-cluster`

关键路径：
- 强一致路由：`src/net/src/raft_network_handle.rs`
- 路由器：`src/raft/src/router.rs`
- Raft 节点：`src/raft/src/node.rs`
- 存储后端：`src/storage/src` 与 `src/raft/src/storage_engine/redis_storage_engine.rs`

更多详见 `docs/CONSISTENCY_README.md`。
