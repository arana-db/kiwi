# RocksDB-only Raft 日志修复设计

## 目标

Kiwi 的正常构建和生产路径只允许使用 `RocksdbLogStore`。删除的内存 Raft `LogStore` 不得以 fallback、feature 或迁移 helper 的形式恢复。

## 边界

- 本修复只处理 Raft 日志存储，不删除 `src/common/runtime` 的 Tokio 运行时与请求调度。
- 不尝试从主数据或 snapshot 自动生成丢失的 vote、membership、term、committed position 或日志尾部。
- 已有有效 RocksDB Raft 状态的节点继续正常打开，即使目录中还留有旧 `raft_logs` 目录。

## 升级安全门禁

`create_raft_node()` 仍只构造 `RocksdbLogStore`。打开 `${raft-data-dir}/raft_logs_rocksdb` 后，如果发现 `${raft-data-dir}/raft_logs` 这个旧内存模式遗留目录，则读取 RocksDB LogStore 的 vote、committed、last log 和 last purged。

若四类 durable Raft 状态全部为空，则拒绝创建 Raft 节点。错误必须说明无法安全原地迁移，运维人员应使用新 node ID 和干净数据目录从健康 leader 重新入群，或者在确定建立全新集群时同时清理旧主数据与 Raft 目录。

这一检查只是迁移防护，不是第二种存储实现。

## 持久化测试

新增的 RocksDB reopen 测试必须通过真实 `RocksdbLogStore::open(path)` 执行两个阶段：

1. 第一阶段通过 `blocking_append()`、`save_vote()`、`save_committed()` 和 `purge()` 写入状态。
2. 销毁所有 store、reader、engine 和 DB handle。
3. 对同一路径重新调用 `RocksdbLogStore::open(path)`。
4. 验证完整 vote、committed `LogId`、last log、last purged 和未被 purge 的日志内容。

测试不得直接向 engine 写入日志代替被测接口。

## 文档与成功标准

- `docs/cluster.md` 明确旧内存日志节点不能原地恢复，应从健康集群重新入群。
- 实施计划的残留引用扫描排除计划文件自身。
- 仓库中没有可执行的内存 Raft LogStore 或运行时选择字段。
- 旧内存模式遗留目录与空 RocksDB Raft 状态的组合必须启动失败。
- 已有 RocksDB Raft 状态的节点可以正常重启。
- 真实 close/reopen 测试以及 `fmt`、`conf`、`raft`、`server check`、workspace Clippy 全部通过。
