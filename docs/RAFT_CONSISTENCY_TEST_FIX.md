# Raft 集群一致性测试修复

## 问题描述

`src/raft/src/cluster_tests.rs` 中的 `test_consistency_after_failover` 此前被 `#[ignore]` 标记，且最终调用的 `verify_data_consistency` 只通过 `cluster.read()` 从当前领导者读取数据，无法检测单个副本是否落后。结果是，文档声称的“验证故障转移后的数据一致性”“多个键值对的一致性验证”在实现上并未真正覆盖。

## 风险分析

1. **测试计划要求**：`tests/raft_test_plan.md` 明确要求覆盖故障与数据一致性场景，缺少该测试会直接违背计划。
2. **风险影响**：若领导者以外的副本落后或数据损坏，现有测试无法发现，线上故障转移后可能读取到陈旧数据。
3. **测试完整性**：ThreeNodeCluster 已具备模拟故障转移与节点控制的能力，只验证领导者让测试结果产生“虚假的安全感”。

## 改进方案

### 1. 启用测试

- 移除 `#[ignore]`，`test_consistency_after_failover` 默认随 `cargo test` 运行。
- 继续保留详尽的日志与较长的超时时间，保证在开发机和 CI 上具有足够稳定性。

### 2. 真正的跨节点读取

- 引入 `read_key_from_node`，直接通过每个节点的状态机执行 `EXISTS`/`GET` 组合（`ConsistencyLevel::Eventual`），避免任何领导者重定向。
- `verify_data_consistency` 现在会遍历所有节点，记录 `(node_id, value)` 列表，只要有缺失、不一致或与期望不符的值就输出详细日志并返回 `false`。

### 3. 测试流程回顾

1. 创建并启动三节点集群。
2. 等待初始 leader 选举并写入基准数据。
3. 故障前校验所有节点数据。
4. 停止 leader，等待新 leader 选举完成。
5. 在 failover 后验证旧数据、新写入数据以及最终 `verify_data_consistency` 的结果。
6. 完整写入/读取路径都在测试中覆盖，便于排查。

## 当前测试状态

测试已默认开启，运行方式：

```bash
cargo test -p raft cluster_tests::test_consistency_after_failover -- --nocapture
```

如需单独观察日志，可加 `RUST_LOG=info`。

## 后续建议

1. **合并前必跑**：在本地或 CI 中保证该测试通过，及时捕获复制层回归。
2. **CI 监控**：若后续在共享 CI 中运行时间过长，可将其纳入夜间回归或单独的“长测”流水线，但不再忽略。
3. **性能调优**：必要时可以缩短 sleep/timeout 或并行运行其他测试，但要确保不会牺牲一致性验证。

## 结论

`test_consistency_after_failover` 现已通过真实的跨节点读取验证所有副本，能够侦测任何节点落后、缺失或数据错误，与文档中“验证故障转移后的数据一致性”和“多个键值对的一致性验证”的描述保持一致。
