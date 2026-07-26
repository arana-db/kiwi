# Kiwi 工程质量门禁

## 1. 完成的定义

工作项只有同时满足以下条件才能进入 Done：

1. 关联 Requirement 和设计规范。
2. 先有能失败的测试或明确的只读文档验证。
3. 实现、测试、文档和兼容 manifest 一致。
4. 运行适当层级的验证并保存命令、环境和结果。
5. 没有隐藏的 required test skip。
6. `.planning/STATE.md`、`.planning/KANBAN.md` 和本机 ACTIVE checkpoint 已更新。

## 2. 每个 PR 的快速门禁

```text
license header / third-party license check
cargo fmt --check
cargo clippy --all-features --workspace -- -D warnings -D clippy::unwrap_used
targeted unit tests
affected compatibility manifest tests
Cache OFF compatibility and recovery evidence
affected deterministic Raft seeds
```

只有系统稳定性门禁通过且用户重新明确批准热层实现后，相关 PR 才增加 Cache OFF/ON differential；当前 required PR 门禁不得依赖 Cache ON。

核心逻辑 PR 必须在 Linux/WSL 验证；Windows 结果不能替代目标运行环境。

## 3. Nightly 门禁

- Redis 8.8.1 exact TCL required set。
- raw wire differential expanded corpus。
- parser/property/fuzz smoke。
- random-seed Raft simulator。
- 3/5 节点故障矩阵。
- sanitizer/Miri 适用集合。
- dependency/license/SBOM audit。
- 性能 smoke 与历史预算比较。

## 4. Release 门禁

- required compatibility profiles 全绿。
- 无未解释 P0/P1。
- 最终 exact HEAD 复核。
- backup/restore、upgrade/rollback 演练。
- RocksDB all-handle close/reopen。
- Snapshot bootstrap 和损坏恢复。
- 进程、网络和磁盘故障报告。
- 第三方许可证、NOTICE、SBOM 和源码提供义务完整。
- Redis 8.8.1 / Kiwi Cache OFF 性能报告；热层获准并完成正确性门禁后再加入 Kiwi Cache ON 对照。

## 5. 热层专项门禁

本节是延期功能的未来验收合同。系统稳定性门禁通过并由用户重新明确批准之前，不执行 Cache ON 测试，不新增生产热层依赖、动态库、FFI、loader、配置或运行时路径。

- Cache OFF 是正确性基线。
- Cache ON 所有结果与 OFF 相同。
- update failure 后无旧值。
- fill/write 竞争不安装旧版本。
- generation change 使旧 task 失效。
- eviction/expire/queue full 只影响性能。
- Snapshot/reopen/restore 后从空 Cache 启动。
- 内存硬上限和 no-progress/OOM 行为可控。

## 6. Raft 专项门禁

- Election Safety、Log Matching、State Machine Safety。
- committed entry 不被覆盖。
- membership change 串行和恢复。
- Leader Transfer 全场景。
- quorum loss 下读写符合合同。
- `SUBMIT_UNKNOWN` 不盲重试。
- Snapshot Install 与并发写入。
- RocksDB/metadata/log 不一致恢复。
- Elle/Jepsen history 无一致性违规。

## 7. 性能报告最低字段

```text
Kiwi commit / Redis commit
OS / kernel / CPU / memory / storage
Rust / C compiler / RocksDB / OpenRaft version
Kiwi config / Redis config
dataset / key-value size / command mix
client / connections / pipeline depth
duration / warmup / repetitions
throughput
P50 / P95 / P99 / P99.9
CPU / RSS / allocator / cache bytes
RocksDB stall / compaction / write amplification
Raft commit / apply latency
```

缺少环境和尾延迟的单个 QPS 数字不能成为架构决定依据。
