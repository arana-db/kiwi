# Kiwi System Stability Gate

> 状态：Required before Embedded Redis Hot Tier implementation
>
> 基线：Redis 8.8.1
>
> Required 运行模式：Cache OFF
>
> 更新日期：2026-07-26

## 1. 目的

本门禁定义 Kiwi 在引入 Embedded Redis Hot Tier 之前必须达到的系统稳定基线。

门禁要证明：即使完全不存在热层，Kiwi 也能以 RocksDB 为全量权威存储，在 OpenRaft 约束下正确实现已声明的 Redis 8.8.1 可观察语义，并能够经受进程、网络、磁盘、恢复、升级和长期运行故障。

热层只能建立在稳定系统之上，不能用于掩盖或推迟兼容、持久化、一致性和恢复问题。

## 2. 适用边界

门禁通过前允许：

- 编写 Redis 8.8.1 来源、许可证、组合发行与 Corresponding Source 文档。
- 设计版本化 C ABI、错误码、allocator、线程、多实例和生命周期合同。
- 设计 pairing manifest、动态库身份校验、安全加载和可观测性接口。
- 设计 update-or-invalidate、generation、applied index 和 cache reset 不变量。
- 编写不会进入生产构建、不会被 Kiwi 加载的文档示例与伪代码。

门禁通过前禁止：

- 创建或修改用于 Kiwi 生产发行的 Redis fork 代码。
- 构建、打包或加载热层 `.so`、`.dylib`、`.dll` 或静态库。
- 在生产 crate 中加入热层依赖、FFI binding、loader、配置或运行时分支。
- 实现任何数据类型的 Cache ON 读写路径。
- 让 required 测试依赖热层，或用未来热层解释当前测试失败。

门禁通过不自动解除上述禁止。还必须取得用户对热层生产实现的新一轮明确批准。

## 3. Gate 判定规则

Gate Review 必须绑定：

- Kiwi exact commit。
- Redis 8.8.1 exact tag 与 commit。
- Rust、C/C++、protoc、RocksDB 和操作系统版本。
- 完整测试命令、配置、seed、开始/结束时间和退出码。
- CI URL 或本地不可变日志、测试报告、history 和 artifact hash。
- 所有 skip、known difference、flaky test 和豁免。

技术门禁判定只有四种：

- `PASS`：全部 required 条目通过，无未解决 P0/P1，证据完整且仍然有效。
- `FAIL`：任一 required 条目失败、缺证据、存在未解决 P0/P1 或状态无法重放。
- `BLOCKED`：由于明确的外部环境、平台或不可获得证据而无法完成 required 验证；必须列出 owner、解除条件和未覆盖风险。
- `EXPIRED`：评审后相关代码、依赖、构建方式、持久化格式或 Raft 协议发生实质变化，必须重新执行受影响门禁。

`PASS` 只是提出新热层规划与授权请求的必要条件，不是生产实现授权。M7 仍须用户在看到 Gate Review 后重新明确批准。

“大部分通过”“偶尔能过”“重跑后变绿”均不是 `PASS`。Flaky required test 按失败处理，直到找到原因并建立稳定回归证据。

## 4. G1：Redis 8.8.1 Cache OFF 兼容

### Required

- [ ] Oracle 固定到 Redis 8.8.1 exact source/binary，来源和 binary hash 可验证。
- [ ] RESP2/RESP3 使用 raw frame 比较，覆盖 binary payload、null、error、push、attribute 和 aggregate 类型。
- [ ] Pipeline 覆盖中间错误、partial I/O、半关闭、连接重置和 reply 顺序。
- [ ] Required 命令矩阵记录参数模式、返回类型、错误、TTL、ACL 和连接状态行为。
- [ ] Redis TCL external-server runner 固定 upstream commit；每个 skip 有 owner、Issue、理由和解除条件。
- [ ] redis-rs 只存在于测试边界，生产 server dependency graph 中不存在。
- [ ] Cache OFF 是 required CI 的显式配置，并且测试不隐式加载任何热层库。
- [ ] Required profile 全绿；不存在未登记差异。

### 阻断条件

- 任一客户端 typed conversion 掩盖 raw RESP 差异。
- 任一 required skip 没有 owner 或解除条件。
- Kiwi 只能依赖未实现的热层才能通过兼容测试。

## 5. G2：RocksDB 权威状态与恢复

### Required

- [ ] RocksDB 单独保存重建服务所需的全部权威业务状态。
- [ ] 写入、Raft metadata、last applied 与业务状态有明确原子性合同和测试。
- [ ] TTL 使用绝对时间，重启、时钟边界和过期清理不改变可观察语义。
- [ ] 所有 DB、Column Family、iterator、snapshot 和后台任务 handle 完全释放后，按原路径重新打开并验证状态。
- [ ] format version、Comparator identity、key encoding 和迁移/拒绝策略有测试。
- [ ] 覆盖部分写、尾部损坏、metadata/log 不一致、磁盘满、I/O error 和只读文件系统。
- [ ] Snapshot 创建、安装、中断、损坏和恢复不会生成未承诺状态。
- [ ] 备份恢复到新目录后能够独立启动并通过一致性核对。

### 阻断条件

- 恢复测试仍持有 `Storage`、`Redis`、`Arc<DB>`、column-family、iterator、snapshot、clone 或后台任务，因此没有证明真实 reopen。
- 成功回复对应的数据在批准 durability profile 下可能丢失。
- 未识别或不兼容的磁盘格式被静默打开。

## 6. G3：OpenRaft 正确性与公开接口

### Required

- [ ] `kiwi_redisraft_public_v1` 机器可读清单冻结，公开行为测试 100% 通过。
- [ ] 写成功回复发生在 quorum commit、本地 apply 和批准 durability profile 满足之后。
- [ ] Linearizable Read 通过 Leader/ReadIndex/Lease 合同，不存在绕行路径。
- [ ] Membership change、Leader transfer、Snapshot、日志回滚和重启有端到端测试。
- [ ] deterministic simulator 检查 Election Safety、Log Matching 和 State Machine Safety。
- [ ] 固定 seed 回归可以稳定重放；nightly random seed 失败会保存 seed、日志和 history。
- [ ] 3 节点和 5 节点均覆盖启动、选举、追赶、替换、缩扩容和滚动重启。
- [ ] 非幂等写断线被分类为 `SUBMIT_UNKNOWN`，不存在盲目自动重试。

### 阻断条件

- 以单元 Mock 或复用中的存储对象代替真实多进程、真实 RocksDB reopen。
- 公开 Raft 命令、错误或 Leader 行为存在未批准差异。
- 任一历史出现多个已提交结果、丢失已确认写或线性一致性违反。

## 7. G4：进程、网络与磁盘故障证明

### Required

- [ ] 覆盖 process kill、pause/resume、crash loop 和 restart storm。
- [ ] 覆盖 partition、drop、duplicate、delay、reorder 和 asymmetric connectivity。
- [ ] 覆盖磁盘满、fsync error、短写、损坏和恢复空间不足。
- [ ] 覆盖 Snapshot Install 与成员变更、Leader 变化和进程故障并发。
- [ ] Elle/Jepsen history checker 对批准 workload 全部通过。
- [ ] 每个故障测试有明确不变量、超时、资源清理和可重放证据。
- [ ] 故障后集群能够恢复服务，或以明确、可诊断的 fail-stop 状态拒绝服务。

### 阻断条件

- 失败历史缺少 seed、exact binary、时间线或客户端 operation ID，无法重放。
- 通过放宽断言、延长到无界超时或跳过失败用例获得绿色结果。
- 故障后只能删除数据目录才能重新启动。

## 8. G5：长期运行与资源边界

### Required

- [ ] 单节点 Cache OFF 稳定运行不少于 24 小时，覆盖持续写、TTL、Pipeline、大 Key 和后台 compaction。
- [ ] 3 节点 Cache OFF 稳定运行不少于 72 小时，期间执行滚动重启、Leader transfer 和至少一次 Snapshot。
- [ ] 长期运行期间无进程崩溃、死锁、任务永久泄漏或不可解释的数据差异。
- [ ] 文件描述符、线程、task、内存、WAL、临时文件和 RocksDB 空间增长具有上界或可解释稳态。
- [ ] P50/P95/P99/P99.9、吞吐、CPU、峰值内存、compaction stall、Raft commit/apply latency 被持续记录。
- [ ] 资源或延迟回归预算在运行前定义；超过预算即失败，不得事后放宽。

### 阻断条件

- 仅以平均吞吐或短时 benchmark 代替稳定性证据。
- 内存、文件、WAL、线程或 task 呈持续无界增长。
- 需要人工定期干预才能维持集群可用。

## 9. G6：工程质量与供应链

### Required

- [ ] Linux、macOS、Windows required CI 在同一 exact commit 上通过。
- [ ] `cargo fmt --check`、项目 lint、unit、integration 和 required Python/TCL 测试通过。
- [ ] 适用的 sanitizer、Miri、fuzz 和 dependency audit 已执行；未覆盖项有风险说明和 owner。
- [ ] 生产路径不存在外部输入可触发的 `unwrap()` 或无依据 `unsafe`。
- [ ] 依赖来源固定，可生成 SBOM，许可证与 notice 完整。
- [ ] Redis 8.8.1 组合发行方案、AGPL 义务和 Corresponding Source 清单已经过发布前法律复核计划确认。
- [ ] 日志不泄露敏感信息，并包含定位 term、index、node、request 和 storage failure 所需上下文。

### 阻断条件

- Required 平台未验证且没有批准的风险接受。
- 依赖使用浮动 branch、无法重建或来源不明的二进制。
- P0/P1 安全、数据、并发、资源或协议问题未关闭。

## 10. G7：运维、升级与恢复演练

### Required

- [ ] 从空环境按文档部署单节点和 3 节点集群。
- [ ] 备份、恢复、节点替换、证书/凭据轮换和容量告警有演练记录。
- [ ] 支持版本间滚动升级，或明确拒绝并提供停机升级流程。
- [ ] 升级失败可以按文档回滚，不依赖未记录的人工修复。
- [ ] 运维手册包含磁盘满、无 Leader、Snapshot 失败、日志损坏和 `SUBMIT_UNKNOWN` 处置。
- [ ] 发布兼容 manifest 能关联 binary、source、配置、磁盘格式和测试证据。

### 阻断条件

- 恢复步骤需要删除权威数据或跳过一致性检查。
- 升级/回滚只在开发目录验证，未在空环境和发行物上演练。
- 故障手册无法区分可安全重试与结果未知的写入。

## 11. P0/P1、差异和豁免

Gate Review 时：

- P0、P1 必须为零。
- P2 必须有 owner 和处理决定，但不机械阻断。
- Required compatibility difference 必须为零；非 required 差异必须写入机器可读清单。
- Required test 不接受永久 skip。
- 临时豁免必须写明范围、依据、owner、Issue、到期条件和移除验证；豁免不能覆盖数据损坏、线性一致性、安全或不可恢复风险。

## 12. Gate Review 证据包

最终证据包至少包含：

```text
gate-review/
  manifest.json
  git-state.txt
  toolchains.txt
  compatibility/
  storage-recovery/
  raft-simulator/
  process-faults/
  histories/
  soak/
  ci/
  security-supply-chain/
  operations/
  findings.md
  decision.md
```

`manifest.json` 必须列出每个产物的 SHA-256。`decision.md` 必须列出每项 Gate 的 PASS/FAIL、未覆盖范围、P0/P1 对账和用户批准记录。

## 13. 进入 Embedded Redis Hot Tier 的条件

只有同时满足以下条件，M7 才能由 Frozen 转为 Ready：

1. G1 至 G7 全部为 `PASS`。
2. Gate Review 绑定的代码和发行候选未发生使证据失效的变化。
3. 无未解决 P0/P1，无不可重放 required failure。
4. Redis 8.8.1 ABI、许可证、Corresponding Source 和安全加载设计已完成审查。
5. 用户在看到 Gate Review 后，重新明确批准开始热层生产实现。

如果后续回归破坏 G1 至 G7 中任何适用不变量，M7/M8 必须暂停，先恢复 Cache OFF 稳定基线。
