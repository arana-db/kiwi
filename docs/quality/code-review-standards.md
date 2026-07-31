# Kiwi 代码审查标准与流程

本文件定义 Kiwi 项目的代码审查标准、严重程度分级、审查流程和门禁规则。所有合并到 `main` 的代码变更都必须经过至少一次符合本标准的审查。

## 1. 目的与适用范围

- **目的**：在保持 Redis 8.8.1 兼容性、RocksDB 权威数据安全和 OpenRaft 正确性的前提下，系统性地拦截缺陷、降低维护成本、促进知识共享。
- **适用范围**：所有 `src/` 下 Rust 源码、`tests/`、`benches/`、`tools/`、`scripts/` 以及影响构建/CI 的配置文件（`Cargo.toml`、`rust-toolchain.toml`、`.yml` 等）。
- **不适用**：`.planning/`、`.codex/recovery/` 等运行时规划状态文件（由各自的更新协议约束）。

## 2. 严重程度分级

审查意见必须标注严重程度，便于作者排定修复优先级。

| 级别 | 标记 | 含义 | 处理要求 |
|------|------|------|----------|
| **阻断** | 🔴 `blocker` | 不修复不能合并。包括安全漏洞、数据丢失/损坏风险、竞态/死锁、破坏 API 兼容契约、关键路径缺失错误处理。 | 合并前必须修复或回退 |
| **建议** | 🟡 `suggestion` | 应修复。包括输入校验缺失、命名/逻辑不清、重要行为缺测试、性能问题（N+1、无界队列、不必要克隆）、应抽取的重复代码。 | 尽量在本次 PR 修复；否则开跟踪 issue |
| **建议讨论** | 🟠 `question` | 意图不明确，需作者澄清。不要在未理解意图时假定代码错误。 | 作者回复后决定是否改动 |
| **细节** | 💭 `nit` | 风格、命名微调、文档补充、可选的替代写法。 | 由作者自行决定，不阻塞 |

## 3. Rust / Kiwi 专项检查清单

### 3.1 错误处理（最高优先级）

Kiwi 是数据库，panic 会拖垮整个进程并威胁数据安全。这是审查的首要关注点。

- 🔴 生产路径禁止 `unwrap()` / `expect()` / `panic!()` / `unreachable!()` / `unimplemented!()`。项目已在 `clippy` 中 `-D clippy::unwrap_used`，审查时同样口径。
  - 测试代码允许，但必须在测试模块顶部加 `#![allow(clippy::unwrap_used)]`。
  - 真正的不变量断言优先用 `expect("说明为什么不可能")` 而非无消息的 `unreachable!()`；能返回 `Result` 的场景一律返回 `Result`。
- 🔴 反序列化/解码路径遇到未知值不得 panic。数据库从磁盘读到的数据可能因版本升级或损坏而包含未知判别式，应返回错误而非 `unreachable!()`。
- 🔴 公开 API 不得用 `unimplemented!()` 占位。未实现的能力要么不暴露，要么返回 `Err`，并附 issue 跟踪。
- 🟡 不要用 `let _ = result` 默默丢弃可能失败的操作结果，除非有明确注释说明为何安全（如关停时向已关闭通道发送）。
- 🟡 `unwrap_or_else(|e| panic!(...))` 等价于 `unwrap`，同样禁止；改用 `?` 或 `map_err`。

### 3.2 并发与异步

- 🔴 禁止在持有锁时 `.await`（除非该锁明确设计为异步锁）。跨 `.await` 持锁易导致死锁或性能塌陷。
- 🔴 无界 channel 必须在审查中明确质询。无界 channel 在慢消费者场景会导致 OOM。新代码默认用有界 channel 并记录背压策略。
- 🟡 `Arc<Mutex<T>>` 中 `T` 较大或锁竞争明显时，考虑分片（参考 `kstd::LockMgr`）。
- 🟡 `tokio::spawn` 的句柄应被等待或显式管理；不要 fire-and-forget 关键任务。

### 3.3 存储与 Raft

- 🔴 任何影响 RocksDB 写路径或列族的改动，必须说明对 `MetaCF`/`HashesDataCF`/`SetsDataCF`/`ListsDataCF`/`ZsetsDataCF`/`ZsetsScoreCF` 的影响，并附回归测试。
- 🔴 Raft 状态机、日志存储、快照路径的改动必须说明对一致性的影响。Cache 命中不得绕过 OpenRaft 一致性门禁。
- 🟡 Key 编码改动必须更新 `docs/key-encoding.md` 和兼容性 manifest。
- 🟡 TTL/过期路径改动必须覆盖 `ExpirationManager` 相关测试。

### 3.4 兼容性

- 🔴 改变 Redis 命令的公开行为必须对照 Redis `8.8.1` commit `77b6c308396c9700672390a210143a8496fb4b10`，并更新机器可读的兼容性 manifest 和差分测试。
- 🔴 跳过某个 Redis 测试必须记录 owner、Issue、原因、引入日期和移除条件。
- 🟡 `redis-rs` 仅限测试/工具依赖，不得进入生产 server crate 依赖。

### 3.5 安全

- 🔴 所有外部输入（客户端 RESP、配置文件、gRPC）必须校验。特别是：键长度、整数溢出、字符串转 UTF-8。
- 🔴 `unsafe` 块必须有 `// SAFETY:` 注释论证不变量。新增 `unsafe` 需在 PR 描述中单独说明理由。
- 🟡 日志不得打印敏感数据（密码、完整键值载荷）。

### 3.6 性能

- 🟡 热路径（命令执行、RESP 编解码、网络读写）避免不必要的 `clone()` 和堆分配。审查时对 `clone()` 提问"能否用引用或 `Cow`"。
- 🟡 批量操作（MGET/MSET/SCAN）注意 N+1：避免在循环中逐次访问 RocksDB。
- 💭 优先用 `&[u8]` / `Bytes` 而非 `Vec<u8>` 传递只读数据。

### 3.7 可维护性

- 🟡 新增命令必须遵循 `CLAUDE.md` 的"Adding a Redis Command"流程：`Cmd` trait、`impl_cmd_meta!`、注册到 `table.rs`、配套测试。
- 🟡 公开项必须有文档注释。复杂逻辑必须有"为什么"注释，不只是"做什么"。
- 🟡 `TODO`/`FIXME` 必须带 issue 编号或负责人，例如 `// TODO(#123): ...`。无主 TODO 视为缺陷。
- 💭 所有 Kiwi-authored `.rs` 必须含 Apache 2.0 license header（CI 强制）。

### 3.8 测试

- 🟡 新功能必须有能失败的测试。bug 修复必须先有复现测试。
- 🟡 测试不得依赖执行顺序（`RUST_TEST_THREADS=1` 是 fd 限制需要，不是顺序依赖许可）。
- 🟡 涉及存储的测试用 `unique_test_db_path()`，避免并行冲突。

## 4. 审查流程

### 4.1 PR 提交前（作者侧）

1. `make fmt && make lint && make test` 全部通过。
2. 自查第 3 节清单，特别是错误处理和并发。
3. PR 描述包含：变更目的、关联 `REQ-*`、测试证据、对兼容性/存储的影响说明。
4. 标注需要重点审查的文件/函数。

### 4.2 审查执行（审查者侧）

1. **先读 PR 描述和关联设计**，理解意图后再看 diff。
2. **先跑门禁**：`cargo fmt --check`、`cargo clippy`、相关测试。
3. **按文件审查**，意见标注严重程度（第 2 节）。
4. **聚焦问题而非风格**：风格交给 `rustfmt`/`clippy`，人审关注正确性、安全、设计。
5. **区分"必须改"和"可以改"**：阻断项明确要求改；建议项给出理由但尊重作者判断。
6. **表扬好代码**：对清晰的抽象、巧妙而可读的解法给出正向反馈。
7. **一个问题一次说清**：不要把同一意见拆成多条评论。

### 4.3 审查者行为准则

- **对代码不对人**：评论针对代码行为，不评价作者能力。
- **解释为什么**：不只说"改成 X"，说清"因为 Y"。
- **建议而非命令**：用"考虑用 X 因为 Y"，不用"改成 X"。
- **不懂就问**：意图不明确时用 🟠 `question` 询问，不要假定是错误。
- **及时**：审查在 1 个工作日内首次响应；阻断性意见 24 小时内闭环。

### 4.4 合并决策

- 至少 1 名审查者 approve，且所有 🔴 `blocker` 已解决。
- 🟡 `suggestion` 要么本次修复，要么作者承诺开 issue 跟踪。
- 💭 `nit` 不阻塞合并。
- 合并方式遵循仓库默认（通常为 squash-merge，保持 Conventional Commits 标题）。

## 5. 工具链门禁

PR 必须通过以下门禁（与 `docs/quality/quality-gates.md` 对齐）：

```text
license header / third-party license check
cargo fmt --check
cargo clippy --all-features --workspace -- -D warnings -D clippy::unwrap_used
targeted unit tests
affected compatibility manifest tests
Cache OFF compatibility and recovery evidence
affected deterministic Raft seeds
```

- 核心逻辑 PR 必须在 Linux/WSL 验证；Windows 结果不能替代目标运行环境。
- Nightly 门禁（TCL 全集、raw wire 差分、fuzz、Raft 模拟器、sanitizer）参见 `docs/quality/quality-gates.md` 第 3 节。

## 6. PR 审查 Checklist 速查

提交/审查时可逐项确认：

- [ ] 无生产路径 `unwrap`/`panic!`/`unreachable!`/`unimplemented!`
- [ ] 错误用 `Result` 传播，关键路径有错误处理
- [ ] 无跨 `.await` 持锁
- [ ] channel 有界或有背压说明
- [ ] 外部输入已校验
- [ ] `unsafe` 有 `// SAFETY:` 论证
- [ ] 改动存储/Raft 路径有回归测试
- [ ] 兼容性变更已更新 manifest
- [ ] 热路径无不必要 clone/分配
- [ ] 新功能有能失败的测试
- [ ] license header 齐全
- [ ] `make fmt && make lint && make test` 通过

## 7. 定期质量扫描

除逐 PR 审查外，项目应定期（建议每周或每个里程碑）做一次全量静态扫描，主动发现存量质量问题并开 issue 跟踪。扫描维度：

1. `unwrap`/`expect`/`panic!`/`unreachable!`/`unimplemented!` 在生产代码的分布。
2. `unsafe` 块清单与 `SAFETY` 注释覆盖率。
3. 无主 `TODO`/`FIXME` 清单。
4. 无界 channel / 跨 await 持锁的静态特征。
5. `cargo clippy` 全量诊断。
6. license header 与第三方依赖合规。

扫描结果汇总为 issue 并打 `code-quality` 标签，按严重程度排期修复。
