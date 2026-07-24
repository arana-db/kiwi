# Storage Runtime Baseline Harness 实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。

**目标：** 为 Issue #350 第一阶段交付一个可重复、fail-closed 的真实 `TCP -> RuntimeManager -> StorageServer -> RocksDB` 基线 harness，并在 CI 中真实执行 4 个 GET/SET smoke case，而不是只编译 benchmark target。

**架构：** `runtime` crate 在 `runtime-baseline` feature 下只定义请求身份、生命周期事件和同步 observer 接口；具体原子指标、采样 collector、控制协议和真实服务编排由新的 `tools/runtime-baseline` crate 实现。`NetworkServer` 增加可取消且可等待的通用生命周期入口，harness 按“停止 accept/连接 -> 关闭 request sender -> 等待 StorageServer -> 停止 runtimes”顺序退出。Python 标准库控制器只负责编排、资源采样、memtier 调用和结果验证，主要负载固定使用可校验 provenance 的 `memtier_benchmark 2.5.1`。

**技术栈：** Rust 2021、Tokio、Tokio CancellationToken/JoinSet、RocksDB、RESP、Clap、Serde/JSON、Python 3 标准库、Bash、memtier_benchmark 2.5.1、GitHub Actions、WSL/Linux。

---

## 实施边界

- 本计划只实现 #350 第一阶段 harness、观测、4-case smoke 和 CI 门禁，不实现 #351 bounded executor。
- 不改变当前 storage 调度模型、RocksDB 配置、命令语义、fsync、一致性或 batching 算法来改善结果。
- 不启用现有 `StorageMetricsTracker`；其 async Mutex 和样本容器会污染热路径。
- 不把 benchmark control 命令加入 Redis 命令表；控制协议只存在于 `kiwi-runtime-baseline` target。
- 不将 GitHub hosted runner 的 QPS/P99 作为性能回归阈值；CI 只验证真实执行、非零结果、状态守恒和 fail-closed。
- 普通 `cargo build` 和 `cargo build -p server` 不得启用 `runtime-baseline` feature；只有工具 target 和显式 `--all-features` 验证启用。
- 第一阶段允许 `MEMTIER_BIN` 运行 smoke，但无可信 provenance 的结果必须标记 `non_publishable`；第二阶段正式 baseline 只接受 bootstrap 产物。
- 所有新增 `.rs`、`.py`、`.sh` 和普通 `.yaml` 文件必须包含仓库认可的 Apache 2.0 license header；不能等到最终 license job 才补。

## 实施前事实校正

已批准设计中的两个时间约束需要在第一个实现提交中同步校正：

1. `memtier_benchmark 2.5.1 --test-time` 使用整数秒，不能直接表达 0.5 秒。smoke 预热改为 1 秒；不得通过 0.5 秒强杀 memtier 模拟预热。
2. 5 分钟只约束 controller 的 4-case smoke 执行 step。GitHub job 总超时设为 45 分钟，因为冷 runner 的 release RocksDB 编译可能超过 5 分钟。

另一个构建边界需要在 workspace 层固定：新增工具 crate 后，虚拟 workspace 的默认 root build 会选择全部 members，并通过 Cargo feature unification 给同一次构建中的 `server` 启用 runtime benchmark feature。因此根 `Cargo.toml` 必须增加 `default-members`，显式保留当前 12 个生产 crate并排除 `tools/runtime-baseline`。

## 文件职责

### 根目录和 CI

- 修改 `Cargo.toml`：新增 workspace member、workspace dependency，并用 `default-members` 隔离工具 target。
- 修改 `Cargo.lock`：记录工具 crate 和新增依赖解析结果。
- 修改 `.github/workflows/benchmark.yml`：保留 `Compile Benchmarks`，新增独立 `Runtime Baseline Smoke` check、artifact 传递和 always-upload 证据。
- 修改 `docs/superpowers/specs/2026-07-24-storage-runtime-baseline-design.md`：同步 1 秒预热、45 分钟 job/5 分钟 smoke step、observer 所有权和 workspace feature 隔离事实。
- 创建 `docs/performance/storage-runtime-baseline.md`：本地/WSL 复现、结果解释、publishability 和故障排查。

### Runtime observer 和生命周期

- 修改 `src/common/runtime/Cargo.toml`：新增 `runtime-baseline` feature。
- 创建 `src/common/runtime/baseline.rs`：logical/physical 身份、attempt 状态机、observer 事件和 RAII token。
- 修改 `src/common/runtime/lib.rs`：feature-gated module/re-export。
- 修改 `src/common/runtime/manager.rs`：observer 注入、同一 observer 传递、显式关闭 manager 自持 `StorageClient`。
- 修改 `src/common/runtime/message.rs`：logical request、physical retry attempt、reserve-before-send、timeout/response/retry 观测。
- 修改 `src/common/runtime/storage_server.rs`：组合 config/pause/observer、batch/gate/running/terminal 状态、benchmark-only blocker 和 operation delay hook。
- 修改 `src/common/runtime/tests.rs`：跨 MessageChannel、StorageClient、StorageServer 的状态守恒和 shutdown 回归。

### NetworkServer 生命周期

- 修改 `src/net/Cargo.toml`：声明 `tokio-util` workspace dependency。
- 修改 `src/net/src/network_server.rs`：新增 `run_until_cancelled`，停止 accept 和已有连接并等待 JoinSet。
- 修改 `src/net/src/pool.rs`：提供显式清空空闲资源的生命周期 API，确保 pool 不再持有 `StorageClient` clone。
- 修改 `src/net/tests/storage_command_e2e_tests.rs`：真实 listener/connection cancellation 和退出回归；保留现有真实 stack 测试语义。

### Rust benchmark target

- 创建 `tools/runtime-baseline/Cargo.toml`：package `runtime-baseline` 和 binary `kiwi-runtime-baseline`。
- 创建 `tools/runtime-baseline/build.rs`：把构建 checkout 的 40 位 Git SHA 嵌入 binary；禁止运行时参数自报版本。
- 创建 `tools/runtime-baseline/src/lib.rs`：公开模块与 `BaselineHarness` API。
- 创建 `tools/runtime-baseline/src/main.rs`：CLI、启动、等待 shutdown 和退出码。
- 创建 `tools/runtime-baseline/src/cli.rs`：严格参数及 loopback/绝对路径校验。
- 创建 `tools/runtime-baseline/src/startup.rs`：startup JSON 临时文件、flush/sync、原子 rename。
- 创建 `tools/runtime-baseline/src/schema.rs`：版本化 startup/control/metrics DTO。
- 创建 `tools/runtime-baseline/src/metrics.rs`：`BaselineObserver` 实现、atomic current/max/counters、bounded sample channel 和 snapshot 守恒。
- 创建 `tools/runtime-baseline/src/control.rs`：loopback NDJSON 控制服务和 64 KiB 单行上限。
- 创建 `tools/runtime-baseline/src/harness.rs`：真实 Storage、RuntimeManager、StorageServer、NetworkServer 的启动和有界 shutdown。
- 创建 `tools/runtime-baseline/tests/cli_test.rs`、`startup_test.rs`、`control_test.rs`、`harness_smoke_test.rs`、`lifecycle_test.rs`。

### Python controller 和 memtier

- 创建 `tools/runtime-baseline/run_baseline.py`：`run`/`verify` CLI 和退出码映射。
- 创建 `tools/runtime-baseline/runtime_baseline/` 下的 `controller.py`、`manifest.py`、`memtier.py`、`process.py`、`resp.py`、`control.py`、`sampling.py`、`results.py`、`schema.py`、`verify.py`。
- 创建 `tools/runtime-baseline/cases/cases.yaml`：带 Apache header 注释的 JSON-compatible YAML；4 个 smoke case 必须显式列出。
- 创建 `tools/runtime-baseline/schema/outcome.schema.json`：JSON Schema 2020-12 发布契约。
- 创建 `tools/runtime-baseline/bootstrap_memtier.sh`：固定 commit 构建、版本/SHA 校验和 provenance。
- 创建 `tools/runtime-baseline/tests/test_manifest.py`、`test_memtier.py`、`test_process.py`、`test_schema.py`、`test_controller_fail_closed.py` 及必要 JSON fixtures。

## 关键接口冻结

### Runtime observer 合同

`runtime` crate 只拥有 observer 协议和随请求传递的 token；具体 `BaselineMetrics` 位于工具 crate，避免 `runtime -> tools/runtime-baseline` 依赖环。

```rust
#[cfg(feature = "runtime-baseline")]
pub trait BaselineObserver: Send + Sync + 'static {
    fn on_event(&self, event: BaselineEvent);
    fn before_execute(&self, trace: &BaselineTrace);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum AttemptState {
    Offered,
    ChannelQueued,
    BatchQueued,
    WaitingGate,
    Running,
    ExecutionFinished,
    ShutdownRejectedAfterAccept,
    Abandoned,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct BaselineTrace {
    pub logical_id: LogicalRequestId,
    pub attempt_id: RequestId,
    pub attempt_index: u32,
}
```

`BaselineAttempt` 内部保存 `BaselineTrace`、当前 `AtomicU8` 状态、上次状态转换的 `Instant` 和 `Arc<dyn BaselineObserver>`。最后一个 token 被 drop 且状态仍非 terminal 时，必须转换为 `Abandoned`；非法、重复或 terminal 后转换必须返回错误并增加 observer invariant violation，而不是静默重复计数。

### Request 状态守恒

每个 snapshot 必须满足“累计 accepted = 当前 active + 累计 terminal”的混合守恒式：

```text
accepted_attempts_total
  = channel_queued_current
  + batch_queued_current
  + waiting_gate_current
  + running_current
  + execution_finished_total
  + shutdown_rejected_after_accept_total
  + abandoned_total
```

`client_timeout`、`response_delivered`、`response_dropped`、`retry_attempt` 是正交累计事件。client timeout 后，服务端仍可进入 `execution_finished + response_dropped`。

shutdown 开始后、尚未进入 request channel 的新 logical request 计入独立的 `rejected_after_shutdown` client counter，不得伪装成 accepted attempt；已经取得 channel permit 的 attempt 才能进入 `shutdown_rejected_after_accept` terminal。

shutdown 完成时只要求 `channel_queued_current`、`batch_queued_current`、`waiting_gate_current` 和 `running_current` 四个 active current 为 0；terminal totals 必须保留，不能为了“清零”丢失执行历史。snapshot 不能在多 counter transition 的中间状态返回。实现使用无锁 writer generation：transition 进入时增加 active writers，更新旧/新状态 counter 和 generation，退出时减少 active writers；snapshot 只接受“前后 active writers 都为 0 且 generation 未变化”的读取，否则有界重试并返回 snapshot busy 错误。

### Channel enqueue 竞态约束

feature-enabled path 不能在 `sender.send(request).await` 返回后才标记 `ChannelQueued`，否则 receiver 可能先 dequeue。使用 timeout 包围 `sender.reserve()`，取得 permit 后先进行 `Offered -> ChannelQueued`，再用同步 `permit.send(request)` 发布请求。reserve/closed/timeout 失败不能计入 `accepted_attempts`。

### Network lifecycle

```rust
pub async fn run_until_cancelled(
    &self,
    shutdown: CancellationToken,
) -> Result<(), Box<dyn Error>>;
```

现有 `ServerTrait::run()` 调用该方法并传入永不取消的 token，保持生产行为。新方法以一个三路 `tokio::select!` 同时处理 listener accept、cancellation 和 `JoinSet::join_next()`：运行期间持续 reap 已完成连接，不能等 shutdown 才清空而导致连接 churn 下 JoinSet 无界增长。普通连接 EOF/协议错误继续记录日志且不终止生产 server；只有 task panic/JoinError 或 server 级错误才使 server 返回 error。

当前 `start_pool_cleanup()` 是 detached 无限任务，`ConnectionPool<NetworkResources>` 的 idle entries 又持有 `StorageClient` clone。实现必须让 pool cleanup 受同一个 cancellation token 控制并被等待；connection tasks 全部结束后显式清空 pool 中的空闲 `NetworkResources`，确认 pool stats 为 active=0、available=0，再允许 `run_until_cancelled` 返回。

### Harness shutdown 顺序

```text
1. control handler 原子标记 shutdown_started，拒绝新 control mutation
2. cancel NetworkServer，停止 accept，并等待已有 connection tasks 退出
3. 确认 NetworkServer cleanup task 已退出、JoinSet 已清空、connection pool 已清空，并 drop NetworkServer 持有的 StorageClient
4. RuntimeManager::close_storage_requests() drop manager 自持 StorageClient，再 drop harness 中最后一个 StorageClient clone，使 request receiver 关闭
5. 等待 StorageServer：已接受 attempt 完成或明确归类 abandoned/rejected
6. 写最终 metrics，验证 `channel_queued_current`、`batch_queued_current`、`waiting_gate_current`、`running_current` 四项为 0，并验证累计状态守恒
7. RuntimeManager::stop() 关闭 storage/network runtimes
8. flush/sync outcome 和 log，target 正常退出
```

任何阶段超过 deadline 都返回非零；不得把 TERM/KILL 后的结果标为 pass。

### Control 协议

每一行是一个 JSON request/response，单行最多 64 KiB：

```json
{"request_id":"r-1","command":"metrics"}
{"request_id":"r-2","command":"pause"}
{"request_id":"r-3","command":"resume"}
{"request_id":"r-4","command":"block_runtime_workers","duration_ms":50,"concurrency":4}
{"request_id":"r-5","command":"arm_operation_delay","duration_ms":50}
{"request_id":"r-6","command":"release_operation_delay"}
{"request_id":"r-7","command":"shutdown","mode":"drain"}
```

响应固定为 `{request_id, ok, result}` 或 `{request_id, ok:false, error:{code,message}}`。未知字段、未知命令、重复 shutdown、非 loopback peer、超限行、invalid JSON 和 timeout 都必须显式失败。

## 任务 1：校正设计约束并创建可独立编译的工具 crate 骨架

**文件：**

- 修改：`docs/superpowers/specs/2026-07-24-storage-runtime-baseline-design.md`
- 修改：`Cargo.toml`
- 修改：`Cargo.lock`
- 创建：`tools/runtime-baseline/Cargo.toml`
- 创建：`tools/runtime-baseline/src/lib.rs`
- 创建：`tools/runtime-baseline/src/main.rs`
- 创建：`tools/runtime-baseline/build.rs`
- 创建：`tools/runtime-baseline/src/cli.rs`
- 创建：`tools/runtime-baseline/src/startup.rs`
- 创建：`tools/runtime-baseline/src/schema.rs`
- 创建：`tools/runtime-baseline/tests/cli_test.rs`
- 创建：`tools/runtime-baseline/tests/startup_test.rs`

- [ ] 在设计规格中将 smoke warmup 从 0.5 秒修订为 1 秒，并写明依据是 memtier 2.5.1 的整数 `--test-time`。
- [ ] 在设计规格中将“整个 job 5 分钟”修订为“controller smoke step 5 分钟、job 45 分钟”。
- [ ] 在设计规格中明确 runtime crate 只定义 observer contract，工具 crate 拥有具体 `BaselineMetrics`；写明 Cargo feature unification 隔离方案。
- [ ] 在 `Cargo.toml` 增加 `tools/runtime-baseline` member，并增加 `default-members`，内容与当前 12 个生产 members 完全相同。
- [ ] 任务 1 的工具 manifest 只声明 CLI/startup 骨架所需的 `clap`、`serde`、`serde_json`、`anyhow`、`tempfile` 等依赖；此时不得请求尚未创建的 runtime `runtime-baseline` feature，保证本提交独立可解析和编译。
- [ ] 添加失败测试：相对 `--data-dir`、非 loopback listen、已含 RocksDB `CURRENT` 的 data dir、相对 startup/metrics path 均被 CLI 拒绝。
- [ ] 运行 `cargo test -p runtime-baseline --test cli_test`，确认测试因 CLI 尚未实现而红灯。
- [ ] 最小实现 `ServerArgs`：`--listen`、`--control-listen`、`--data-dir`、`--startup-event`、`--metrics-output`、`--expected-git-sha`、runtime thread/capacity/timeout/batching/instrumentation 参数。
- [ ] 在 `build.rs` 中优先读取 CI 显式提供的 `KIWI_BASELINE_BUILD_GIT_SHA`，否则从当前 checkout 执行 `git rev-parse HEAD`；两者都必须是 40 位 hex，结果通过 `cargo:rustc-env=KIWI_BASELINE_COMPILED_GIT_SHA=...` 编入 binary。构建时同时执行受边界限制的 `git status --porcelain`，嵌入 `KIWI_BASELINE_SOURCE_DIRTY=true|false`。设置 `rerun-if-env-changed`，并覆盖 worktree `.git` 指针/HEAD 的 rerun 依赖。
- [ ] dirty build 允许执行 smoke，但 startup/outcome 的 `publishability` 必须为 `non_publishable` 且包含 `dirty_source_tree`；CI Compile job 必须在 build 前断言 checkout clean。
- [ ] 添加 build identity 测试：startup 报告 `env!("KIWI_BASELINE_COMPILED_GIT_SHA")`；`--expected-git-sha` 不同则 binary 在 listener/startup 之前失败，不能回显 caller 输入。
- [ ] 添加 startup JSON 测试：临时文件与目标同目录；目标出现时 JSON 完整；PID、两个地址、canonical data dir、compiled Git SHA 和 schema version 正确；临时文件不残留。
- [ ] 运行 `cargo test -p runtime-baseline --test startup_test`，确认先红后绿。
- [ ] 保持 `main.rs` 只解析参数并返回“harness not initialized”非零；不得伪装成可运行 smoke。
- [ ] 运行 `cargo build -p server`，并用 `cargo tree -p server -e features | rg "runtime-baseline"` 确认普通 server 构建未启用 feature。
- [ ] 运行 `cargo build`，确认默认 root build 不构建 `runtime-baseline` package。
- [ ] 运行 `cargo test -p runtime-baseline --test cli_test --test startup_test` 和 `git diff --check`。
- [ ] 提交：`build(runtime): add baseline tool workspace`。

## 任务 2：让 StorageServer 同时接收自定义 config 和 pause controller

**文件：**

- 修改：`src/common/runtime/storage_server.rs`
- 修改：`src/common/runtime/tests.rs`

- [ ] 添加单元测试：传入 `batching_enabled=true`、自定义 batch size/timeout 和一个 `StorageServerPauseController`，构造出的 server 必须复用 controller 的同一个 access gate，而不是新建 gate。
- [ ] 添加行为测试：先 `request_pause()`，再发送请求；请求不得开始执行；`resume()` 后请求完成。
- [ ] 运行 `cargo test -p runtime storage_server::tests::test_config_and_pause_controller_share_gate -- --exact`，确认当前无组合 API 而红灯。
- [ ] 新增公开构造器：

```rust
pub fn with_config_and_pause_controller(
    global_storage: Arc<GlobalStorage>,
    receiver: mpsc::Receiver<StorageRequest>,
    config: StorageServerConfig,
    pause_controller: StorageServerPauseController,
) -> Self
```

- [ ] 让 `new`、`with_pause_controller`、`with_config` 和新构造器统一调用一个 private inner constructor；不得改变默认 config 和现有 gate 语义。
- [ ] 运行新增测试、现有 storage server tests 和 `cargo test -p runtime`。
- [ ] 提交：`refactor(runtime): compose storage server controls`。

## 任务 3：为 NetworkServer 增加可取消、可等待的生命周期

**文件：**

- 修改：`src/net/Cargo.toml`
- 修改：`src/net/src/network_server.rs`
- 修改：`src/net/src/pool.rs`
- 修改：`src/net/tests/storage_command_e2e_tests.rs`

- [ ] 添加测试：在 `127.0.0.1:0` bind 后运行 server，连接并完成 `PING`，cancel token 后外层 JoinHandle 必须在 1 秒内成功返回。
- [ ] 添加测试：cancel 后新 TCP connect 必须失败；cancel 前建立的 keep-alive connection 再发送命令必须 EOF/connection reset，不能继续接受新命令。
- [ ] 添加高频短连接测试：反复建立/断开连接后，已完成 task 被持续 reap，JoinSet 活跃数不随历史连接数无界增长。
- [ ] 添加 pool/cleanup 测试：cancel 后 cleanup task 已 join，pool stats 为 active=0、available=0；drop manager/harness 最后 client 后 storage receiver 能收到 EOF。
- [ ] 运行目标测试，确认当前 `run()` 无限 accept、detached connection tasks 导致超时红灯。
- [ ] 在 `src/net/Cargo.toml` 增加 `tokio-util.workspace = true`。
- [ ] 实现 `NetworkServer::run_until_cancelled(CancellationToken)`，accept、cancellation 和 `JoinSet::join_next()` 使用三路 `tokio::select!`，运行期间持续 reap 完成 task。
- [ ] 用 `JoinSet` 保存 connection tasks；每个 task 使用 child token，在 shutdown 后停止从连接读取新命令并退出。普通连接错误在 task 内日志记录后正常结束，不升级为 server 失败。
- [ ] 把 pool cleanup task 纳入可取消、可等待生命周期；为 `ConnectionPool` 增加清空 idle entries 的 API。
- [ ] cancellation 后停止 accept、取消 child token、持续等待所有 JoinSet task和 cleanup task；JoinError/panic 必须传播为 server error；最后清空 pool 并验证 active/available 均为 0。
- [ ] 现有 `ServerTrait::run()` 调用 `run_until_cancelled(CancellationToken::new())`，保持普通生产 server 的无限运行语义。
- [ ] 运行新增测试、`cargo test -p net --test storage_command_e2e_tests` 和 `cargo test -p net`。
- [ ] 提交：`feat(net): add cancellable server lifecycle`。

## 任务 4：增加显式 request sender 关闭点

**文件：**

- 修改：`src/common/runtime/manager.rs`
- 修改：`src/common/runtime/tests.rs`

- [ ] 添加测试：`RuntimeManager` 初始化 storage components 后，receiver 仍因 manager 自持 `StorageClient` 保持 open；调用新 API 后，在其他 client clones 全部 drop 的前提下 receiver 返回 `None`。
- [ ] 添加幂等测试：第二次关闭返回 `false`，不会 panic。
- [ ] 运行目标测试，确认当前没有关闭 manager 自持 sender 的 API 而红灯。
- [ ] 实现：

```rust
pub fn close_storage_requests(&mut self) -> bool {
    self.storage_client.take().is_some()
}
```

- [ ] 不在 `RuntimeManager::stop()` 中隐式调用该方法；生产 stop 顺序保持不变，harness 负责显式生命周期顺序。
- [ ] 运行 `cargo test -p runtime`。
- [ ] 提交：`feat(runtime): close storage requests explicitly`。

## 任务 5：实现 feature-gated observer contract、请求身份和状态 token

**文件：**

- 修改：`src/common/runtime/Cargo.toml`
- 创建：`src/common/runtime/baseline.rs`
- 修改：`src/common/runtime/lib.rs`
- 修改：`src/common/runtime/message.rs`

- [ ] 在 runtime manifest 新增 `[features] default = []` 和 `runtime-baseline = []`。
- [ ] 先在 `baseline.rs` 写状态机测试：合法 transition、非法跳转、重复 terminal、最后 token drop -> abandoned、clone 未全部 drop 时不提前 abandoned。
- [ ] 写 identity 测试：一个 logical request 的 retry attempts 共享 logical id，但 physical `RequestId` 和 `attempt_index` 不同。
- [ ] 写 terminal 正交测试：client timeout 之后仍允许 execution finished，再记录 response dropped。
- [ ] 运行 `cargo test -p runtime --features runtime-baseline baseline::tests -- --nocapture`，确认 module/type 不存在而红灯。
- [ ] 最小实现 `LogicalRequestId`、`BaselineTrace`、`AttemptState`、`ExecutionOutcome`、`BaselineEvent`、`BaselineObserver` 和 `BaselineAttempt`。
- [ ] 给 `StorageRequest` 增加 feature-gated `baseline_attempt: Option<BaselineAttempt>`；修复 `message.rs`、`storage_server.rs`、`tests.rs` 中全部结构体字面量在 `--all-features` 下的初始化。
- [ ] 搜索并修复所有对 `StorageCommand` 的 exhaustive match，使后续 feature-only blocker variant 在 `--all-features` 下不会造成遗漏或把内部命令当普通 Execute/Batch。
- [ ] 事件必须包含 transition 前后状态、单调 elapsed、logical/attempt identity；不得包含系统时间作为延迟计算来源。
- [ ] 运行 feature tests、`cargo test -p runtime` 和 `cargo check -p server`，确认 feature off 生产调用方无需 observer。
- [ ] 提交：`feat(runtime): add baseline request observer`。

## 任务 6：接通 MessageChannel、StorageClient、retry、timeout 和 response 事件

**文件：**

- 修改：`src/common/runtime/manager.rs`
- 修改：`src/common/runtime/message.rs`
- 修改：`src/common/runtime/tests.rs`

- [ ] 添加 channel 测试：reserve 成功后 receiver 看到 request 时状态已经是 `ChannelQueued`；不得出现 receiver 先 dequeue、observer 后 enqueue。
- [ ] 添加 closed/full timeout 测试：reserve 失败计 `send_failed`，但 `accepted_attempts == 0`。
- [ ] 添加 retry 测试：logical requests 为 1，physical attempts 为 N，attempt index 从 0 单调递增，retry counter 为 N-1。
- [ ] 覆盖 normal、degraded、recovery 和 queued retry helper，确保所有 helper 复用同一个 logical context，而不是重新计 logical request。
- [ ] 添加 timeout 测试：client 先超时，server 后完成，最终同时出现 `client_timeout=1`、`execution_finished=1` 和 `response_dropped=1`。
- [ ] 添加 shutdown race 测试：shutdown flag 之后、reserve 之前的 request 计 `rejected_after_shutdown` 且不增加 accepted；已 reserve 的 request 最终进入执行 terminal、`shutdown_rejected_after_accept` 或 `abandoned`。
- [ ] 运行目标 tests，确认当前代码没有 observer/identity 而红灯。
- [ ] 给 `RuntimeManager` 增加 feature-only `with_baseline_observer(config, Arc<dyn BaselineObserver>)`，并统一复用 private `new_inner`；`new` 继续传 `None`。
- [ ] 让 `MessageChannel` 和 `StorageClient` 从 manager 得到同一个 observer；不要让 manager 创建第二个 observer。
- [ ] feature-enabled send 使用 timeout-wrapped `sender.reserve()`；取得 permit 后 transition 到 `ChannelQueued`，再同步 `permit.send(request)`。
- [ ] 在 `send_request_with_priority` 开始 logical request；每次进入 `try_send_request` 创建 physical attempt；degraded/recovery/queued retry 显式传递 logical context。
- [ ] 在 response received、client timeout、send closed/backpressure timeout 和 retry 决策点发送对应事件。
- [ ] 不使用现有 `StorageRequest.timestamp` 计算 channel wait；它包含 send/backpressure 等待。
- [ ] 运行 `cargo test -p runtime --features runtime-baseline`、`cargo test -p runtime` 和 `cargo check -p server`。
- [ ] 提交：`feat(runtime): trace storage request attempts`。

## 任务 7：接通 batching、access gate、running、terminal 和 benchmark delay/blocker

**文件：**

- 修改：`src/common/runtime/storage_server.rs`
- 修改：`src/common/runtime/tests.rs`

- [ ] 添加 batching-off 测试：receive 后为 `WaitingGate`，gate permit 后为 `Running`，成功后为 `ExecutionFinished`。
- [ ] 添加 batching-on 测试：receive 后为 `BatchQueued`；分别制造 size trigger、timeout trigger 和 high-priority trigger，三类 counter 必须分开。
- [ ] 添加 paused batch 测试：batch 中每个 request 都从 `BatchQueued -> WaitingGate`；不得把 access gate 的一个 batch permit 当成一个 running request。
- [ ] 添加 command error、internal failure、oneshot receiver drop、task panic/cancel 测试；所有路径 running current 最终为 0，panic/cancel 归 `Abandoned`。
- [ ] 添加 receiver close 测试：BatchProcessor 中尚未 flush 的 accepted requests 不能静默消失，必须执行或归 `Abandoned` 并保持守恒。
- [ ] 添加 operation delay 测试：armed 后的普通 storage execution 在 storage runtime thread 同步延迟；release 后新请求不再延迟。
- [ ] 添加 blocker 测试：`concurrency=N` 时必须收到 N 个 started 且在 probe 开始前 0 个 completed；无法形成重叠窗口必须失败。
- [ ] 运行目标 tests，确认当前状态和 benchmark command 不存在而红灯。
- [ ] 在组合构造 API 上增加 feature-only observer builder/参数，使同一 observer 到达 BatchProcessor 和 execution path。
- [ ] 在 batching off/on 的 receive、batch add/flush、gate wait、running、execute terminal 和 oneshot send 处进行状态转换。
- [ ] 用 RAII guard 回收 running current；显式 finish 后 guard 不得重复 abandoned。
- [ ] 增加 feature-only internal `StorageCommand` blocker variant 或等价内部执行入口；不得注册为 Redis 命令。
- [ ] `before_execute` 只在 feature on 且 observer 存在时调用；feature off 不得保留 sleep/branch。
- [ ] 运行 `cargo test -p runtime --features runtime-baseline`、`cargo test -p runtime` 和 `cargo clippy -p runtime --all-features -- -D warnings -D clippy::unwrap_used`。
- [ ] 提交：`feat(runtime): observe storage execution lifecycle`。

## 任务 8：实现原子指标、NDJSON control 和真实 Rust harness

**文件：**

- 修改：`tools/runtime-baseline/Cargo.toml`
- 修改：`tools/runtime-baseline/src/lib.rs`
- 修改：`tools/runtime-baseline/src/main.rs`
- 创建：`tools/runtime-baseline/src/metrics.rs`
- 创建：`tools/runtime-baseline/src/control.rs`
- 创建：`tools/runtime-baseline/src/harness.rs`
- 修改：`tools/runtime-baseline/src/schema.rs`
- 创建：`tools/runtime-baseline/tests/control_test.rs`
- 创建：`tools/runtime-baseline/tests/harness_smoke_test.rs`
- 创建：`tools/runtime-baseline/tests/lifecycle_test.rs`

- [ ] 先写 `BaselineMetrics` 单测：四个 active 状态的 current/max、三个 terminal totals 和其它 cumulative、sample channel 满时只增加 `dropped_metric_samples`、snapshot generation 守恒、有界重试失败返回结构化错误。
- [ ] 写守恒红灯测试：`accepted_attempts_total == active_current_sum + terminal_total_sum`；执行过请求并 shutdown 后，四个 active current 为 0，但 execution/abandoned/rejected terminal totals 保留且公式继续成立。
- [ ] 写 control protocol tests：metrics、pause/resume、arm/release delay、block workers、drain shutdown；未知命令/字段、malformed JSON、重复 request id、64 KiB 超限、非 loopback peer 均失败。
- [ ] 写真实 harness smoke test：临时 RocksDB、RuntimeManager、StorageServer 和 NetworkServer 启动后真实 RESP `PING`、`SET`、`GET` 成功；分别覆盖 batching off/on。
- [ ] 写 lifecycle test：pause 等 active owners drain 后才返回；shutdown 后 listener 关闭、cleanup/connection tasks和 pool 全清、sender 关闭、storage task 退出、四个 active current 全为 0、terminal totals 保留且 accepted 状态分解守恒。
- [ ] 运行 `cargo test -p runtime-baseline`，确认实现缺失而红灯。
- [ ] 此时才在工具 manifest 增加 `runtime = { workspace = true, features = ["runtime-baseline"] }` 以及 `net`、`storage`、`cmd`、`executor`、`resp`、`tokio`、`tokio-util`、`log`、`env_logger` 等真实 stack 依赖。
- [ ] 运行 `cargo tree -p runtime-baseline`，确认依赖方向只有 `runtime-baseline -> runtime/net/...`，`runtime` 和 `net` 不反向依赖工具 crate，且不存在循环。
- [ ] 实现具体 `BaselineMetrics: BaselineObserver`；热路径只使用 atomics、bounded nonblocking sample send 和单调时间。
- [ ] 实现 `BaselineHarness::start`：校验 data dir、打开真实 `Storage`、创建 observer、RuntimeManager、StorageServerPauseController、自定义 StorageServer config、NetworkServer 和 control listener。
- [ ] startup JSON 只在 Redis/control 两个 listener 都 bind 后原子发布；harness 自身不把“startup file 已写”当作 RESP ready 证明。
- [ ] 实现 control response request-id 回显和结构化错误；`pause` 必须等待 active drain；blocker 必须返回 started/completed event 进度。
- [ ] 实现 harness shutdown 顺序和各阶段 deadline；任一 JoinHandle panic、timeout、metrics 不守恒，或四个 active current 任一非零，均使 binary 非零退出。
- [ ] shutdown 最后关闭 sample sender、等待 collector JoinHandle flush 完成，再生成最终 snapshot；collector 丢样可以计数，但 collector 未退出/未 flush 不能 pass。
- [ ] `main.rs` 删除任务 1 的占位失败，进入真实 `start -> wait control shutdown -> drain -> exit`。
- [ ] 运行 `cargo test -p runtime-baseline`、`cargo test -p net`、`cargo test -p runtime --features runtime-baseline`。
- [ ] 提交：`test(runtime): add baseline harness control plane`。

## 任务 9：实现标准库 Python manifest、schema、process 和 fail-closed controller

**文件：**

- 创建：`tools/runtime-baseline/run_baseline.py`
- 创建：`tools/runtime-baseline/runtime_baseline/__init__.py`
- 创建：`tools/runtime-baseline/runtime_baseline/controller.py`
- 创建：`tools/runtime-baseline/runtime_baseline/manifest.py`
- 创建：`tools/runtime-baseline/runtime_baseline/memtier.py`
- 创建：`tools/runtime-baseline/runtime_baseline/process.py`
- 创建：`tools/runtime-baseline/runtime_baseline/resp.py`
- 创建：`tools/runtime-baseline/runtime_baseline/control.py`
- 创建：`tools/runtime-baseline/runtime_baseline/sampling.py`
- 创建：`tools/runtime-baseline/runtime_baseline/results.py`
- 创建：`tools/runtime-baseline/runtime_baseline/schema.py`
- 创建：`tools/runtime-baseline/runtime_baseline/verify.py`
- 创建：`tools/runtime-baseline/cases/cases.yaml`
- 创建：`tools/runtime-baseline/schema/outcome.schema.json`
- 创建：`tools/runtime-baseline/tests/test_manifest.py`
- 创建：`tools/runtime-baseline/tests/test_process.py`
- 创建：`tools/runtime-baseline/tests/test_schema.py`
- 创建：`tools/runtime-baseline/tests/test_controller_fail_closed.py`

- [ ] 所有新增 `.py` 和 `.yaml` 加 Apache 2.0 header；JSON/Markdown 按现有 `.licenserc.yaml` 规则处理。
- [ ] 在 `cases.yaml` 显式列出且只列出 4 个 smoke case：GET c1/p1 batch off、GET c8/p8 batch on、SET c1/p1 batch off、SET c8/p8 batch on。
- [ ] manifest 采用“开头只允许空行/`#` 注释，剩余正文必须可被 `json.loads` 解析”的 JSON-compatible YAML；禁止 anchor、alias、隐式类型和组合展开。
- [ ] 写 manifest 红灯测试：case id 重复、未知字段、缺/多/替换 smoke case、0 connection/pipeline/value/duration、keyspace 不是精确 100,000、batch-on 非 100 requests/10 ms 均拒绝。
- [ ] 写 process 红灯测试：binary 缺失/不可执行、child 立即退出、startup timeout/invalid/mismatch、非 loopback、PID/data dir/Git SHA 不一致、cleanup root 外/`..`/symlink escape 拒绝。
- [ ] 写 schema 红灯测试：缺字段、错类型、额外字段、pass+failure reason、fail+空 reason、absolute/traversal artifact path、NaN/Infinity、case 集不守恒。
- [ ] 写 controller fail-closed 红灯测试：`/bin/false`、PING 失败、server load 中 crash、memtier exit 0 但 zero requests、control malformed/timeout、shutdown timeout、cleanup 后四个 active current 任一非零、4 case 任一失败、只产 3 个或多产第 5 个 outcome。
- [ ] 添加路径真实性红灯测试：GET prefill 的 prefix/range 少写或错写一个 key、1,000-key probe 出现 nil/错长度/错内容必须失败；batch-on 配置被 controller/harness 忽略、batch metrics 全零必须失败；batch-off 出现任何 batch queue/flush 也必须失败。
- [ ] 运行 `python3 -m unittest discover -s tools/runtime-baseline/tests -p 'test_*.py'`，确认实现缺失而红灯。
- [ ] 实现每 case 独立进程/独立 data dir；Popen 使用 argv list 和 `start_new_session=True`，禁止 `shell=True`。
- [ ] case 的 15 秒 deadline 覆盖 startup、PING、prefill/probe、1 秒 warmup、2 秒 measurement、metrics、drain shutdown 和 cleanup 全流程；内部子步骤 timeout 不得把总 deadline 延长。
- [ ] readiness 同时验证 child 存活、startup JSON、PID、compiled Git SHA 等于 controller expected SHA、canonical data dir、loopback 地址和真实 RESP `PING +PONG`；不得比较两个都来自同一 CLI 参数的值。
- [ ] 实现 `/proc/<pid>` CPU/RSS/thread/io 采样；目标平台无 `/proc` 时明确失败或标记 unsupported，CI Linux 不得跳过。
- [ ] 实现 INT -> TERM -> KILL 分级回收和 `wait()`；强杀或 cleanup 失败必须使 case fail。
- [ ] outcome failure code 至少覆盖设计列出的 binary、startup、PING、memtier、server crash、control、metrics invariant、shutdown、cleanup、schema、missing/unexpected case。
- [ ] `outcome.schema.json` 是发布契约；由于 Python 标准库没有通用 JSON Schema 2020-12 validator，`schema.py` 明确实现“仅针对本仓库 outcome/run-outcome schema”的严格字段、类型、枚举、additional-properties、数值有限性、路径和跨字段语义校验，不得声称实现通用 JSON Schema 引擎。
- [ ] 用一组 valid fixture 和逐字段 invalid fixtures 同时驱动 `outcome.schema.json` 与 `schema.py` 的一致性测试；不能只做 `json.loads` 或零散 required-field 检查。
- [ ] 独立 `verify` 子命令比较 manifest expected ids、executed ids 和 outcome ids 完全相等，`skipped == 0`。
- [ ] 运行 Python 全部 tests，预期全绿。
- [ ] 提交：`test(runtime): add fail-closed baseline controller`。

## 任务 10：固定 memtier provenance 并跑真实 4-case smoke

**文件：**

- 创建：`tools/runtime-baseline/bootstrap_memtier.sh`
- 创建：`tools/runtime-baseline/tests/test_memtier.py`
- 创建：`tools/runtime-baseline/tests/fixtures/memtier-success.json`
- 创建：`tools/runtime-baseline/tests/fixtures/memtier-zero.json`
- 修改：`tools/runtime-baseline/runtime_baseline/memtier.py`
- 修改：`tools/runtime-baseline/runtime_baseline/controller.py`

- [ ] 为 bootstrap 写 shell-level/fixture tests：缺工具、错误 commit、错误 version、digest mismatch 和 cache binary 被替换均失败。
- [ ] bootstrap 固定 repository `https://github.com/redis/memtier_benchmark.git`、commit `5f634d171b83efca9640c5a87606c47b34d3d330`、expected version `2.5.1`。
- [ ] 使用 `git init -> fetch --depth 1 <commit> -> checkout --detach FETCH_HEAD`；构建前后都验证 `git rev-parse HEAD`。
- [ ] 检查 `git`、autoreconf/automake、pkg-config、make、C++ compiler、sha256sum；缺失时打印准确 apt 命令并失败，不在脚本内隐式 sudo 安装。
- [ ] 用 `autoreconf -ivf && ./configure && make` 在 bootstrap 自己的 build root 构建，并写 `memtier-provenance.json`：repository、commit、完整 version output、binary SHA-256、build method。
- [ ] cache 命中时重新执行 executable、commit、version、digest 校验；不一致时仅清理 bootstrap 自己拥有的 prefix 后重建。
- [ ] 写 memtier parser tests：missing/non-executable、version mismatch、provenance/digest mismatch、nonzero、timeout、invalid/empty JSON、缺 `ALL STATS/Totals`、Count/Ops 为 0、NaN/Infinity 都失败。
- [ ] memtier argv 固定 protocol、server/port、threads=1、clients、pipeline、data size=64、key range/prefix、ratio、test-time、run-count、percentiles、JSON output；禁止 `--randomize`。
- [ ] GET case 使用确定性的单 client 顺序写入完整 100,000-key keyspace，验证 memtier/loader 写入成功数恰为 100,000；prefix、minimum、maximum 和 value generator 必须与正式 GET measurement 完全相同。
- [ ] GET prefill 后使用固定 seed 选择 1,000 个不重复 key，通过原始 RESP GET 确认每个响应非 nil、value 长度为 64 B 且内容与 generator 一致；允许在一个 socket 上流水发送以满足 15 秒总 deadline，但必须逐响应校验并保证 request/response 数量恰为 1,000。任一 miss/错内容都失败。不能把 memtier 的“命令响应成功”当成 cache hit 证明。
- [ ] SET case 先执行真实 SET probe，并限制写入同一固定 100,000-key 范围，不持续创建新 key。
- [ ] 在 WSL/Linux 构建 target：`cargo build --release -p runtime-baseline --bin kiwi-runtime-baseline`。
- [ ] 在 WSL/Linux bootstrap memtier，并运行：

```bash
timeout --signal=TERM --kill-after=30s 5m \
  python3 tools/runtime-baseline/run_baseline.py run \
    --suite smoke \
    --cases tools/runtime-baseline/cases/cases.yaml \
    --server-binary target/release/kiwi-runtime-baseline \
    --memtier-binary "$MEMTIER_PREFIX/bin/memtier_benchmark" \
    --memtier-provenance "$MEMTIER_PREFIX/memtier-provenance.json" \
    --results-root "$TMPDIR/runtime-baseline-results" \
    --expected-git-sha "$(git rev-parse HEAD)"
```

- [ ] 独立运行 verify，确认 expected/executed/outcome 精确为 4、passed=4、failed=0、skipped=0、每 case successful requests/QPS 和 execution terminal 非零。
- [ ] verifier 对 batch-off case 强制 `batch_queued_max=0`、`batch_count=0`、所有 flush counters=0；对 batch-on case强制 `batch_queued_max>0`、`batch_count>0`，且 size/timeout/high-priority flush counter 至少一个大于 0。任一不满足都标记路径未覆盖并使 smoke 失败。
- [ ] 检查结果不含 RocksDB data dir、core dump、本机凭据或仓库外绝对用户路径。
- [ ] 提交：`build(runtime): pin memtier smoke dependency`。

## 任务 11：把 Benchmark workflow 拆成 compile 与真实 smoke

**文件：**

- 修改：`.github/workflows/benchmark.yml`

- [ ] 保留 job/check 名称 `Compile Benchmarks`，继续执行 `cargo bench --no-run`。
- [ ] Compile job 先断言 `git status --porcelain` 为空，再安装 protoc/memtier build dependencies、bootstrap memtier，并设置 `KIWI_BASELINE_BUILD_GIT_SHA=${{ github.sha }}` 构建 exact-head `kiwi-runtime-baseline` release binary；生成包含 compiled Git SHA、source_dirty=false、harness SHA-256 和两个 binary `ldd` 输出的 artifact manifest，并把 harness binary、memtier binary、provenance/manifest 上传为短期 artifact。
- [ ] 新增独立 job/check `Runtime Baseline Smoke`，依赖 Compile job，`timeout-minutes: 45`；checkout 同一个 `${{ github.sha }}` 以取得 controller/tests/cases，然后只下载 Compile job 的 exact-run binary artifact，不重新使用 runner 上未知 binary。
- [ ] Smoke job 显式安装两个 binary 所需的 runtime packages。download-artifact 后执行 `chmod +x`，重新校验 harness artifact digest、compiled SHA/source_dirty、memtier provenance digest/commit/version；分别运行 `ldd` 并拒绝任何 `not found`，同时保存实际 `ldd` 与 Compile manifest 对账。GitHub artifact 不保留 Unix executable bit，不能直接假定可执行。
- [ ] Smoke job 先运行 Python unit tests，再用 GNU `timeout ... 5m` 执行 controller，随后独立 verify 4 个结果。
- [ ] verify 除 case 集合外还检查 GET 的 `prefilled_keys=100000`、`verified_hits=1000`、`misses=0`，以及每个 case 的 batching path metrics 与 manifest 配置一致。
- [ ] 所有使用 `tee` 的 shell step 开头使用 `set -Eeuo pipefail`，防止非零退出被覆盖。
- [ ] 在 job 开头创建 results root；artifact upload 使用 `if: always()` 和 `if-no-files-found: error`，成功/失败都保存 run-outcome、controller/kiwi/memtier logs、metrics、samples 和 outcomes。
- [ ] 不上传 case RocksDB data dir；上传前用 controller verifier 检查 artifact 相对路径和允许列表。
- [ ] 用 actionlint（若仓库/环境可用）或 YAML parser 检查 workflow；工具缺失时记录环境问题，至少执行 shell 命令 dry-run/语法检查。
- [ ] 运行 `git diff --check` 和 `skywalking-eyes`/仓库 license check，确认新增 Shell/Python/YAML header 合规。
- [ ] 提交：`ci(runtime): run baseline smoke`。

## 任务 12：文档、全量验证、独立审查和 GitHub 门禁核验

**文件：**

- 创建：`docs/performance/storage-runtime-baseline.md`
- 修改：`tools/runtime-baseline/README.md`（若任务 9 已创建则补齐，否则本任务创建）
- 必要时修改：前述实现文件中的审查问题

- [ ] 文档记录 WSL/Linux prerequisites、bootstrap、build、run、verify、结果目录、publishable/non-publishable、4-case smoke 非性能门限、常见 failure code 和安全 cleanup 边界。
- [ ] 明确第二阶段仍需固定机器、完整 `cases.yaml`、重复 5 次、CV 稳定性和 #351 阈值冻结；第一阶段不关闭 #350。
- [ ] 运行格式检查：`cargo fmt --all -- --check`。
- [ ] 运行 feature 测试：`cargo test -p runtime --features runtime-baseline`。
- [ ] 运行受影响 crates：`cargo test -p runtime`、`cargo test -p net`、`cargo test -p runtime-baseline`。
- [ ] 运行严格 lint：`cargo clippy --workspace --all-features -- -D warnings -D clippy::unwrap_used`。
- [ ] 在 WSL/Linux 运行 `cargo test --workspace`。
- [ ] 在 WSL/Linux 运行 `bash tests/run_python_integration.sh`，确认现有 55 个 Python 集成测试仍真实执行且没有 skip。
- [ ] 在 WSL/Linux 重新运行真实 4-case smoke 和独立 verify；保存命令、exact SHA、memtier version、binary SHA 和结果摘要。
- [ ] 运行 production isolation 检查：`cargo tree -p server -e features` 不含 `runtime-baseline`；普通 `cargo build -p server` 生成的 `kiwi` 不监听 control port、不包含 benchmark CLI。
- [ ] 运行 build identity 负路径：用 SHA A 构建 binary，controller 传 expected SHA B，必须在 startup/readiness 阶段失败；不能仅检查 runtime 传入值的自洽。
- [ ] 运行 `git diff --check`、license check，并确认 worktree 没有临时 RocksDB/data/results/build prefix 被跟踪。
- [ ] 启动一个只读子代理做规格符合性审查：逐项对照本计划和设计规格，阻断任何遗漏的 P0/P1。
- [ ] 修复后启动另一个只读子代理做代码质量/并发/生命周期审查，重点检查 reserve 竞态、token drop、JoinSet shutdown、batch residual、path cleanup 和 feature leakage。
- [ ] 最终重新运行受修复影响的最小测试及完整门禁，不使用旧测试结果宣称完成。
- [ ] 提交：`docs(runtime): document baseline harness`；若审查修复涉及代码，按关注点使用独立 conventional commits，不把全部修复压入文档 commit。
- [ ] push/创建 PR 前重新获取 GitHub main、确认 branch base 和 diff；push 与 PR 创建仍等待用户单独授权。
- [ ] PR 创建后实时查询 ruleset/branch protection，确认 `Runtime Baseline Smoke` 是否被配置为 required；无管理权限或尚未 required 时，在 PR 和收尾对账中列为仓库配置待办，不得声称门禁已生效。

## 最终验收证据

完成第一阶段时必须同时具备：

- `cargo build -p server` 的 feature tree 不含 `runtime-baseline`。
- `cargo test -p runtime --features runtime-baseline` 覆盖 logical/physical identity、channel/batch/gate/running/terminal、timeout/drop、panic/cancel，以及 `accepted_total = active_current + terminal_total` shutdown 守恒。
- `cargo test -p runtime-baseline` 覆盖 CLI、startup atomic write、control protocol、真实 RocksDB stack 和 lifecycle。
- Python unit tests证明 binary/server/memtier/control/schema/cleanup 任一失败均返回非零，且不存在 skip 分支。
- WSL/Linux 的 4 个显式 smoke case 全部真实执行，GET/SET、batch off/on 都有非零 successful requests/QPS。
- 两个 GET case 均完成 100,000-key 精确预填和固定 seed 1,000-key 非 nil/长度/内容验证；batch-off/on 的 observer 指标证明实际执行路径与 manifest 一致。
- run-level verifier 证明 expected/executed/outcome case 集合完全一致，passed=4、failed=0、skipped=0。
- CI `Compile Benchmarks` 和 `Runtime Baseline Smoke` 是两个名称清晰的 check；后者保存可审计 artifact。
- production `kiwi` 不包含 benchmark control endpoint 或 benchmark CLI。
- 当前 GitHub Python integration 继续真实通过；#350 不重新引入 server-missing 全 skip 假绿。
- 没有把 hosted runner 性能数值作为 #351 阈值，也没有提前改变 storage 调度模型。
