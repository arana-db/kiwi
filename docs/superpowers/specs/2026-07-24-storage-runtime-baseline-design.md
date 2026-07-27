# Storage Runtime 性能与背压基线设计

## 背景

Issue #350 是 Epic #347 的第三阶段。#348 已删除生产路径中的内存 Raft LogStore，#349 已通过 PR #365 删除 `Engine` 伪抽象并明确 RocksDB 所有权。下一步必须先测量当前 storage 执行链路，再进入 #351 的 bounded RocksDB executor 重构。

当前生产请求链路为：

```text
RESP TCP
  -> network runtime
  -> bounded mpsc request channel
  -> StorageServer
  -> batching on: 收集请求后逐请求 tokio::spawn
     batching off: 每请求 tokio::spawn
  -> 同步 RocksDB command
  -> oneshot response
```

bounded mpsc 只能限制仍在 channel 中的请求。StorageServer 取出请求后会继续创建 Tokio task，`StorageServerConfig.worker_count` 没有进入调度路径，因此 queued 加 running 的总量没有真实上界。当前 batching 也没有形成 RocksDB WriteBatch 或 MultiGet，只是在执行前等待并重新分组请求。

仓库现有性能代码不能作为 #350 的验收基线：根目录 `benches/dual_runtime_performance_benchmark.rs` 没有注册为 Cargo target，包含 simulated workload、placeholder 指标和过期 API；已注册的 `network_benchmark` 只测 mock 网络组件，不经过 StorageServer 或 RocksDB；Benchmark workflow 只运行 `cargo bench --no-run`，只证明 benchmark target 可以编译。

## CI 真实性基线

PR #365 已修复 GitHub Python integration 的 server-missing/全 skip 假绿，不能再以旧
workflow 作为当前设计前提。该结论只适用于 Python integration，不代表现有 Benchmark
workflow 已运行真实负载。当前 `.github/workflows/ci.yml` 调用
`tests/run_python_integration.sh`，该脚本：

- 拒绝复用已占用的 `127.0.0.1:6379`；
- 启动本次构建的 `target/debug/kiwi`；
- 使用独立 data、log 和配置目录；
- 通过真实 RESP `PING` 等待 ready；
- 设置 `KIWI_TEST_REQUIRE_SERVER=1`，使连接失败从 `pytest.skip` 变为 `pytest.fail`；
- 测试后检查 Kiwi 进程仍存活；
- 通过 EXIT trap 回收进程并保留原始退出码。

`main@89bd840919a4650ea718ea39f27cde869f49da74` 的 integration run `30079482937` 实际 collected 55、passed 55、skipped 0。#350 harness 必须复用同样的 fail-closed 原则，不得退回“外部服务不存在时全部 skip 仍返回 0”的模式。

直接运行 `make -C tests test-python` 时仍保留本地宽松语义：没有服务可以 skip。这不是当前 GitHub CI 路径，也不作为 #350 的阻断修改。#350 新增的 benchmark smoke 和完整基线入口必须始终自行启动目标进程并在 ready、执行或 cleanup 失败时返回非零状态。

## 目标

- 建立可重复执行的真实 `TCP -> runtime -> StorageServer -> RocksDB` 基线工具。
- 测量吞吐、P50/P95/P99、错误率、背压、资源占用、storage-gate pause/drain 和 shutdown。
- 比较 batching 开关、runtime thread 数、channel capacity、pipeline 和 value size。
- 验证或证伪双 runtime 在慢 storage 场景下保持网络可响应性的收益。
- 保存机器可读原始结果、环境清单和人类可读摘要。
- 用测量结果冻结 #351 的性能、错误率、容量和生命周期验收阈值。
- 为固定同步 worker pool 与 `spawn_blocking + Semaphore` 的后续选择提供数据依据。

## 非目标

- 不在 #350 中实现 bounded executor、Semaphore 或固定 worker pool。
- 不改变当前请求调度、命令语义、持久化、fsync、RocksDB 参数或一致性语义来改善数字。
- 不在测量前修改 MGET 的 `STORAGE_EXCLUSIVE`，避免污染当前基线。
- 不在本任务中删除 batching、priority、scaling、fault injection 或 metrics 脚手架；这些属于 #352。
- 不把 unit microbenchmark、mock 网络测试或 `cargo bench --no-run` 当作端到端结果。
- 不把 GitHub hosted runner 的波动性能数字设为稳定的 Merge 阈值。
- 不混入 ZSCAN 二进制 pattern、旧 `.restore_temp_*` 自动清理或其他 Redis/恢复功能修改。

## 方案选择

采用方案 C：固定版本 `memtier_benchmark` 生成主负载，仓库内薄控制器负责启动、编排、采样和结果归档，Kiwi 只增加 benchmark 所需的最小只读观测能力。

主负载工具固定为 `memtier_benchmark 2.5.1`，对应 upstream commit
`5f634d171b83efca9640c5a87606c47b34d3d330`。仓库提供 bootstrap 脚本从该 commit
构建工具，并校验 checkout SHA。bootstrap 在 Linux 安装或检查 `autoconf`、`automake`、
`libevent-dev`、`pkg-config`、`libssl-dev` 和 `zlib1g-dev` 等 upstream 构建依赖；缓存只
用于加速，不能跳过 SHA 与版本校验。

正式可发布 baseline 默认只接受 bootstrap 构建的 binary。控制器保存 upstream commit、
完整 `memtier_benchmark --version` 输出和 binary SHA-256。允许通过 `MEMTIER_BIN` 使用
预装二进制，但无法提供受信 provenance 的 binary 只能运行 CI smoke，结果标记为
`non_publishable`。`redis-benchmark` 只用于少量 GET、SET 和 pipeline 交叉验证，不作为
唯一结果来源。

不采用仅使用 `redis-benchmark` 的方案，因为它无法完整表达混合负载、storage-gate
pause、慢 storage 和 pending shutdown 场景。不采用完全自研高性能客户端，因为开发
成本高，且客户端实现本身会成为新的性能变量。

## 交付拓扑

#350 通过三个有序 PR 完成，避免把基础设施、真实接线与 smoke、完整性能结果混在一个
PR 中：

```text
Foundation PR（Tasks 1-5，当前 PR #378）
  -> Wiring and Smoke PR（Tasks 6-12，完成原第一阶段）
  -> Baseline Results PR（原第二阶段）
```

### Foundation PR：Tasks 1-5（当前 PR #378）

交付 workspace/tool binary 骨架、`StorageServer` 组合入口、`NetworkServer` 可等待生命周期、
request sender 显式关闭点，以及 feature-gated observer contract、请求身份和状态 token。

当前 binary 必须保持 fail-closed：只解析参数后以非零状态返回
`harness not initialized`。它尚未接入真实 runtime、控制协议或 smoke，不能被描述为可运行
的 baseline harness。

PR #378 的 Ready/验收门禁是 Tasks 1-5 的构建、测试、feature 隔离和生产路径无影响均有
证据；不得提前声称原第一阶段完成。PR #378 不关闭 #350。

### Wiring and Smoke PR：Tasks 6-12（完成原第一阶段）

建议标题：

```text
test(runtime): add reproducible storage baseline harness
```

交付内容：

- 可参数化启动真实 Kiwi runtime stack 的 benchmark-only server/harness。
- 薄控制器启动 harness、等待 RESP PING、运行 memtier、采样资源、停止进程并写出 JSON。
- 只读 runtime 观测快照及其测试。
- 一个短时间 smoke matrix，验证工具链、结果 schema 和 fail-closed 行为。
- CI 继续编译所有 benchmark target，并额外运行不用于性能判定的短 smoke。
- smoke 失败时上传 controller log、Kiwi log、环境清单和部分 JSON。

该 PR 的 Ready/验收门禁包括：observer 已接入真实请求生命周期，benchmark-only server、
控制器、4 个显式 smoke case、结果校验和 CI fail-closed 门禁均真实可运行，且生产 binary
不暴露 benchmark 控制面。该 PR 不发布“优化后更快”等性能结论，也不关闭 #350。

### Baseline Results PR（原第二阶段）：完整基线与阈值冻结

建议标题：

```text
perf(runtime): publish storage execution baseline
```

交付内容：

- 在固定 WSL/Linux 或固定 self-hosted 环境执行完整矩阵。
- 提交环境说明、运行清单、原始 JSON/CSV 和摘要。
- 分析 batching、runtime threads、pipeline、value size、queue saturation 和慢 storage。
- 记录 storage-gate pause 与 shutdown 的请求归属和 drain 行为。
- 在 #350 文档中冻结 #351 的量化验收阈值。
- 给出 #351 executor 模型的选择建议，但不在该 PR 中实施。

该 PR 的 Ready/验收门禁是：在固定 WSL/Linux 或固定 self-hosted 环境按版本化
`cases.yaml` 完成全量矩阵，每个正式 case 重复 5 次并满足稳定性要求，提交可追溯原始结果，
冻结 #351 阈值并给出 executor 选择建议。只有该 PR 验收后才关闭 #350。

## Harness 架构

### Benchmark-only server

新增 workspace 工具 crate `tools/runtime-baseline`，其中提供可直接执行的
`kiwi-runtime-baseline` binary，而不是使用 `[[bench]]` 或通过 `cargo bench` 间接启动。
构建命令固定为：

```bash
cargo build --release -p runtime-baseline --bin kiwi-runtime-baseline
```

Python 控制器只接收该明确 binary 的绝对路径，PID、signal 和退出码都属于这个直接
子进程。已有 `cargo bench --no-run` 继续只负责正式 Cargo bench target，不能代表 server
harness 已运行。

该工具 crate 不依赖生产配置文件暴露尚未稳定的 runtime 参数，而是复用现有 E2E
`TestServer` 的真实组件组合：

- `RuntimeManager`；
- bounded request channel；
- `StorageServer::with_config`；
- `NetworkServer`；
- 真实临时 RocksDB；
- `StorageServerPauseController`。

当前 StorageServer 的 `with_config`、`with_pause_controller` 和 `with_metrics` 不能同时组合
自定义 config、共享 access gate 和 observer；RuntimeManager 又在内部创建 MessageChannel
和 StorageClient。Foundation PR 与 Wiring and Smoke PR 允许增加最小的组合构造入口，把同一个可选 observer contract
注入完整请求链。`runtime` crate 只定义该 contract 和随请求传递的 token；具体
`BaselineMetrics` 由工具 crate 拥有并实现 contract，避免形成 `runtime -> tools/runtime-baseline`
依赖环：

- RuntimeManager/MessageChannel：logical request 开始、physical attempt 创建、send accepted、
  send failed 和 channel queued；
- StorageClient：retry、client timeout 和 response received；
- StorageServer/BatchProcessor：dequeue、batch queued、waiting gate、running、execution terminal
  和 oneshot send result；
- lifecycle handle：shutdown rejection 和 abandoned 归类。

logical request id 与每次 retry 的 physical attempt id 分离；attempt id 从 enqueue 到 execution
terminal 保持不变。现有生产构造路径统一传入 `None`，benchmark observer、原子更新和控制
协议通过 `runtime-baseline` feature 编译，普通生产 `kiwi` 不包含这些 hooks。根 workspace 的
`default-members` 精确保留 12 个生产 crate 并排除工具 crate，使普通 root build 不与工具 target
处于同一 Cargo invocation，隔离 feature unification。组合 API 不改变 worker/batching 调度。
构建身份中的 `source_dirty` 是受限的可编译输入状态：根 `src/`、`.cargo/`、根
`Cargo.toml`/`Cargo.lock`/`rust-toolchain.toml`，以及工具 crate 的 manifest、build script、
`src/` 和 `tests/`。build script 以同一集合执行有 pathspec 的 `git status --porcelain` 并注册
`rerun-if-changed`；`.git`、Cargo target 和 benchmark results 都不属于该集合，避免构建产物或
结果归档造成自触发重编译。若这个受限 source 集合有未暂存或未跟踪修改，startup/outcome 必须
标为 `non_publishable` 并包含 `dirty_source_tree`。

target 接受明确参数：

- listen host/port；
- data directory；
- network runtime threads；
- storage runtime threads；
- request channel capacity；
- request timeout；
- batching enabled；
- batch size；
- batch timeout；
- metrics output path；
- control listen address；
- startup event output path。

默认绑定 loopback，并拒绝复用已占用端口。所有数据目录必须由控制器创建并显式传入。benchmark target 不读取用户生产数据目录，也不修改生产配置默认值。

### 启动与控制通道

benchmark target 的 Redis listener 和控制 listener 都绑定 loopback，并允许端口为 0，
由操作系统分配可用端口。两个 listener 就绪后，target 以原子 rename 写出一个 startup
JSON 文件：

```text
{
  "pid": 1234,
  "redis_addr": "127.0.0.1:41001",
  "control_addr": "127.0.0.1:41002",
  "data_dir": "...",
  "git_sha": "..."
}
```

控制器读取该文件后仍必须通过 Redis listener 执行真实 `PING`，不能只相信 startup
事件。startup JSON 中的 PID、data dir 和 Git SHA 必须与控制器启动参数一致。

控制器需要触发 storage access gate pause/resume、受控慢 storage、读取 metrics snapshot 和执行
shutdown。控制通道使用 newline-delimited JSON request/response，至少支持：

```text
{"command":"metrics"}
{"command":"pause"}
{"command":"resume"}
{"command":"block_runtime_workers","duration_ms":50,"concurrency":4}
{"command":"arm_operation_delay","duration_ms":50}
{"command":"release_operation_delay"}
{"command":"shutdown","mode":"drain"}
```

每个 response 都包含 request id、`ok` 和结构化结果或错误。`pause` 只有在 access gate
active owners 已 drain 后才返回；channel queued、batch queued 和 running metrics 同时被
记录。该场景和结果统一命名为 `storage-gate-pause`，不声称执行了完整 Raft snapshot
build/install；完整 snapshot 行为继续由现有 raft integration tests 负责。

慢 storage 分为两个独立机制：

- `block_runtime_workers` 向当前 request channel 注入指定 `concurrency` 的 benchmark-only
  内部 command，在 storage runtime 线程中执行 `std::thread::sleep`。每个阻塞 command
  产生 started/completed event，用于确认负载和网络 probe 确实落在阻塞窗口内。
- `arm_operation_delay` 在 benchmark feature 下为后续每个 storage execution 增加同步
  等价延迟，用于稳定制造 queue saturation、timeout 和背压。控制器等待 armed event 后
  才开始正式采样，并通过 `release_operation_delay` 解除。

两种机制都不进入 Redis 命令表，也不编译进普通生产 `kiwi` 二进制。

控制器只有在目标数量的 blocker 全部进入 started 且尚未 completed 后才能启动 network
probe。若达到目标并发前已有 blocker completed，该 case 失败；控制器可以按 manifest 中
规定的更长 duration 重新执行，但不能把未形成重叠阻塞窗口的样本记为成功。

`shutdown` 触发 benchmark target 中与当前 NetworkServer、StorageServer 和 RuntimeManager
一致的 stop 路径，不预设当前实现一定 drain 或一定取消。Foundation PR 增加最小的 lifecycle
handle：停止 accept、停止从现有连接接受新命令、保留 accept/connection/StorageServer
JoinHandle、关闭 request sender，然后观察当前 queued/batch/waiting/running 请求如何结束，
最后关闭 runtime。该 handle 复用真正的 NetworkServer，不另写简化 TCP server。

shutdown 开始后新 offered requests 归为 `rejected_after_shutdown`。shutdown 前已 accepted 的
storage attempts 必须最终归类为 execution finished、shutdown rejected after accept 或
abandoned；timeout 和 response dropped 作为客户端观察/交付结果单独统计，不能与 execution
终态混为一类。

控制协议只属于 benchmark target，不进入 Redis 命令表、不暴露在生产 `kiwi` 二进制中。
控制请求失败、超时或返回格式错误必须使对应 case 失败。

### Python 控制器

Python 只负责编排，不生成主要负载。控制器使用标准库完成：

- 临时目录与配置生成；
- 子进程启动和有界 readiness polling；
- memtier 命令构造与超时；
- `/proc/<pid>` CPU、RSS、线程数和进程 I/O 采样；
- control channel 请求；
- stdout/stderr、server log 和结果文件归档；
- SIGINT、SIGTERM、SIGKILL 分级回收；
- 最终 JSON schema 校验。

任何阶段失败都返回非零状态。控制器必须检查：目标 PID 仍为本次启动进程、RESP PING 成功、memtier 实际完成、至少有一个成功请求、结果不是全零、server 在非 crash case 中保持存活、cleanup 完成。

## 只读观测模型

不直接启用现有 `StorageMetricsTracker` 的每请求异步 Mutex 路径。其共享锁和样本
`Vec::remove(0)` 会改变被测热路径，不能作为当前 runtime 基线的既定实现。

工具 crate 使用独立的 `BaselineMetrics`，并通过 `runtime` crate 定义的 observer contract
接收事件；`runtime` 不拥有或依赖这个具体指标实现。热路径只执行原子 current/max/counter
更新和单调时间戳采样；需要分位数的阶段延迟通过有界 non-blocking sample channel 交给单独
collector，channel 满时只增加 `dropped_metric_samples`，不得阻塞请求。默认生产 `kiwi`
不创建该 observer、不暴露 endpoint，也不编译 benchmark 控制通道。

正式 anchor case 同时运行 instrumentation off/on 配对测试：

- 使用普通生产 `kiwi` binary 的默认配置运行 control case，量化 benchmark target 与生产
  启动路径的总开销；
- #351 的 QPS/P99 阈值以 instrumentation off 结果为主；
- instrumentation on 用于解释 queue、running、batch 和 lifecycle 行为；
- 开启观测后吞吐中位数下降超过 2%，或 P99 上升超过 5% 时，该 case 的 instrumented
  QPS/P99 不得用于冻结性能阈值，并在摘要中披露观测开销。

若 benchmark target 的 instrumentation-off control 相对普通生产 `kiwi` 吞吐下降超过 2%
或 P99 上升超过 5%，不得用 target 的绝对 QPS/P99 代表当前生产基线；应以普通 `kiwi`
结果冻结性能阈值，target 只提供内部诊断数据。

观测快照至少包含：

- channel current/max queued；
- BatchProcessor current/max queued；
- dispatch waiting for access gate current/max；
- batch count、实际 batch size 分布；
- timeout flush 与 size-trigger flush 次数；
- storage task current/max running；
- client logical requests 与 physical storage attempts 分离；
- accepted attempts、execution success、command error、internal failure；
- response delivered、response dropped、client timeout、retry attempt；
- queue wait、batch wait、execution time；
- storage-gate pause 请求时间、active drain 时间、resume 后 backlog drain 时间；
- shutdown 开始/结束时间、完成/拒绝/遗留请求数。

current/max 计数使用原子计数和 RAII guard，确保 task panic、early return 和 response drop
都能回收 current 值。延迟以单调时钟记录。storage-gate active、channel queued、batch
queued、waiting gate 和 running 必须分别报告，不能合并成一个含义不清的 `pending`。

每个 physical attempt 按以下互斥生命周期对账：

```text
accepted_attempts
  = channel_queued
  + batch_queued
  + waiting_gate
  + running
  + execution_finished
  + shutdown_rejected_after_accept
  + abandoned
```

这是某一时刻的状态分解；累计 counters 另行保存。client timeout 可与后续
`execution_finished + response_dropped` 同时发生，因此不能把 timeout 当作 execution 终态。

## 数据集与重复隔离

所有 GET 和混合负载使用固定的 100,000-key keyspace，key pattern 为
`baseline:<seed>:<zero-padded-id>`。每个 value size 创建独立预填充模板数据库：

- 预填充全部 100,000 个 key，并校验 memtier 成功数；
- 随机抽样 1,000 个 key 执行 GET，必须全部命中且 value 长度正确；
- GET case 为 100% hit；
- 80/20 mixed case 的 SET 只覆盖同一固定 keyspace，不持续创建新 key；
- SET-only case 同样覆盖固定 keyspace，使每轮 WAL、flush 和 compaction 输入可比较；
- 正式基线禁用 memtier `--randomize`，记录是否启用 `--distinct-client-seed`，并把 key
  pattern/range 写入 case manifest。

每个正式重复从同一个预填充 RocksDB checkpoint 复制到新的 case data dir，启动新的
target 进程，不跨重复复用可变数据库。环境清单保存 template manifest：文件名、大小、
CURRENT、MANIFEST 和 OPTIONS 文件 SHA-256。主矩阵统一测 warm workload：启动后完成规定
预热再计时；#350 不发布无法稳定复现的 OS cold-page-cache 结论。

## 测量矩阵

Wiring and Smoke PR 的 smoke 固定为 4 个显式 case，不做组合展开：

- GET，connections 1，pipeline 1，batching off；
- GET，connections 8，pipeline 8，batching on；
- SET，connections 1，pipeline 1，batching off；
- SET，connections 8，pipeline 8，batching on。

smoke value 固定 64 B。memtier 2.5.1 的 `--test-time` 只接受整数秒，不能表达 0.5 秒；因此每
case 预热 1 秒、测量 2 秒、总超时 15 秒。controller 的 4-case smoke step 总超时 5 分钟；整个
GitHub job 总超时为 45 分钟，以覆盖冷 runner 的 release RocksDB 编译。它只验证真实执行和
fail-closed，不用于性能比较。

Baseline Results PR 使用版本化 `cases.yaml` 作为唯一可执行 case 清单。下表是可选维度，不做全
笛卡尔积。manifest 分为以下组，合计不得超过 60 个 case variant：

1. 基础吞吐：在 1 KiB value 下扫描 3 种 operation、3 种 connections、3 种 pipeline，
   共 27 个 case。
2. Value size：固定 anchor `GET 100% / connections 16 / pipeline 16`，扫描 64 B、1 KiB、
   4 KiB，共 3 个 case。
3. 单因素 runtime 扫描：固定 anchor，分别扫描 storage threads、batching 和 channel
   capacity，不与其他变化维度组合。
4. Offered-load 扫描：先以 anchor 校准饱和吞吐，再执行 50%、80%、100%、120% 四个
   case。memtier 的 rate limit 按每 connection 生效，控制器必须根据 threads × clients
   换算并记录实际 aggregate target 与舍入值。
5. Lifecycle：慢 storage、storage-gate-pause 和 shutdown 各自独立执行，不与基础吞吐
   矩阵交叉。

`cases.yaml` 为审查和复现依据；新增 case 必须显式提交 Diff，不能由脚本隐式生成组合。

可选维度定义如下：

| 维度 | 档位 |
|---|---|
| 操作 | GET、SET、GET/SET 80/20 |
| 连接数 | 1、16、64 |
| Pipeline | 1、16、64 |
| Value | 64 B、1 KiB、4 KiB |
| Storage threads | 1、2、4、固定机器物理核心数内的代表值 |
| Batching | off；on，100 requests / 10 ms |
| Channel capacity | 16、256、1000 |
| Offered load | 约饱和吞吐的 50%、80%、100%、120% |
| 慢 storage | 10 ms、50 ms、200 ms 等价延迟 |
| Storage gate pause | 无在途、running、channel queued、batch queued |
| Shutdown | pending 0、queued、running |

基础吞吐、value size、runtime 和 offered-load case 预热 30 秒、测量 60 秒、重复 5 次。
lifecycle case 重复 3 次并记录每次完整事件序列。报告保留全部原始轮次，以中位数为主，
同时输出 min、max 和变异系数。变异系数超过 5% 的 case 标记为环境不稳定，不用于冻结
#351 阈值，必须在降低环境噪声后重跑。成功标准是提交的 `cases.yaml` 中所有 case 完成，
不是让所有维度互相组合。

## 结果格式

每次运行产生独立目录：

```text
results/<git-sha>/<timestamp>/<case-id>/
  environment.json
  config.json
  memtier.json
  runtime-metrics.json
  process-samples.jsonl
  controller.log
  kiwi.log
  outcome.json
```

`outcome.json` 至少包含：

- exact Git SHA 与 Cargo.lock hash；
- case 参数；
- start/end/duration；
- QPS、successful QPS；
- P50/P95/P99/max；
- success/error/timeout/connect failure；
- runtime metrics 最终值和峰值；
- CPU、RSS、threads、read/write bytes；
- storage-gate pause/shutdown 时间；
- pass/fail 及机器可解析原因。

仓库只提交经过脱敏、体积受控的原始结果和摘要。不得提交临时 RocksDB 数据目录、完整 core dump 或本机绝对用户路径。

## 环境冻结

正式基线必须记录：

- OS、kernel、WSL/native、文件系统和 mount options；
- CPU 型号、物理/逻辑核心、governor、turbo、affinity；
- 内存、磁盘类型和可用空间；
- Rust/rustc、profile、Cargo.lock hash；
- memtier 精确版本、upstream commit、binary SHA-256 和构建方式；
- Kiwi exact SHA 和全部 harness 参数；
- 数据集 key 数、value size、总大小、template manifest 和 warmup 状态；
- 客户端与服务端是否同机；
- 预热、持续时间、重复次数、key pattern/range 和 distinct-client-seed 设置；
- 测试期间其他已知负载。

## CI 设计

普通 PR CI 不把 hosted runner 的 QPS 或 P99 作为硬阈值。Benchmark workflow 拆成名称明确
的 `Compile Benchmarks` 和 `Runtime Baseline Smoke`；smoke 作为独立、可见并应配置为
required 的 check。CI 只验证：

- `cargo build --release -p runtime-baseline --bin kiwi-runtime-baseline` 可以编译；
- memtier 版本检查生效；
- smoke matrix 实际启动本次构建的 target；
- RESP PING 和至少一个 GET/SET 成功；
- 结果 JSON 符合 schema，非全零且没有 skipped case；
- 无服务、错误 binary、ready timeout、memtier 非零退出、server crash 和 cleanup failure 均使 job 失败；
- 失败时日志和部分结果作为 artifact 上传。

完整矩阵通过 `workflow_dispatch` 在固定环境运行，或由维护者在文档规定的固定 WSL/Linux
主机执行。Benchmark workflow 的名称和摘要必须区分“compile/smoke”与“正式 baseline”，
避免把 target 编译成功表述为性能验证成功。

workflow 只能创建独立 check，是否 required 由 GitHub ruleset/branch protection 控制。
Wiring and Smoke PR 收尾必须实时核验 ruleset；若执行账号无权修改，应把“将 Runtime Baseline Smoke
设为 required”记录为明确的仓库配置待办，不能仅凭 job 存在声称 required 门禁已生效。

## 错误处理

- 目标端口被占用：立即失败，不连接未知 Redis/Kiwi。
- binary 缺失或不可执行：立即失败。
- 进程提前退出：保存日志并失败。
- PING 未在总超时内成功：终止进程、保存日志并失败。
- memtier 版本不匹配或执行失败：该 case 失败，不生成可发布基线。
- 全部请求失败、结果为空或全零：失败。
- control channel 失败：对应 lifecycle case 失败。
- metrics 计数不守恒或 current 在 cleanup 后非零：失败。
- shutdown 超时：按 INT、TERM、KILL 回收并将 case 标记失败。
- cleanup 删除路径必须位于控制器创建的临时根目录；路径验证失败时停止删除并失败。

## 测试设计

### Fail-closed 测试

- binary 不存在时 controller 返回非零。
- 使用 `/bin/false` 或立即退出的 test child 时 readiness 返回非零。
- 端口被占用时不启动 target。
- PING 失败不能转为 skip。
- memtier 不存在、版本不匹配、超时或返回非零时 case 失败。
- server 在负载期间退出时 case 失败。
- outcome 中成功请求为零时 case 失败。

### Metrics 测试

- enqueue/dequeue 更新 channel current/max。
- batch timeout 与 size trigger 分别计数。
- receiver 取出后等待 access gate 的请求进入 waiting_gate，并在进入 running 后归零。
- running guard 在成功、错误和 task panic 后归零。
- response receiver drop 不泄漏 current，并增加 dropped counter。
- storage-gate pause 分别报告 active、channel queued、batch queued、waiting gate 和 running。
- retry 时 logical request 与 physical attempts 分开计数。
- client timeout 后 execution completion 与 response dropped 可以同时出现且对账正确。
- 请求仍在 channel 中时触发 storage-gate pause/shutdown，验证它已计入 accepted attempt，
  attempt id 在 MessageChannel、StorageClient 和 StorageServer 之间保持一致，最终进入明确
  terminal 分类。
- shutdown 后 current 状态归零，accepted attempt 状态分解守恒。

### 真实 smoke

- 启动真实临时 RocksDB、runtime、StorageServer 和 NetworkServer。
- 使用 memtier 2.5.1 执行短 GET/SET case。
- 验证 client result、runtime metrics 和 process samples 均有非零数据。
- 验证 batching off/on 都经过真实链路。
- 验证 controller 能优雅停止 target 并释放临时目录。

## 验证命令

Wiring and Smoke PR 达到 Ready 前至少执行：

```bash
cargo fmt --all -- --check
cargo clippy --all-features --workspace -- -D warnings -D clippy::unwrap_used
cargo test --package runtime
cargo test --package net
cargo test --package runtime-baseline
cargo test --workspace
bash tests/run_python_integration.sh
python3 tools/runtime-baseline/run_baseline.py \
  --smoke \
  --server-binary target/release/kiwi-runtime-baseline
git diff --check
```

完整矩阵只在固定 Linux 环境执行，不能用 Windows 结果替代。普通 Windows/macOS CI 仍负责代码可编译、Clippy 和现有行为测试。

## #351 阈值冻结规则

Baseline Results PR 不预先拍脑袋指定 executor 参数，但必须从稳定 baseline 产生以下阈值：

- 正常负载吞吐允许回退比例；
- P99 与 max latency 允许变化；
- 正常负载错误率；
- 120% offered load 下允许的错误类型和上限；
- queued 加 running 的最大允许值；
- storage-gate pause/drain 和 shutdown 最大时间；
- 慢 storage 时新连接和 PING 的最大延迟；
- CPU、RSS 和线程数允许变化。

阈值只使用变异系数不超过 5% 的 case。#351 的同机对比使用相同数据、配置、工具版本、
key pattern/range、`--distinct-client-seed` 设置和重复次数；同时报告绝对值与相对变化。

## 成功标准

- 其他开发者可以按文档在固定 Linux 环境复现相同矩阵。
- benchmark 测量真实 TCP、真实 runtime、真实 StorageServer 和真实 RocksDB。
- smoke CI 不能在 server 未启动、全部请求失败或全部 case skipped 时保持绿色。
- 原始结果包含客户端、runtime、资源和生命周期指标。
- batching 开关、runtime threads、queue saturation、慢 storage、storage-gate pause 和
  shutdown 均有结果。
- 完整基线有稳定性判断，不用单次数字冻结阈值。
- #351 的量化验收阈值和 executor 模型选择依据已经记录。
- #350 没有提前改变调度模型或通过降低持久化/一致性要求换取性能数字。
