# Kiwi 产品需求文档（PRD）

> 综合来源：`.planning/PROJECT.md`（项目宪法）、`.planning/REQUIREMENTS.md`（可验收需求 v2）、`.planning/ROADMAP.md`（唯一路线图）、`.planning/DECISIONS.md`（已批准决定）、`.planning/STATE.md` 与 `.planning/KANBAN.md`（执行状态）。
> 稳定性门禁 G1–G7 引用：`docs/quality/system-stability-gate.md`。
> 文档性质：版本化的长期产品目标与验收合同；PR、Head、checks、dirty ownership 和任务授权等瞬时状态只由 `.planning/STATE.md`、`.planning/KANBAN.md`、本地 recovery 记录和 GitHub 实时状态维护。
> 对齐日期：2026-07-30。

---

## 1. 产品目标 / 北极星

**北极星（来源：`.planning/PROJECT.md` L9–L15）**

Kiwi 是一个以 **Redis 8.8.1 exact tag 可观察语义兼容**为目标的生产级 **Rust Redis-compatible 数据库**。

- 使用 **RocksDB** 保存全量、权威、可恢复的数据。
- 目标是使用 **OpenRaft** 提供强一致、高可用、成员变更、快照和恢复能力，并在 M4/M5 以故障测试建立证据；此目标不表示相关门禁已经通过。
- 内嵌 Redis 8.8.1 原生内存热数据层（Embedded Redis Hot Tier）属于**后续性能增强**：现在只冻结来源、许可证、ABI、正确性和发行接口合同，**不进入生产实现**；只有整体系统通过稳定性门禁且用户明确批准一个单独的 implementation task 后，才能启动（PROJECT.md L13；REQ-STABILITY-006）。

**固定基线（来源：`.planning/PROJECT.md` L17–L32）**

| 维度 | 取值 |
|---|---|
| Redis tag / commit | `8.8.1` / `77b6c308396c9700672390a210143a8496fb4b10` |
| Redis 许可证选项 | RSALv2 / SSPLv1 / AGPLv3 |
| 选定 Redis fork 许可证 | `AGPL-3.0-only` |
| Kiwi 语言 | Rust |
| Kiwi 自有源码许可证 | Apache-2.0 |
| 持久化真相 | RocksDB |
| 共识 | OpenRaft |
| 未来热层来源 | `arana-db/redis`（下游 exact pin 待定） |
| Raft API 模型 | RedisRaft public compatibility profile |
| Rust 客户端 | redis-rs（仅测试） |
| Oracle 来源证明 | independent rebuild + binary-hash equality |

> 约束红线（PROJECT.md L15）：任何 Raft、分片、存储格式、兼容性或未来热层优化，都**不得改变** Redis 8.8.1 的可观察语义、已声明的持久化边界和一致性承诺。

---

## 2. 用户画像与场景（反推）

> ⚠️ 本节为**反推**：`.planning/` 文档是工程与审计视角，未显式定义终端用户。**以下画像从 REQUIREMENTS / ROADMAP / STATE 中可验证的工程事实反推得出**，非用户调研结论。详细用户故事与 REQ 映射见 `docs/personas-and-user-stories.md`。

| 反推画像 | 反推依据（来源） |
|---|---|
| 平台 / SRE 工程师 | 可观测（REQ-OBS-001/002）、运维演练（G7, ROADMAP M9）、稳定门禁（REQ-STABILITY-001~006） |
| Redis 迁移用户 | RESP2/3 原始 frame differential（REQ-COMPAT-002）、兼容矩阵与已知差异（REQ-COMPAT-003）、Pipeline/连接行为回归（REQ-COMPAT-006） |
| 存储 / 内核开发者 | RocksDB 权威（REQ-STORAGE-001~006）、encoding 规范（REQ-STORAGE-005）、format version（REQ-STORAGE-003） |
| QA / 稳定性工程师 | Oracle provenance（REQ-COMPAT-008~010）、Jepsen/Elle（REQ-RAFT-007）、确定性 simulator（REQ-RAFT-006） |

---

## 3. 需求池（REQ-\* 分级）

**分级依据（可辩护）**

- **P0 = 稳定性门禁与当前里程碑阻塞项**：进入 M7 前的硬门禁（REQ-STABILITY-\*）、M1-001-T2 的 Oracle provenance 信任根（REQ-COMPAT-008/009/010）、以及门禁证据可复现的前提（REQ-WORK-\*）。这些不满足则 M7 保持冻结（ROADMAP M6/M7）。
- **P1 = 核心兼容、存储、一致性与可观测/性能**：当前 Cache OFF 产品主线的必修项，纳入 M1–M6 验收（REQ-COMPAT-001~007、REQ-STORAGE-\*、REQ-RAFT-\*、REQ-OBS-001/002、REQ-PERF-\*、REQ-LICENSE-001）。
- **P2 = 未来热层与组合发行**：生产实现与发行接入当前冻结；M7 前允许且必须完成适用的来源、许可证、ABI、安全加载和验收合同设计（REQ-HOT-\*、REQ-LICENSE-002~008、REQ-OBS-003）。

### P0（门禁 / 当前阻塞）

- `REQ-STABILITY-001`：启动热层生产实现前须门禁明确通过，且用户须明确批准一个单独的 implementation task。
- `REQ-STABILITY-002`：门禁至少覆盖 Redis 8.8.1 differential、RocksDB close/reopen、Raft commit/apply/durability、Snapshot、成员变更、进程级故障注入、资源边界、可观测性。
- `REQ-STABILITY-003`：门禁证据须绑定 branch/HEAD/平台/工具链/命令/结果/未覆盖风险；仅单测不算稳定。
- `REQ-STABILITY-004`：门禁未过期间只允许设计热层合同，禁止新增 Redis-derived 生产依赖/loader/数据路径/默认配置。
- `REQ-STABILITY-005`：任何解除冻结决定须追加到 DECISIONS.md 并同步 Roadmap/State/Kanban，不得由 PR 隐式解除。
- `REQ-STABILITY-006`：门禁通过只允许提交新规划与授权请求；即使用户同意 M7 转为 Ready，fork/loader/发行/热层实现仍只能在单独获批的 implementation task 中执行。
- `REQ-COMPAT-008`：Oracle provenance 须 primary build 与 verifier 独立重建 binary SHA-256 完全一致，正式证据只来自独立重建产物（M1-001 Task 2 核心，见 STATE.md“Oracle provenance 历史状态”）。
- `REQ-COMPAT-009`：Oracle controller bootstrap/Git/CC/Make 须来自声明 Linux 信任边界，记录路径/版本/SHA-256/identity，路径替换风险下经 held FD 执行。
- `REQ-COMPAT-010`：provenance 须在所有步骤成功后原子发布；不支持平台显式 FAIL 或带原因静态忽略，不得 early-return 假 PASS。
- `REQ-WORK-001`：长期事实写入 `.planning/`，不只在会话记忆。
- `REQ-WORK-002`：当前任务状态写入 `.codex/recovery/ACTIVE.md`，checkpoint 追加式保存。
- `REQ-WORK-003`：恢复状态须记录 branch/HEAD/授权/dirty 归属/证据/剩余工作/下一条安全动作。
- `REQ-WORK-004`：branch/HEAD/dirty 漂移时新会话停止写操作并报告差异。
- `REQ-WORK-005`：规划 task 与实施 task 使用不同任务边界；提前产生的实现草稿必须冻结（D012）。

### P1（核心兼容 / 存储 / 一致性 / 可观测 / 性能）

- `REQ-COMPAT-001`：普通 Redis 行为以 Redis 8.8.1 exact commit 为唯一 Oracle。
- `REQ-COMPAT-002`：RESP2 与 RESP3 原始 frame 须做 differential，不依赖 typed conversion。
- `REQ-COMPAT-003`：兼容矩阵须记录命令/模式/返回类型/错误/known difference/测试证据。
- `REQ-COMPAT-004`：Redis 官方 TCL suite 固定到同一 exact upstream commit；skip 须有 owner/Issue/理由/解除条件。
- `REQ-COMPAT-005`：redis-rs 只进独立测试工具或 dev dependency，生产 server crate 不得依赖。
- `REQ-COMPAT-006`：Pipeline 中间错误、partial I/O、连接关闭、Push 交错、二进制 payload 须有回归测试。
- `REQ-COMPAT-007`：新增公共命令/配置/错误/RESP/客户端接口须先取得 Redis 8.8.1 exact Oracle 证据。
- `REQ-STORAGE-001`：RocksDB 保存全量权威数据；Raft metadata/last_applied/业务状态须有原子性合同。
- `REQ-STORAGE-002`：恢复测试须释放所有 DB handle 再从路径重新打开。
- `REQ-STORAGE-003`：磁盘格式须带 format version、Comparator 身份和迁移策略。
- `REQ-STORAGE-004`：部分写、尾部损坏、metadata/log 不一致、Snapshot 损坏、磁盘满须有故障测试。
- `REQ-STORAGE-005`：Kiwi key/value encoding 须 binary-safe/round-trip/order-preserving/prefix-safe/canonical/stable（`format_base_key.rs`、`format_*`、`custom_comparator.rs`）。
- `REQ-STORAGE-006`：派生状态/索引/未来热层须能从 RocksDB 权威数据删除后重建，不能成为成功回复/恢复唯一依据。
- `REQ-RAFT-001`：写成功回复须发生在 quorum commit + 本地 apply + 所选 durability profile 满足之后。
- `REQ-RAFT-002`：Linearizable Read 即使经未来热层也须通过 Leader/ReadIndex/Lease 门禁。
- `REQ-RAFT-003`：实现并冻结 `kiwi_redisraft_public_v1`，公开清单内行为 100% 通过。
- `REQ-RAFT-004`：RedisRaft 内部 `RAFT.AE`/`RAFT.REQUESTVOTE`/`RAFT.SNAPSHOT` 不是公共兼容要求。
- `REQ-RAFT-005`：成员变更、Leader Transfer、Snapshot、日志回滚、真实 close/reopen 须进 required CI/分层门禁。
- `REQ-RAFT-006`：建立带 seed 的 OpenRaft deterministic simulator，检查 Election/Log Matching/State Machine Safety。
- `REQ-RAFT-007`：建立 3/5 节点进程级 kill/pause/partition/restart/disk fault 与 Elle/Jepsen history 测试。
- `REQ-RAFT-008`：非幂等写断线须标记 `SUBMIT_UNKNOWN`，测试和客户端不得自动重放。
- `REQ-OBS-001`：核心数据路径须暴露请求结果/错误类别/延迟/资源使用/恢复状态；日志不泄露敏感数据。
- `REQ-OBS-002`：Raft 须暴露 term/role/leader/commit index/last applied/snapshot/membership/replication/fsync 指标。
- `REQ-PERF-001`：性能基线至少含 Redis 8.8.1 与同数据集/协议/持久化声明下的 Kiwi；热层获准后增加 Cache OFF/ON 对照。
- `REQ-PERF-002`：性能报告须含 P50/P95/P99/P99.9/吞吐/CPU/峰值内存/写放大/测试环境。
- `REQ-PERF-003`：平均吞吐提升不能掩盖尾延迟/内存失控/语义变化/恢复退化。
- `REQ-LICENSE-001`：Kiwi 自有、可独立识别源码保持 Apache-2.0，保留文件级版权与 SPDX 声明。

### P2（未来热层 / 组合发行 —— 冻结中，仅合同/设计）

- `REQ-HOT-001` ~ `REQ-HOT-012`：热层全部 12 条验收合同（RocksDB 权威、arana-db/redis 来源、不进 Raft/Snapshot/Backup、可观察结果不变、update-or-invalidate、异步 fill 校验、TTL 绝对毫秒、Cache ON/OFF 同测、首期仅 String、版本化 C ABI、安全加载校验、专项审计）。详见 REQUIREMENTS.md L38–L53。
- `REQ-LICENSE-002`：未来 Redis-derived native library 须源自 Redis 8.8.1 exact commit 并明确选 `AGPL-3.0-only`。
- `REQ-LICENSE-003`：Redis 派生源码须保留上游版权/许可证/来源/修改记录/下游 exact commit/补丁/构建选项。
- `REQ-LICENSE-004`：含 Redis-derived native library 的官方组合发行不得标 Apache-2.0-only，须履 AGPL-3.0-only 义务。
- `REQ-LICENSE-005`：组合发行须提供与二进制精确匹配的 Kiwi 源码/Redis fork 源码/修改/ABI/绑定生成/构建脚本/许可证/第三方通知/SBOM。
- `REQ-LICENSE-006`：运行时源码身份与对应源码入口须绑定 exact release/tag/commit，不指向浮动分支。
- `REQ-LICENSE-007`：动态链接/独立仓库/运行时加载只能作工程边界，不得作免除组合发行许可证义务依据。
- `REQ-LICENSE-008`：首次公开发布含 Redis-derived native library 组合发行前须完成开源许可证专项复核并记录结论。
- `REQ-OBS-003`：未来热层启用后须暴露 hit/miss/fill/fill-drop/eviction/expire/update-failure/invalidate/generation-reset/load latency。

---

## 4. 非功能需求（来自 REQUIREMENTS）

| 类别 | 要求摘要 | 来源 REQ |
|---|---|---|
| 稳定性 | 系统稳定性门禁 G1–G7 全 PASS 只允许提出新的热层规划与授权请求；用户还须明确批准一个单独的 implementation task，生产实现不得自动启动。门禁覆盖兼容/恢复/Raft/故障/可观测/资源/运维；仅单测或 CI 全绿不构成稳定（PROJECT.md 不可妥协原则 #2、#10）。 | REQ-STABILITY-001~006；G1–G7 |
| 性能 | 基线含 Redis 8.8.1 vs Kiwi Cache OFF；报告须含 P50/P95/P99/P99.9/吞吐/CPU/峰值内存/写放大/环境；平均吞吐不得掩盖尾延迟、内存失控、语义变化、恢复退化。 | REQ-PERF-001~003 |
| 可观测 | 核心路径暴露结果/错误/延迟/资源/恢复状态；Raft 暴露 term/role/leader/commit/fsync 等指标；日志不泄露敏感数据。 | REQ-OBS-001/002 |
| 可靠（正确性/恢复/一致性） | RocksDB 全量权威；真实 close/reopen；原子性合同；编码 binary-safe/stable；写成功须 commit+apply+durability；线性一致门禁；崩溃后可由持久证据恢复（不可妥协原则 #1、#3、#6）。 | REQ-STORAGE-001~006；REQ-RAFT-001~008；REQ-COMPAT-001~007 |
| 安全供应链 | Oracle provenance 独立重建 + binary hash equality；依赖固定 exact commit；可生成 SBOM；许可证/notice 完整；组合发行对应源码精确（不可妥协原则 #9）。 | REQ-COMPAT-008~010；REQ-LICENSE-001~008 |
| 治理与工作连续性 | 长期事实、任务状态、branch/HEAD/授权、dirty ownership 和规划/实施边界可恢复；发生漂移时停止写操作。 | REQ-WORK-001~005 |

---

## 5. 范围：含 / 不含 / 延期

### 含（当前产品主线，Cache OFF）

> 本节定义当前主线的目标与验收范围，不表示各项已经实现或通过门禁；实际进度只以 `.planning/STATE.md`、`.planning/KANBAN.md` 和可重复验证证据为准。

- Redis 8.8.1 可观察语义兼容（Oracle 验证，不依赖热层）。
- RocksDB 全量权威存储与真实恢复。
- OpenRaft 单 Raft Group 强一致、高可用、成员变更、Snapshot（PROJECT.md L47–L59；ROADMAP 依赖顺序 L19–L31）。
- 系统稳定性门禁 G1–G7（ROADMAP M6）。

### 不含（当前非目标，来源 PROJECT.md L108–L116）

- 在系统稳定性门禁批准前实现/集成/打包/启用 Embedded Redis Hot Tier。
- Redis Stack/Search/JSON/TimeSeries/Bloom 完整实现（除非后续独立需求明确纳入）。
- 单 Group 正确性闭环前建设 Multi-Raft。
- 用 S3 直接替代本地 RocksDB 在线文件系统。
- AI 向量数据库、Agent Memory、语义缓存、推理 KV Cache。
- 将 RedisRaft 内部 RPC 命令暴露为 Kiwi 公共接口。
- 把历史 `pikiwidb/rediscache` 代码未经来源和许可证审计直接复制到 Kiwi。

### 延期（冻结，来源 PROJECT.md L62–L82；ROADMAP M7 L158–L160）

- 内嵌 Redis 8.8.1 原生内存热数据层（Embedded Redis Hot Tier）：仅冻结来源/许可证/ABI/正确性/发行接口合同，**不进入生产实现**；须 M6 通过且用户重新明确批准一个单独的 implementation task 后才解冻（REQ-STABILITY-001/005/006；D009）。
- Multi-Raft 与远期容量（ROADMAP M10，启动条件见 L222）。

---

## 6. 验收标准

### 6.1 稳定性门禁 G1–G7（来源 `docs/quality/system-stability-gate.md`）

| 门禁 | 主题 | 核心 Required（摘要） |
|---|---|---|
| **G1** | Redis 8.8.1 Cache OFF 兼容 | Oracle exact commit 固定；primary/verifier rebuild binary SHA-256 一致；正式 `INFO server` 仅来自 rebuild；RESP2/3 raw frame 比较；TCL skip 有 owner；redis-rs 不在生产依赖图（L61–L78）。 |
| **G2** | RocksDB 权威状态与恢复 | RocksDB 单独保存权威状态；写入/Raft metadata/last_applied 原子性；TTL 绝对时间；全部 handle 释放后真实 reopen；format version/Comparator/encoding 测试；部分写/磁盘满/只读 FS 故障（L80–L97）。 |
| **G3** | OpenRaft 正确性与公开接口 | `kiwi_redisraft_public_v1` 100% 通过；写成功在 commit+apply+durability 后；Linearizable Read 无绕行；deterministic simulator；3/5 节点端到端；`SUBMIT_UNKNOWN` 不自动重试（L99–L115）。 |
| **G4** | 进程/网络/磁盘故障证明 | kill/pause/partition/磁盘满/fsync error；Snapshot Install 与成员变更并发；Elle/Jepsen checker；每故障有可重放证据（L117–L133）。 |
| **G5** | 长期运行与资源边界 | 单节点 ≥24h、3 节点 ≥72h Cache OFF 稳定；FD/线程/内存/WAL 有上界；P50–P99.9 持续记录；超预算即失败（L135–L150）。 |
| **G6** | 工程质量与供应链 | Linux/macOS/Windows required CI 同 commit 通过；sanitizer/Miri/fuzz/dependency audit；生产路径无外部输入触发 `unwrap()`；依赖可固定可 SBOM；AGPL 义务与 Corresponding Source 已计划法律复核（L152–L168）。 |
| **G7** | 运维/升级/恢复演练 | 空环境部署单/3 节点；备份/恢复/证书轮换演练；滚动升级或明确停机流程；回滚不依赖人工修复；故障手册可区分安全重试与结果未知写（L171–L186）。 |

**门禁判定（进入条件 L234–L240；G1 FAIL 条件 L228–L230）**：满足全部五项进入条件后，M7 才能由 Frozen 转 Ready；用户批准的对象必须是一个单独的 implementation task，不能由 Gate PASS、PR 合并或 Ready 状态自动推导生产实现授权。任一 required artifact 缺失、schema 校验失败、toolchain 不受控、binary hash 不一致、runtime evidence 未绑定 verifier rebuild、cleanup 任一步失败或仅 self-reported metadata → **G1 必须 FAIL**。

### 6.2 REQ 证据规则（来源 REQUIREMENTS.md L5）

- 每个实现工作项**必须引用至少一个 `REQ-*`**。
- 每个 `REQ-*` **必须有可重复证据**（可重复、可重放、可审计）。
- Gate Review 时：P0/P1 必须为零；required compatibility difference 必须为零；required test 不接受永久 skip；临时豁免须写明范围/依据/owner/Issue/到期条件/移除验证，且**不得覆盖数据损坏、线性一致性、安全或不可恢复风险**（system-stability-gate.md L188–L196）。

---

## 7. 路线图（M0–M10 摘要，来源 ROADMAP.md）

| 里程碑 | 目标（摘要） | 关键退出门禁 |
|---|---|---|
| **M0** | 项目宪法、许可证与恢复机制（`.planning/*`、recovery 脚本）。 | 权威文档一致；热层“只设计禁实现”边界可恢复；恢复脚本 append-only 不改性 Git（L36–L56）。 |
| **M1** | Redis 8.8.1 Cache OFF 兼容 Oracle（含 trusted provenance）。 | Oracle provenance 不可由 metadata+binary 拼接；双构建 hash 一致；正式证据来自 verifier rebuild；基础命令可重复 transcript（L58–L74）。 |
| **M2** | RocksDB 权威状态与恢复正确性。 | Cache OFF 满足 Profile；崩溃恢复/格式兼容/故障注入证明 RocksDB 可独立恢复（L76–L90）。 |
| **M3** | Redis 8.8.1 Cache OFF 核心语义闭环（String/Hash/List/Set/ZSet/Bitmap/HLL/Geo/Streams/Transaction/Lua/Pub-Sub/ACL/RESP3）。 | required profile 全绿；保留差异经显式批准，无未启用热层掩盖的正确性缺口（L92–L105）。 |
| **M4** | 生产级单 Raft Group（`kiwi_redisraft_public_v1`、Linearizable Read、Membership、Snapshot）。 | 公开 Profile 100% 通过；3/5 节点正常/成员变更/恢复可重复；Cache OFF 无一致性绕行（L107–L122）。 |
| **M5** | 分布式故障与一致性证明（partition/kill/disk fault/Jepsen/SUBMIT_UNKNOWN）。 | Election/Log Matching/State Machine Safety/线性一致/durability 均有机器可检查证据（L124–L139）。 |
| **M6** | 系统稳定门禁（G1–G7）。 | 所有 P0/P1 清零；保留差异/豁免有 owner/期限/退出条件；Gate Review 绑定 exact commit（L141–L156）。 |
| **[用户重新明确批准]** | 将 M7 转为 Ready，并批准一个范围明确、与规划 task 分离的 implementation task。 | Gate PASS 或 Ready 状态均不自动授权生产实现（REQ-STABILITY-006）。 |
| **M7** | Embedded Redis Hot Tier 资格验证与实现（**延期且冻结**）。 | 仅在单独 implementation task 获批后执行；关闭/删除热层不影响正确性，热层失败不返回旧值，组合发行 Corresponding Source 检查通过（L158–L187）。 |
| **M8** | Cache ON 正确性、故障与性能证明。 | Cache ON 与 Cache OFF 可观察结果一致；故障只降级性能；收益可重复且尾延迟/内存/恢复/写放大不越预算（L189–L202）。 |
| **M9** | 生产发行门禁（CI/SBOM/回滚/运维手册）。 | 同版本可空环境部署/验证/备份/恢复/升级/回滚；发行包/源码/运行时身份完全对应（L204–L218）。 |
| **M10** | Multi-Raft 与远期容量（启动条件：单 Group 门禁完成且有瓶颈证据）。 | 候选：Redis Cluster slot、Meta Group、S3 Snapshot、冷对象归档（L220–L232）。 |

> 依赖顺序：M1/M2 可有限并行；M3 依赖二者；M4/M5 须 Cache OFF 为唯一 required 模式；M7 在 M6 通过且用户批准单独 implementation task 前保持冻结；M10 不得在单 Group 稳定性与故障证明完成前启动主线（ROADMAP L34）。

---

## 8. 未决项与已确认历史

> 以下条目区分仍待决定的产品问题与已经确认的历史状态；PRD 不缓存 checks、review threads 或 dirty ownership 等瞬时执行状态。

1. **Hot Tier 批准状态**：M7 仍冻结。M6（G1–G7）通过只允许提出授权请求；用户必须在看到 Gate Review 后明确批准一个单独的 implementation task（PROJECT.md L13；ROADMAP M7 L160；REQ-STABILITY-001/005/006）。当前无批准记录。
2. **PR `#383` 历史状态**：规划 PR `codex/redis-8.8.1-oracle-provenance-plan` 已于 2026-07-28 合并，final Head `42c16bef899385bd2e1b1e16e2e0202d4a614590`，merge commit `58030e1331655546ea4547a9a94efc493534ef7d`；它只固化 planning/docs，不带入实现。
3. **provenance 实现进度**：方案 A（D011）已批准，但实现**未在接受边界内开始**（STATE.md Oracle provenance 历史状态）。PR `#383` 只完成规划闭环，不代表方案 A 已实现。
4. **`arana-db/redis` 下游 baseline**：下游 exact commit、patch 清单、构建配置、产物 hash 尚未建立，当前不是 Kiwi 构建输入（PROJECT.md L28、L34）。
5. **组合发行开源许可证专项复核**：首次公开发布含 Redis-derived native library 的组合发行前必须完成，当前未执行（REQ-LICENSE-008；D002）。
6. **Oracle 草稿接受边界**：PR `#383` 未接受旧六文件实现草稿；后续独立 Oracle implementation task 只能将其作为只读参考，并须逐项对照 D011、REQ-COMPAT-008~010 重新审计。精确 worktree、dirty ownership 和 recovery checkpoint 不在 PRD 中缓存。
7. **Multi-Raft 启动条件**：单 Group 兼容/持久化/一致性/故障/稳定门禁未完成，且无 Leader 吞吐/容量/隔离/故障域瓶颈证据 → M10 不启动（ROADMAP M10 L222）。
8. **当前运行模式**：Cache OFF 为当前唯一 required 模式（ROADMAP L7；REQ-PERF-001）。Cache ON 测试与对比基线只能在 M7 条件满足且单独 implementation task 获批后引入。

---

## 9. 许可证边界（Apache-2.0 vs 未来 AGPL-3.0-only）

**当前（来源 PROJECT.md L6–L7、L84–L91；REQ-LICENSE-001；D002）**

- **Kiwi 自有、可独立识别的源码**：Apache-2.0，保留现有版权与 SPDX 声明（REQ-LICENSE-001）。
- **Redis 8.8.1 上游许可证选项**：RSALv2 / SSPLv1 / AGPLv3；选定 fork 许可证为 `AGPL-3.0-only`（PROJECT.md L22–L23）。

**未来组合发行边界**

- 未来 `arana-db/redis` 派生源码与原生动态库须选 `AGPL-3.0-only`，并完整保留 Redis 上游版权、许可证、修改记录（REQ-LICENSE-002/003）。
- 若官方发行物包含 Redis-derived native library，**完整组合发行不得声明为 Apache-2.0-only**，必须履行 `AGPL-3.0-only` 适用义务（REQ-LICENSE-004）。
- 组合发行须提供与二进制精确对应的 Kiwi 源码、Redis fork 源码、全部修改、ABI 头、绑定生成方式、构建脚本、版本清单、许可证、第三方通知和 SBOM（REQ-LICENSE-005）。
- 远程用户对应源码入口、发行文案、打包方式须在公开发布前通过开源许可证专项复核（REQ-LICENSE-008）。
- 拆分仓库、动态链接、运行时加载只用于**工程隔离**，不得被描述为规避组合发行许可证义务（REQ-LICENSE-007；D002 限定）。

**关键不变量**：Kiwi 当前以 Apache-2.0 开发的源码与未来组合发行的 AGPL-3.0-only 义务彼此独立；组合发行的义务仅因纳入 Redis-derived native library 而触发，且不可通过工程拆分规避（PROJECT.md 不可妥协原则 #9）。
