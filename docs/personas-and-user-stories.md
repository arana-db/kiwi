# Kiwi 用户画像与用户故事

> 补齐审计发现的缺失项：`.planning/` 与 `docs/quality/` 为工程/审计视角，**未显式定义终端用户画像与用户故事**。本文档从 REQUIREMENTS / ROADMAP / STATE / system-stability-gate 中可验证的工程事实反推画像，并补全用户故事。
> 标注约定：
> - **[事实/证据]**：用户故事映射到的 `REQ-*` 或门禁条目是项目批准需求（来源可追溯到 `.planning/REQUIREMENTS.md` 或 `docs/quality/system-stability-gate.md`）。
> - **[反推/产品假设]**：画像本身、用户动机措辞、故事包装为产品视角推断，非用户调研结论；其底层 REQ 仍为真实事实。

---

## 画像 1：平台 / SRE 工程师

> **[反推/产品假设]** 画像存在性反推自：可观测需求（REQ-OBS-001/002）、运维演练门禁（G7）、系统稳定性门禁（REQ-STABILITY-\*）、发行门禁（SDD M9）。该角色关注“系统可运维、可观测、可恢复、风险可控”。

### 用户故事

1. **[事实/证据]** As a 平台/SRE 工程师, I want Redis 8.8.1 exact Oracle 可重复验证 Kiwi 兼容, so that 升级或重构不会在客户端无感知的情况下破坏语义。
   → 映射：`REQ-COMPAT-001`、`REQ-COMPAT-002`、`REQ-COMPAT-003`。

2. **[事实/证据]** As a 平台/SRE 工程师, I want 系统稳定性门禁 G1–G7 全部 PASS 后只提出新的授权请求，并由用户明确批准一个单独的 implementation task, so that Gate PASS、PR 合并或 M7 Ready 都不会被误当成生产实现授权。
   → 映射：`REQ-STABILITY-001`、`REQ-STABILITY-002`、`REQ-STABILITY-005`、`REQ-STABILITY-006`；门禁 G1–G7 与授权边界（system-stability-gate.md L37、L57、L234–L240）。

3. **[事实/证据]** As a 平台/SRE 工程师, I want Raft 暴露 term/role/leader/commit index/last applied/snapshot/fsync 等指标, so that 排障时无需侵入进程即可定位一致性问题。
   → 映射：`REQ-OBS-002`。

4. **[事实/证据]** As a 平台/SRE 工程师, I want 崩溃后所有 RocksDB handle 释放并按路径真实 reopen 恢复, so that 我能确信恢复不是“复用旧对象”的假象。
   → 映射：`REQ-STORAGE-002`；门禁 G2（system-stability-gate.md L80–L97）。

5. **[事实/证据]** As a 平台/SRE 工程师, I want 运维手册覆盖磁盘满/无 Leader/Snapshot 失败/`SUBMIT_UNKNOWN` 处置, so that 故障时有可执行的处置流程而非依赖人工救火。
   → 映射：门禁 G7（system-stability-gate.md L171–L186）；`REQ-RAFT-008`。

6. **[事实/证据]** As a 平台/SRE 工程师, I want Gate Review 绑定可重放证据，并在 M7 前完成许可证、ABI、安全加载和验收合同设计, so that 前置设计能够接受审查而 Redis-derived 生产实现仍保持冻结。
   → 映射：`REQ-STABILITY-003`、`REQ-STABILITY-004`、`REQ-STABILITY-006`；`REQ-LICENSE-002~008`（仅设计与发布前复核）；门禁前允许项（system-stability-gate.md L21–L27）与 M7 进入条件（L234–L240）。

---

## 画像 2：Redis 迁移用户

> **[反推/产品假设]** 画像存在性反推自：RESP2/3 字节级兼容（REQ-COMPAT-002）、兼容矩阵（REQ-COMPAT-003）、redis-rs 仅测试边界（REQ-COMPAT-005）、Pipeline/连接行为回归（REQ-COMPAT-006）。该角色关心“以最小改造从 Redis 迁移到 Kiwi”。

### 用户故事

1. **[事实/证据]** As a Redis 迁移用户, I want RESP2 与 RESP3 原始 frame 与 Redis 8.8.1 做字节级 differential（含二进制 payload、null、error、push、attribute、aggregate 类型）, so that 我能评估迁移改造并通过兼容矩阵识别仍需处理的差异。
   → 映射：`REQ-COMPAT-002`；门禁 G1（system-stability-gate.md L65–L66）。

2. **[事实/证据]** As a Redis 迁移用户, I want Pipeline 中间错误、partial I/O、连接关闭、Push 交错都有回归测试, so that 高并发/弱网场景下的客户端行为不漂移。
   → 映射：`REQ-COMPAT-006`；门禁 G1（L67）。

3. **[事实/证据]** As a Redis 迁移用户, I want 兼容矩阵记录命令/模式/返回类型/错误/known difference 与测试证据, so that 我能明确知道哪些命令可用、哪些有已知差异。
   → 映射：`REQ-COMPAT-003`；门禁 G1（L68）。

4. **[事实/证据]** As a Redis 迁移用户, I want 任何新增公共命令/配置/错误/RESP 字段先有 Redis 8.8.1 exact Oracle 证据, so that 我不会遇到“凭经验推断”的语义偏差。
   → 映射：`REQ-COMPAT-007`、`REQ-COMPAT-001`。

5. **[事实/证据]** As a Redis 迁移用户, I want Redis 官方 TCL suite 固定到 exact upstream commit 且每个 skip 都有 owner/Issue/理由/解除条件, so that 兼容覆盖透明、不被静默跳过。
   → 映射：`REQ-COMPAT-004`；门禁 G1（L69）。

---

## 画像 3：存储 / 内核开发者

> **[反推/产品假设]** 画像存在性反推自：RocksDB 权威存储（REQ-STORAGE-001）、encoding 规范（REQ-STORAGE-005）、磁盘格式版本（REQ-STORAGE-003）、故障注入（REQ-STORAGE-004）。该角色关心“数据真相层正确、可恢复、格式可迁移”。

### 用户故事

1. **[事实/证据]** As a 存储/内核开发者, I want RocksDB 保存全量权威状态且恢复时所有 DB/CF/iterator/snapshot/后台任务 handle 完全释放后再按原路径重新打开, so that 恢复的真实性与不变量可被测试证明。
   → 映射：`REQ-STORAGE-001`、`REQ-STORAGE-002`；门禁 G2（L84–L87）。

2. **[事实/证据]** As a 存储/内核开发者, I want key/value encoding 满足 binary-safe/round-trip/order-preserving/prefix-safe/canonical/stable, so that 升级、comparator 变更、compaction 不会破坏数据顺序或语义。
   → 映射：`REQ-STORAGE-005`（`format_base_key.rs`、`format_*`、`custom_comparator.rs`）。

3. **[事实/证据]** As a 存储/内核开发者, I want 磁盘格式带 format version、Comparator 身份与迁移/拒绝策略, so that 不兼容的磁盘格式不会被静默打开。
   → 映射：`REQ-STORAGE-003`；门禁 G2 阻断条件（L97）。

4. **[事实/证据]** As a 存储/内核开发者, I want 部分写/尾部损坏/metadata-log 不一致/Snapshot 损坏/磁盘满都有故障测试, so that 真实介质故障下数据可恢复、不生成未承诺状态。
   → 映射：`REQ-STORAGE-004`；门禁 G2（L89–L90）。

5. **[事实/证据]** As a 存储/内核开发者, I want 任何派生状态/索引/未来热层能从 RocksDB 权威数据删除后重建, so that 热层失败不会导致权威真相丢失或被错误状态替代。
   → 映射：`REQ-STORAGE-006`；关联 `REQ-HOT-001`（P2 冻结）。

---

## 画像 4：QA / 稳定性工程师

> **[反推/产品假设]** 画像存在性反推自：Oracle provenance（REQ-COMPAT-008~010）、确定性 simulator（REQ-RAFT-006）、Jepsen/Elle（REQ-RAFT-007）、性能报告（REQ-PERF-002/003）。该角色关心“证据可重复、一致性可证明、性能可解释”。

### 用户故事

1. **[事实/证据]** As a QA/稳定性工程师, I want Oracle provenance 通过 primary build 与 verifier fresh-checkout 独立重建且 binary SHA-256 完全一致来建立信任, so that 测试 Oracle 不可由自洽 metadata + 任意 binary 拼接伪造。
   → 映射：`REQ-COMPAT-008`、`REQ-COMPAT-009`、`REQ-COMPAT-010`；D011；门禁 G1（L65、L230）。

2. **[事实/证据]** As a QA/稳定性工程师, I want 确定性 OpenRaft simulator + 3/5 节点进程级故障 + Elle/Jepsen history checker, so that Election/Log Matching/State Machine Safety 与线性一致性有机器可检查证据。
   → 映射：`REQ-RAFT-006`、`REQ-RAFT-007`；门禁 G3（L107）、G4（L126）。

3. **[事实/证据]** As a QA/稳定性工程师, I want 非幂等写断线被分类为 `SUBMIT_UNKNOWN` 且测试/客户端不自动重放, so that 不会因盲目重试造成重复写或状态分裂。
   → 映射：`REQ-RAFT-008`；门禁 G3（L110）。

4. **[事实/证据]** As a QA/稳定性工程师, I want 性能报告包含 P50/P95/P99/P99.9/吞吐/CPU/峰值内存/写放大与测试环境, so that 平均吞吐提升不会掩盖尾延迟、内存失控或恢复退化。
   → 映射：`REQ-PERF-002`、`REQ-PERF-003`；门禁 G5（L144）。

5. **[事实/证据]** As a QA/稳定性工程师, I want 每条 REQ 都有可重复证据且 P0/P1 在 Gate Review 时为零, so that “绿色测试”不能替代真实边界验证（不可妥协原则 #10）。
   → 映射：REQUIREMENTS.md L5 证据规则；system-stability-gate.md L188–L196（P0/P1 清零、required difference 为零）。

---

## 汇总：画像 → REQ 映射矩阵

| 画像 | 主要 REQ 覆盖 | 关联门禁 |
|---|---|---|
| 平台/SRE 工程师 | REQ-COMPAT-001~003、REQ-STABILITY-001~006、REQ-OBS-002、REQ-STORAGE-002、REQ-RAFT-008、REQ-LICENSE-002~008（设计/复核） | G1–G7（尤其 G2/G6/G7） |
| Redis 迁移用户 | REQ-COMPAT-001~004、REQ-COMPAT-006/007 | G1 |
| 存储/内核开发者 | REQ-STORAGE-001~006；关联 REQ-HOT-001 验收合同 | G2 |
| QA/稳定性工程师 | REQ-COMPAT-008~010、REQ-RAFT-006~008、REQ-PERF-002/003 | G1/G3/G4/G5 |

> 说明：当前没有任何 P2 **生产实现**用户故事获得授权。M7 前的来源、许可证、ABI、安全加载、可观测性接口和验收合同设计仍是活跃前置工作；上文对 `REQ-HOT-001`、`REQ-LICENSE-002~008` 的关联只表示设计/复核责任，不授权 fork、动态库、loader、发行接入或 Cache ON 数据路径。
