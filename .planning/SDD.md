---
document: kiwi-sdd
title: Kiwi 架构设计与 SDD 开发计划
status: accepted-design
authority: sole-project-entry
version: 4
updated_at: 2026-08-11
baseline_repository: arana-db/kiwi
baseline_branch: main
baseline_ref: 733888fc90ad8ef039947e87b08d7500a405954a
wp0_pr_number: 414
wp0_pr_base_ref: 0c4795ec716299598686fc7c5e0fac03a30e044d
wp0_pr_head_ref: e2bfc7deb481590a757f0034874b7f21a4a31aa2
wp0_merge_parent_ref: cbcbadc27068634d851ab0ed63989d2214ab2408
wp0_merge_ref: 9820162ebdf2d26aa6349e704efe8737b2e73e4a
wp0_exact_main_verification_ref: 688d905fec31b54aec76f36676f55efd8b5cfa17
wp0_exact_main_verification_run: 30801285622
wp0_exact_main_verification_status: passed
github_snapshot_at: 2026-08-06T04:36:21Z
redis_oracle_tag: 8.8.1
redis_oracle_ref: 77b6c308396c9700672390a210143a8496fb4b10
required_runtime_mode: cache-off
executable_scope: M0-M6
long_term_scope: M0-M10
current_work_package: WP8
current_work_package_status: in-progress
current_plan: docs/superpowers/plans/2026-08-07-vector-set-post-merge-remediation.md
current_issue: 421
current_pr: 422
next_safe_action: execute-wp8-runtime-raw-resp-client-red-tests
---

# Kiwi 架构设计与 SDD 开发计划

## 1. 文档合同

本文件是 Kiwi 项目唯一权威入口，统一回答以下问题：

- 当前系统实际是什么；
- 目标架构是什么；
- 哪些架构不变量不可破坏；
- M0 到 M10 如何演进；
- 当前允许执行哪些工作包；
- 每个工作包关联哪些 Requirement、Decision、Issue、Discussion 和 PR；
- 当前处于什么状态；
- 下一条安全动作是什么；
- 什么证据允许工作包、里程碑或系统门禁进入下一状态。

其他规划文件、专题设计、历史计划、Issue、Discussion 和 PR 都是本文件的来源、下属材料或验证证据，不得建立第二套项目状态、路线图或架构真相。

### 1.1 冲突处理

发生冲突时按以下顺序处理：

1. 当前 exact-ref 源码、真实运行结果、可重复测试和 GitHub 实时状态决定当前事实。
2. 本文件决定项目范围、架构方向、阶段状态、工作包依赖和下一安全动作。
3. REQUIREMENTS.md 提供批准的 Requirement 注册表；本文件引用而不重新定义其优先级。
4. DECISIONS.md 提供已批准 Decision 注册表；本文件将其映射到架构和工作包。
5. 专题合同提供领域细节，但不得改变本文件的项目级边界。
6. Issue 和 Discussion 提供问题、提案和讨论历史；只有进入本文件后才成为执行范围。
7. 历史 specs、plans、周报和草稿只提供背景。

### 1.2 当前态与目标态

所有架构结论必须标明：

- Current：baseline_ref 对应源码已存在的事实；
- Target：本 SDD 批准的目标；
- Gap：Current 到 Target 的可定位差距；
- Work Package：负责闭合差距的工作包；
- Acceptance：允许宣布闭合的证据。

计划中的 Redis Oracle、TCL、Raft simulator、Hot Tier 或故障矩阵不得描述为当前能力。

### 1.3 状态机

工作包使用统一状态：

~~~text
proposed
→ accepted-design
→ ready
→ in-progress
→ implemented
→ verified
→ accepted
→ released
~~~

旁路状态：

~~~text
blocked
deferred
frozen
superseded
abandoned
~~~

含义：

- accepted-design：设计已批准，但未授权源码实施。
- ready：前置决策、Issue、计划和环境均已就绪。
- in-progress：唯一当前实施任务。
- implemented：代码或文档已进入指定引用，但尚未完成合并后验证。
- verified：已在 exact ref、指定平台和指定门禁上复验。
- accepted：Requirement、Issue、文档和残留风险已经对账。
- frozen：只允许维护设计和门禁，禁止生产实现。

WP0 是一次性的 planning-only bootstrap：它的交付物就是本 SDD，因此 WP0
章节同时承担 spec 和 implementation plan，`current_plan` 指向本文件内的 WP0
锚点。WP1-WP8 不适用该例外，进入 ready 前必须建立独立 spec 和 plan。

## 2. 产品目标、范围与非目标

### 2.1 北极星

Kiwi 是一个以 Redis 8.8.1 exact tag 可观察语义兼容为目标的生产级 Rust Redis-compatible 数据库。RocksDB 保存完整、权威、可恢复的数据，OpenRaft 提供经过故障验证的强一致、高可用、成员变更、快照和恢复能力。

### 2.2 当前可执行范围

当前只执行 Cache OFF 的 M0 到 M6：

- M0：权威控制面、事实基线和恢复纪律；
- M1：Redis 8.8.1 Oracle 与兼容性基础；
- M2：RocksDB 权威状态、格式、拓扑和恢复；
- M3：Redis Core 可观察语义；
- M4：生产级单 Raft Group；
- M5：分布式故障和一致性证明；
- M6：系统稳定门禁。

### 2.3 长期范围

M7 到 M10 进入长期架构，但不进入当前实现：

- M7：Embedded Redis Hot Tier 资格验证与实现；
- M8：Cache ON 正确性、故障和性能证明；
- M9：生产发行门禁；
- M10：Multi-Raft 和远期容量。

### 2.4 当前非目标

- M6 通过并由用户另行批准前实现 Embedded Redis Hot Tier。
- 当前实现真正的 Multi-Key、跨 Slot 原子性或 2PC。
- Multiple DB、Multi-Raft、resharding。
- Vector Set Phase 2、HNSW、量化扩展、全文索引、AI 数据库或 Agent Memory 主线；PR #356 已合并的 Phase 1 表面只允许在 WP8 内做正确性、恢复、兼容性和门禁闭环。
- TOML 配置迁移。
- Small Object Compact Encoding。
- 重新引入通用 Engine facade。
- 为跳过 RocksDB native build 改变生产存储后端。
- 建立与现有 checkpoint/install-marker 平行的第二套恢复机制。

## 3. 事实来源与 GitHub 快照

### 3.1 证据等级

| 等级 | 来源 | 用途 |
|---|---|---|
| A | 当前 exact-ref 源码、已合并 PR、可复现测试 | 当前事实和验收 |
| B | 已批准 Requirement、Decision、规范性领域合同 | 目标和约束 |
| C | 边界明确的开放 Issue、未合并 PR | 候选工作项 |
| D | Discussion、RFC、Proposal | 设计输入，必须显式采纳 |
| E | 历史计划、旧周报、过期 Issue、草稿 | 背景，不形成当前任务 |

### 3.2 2026-08-06T04:36:21Z 实时快照

- Repository：arana-db/kiwi。
- Default branch：main。
- main：733888fc90ad8ef039947e87b08d7500a405954a。
- Open Issues：64；#415 是 WP1 Oracle provenance 实施 Issue，#418 跟踪 Vector differential，#421 是 WP8 primary Issue。
- Discussions：18，其中 1 个关闭，没有 accepted answer。
- PR #409：MERGED，对应 merge commit 0c4795ec716299598686fc7c5e0fac03a30e044d。
- Issue #407：CLOSED，关闭时间 2026-08-02T04:44:40Z。
- PR #412：MERGED，Head 9d1f83360eb52ed23b48b2e5cb1159c93e26e7af，merge commit cbcbadc27068634d851ab0ed63989d2214ab2408；Issue #143 已关闭。
- PR #414：MERGED，base 0c4795ec716299598686fc7c5e0fac03a30e044d，Head e2bfc7deb481590a757f0034874b7f21a4a31aa2，merge commit 9820162ebdf2d26aa6349e704efe8737b2e73e4a；Issue #413 已关闭。
- PR #417：MERGED，merge commit 688d905fec31b54aec76f36676f55efd8b5cfa17；其 main push CI run 30801285622 成功，完成 WP0 exact-main verification。
- PR #356：MERGED，最终 Head 4e404b61cec1ece8f8750e0a0631839cce7f4ddc，merge commit 733888fc90ad8ef039947e87b08d7500a405954a；VectorSet Phase 1 已进入当前主线。
- main commit 733888fc90ad8ef039947e87b08d7500a405954a 的 push CI run 31070395799、CodeQL run 31070395803 和 Benchmark run 31070395773 已成功；这些 job 没有执行 Trusted Vector differential 或 required 三节点 Vector cluster gate。
- PR #261：OPEN，Head fb092812234a54ad3757d35a62ad033136e422c7，GitHub 当前返回 mergeable/mergeStateStatus UNKNOWN，属于长期滞留 PR。

该快照只用于编制本版本。开始任一工作包、创建 PR、复审或验收前必须重新查询实时状态。

### 3.3 开放 Issue 分类

当前 64 个开放 Issue 继续按 SDD 用途分类；数量会随实时状态变化，不作为工作包授权：

| 分类 | 处理 |
|---|---|
| 当前主路线 | 映射到 M0-M6 当前主路线的 WP0-WP8 工作包 |
| 需要源码复核、重新定界或产品决定 | 先审计或决策，不直接实施 |
| 已过期、被替代或与当前方向冲突 | 不进入当前实现，后续单独治理 |

Issue 数量和分类会变化，工作包只依赖明确列出的 Issue，不依赖总数。

### 3.4 当前主路线 Issue 索引

| 主题 | Issue | SDD 用途 |
|---|---|---|
| Compaction | [#88](https://github.com/arana-db/kiwi/issues/88) | WP5 物理清理 |
| Compaction filter | [#138](https://github.com/arana-db/kiwi/issues/138) | WP5 filter 安装与生命周期 |
| Block cache | [#143](https://github.com/arana-db/kiwi/issues/143) | 支持轨道，PR #412 已合并，Issue 已关闭，待能力证据评估 |
| Error model | [#315](https://github.com/arana-db/kiwi/issues/315) | WP6 |
| Test strategy | [#325](https://github.com/arana-db/kiwi/issues/325) | WP1/WP6/WP7 |
| Raft apply Epic | [#332](https://github.com/arana-db/kiwi/issues/332) | WP4 Epic |
| Applied metadata | [#334](https://github.com/arana-db/kiwi/issues/334) | WP4 child |
| Apply marker | [#335](https://github.com/arana-db/kiwi/issues/335) | WP4 child |
| RESP write proposal | [#336](https://github.com/arana-db/kiwi/issues/336) | WP4 child |
| Apply routing validation | [#337](https://github.com/arana-db/kiwi/issues/337) | WP4 child |
| Deterministic TTL | [#338](https://github.com/arana-db/kiwi/issues/338) | WP5 primary |
| Applied frontier snapshot | [#339](https://github.com/arana-db/kiwi/issues/339) | WP5 child |
| Crash matrix | [#340](https://github.com/arana-db/kiwi/issues/340) | WP7 primary |
| StorageManifest | [#342](https://github.com/arana-db/kiwi/issues/342) | WP2 primary |
| Persisted topology | [#343](https://github.com/arana-db/kiwi/issues/343) | WP2 child |
| RocksDB architecture Epic | [#347](https://github.com/arana-db/kiwi/issues/347) | WP2/WP3 parent |
| Bounded executor | [#351](https://github.com/arana-db/kiwi/issues/351) | WP3 primary |
| Runtime cleanup | [#352](https://github.com/arana-db/kiwi/issues/352) | WP3 child |
| Native build Epic | [#353](https://github.com/arana-db/kiwi/issues/353) | 支持轨道 |
| Graceful shutdown | [#408](https://github.com/arana-db/kiwi/issues/408) | WP3 child |
| Real INFO state | [#410](https://github.com/arana-db/kiwi/issues/410) | WP7 related |
| SDD control plane | [#413](https://github.com/arana-db/kiwi/issues/413) | WP0 primary |
| Trusted Redis Oracle | [#415](https://github.com/arana-db/kiwi/issues/415) | WP1 implementation dependency；WP8 full-scope PR 内闭环 |
| Vector differential | [#418](https://github.com/arana-db/kiwi/issues/418) | WP8 related |
| VectorSet lifecycle | [#421](https://github.com/arana-db/kiwi/issues/421) | WP8 primary |

[Issue #407](https://github.com/arana-db/kiwi/issues/407) 已关闭，
[PR #409](https://github.com/arana-db/kiwi/pull/409) 已合并。后续只对账剩余
TTL、generation、compaction、snapshot 和 replay 语义，不重复规划 DEL 基础实现。

### 3.5 需要复核或决策的 Issue

先基于当前 main 和 Redis 8.8.1 Oracle 复现，再决定关闭、改写或拆 child Issue：

- #95、#117、#121；
- #127、#128、#129、#130、#131；
- #141、#142、#144；
- #195、#214、#215、#219、#220、#223、#252。

先做产品或架构决策，不直接实施：

- #106 Multiple DB；
- #192 MONITOR；
- #196 cross-slot atomicity；
- #205 skip RocksDB compilation；
- #230 Raft source research；
- #247 TOML；
- #368 Runtime RFC。

从当前 M0-M6 实现范围排除：

- #39、#42、#45、#47；
- #55、#57、#65、#66、#74、#90；
- #132、#133、#204、#213、#218、#341。

### 3.6 Discussion 处理

| 分类 | Discussion | 用途 |
|---|---|---|
| 当前设计输入 | [#330](https://github.com/arana-db/kiwi/discussions/330) | Raft apply 工作分解 |
| 当前决定来源 | [#331](https://github.com/arana-db/kiwi/discussions/331) | VectorSet Phase 1 设计来源；Phase 2 继续 Frozen，合并后闭环由 WP8 管理 |
| 当前设计输入 | [#344](https://github.com/arana-db/kiwi/discussions/344) | Storage 生产化候选项 |
| 当前设计输入 | [#345](https://github.com/arana-db/kiwi/discussions/345) | Compact Encoding 预研，Deferred |
| 当前决定来源 | [#346](https://github.com/arana-db/kiwi/discussions/346) | 当前不支持真正 Multi-Key |
| 当前设计输入 | [#377](https://github.com/arana-db/kiwi/discussions/377) | Redis AI 候选方向，Frozen |
| 当前优先级参考 | [#411](https://github.com/arana-db/kiwi/discussions/411) | Rust 与 kiwi-cpp 差距参考 |
| 历史背景 | #48、#49、#169、#176、#232、#235、#236、#241、#257 | 不进入当前实现 |
| 已过期 | [#301](https://github.com/arana-db/kiwi/discussions/301) | 不再作为当前 Vector/FT 路线 |

Discussion 没有 accepted answer；#346 已被本 SDD 采纳为 D017，#331 的已合并 Phase 1 事实和闭环边界由 D019/WP8 接纳；其他 Discussion 不能单独定义当前产品行为。

## 4. 当前系统架构

### 4.1 Workspace 与主要职责

当前 workspace 包含：

- storage；
- kstd；
- common/macro；
- common/runtime；
- net；
- resp；
- server；
- conf；
- cmd；
- executor；
- client；
- raft；
- tools/runtime-baseline；
- tools/compat。

历史 engine crate 已不在 workspace。主数据路径直接依赖具体 RocksDB，不存在第二生产存储后端。

### 4.2 当前请求链路

~~~mermaid
flowchart LR
    Client["Redis Client"]
    Net["NetworkServer<br/>连接、RESP、认证、网络侧 admission"]
    Channel["StorageClient<br/>有界 MPSC + oneshot"]
    StorageServer["StorageServer<br/>存储 Runtime 调度"]
    Cmd["Cmd::execute<br/>命令语义"]
    Storage["Storage / Redis instances"]
    RocksDB["RocksDB<br/>当前 7 个 CF"]

    Client --> Net
    Net --> Channel
    Channel --> StorageServer
    StorageServer --> Cmd
    Cmd --> Storage
    Storage --> RocksDB
~~~

当前事实：

- 进程创建 bootstrap、network、storage 三个 Tokio Runtime。
- 双 Runtime 只准确描述请求数据面。
- network 到 storage 的主 MessageChannel 有界。
- 活跃存储命令路径没有经过 CmdExecutor worker queue。
- 网络侧和 StorageServer 侧存在双阶段初始化或校验约束。
- standalone mutation 最终直接提交 RocksDB。
- cluster mutation 通过 BinlogBatch 和 append-log callback 进入 OpenRaft。
- Raft state-machine apply 使用直接 RocksDB batch，避免递归提交 Raft。

### 4.3 当前存储事实

- RocksDB 是业务数据、类型、编码和 TTL etime 的唯一持久化权威。
- 主数据库使用七个 CF：default/meta/string、hash、set、list、zset-data、zset-score、vector-data。
- CF 名称、index、comparator、compaction filter、batch 路由和 Raft checkpoint 列表分散硬编码。
- 当前每个 instance 有只保存 version、storage incarnation 和 next generation 的 StorageManifest v1；它尚不能表达全局 topology、CF/comparator/codec、snapshot compatibility 和可恢复 migration。
- ExpirationManager 是进程内索引，启动时不从 RocksDB 重建。
- CompactSpecificKey 当前只记录日志并返回成功，未执行真实物理清理。

### 4.4 当前 Raft 与 Snapshot 事实

- 生产节点使用独立 RocksDB Raft log store。
- RocksDB batch write 返回成功后才推进进程内 last_applied；该写入是否满足最终批准的 durability profile 尚未完成证明。
- Raft log、vote 和 committed state 使用普通 RocksDB write/put 后即确认完成，尚缺显式 stable-storage 语义证明。
- Snapshot install 已有 staged restore、pause、install marker、RocksDB reopen 和切换骨架。
- Snapshot archive 当前完整驻留内存。
- Snapshot writer 当前使用 v2，reader 拒绝真实 Base v1 和未来版本；Base v1 compatibility、install phase recovery 和完整 manifest pairing 尚未闭合。

### 4.5 当前生命周期和验证事实

- RuntimeManager 当前先停止 storage runtime，再停止 network runtime。
- network、StorageServer、cluster gRPC 和 Raft bridge 的长期任务没有统一 supervisor。
- Storage 到 Raft bridge 使用无界 channel。
- Redis 8.8.1 compatibility manifest 已登记 Vector 命令和 known differences，但正常 CI 排除 Vector differential，尚无可信 Oracle 执行证据。
- CI 尚未形成完整 Oracle、raw RESP differential、TCL、deterministic Raft simulator、process crash matrix 和真实磁盘 upgrade/rollback 矩阵。
- Embedded Redis Hot Tier 当前没有生产依赖、loader、FFI 或 Cache ON 数据路径。

## 5. 目标系统架构

~~~mermaid
flowchart TB
    subgraph External["外部边界"]
        Client["Redis Client"]
        Admin["运维 / 管理接口"]
        Oracle["Redis 8.8.1 Oracle<br/>仅测试进程"]
    end

    subgraph NetworkPlane["网络与准入平面"]
        Listener["Listener / Connection Supervisor"]
        Resp["RESP2/RESP3 Parser / Encoder"]
        Admission["认证、命令分类、背压、绝对 Deadline"]
    end

    subgraph CommandPlane["确定性命令平面"]
        Contract["Command Contract<br/>参数、错误优先级、权限、Key/Slot"]
        Operation["Deterministic Operation<br/>Read / Mutation / Admin"]
    end

    subgraph RuntimeBoundary["Runtime 边界"]
        Gateway["Storage Gateway<br/>有界队列、取消、Deadline、指标"]
        Scheduler["Storage Scheduler / Bounded Executor"]
    end

    subgraph ConsensusPlane["共识平面"]
        ReadBarrier["Linearizable Read Barrier"]
        Proposal["Raft Proposal"]
        OpenRaft["OpenRaft"]
        Apply["State-Machine Apply"]
    end

    subgraph StoragePlane["权威存储平面"]
        Owner["Storage Owner"]
        Manifest["StorageManifest"]
        Rocks["RocksDB<br/>唯一权威状态"]
        TTL["TTL / Generation / Lifecycle"]
        Snapshot["Checkpoint / Snapshot / Restore"]
    end

    subgraph LifecyclePlane["生命周期和可观测性"]
        Supervisor["Process Supervisor"]
        Provider["Runtime / Storage / Raft State Provider"]
        Info["INFO / Metrics / Health"]
    end

    subgraph Future["冻结能力"]
        HotTier["Embedded Redis Hot Tier"]
        MultiRaft["Multi-Raft / Resharding"]
    end

    Client --> Listener --> Resp --> Admission --> Contract --> Operation --> Gateway --> Scheduler
    Scheduler -->|"standalone"| Owner
    Scheduler -->|"cluster read"| ReadBarrier --> Owner
    Scheduler -->|"cluster mutation"| Proposal --> OpenRaft --> Apply --> Owner
    Owner --> Manifest
    Owner --> Rocks
    Owner --> TTL
    Owner --> Snapshot
    Supervisor --> Listener
    Supervisor --> Gateway
    Supervisor --> Scheduler
    Supervisor --> OpenRaft
    Supervisor --> Owner
    Owner --> Provider
    OpenRaft --> Provider
    Supervisor --> Provider
    Provider --> Info
    Admin --> Info
    Oracle -.测试比较，不进入生产依赖.-> Contract
    Rocks -.M6 后且另行批准.-> HotTier
    OpenRaft -.容量证据充分后.-> MultiRaft
~~~

### 5.1 强制依赖方向

~~~text
Network
→ Command Contract
→ Storage Gateway
→ Storage Scheduler
→ Consensus / Storage
→ RocksDB
~~~

禁止：

- Storage 依赖 Network；
- RocksDB 依赖命令解析；
- Raft 持有 RESP connection；
- Oracle 进入生产 runtime；
- Hot Tier 成为权威存储；
- 通过全局单例、逃逸 Arc 或无 owner callback 建立隐式反向依赖。

### 5.2 组件职责

| 组件 | 负责 | 不负责 |
|---|---|---|
| Network/RESP | 连接、partial I/O、协议、网络资源上界 | 业务状态和 RocksDB |
| Admission | 认证、分类、背压、请求预算、absolute deadline | 执行业务 mutation |
| Command Contract | 参数、错误优先级、权限、Key/Slot、能力声明 | 线程和物理存储 |
| Storage Gateway | network 到 storage 的唯一异步边界 | 命令语义 |
| Storage Scheduler | 有界调度、取消、deadline、并发控制 | 第二套业务模型 |
| OpenRaft | 日志、投票、提交、membership、一致性顺序 | Redis 命令错误语义 |
| State-Machine Apply | 确定性应用已提交 mutation | 吞掉 apply fatal error |
| Storage Owner | DB、instance、CF、gate、reopen、snapshot 生命周期 | 网络连接 |
| StorageManifest | 格式、CF、comparator、DataType、topology、版本 | 动态业务状态 |
| Lifecycle Supervisor | task owner、cancel、drain、join 和退出次序 | 命令语义 |
| State Provider | 输出实际 runtime/storage/Raft 状态 | 自建影子状态 |
| Redis Oracle | 兼容性实验和差异证据 | 生产请求和数据 |
| Hot Tier | 获批后的可丢弃读加速 | 权威数据、Raft Log、Snapshot |

### 5.3 部署模式

Standalone：

- 单进程；
- network/storage 请求数据面隔离；
- mutation 通过 Storage Scheduler 直接进入 RocksDB atomic batch；
- 不创建生产 Raft proposal 快速路径；
- 仍使用相同 StorageManifest、TTL、Snapshot、shutdown 和兼容性合同。

Cluster：

- 当前目标是生产级单 Raft Group；
- 所有 client mutation 经过 OpenRaft；
- linearizable read 由当前 Leader 提供，并经过 OpenRaft `ensure_linearizable`/ReadIndex，或经过批准且证明安全的 Lease read protocol；
- Raft log store 与业务 RocksDB 生命周期分别管理，但由统一 supervisor 排序；
- snapshot、membership、leader transfer 和 reopen 进入 required gate。

Standalone 和 Cluster 不能形成两套 Redis 可观察语义、磁盘格式或 TTL 规则。Multi-Raft、resharding 和跨 Slot 原子性不属于当前部署模式。

## 6. 架构不变量

下表定义目标架构不变量，不把目标能力写成当前事实。`Current` 记录本 PR
绑定 baseline_ref 的源码状态；`Gap`、`Work Package` 和 `Acceptance` 给出从
当前状态闭合到 Target 的责任和可重复证据。

| ID | Target invariant | Current | Gap | Work Package | Acceptance |
|---|---|---|---|---|---|
| `INV-01` | 所有架构和验收结论绑定 exact Git SHA。 | 本 SDD 和源码证据索引已绑定 baseline_ref。 | 后续实现、PR 和 exact-main 证据仍需逐次绑定最终 SHA。 | WP0-WP8 | 每个工作包记录 Base、Head、merge SHA 和 exact-main 验证 SHA。 |
| `INV-02` | RocksDB 保存完整、权威、可恢复的业务状态。 | RocksDB 是当前业务数据权威。 | CF/格式 manifest、真实 close/reopen 和故障恢复证据未闭合。 | WP2、WP4、WP5、WP7、WP8 | manifest、durable apply、重启、损坏和 snapshot 恢复门禁全部通过。 |
| `INV-03` | standalone mutation 直接写 RocksDB；cluster client mutation 必须先经 Raft；Raft apply 不得再次提交 Raft。 | standalone 与 cluster 路由已经分离，apply 有独立入口。 | cluster 全命令覆盖和反向绕过证明不足。 | WP4、WP6 | 路由矩阵和回归测试证明所有 mutation 只有一条合法路径。 |
| `INV-04` | mutation 持久化成功后才能推进 last_applied。 | 当前只能证明 RocksDB batch write 返回成功后才推进进程内 last_applied。 | 原子性、批准的 durability profile、I/O failure 和崩溃窗口证据不足。 | WP4 | fault injection 证明 Durable 前不会推进 last_applied。 |
| `INV-05` | Raft vote、log 和 committed state 只能在满足 OpenRaft 持久化合同后确认完成。 | 当前实现使用普通 RocksDB write/put 持久化 vote、log 和状态。 | sync 语义和 callback 完成点缺少完整故障证明。 | WP4 | OpenRaft storage suite、fsync profile 和故障矩阵通过。 |
| `INV-06` | Linearizable Read 必须由当前 Leader 提供，并经过 OpenRaft `ensure_linearizable`/ReadIndex，或经过批准且证明安全的 Lease read protocol；单纯 leader 身份检查不构成读屏障。 | network leader gate 只拦截非 leader write，read 没有 ReadIndex/Lease barrier。 | follower、leader transfer、partition 和 term 变化下可能读取 stale state。 | WP4、WP7 | 读屏障测试和线性一致性 history 通过。 |
| `INV-07` | 所有跨 Runtime 或跨长期任务队列必须有界。 | MessageChannel 有界；Storage→Raft 仍使用 unbounded channel。 | 需要统一容量、过载语义和指标。 | WP3 | 队列容量、queue-full 响应、压力和内存上界门禁通过。 |
| `INV-08` | 一个请求只使用一个 absolute deadline，排队和执行共享预算。 | pipeline 只有局部共享 timeout，其他阶段各自计时。 | admission、排队、Raft、apply 和响应尚未共享同一预算。 | WP3、WP4 | deadline 传播测试证明各阶段只消费剩余预算。 |
| `INV-09` | 所有长期任务必须有 owner、cancellation token、JoinHandle 和确定性 join。 | 多个长期任务仍由裸 `tokio::spawn` 启动。 | owner、取消和 join 责任未统一。 | WP3 | lifecycle registry 和退出测试证明无 detached task。 |
| `INV-10` | shutdown 必须先停止 admission，再 drain 依赖，最后关闭 RocksDB。 | 当前 manager 先停止 storage，再停止 network。 | 关闭顺序与依赖方向相反。 | WP3 | 并发 shutdown 测试证明 admission→drain→RocksDB 顺序。 |
| `INV-11` | persisted etime 是 TTL 权威；内存索引只能是可丢失优化。 | etime 已持久化，expiration manager 是辅助索引。 | restart、compaction、generation 和 stale-index 证据不足。 | WP5 | 删除内存索引后重建、重启和 TTL differential 通过。 |
| `INV-12` | 所有 CF 消费者由同一可验证 manifest 闭合。 | VectorDataCF 已加入，当前 per-instance manifest v1 只保存 incarnation/generation，CF 列表仍分散在创建、扫描、TTL、compaction 和 snapshot 路径。 | 缺少 Root/Instance manifest v2 和消费者闭包检查。 | WP2、WP8 | manifest consumer-closure checker 和新增 CF 变异测试通过。 |
| `INV-13` | 未知 disk、snapshot、comparator 或 manifest 版本默认 fail closed。 | snapshot v2 和 marker 对未知版本 fail closed，但真实 Base v1 snapshot 尚未进入显式兼容范围。 | 已知历史版本兼容与未知未来版本拒绝需要同时证明。 | WP2、WP5、WP8 | Base v1 正向恢复和未知/未来版本负向测试通过。 |
| `INV-14` | Snapshot build、install、普通 apply、reopen 和 shutdown 必须由同一 gate 建立顺序。 | 各路径有局部锁和 staged install。 | 缺少覆盖全部状态转换的统一 gate。 | WP3、WP5 | 并发 build/install/apply/reopen/shutdown 矩阵无竞态和旧状态可见。 |
| `INV-15` | 旧 Storage、Redis、DB、CF、iterator 或 snapshot handle 不得跨 reopen/swap 继续使用。 | reopen 和 install 会替换部分顶层对象。 | 跨层缓存 handle 的失效证明不足。 | WP2、WP5 | generation/handle 负向测试证明旧对象全部拒绝使用。 |
| `INV-16` | 每个 Binlog 必须有明确 db、instance、slot/group 和 generation 语义。 | db_id 固定为 0，slot 从 key 推导，instance 由本机 topology 推导，缺少 group/generation。 | replay、迁移和 stale generation 语义未定义。 | WP2、WP4、WP5 | 编码 round-trip、cluster replay 和 generation rejection 通过。 |
| `INV-17` | Redis 兼容结论必须来自固定 Oracle、raw response 和最终状态的可复现实验。 | 已固定 Redis 8.8.1 tag/commit 和 provenance 合同。 | Vector differential 被正常 CI 排除，独立重建和 runtime identity 尚未完成。 | WP1、WP6、WP7、WP8 | exact Oracle 重建 hash equality、raw RESP2/RESP3 differential 与兼容矩阵通过。 |
| `INV-18` | 非幂等写结果未知时标记 SUBMIT_UNKNOWN，不盲目重试。 | 当前没有端到端 typed SUBMIT_UNKNOWN，相关失败压成通用错误。 | 断线、超时和提交后响应丢失仍可能混同普通失败。 | WP4、WP7 | fault history 区分 safe failure、success 和 SUBMIT_UNKNOWN。 |
| `INV-19` | INFO 和 metrics 只消费真实 provider，不维护硬编码影子状态。 | INFO 仍包含硬编码版本、平台、PID、端口和 uptime。 | runtime、storage 和 Raft provider 未闭合。 | WP7 | provider contract 测试和真实进程 INFO/metrics 对账通过。 |
| `INV-20` | M6 前 Embedded Redis Hot Tier 保持 frozen。 | D009 和当前 scope 已冻结 M7/M8 热层实施，生产路径中不存在 Hot Tier。 | 无实现 Gap；必须持续防止实现 PR 隐式解除冻结。 | WP7 | M6 gate、用户批准和新 Decision 同时存在后才可解除。 |

## 7. 关键状态机

### 7.1 请求 Deadline

~~~text
Accepted
→ Admitted
→ Queued
→ Executing
→ Completed

旁路：
Rejected(queue-full)
Cancelled(client-or-shutdown)
TimedOut(deadline)
SubmitUnknown(cluster-non-idempotent)
~~~

规则：

- 在网络准入时生成单一 absolute deadline。
- 入队、等待 gate、Raft proposal、apply 和 response 共享剩余预算。
- queue full 立即返回有类型的过载错误，不无限等待。
- deadline 到期不等于 mutation 未提交；cluster 非幂等写必须区分 safe-failure 和 SUBMIT_UNKNOWN。

### 7.2 Cluster Write / Apply

~~~text
Validated
→ Routed
→ Proposed
→ Committed
→ Applying
→ Durable
→ Applied
→ Replied
~~~

规则：

- commit 前不得改变权威业务状态。
- apply corruption、CF、slot、instance 或 I/O failure 是 state-machine fatal error，不是普通 Redis response。
- last_applied 在 Durable 后推进。
- callback 只有在相应 stable-storage 语义满足后完成。
- 客户端取消不能取消已经进入共识并可能提交的 mutation。

### 7.3 TTL、DEL 和 Generation

~~~text
Live(generation=N, etime)
→ LogicallyExpired
→ Invisible
→ PhysicallyReclaimed

DEL:
Live(N)
→ Tombstoned/GenerationAdvanced(N+1)
→ Invisible
→ Reclaimed
~~~

规则：

- persisted etime 和 generation 决定可见性。
- 物理清理可以延迟，不能决定正确性。
- 旧 generation 的 collection data、异步任务和未来 cache fill 不得重新变为可见。
- Issue #407 和 PR #409 作为已完成 DEL generation 基础证据；WP5 只处理剩余跨类型、TTL、compaction、snapshot 和 replay 闭环。

### 7.4 Snapshot Install

~~~text
Receiving
→ Staged
→ Validated
→ Paused
→ MarkerPersisted
→ OldStorageClosed
→ CheckpointSwapped
→ NewStorageOpened
→ MetadataCommitted
→ MarkerRemoved
→ Resumed
~~~

规则：

- archive 和 unpack 必须流式、有总字节、文件数、路径和展开比限制。
- unknown version 默认拒绝。
- 每个 marker 阶段必须定义重启恢复动作。
- collector 或 topology metadata 损坏只有在可证明重建时才能忽略，否则拒绝安装。

### 7.5 Process Shutdown

~~~text
Running
→ Quiescing
→ DrainingNetwork
→ DrainingStorageAndRaft
→ StoppingBackgroundTasks
→ ClosingStorage
→ StoppingRuntimes
→ Stopped
~~~

顺序：

1. 停止 accept 和 admission。
2. 取消并等待连接任务。
3. 关闭 request sender。
4. drain StorageServer 和 Raft in-flight。
5. 结束 snapshot、expiration、compaction、gRPC 和 bridge tasks。
6. 关闭 Storage、Redis、DB、CF 和相关 handle。
7. 停止 storage、network 和 bootstrap runtime。

超时后允许强制退出，但必须输出未完成阶段和 SUBMIT_UNKNOWN 风险。

## 8. 数据格式、CF、Topology 和恢复

### 8.1 StorageManifest 最小合同

WP2 的详细规格必须至少冻结：

- manifest schema version；
- Kiwi disk format version；
- DataType tag table；
- CF identity、name、index 和 role；
- comparator name、version 和 ordering contract；
- key/value encoding version；
- instance count 和 instance identity；
- slot/topology generation；
- RocksDB option compatibility identity；
- snapshot compatibility range；
- created_by 和 last_migrated_by；
- checksum。

### 8.2 Consumer Closure

新增或修改 DataType、CF 或格式时必须检查：

- DataType 解析和显示；
- ColumnFamilyIndex；
- descriptor 和 options；
- comparator；
- compaction filter；
- batch routing；
- infer_user_key；
- TTL；
- SCAN TYPE 和全局 enumeration；
- RANDOMKEY；
- manifest；
- checkpoint/snapshot；
- Raft replay；
- upgrade/rollback；
- cluster failover；
- backup/restore。

### 8.3 Upgrade / Rollback

- old binary 打开 newer unsupported format：拒绝。
- new binary 打开 supported old format：只通过显式 migration。
- migration 必须可中断恢复，不能依赖内存状态。
- rollback 仅在兼容矩阵明确允许时进行。
- 未知 comparator 或 CF schema：拒绝启动。
- 所有矩阵使用真实磁盘目录和真实 reopen，不使用仍持有 DB 的对象替代。

## 9. 错误、资源和可观测性

### 9.1 错误分类

至少区分：

- ProtocolError；
- AuthenticationError；
- CommandValidationError；
- WrongType；
- UnsupportedCommand；
- Overloaded；
- DeadlineExceeded；
- NotLeader / Redirect；
- SubmitUnknown；
- StorageUnavailable；
- StorageCorruption；
- DurabilityFailure；
- SnapshotRejected；
- ShutdownInProgress；
- InternalInvariantViolation。

客户端错误、可重试基础设施错误和必须终止 state machine 的 fatal error 不得共享模糊字符串分支。

### 9.2 资源上界

必须显式配置并测试：

- connection count；
- unauthenticated buffer；
- RESP frame、bulk、nesting、node 和 parse-work 上限；
- network 到 storage queue；
- storage executor queue 和 concurrency；
- storage 到 Raft queue；
- snapshot archive 和 unpack；
- in-flight requests；
- per-request deadline；
- shutdown drain deadline；
- log、metric label 和 artifact 输出上限。

### 9.3 真实状态提供者

统一 State Provider 输出：

- runtime state、queue depth、rejection、deadline；
- storage open/closing/reopening、instance、format、CF、block cache；
- Raft node id、role、term、leader、commit、applied、snapshot、membership；
- lifecycle phase 和未 join tasks；
- compatibility profile identity；
- 当前 Cache OFF 状态。

INFO、metrics、health 和管理命令只消费该 provider。Issue #410 在 WP7 中实施。

### 9.4 安全、供应链和许可证

- 外部输入可触发的生产路径不得使用 unwrap/expect 制造进程崩溃。
- 日志、metric label、错误和 evidence artifact 不得泄露凭据、原始敏感 payload 或内部地址。
- 第三方源码和行为基线固定 exact tag/commit，记录许可证、patch 和构建输入。
- Kiwi 自有源码保持 Apache-2.0。
- 未来 Redis-derived native library 按 AGPL-3.0-only 管理；组合发行不得声明 Apache-2.0-only。
- Embedded Redis Hot Tier 的许可证、ABI、loader、allocator、线程、多实例和发行必须在 M7 独立批准。
- RedisRaft 只作 clean-room 公共行为参考，不复制其内部实现或测试。

## 10. Redis 8.8.1 兼容性与测试体系

### 10.1 Oracle 边界

- 唯一普通 Redis Oracle：tag 8.8.1、commit 77b6c308396c9700672390a210143a8496fb4b10。
- Oracle 是独立测试进程，不保存 Kiwi 数据，不参与 Raft。
- 正式 provenance 要求 primary build 与 fresh-checkout verifier rebuild binary hash 一致。
- 正式 INFO server 证据只来自 verifier rebuild。
- 任一 cleanup、identity 或 hash 复核失败不得发布成功 provenance。

### 10.2 分层门禁

PR fast gate：

- manifest schema；
- targeted raw RESP transcript；
- affected command unit/integration；
- parser/partial-I/O regression；
- changed storage/Raft targeted tests；
- formatting、lint、diff check。

Nightly/full gate：

- Redis TCL external-server；
-完整 raw RESP2/RESP3 differential；
- property/fuzz corpus；
- deterministic Raft seeds；
- process fault matrix；
- sanitizer；
- long-running and resource-bound tests。

Release/M6 gate：

- fresh environment rebuild；
- upgrade/rollback；
- backup/restore；
- real close/reopen；
- crash/power-loss model；
- 3/5 node histories；
- exact evidence bundle。

### 10.3 Skip 与 Difference

每个 skip 或 known difference 必须包含：

- owner；
- GitHub Issue；
- exact reason；
- introduced date；
- affected mode/platform；
- removal condition；
- last verified ref。

没有 Issue 的 skip 不能合并。

## 11. M0-M10 路线

~~~mermaid
flowchart LR
    M0["M0<br/>控制面与恢复"]
    M1["M1<br/>Oracle"]
    M2["M2<br/>RocksDB 权威"]
    M3["M3<br/>Redis Core"]
    M4["M4<br/>单 Raft Group"]
    M5["M5<br/>故障证明"]
    M6["M6<br/>稳定门禁"]
    Approval["用户重新批准"]
    M7["M7<br/>Hot Tier"]
    M8["M8<br/>Cache ON"]
    M9["M9<br/>Release"]
    M10["M10<br/>Multi-Raft"]

    M0 --> M1
    M0 --> M2
    M1 --> M3
    M2 --> M3
    M2 --> M4
    M3 --> M4
    M4 --> M5
    M5 --> M6
    M6 --> Approval --> M7 --> M8 --> M9 --> M10
~~~

M1 和 M2 可在边界清晰的独立工作包中有限并行。M4、M5 只以 Cache OFF 为 required 模式。M7-M8 在 M6 PASS 和用户重新批准前保持 frozen。M10 需要单 Group 容量和故障域证据。

## 12. WP0-WP8 可执行工作包

### 12.1 工作包依赖

~~~mermaid
flowchart LR
    WP0["WP0<br/>单一控制面"]
    WP1["WP1<br/>Oracle"]
    WP2["WP2<br/>Manifest/Topology"]
    WP3["WP3<br/>Runtime/Lifecycle"]
    WP4["WP4<br/>Raft Durable Apply"]
    WP5["WP5<br/>TTL/Snapshot/Compaction"]
    WP6["WP6<br/>Redis Core Semantics"]
    WP7["WP7<br/>Fault Matrix/M6"]
    WP8["WP8<br/>VectorSet Post-Merge Closure"]

    WP0 --> WP1
    WP0 --> WP2
    WP0 --> WP3
    WP2 --> WP4
    WP3 --> WP4
    WP1 --> WP6
    WP2 --> WP6
    WP2 --> WP5
    WP4 --> WP5
    WP1 --> WP7
    WP3 --> WP7
    WP4 --> WP7
    WP5 --> WP7
    WP6 --> WP7
    WP0 --> WP8
~~~

建议执行波次：

1. Wave 0：WP0。
2. Wave 1：WP1、WP2、WP3 使用独立 worktree 有限并行。
3. Wave 2：WP4 和 WP6；两者不能共享实现分支。
4. Wave 3：WP5。
5. Wave 4：WP7 和 M6 Gate Review。
6. Wave X：WP8 是 D019 明确授权的合并后紧急闭环；它在一个 Draft PR 内实现并验收所消费的 WP1-WP7 特定合同，不把这些工作包的无关范围伪装为已 accepted。

同一 worktree 和同一 current_work_package 仍只允许一个 in-progress 工作包。并行工作包必须有独立 Issue、spec、plan、branch 和 recovery；D019 只允许这些隔离任务按顺序聚合到同一个 WP8 Draft PR，不允许并发写同一分支。

每个工作包都必须显式给出 Status、Primary Issue handling、Parent/Related、
Requirements、Dependencies、Scope、Non-goals、Acceptance criteria 和
Verification gates。下文中的“主要范围”“退出门禁”分别对应 Scope 和
Acceptance criteria；缺少任一字段时，工作包不得进入 ready。

<a id="wp0"></a>

### WP0：单一 SDD 控制面与事实对账

状态：accepted。

目标：

- 建立本文件作为唯一入口。
- 清除 STATE、KANBAN、ROADMAP 的独立实时状态。
- 修正文档中的三 Runtime、真实请求链路、TTL 权威和当前验证能力。
- 建立 Issue、REQ、工作包、PR 和验证追踪。

主要范围：

- .planning/SDD.md；
- .planning/README.md；
- .planning/STATE.md；
- .planning/KANBAN.md；
- .planning/ROADMAP.md；
- CLAUDE.md；
- CONTRIBUTING.md；
- README.md；
- docs/INDEX.md；
- docs/prd.md；
- docs/architecture/redis-8.8.1-system-boundaries.md；
- docs/quality/quality-gates.md；
- docs/quality/system-stability-gate.md；
- .planning/DECISIONS.md；
- .planning/OPEN_QUESTIONS.md；
- .planning/REQUIREMENTS.md；
- docs/personas-and-user-stories.md；
- scripts/validate_sdd.py；
- .github/workflows/ci.yml；
- .github/pull_request_template.md。

Primary Issue handling：

- Primary Issue：[#413](https://github.com/arana-db/kiwi/issues/413)。
- Issue #413 只覆盖单一 SDD 控制面、事实基线与交付追踪，不吸收 WP1-WP8 的运行时实施。
- 只有完整满足本工作包退出门禁的 PR 才能使用 `Fixes #413` 或 `Closes #413`；部分交付使用 `Refs #413` 或 `Related #413`。

Parent / Related：N/A。

Implementation PR：[#414](https://github.com/arana-db/kiwi/pull/414)。

Post-merge validation repair：[#416](https://github.com/arana-db/kiwi/issues/416)。

合并证据：

- PR 固定区间：0c4795ec716299598686fc7c5e0fac03a30e044d..e2bfc7deb481590a757f0034874b7f21a4a31aa2；
- merge 固定区间：cbcbadc27068634d851ab0ed63989d2214ab2408..9820162ebdf2d26aa6349e704efe8737b2e73e4a；
- PR #414 于 2026-08-02T12:17:13Z 合并，Issue #413 随后关闭；
- main push CI run 30747510551 / job 91495496924 失败，原因是旧 baseline_ref 把先合并的 PR #412 的 7 个源码路径计入 WP0，而不是 WP0 产物本身失败。
- PR #417 修复固定提交区间验证并于 2026-08-03 合并为 688d905fec31b54aec76f36676f55efd8b5cfa17；
- WP0 exact-main verification：status=passed，ref=688d905fec31b54aec76f36676f55efd8b5cfa17，run=30801285622。
- baseline_ref 已推进到该 verification ref 的后继 main commit 733888fc90ad8ef039947e87b08d7500a405954a，WP0 的 Requirement、Issue、文档和残留风险完成对账。

Requirement：

- REQ-WORK-001 至 REQ-WORK-007。

依赖：

- PR base main@0c4795ec716299598686fc7c5e0fac03a30e044d 的源码事实基线；
- 2026-08-06T04:36:21Z 的 GitHub 快照；
- 无前置工作包；本节按 planning-only bootstrap 例外同时作为 WP0 plan。

非目标：

- 不修改 Runtime、Storage、Raft、协议、构建或测试行为；
- 不实现 WP1-WP8；
- 不实现未来的自动 PR traceability checker；
- 不以 PR #414 已合并或 Issue #413 已关闭自动授权后续源码工作；WP0 已用独立 exact-main run 完成 accepted 对账，后续工作仍需各自授权。

退出门禁：

- front matter 是唯一机器可解析的当前状态；工作包块和状态表必须与其一致。
- 所有链接和 REQ/Decision 定义、范围及引用全集闭包通过。
- PR 模板要求工作包、Issue 和 REQ，并由评审门禁确认没有保留占位符。
- 原草稿被吸收或删除。

验证门禁：

- `git diff --check` 和 committed-diff whitespace check；
- `python scripts/validate_sdd.py --self-test` 的失败路径变异测试；
- `python scripts/validate_sdd.py` 的 Markdown 链接、占位词、围栏和状态断言；
- WP0 exact-main 状态提升时，baseline_ref 必须推进到 verification ref 或其后的 main 提交，并在线核验 recorded GitHub Actions run 与 ci workflow、main push、精确 SHA 和 success 结论一致；
- 68 个 REQ 和 19 个 Decision 的唯一注册、范围展开和引用全集闭包；
- WP0、primary Issue #413、PR #414 和 20 个预期产物的一致性断言；
- live Issue #413、开放 Issue 数量、关键 PR 状态和远端 main 复核；
- 独立只读审查不得留下 Critical 或 Important finding。

### WP1：Redis 8.8.1 Oracle 与兼容性基础

状态：proposed。

Primary Issue：#325。

Related：

- #315；
- #415（M1-001-T2 Oracle provenance）；
- OQ-3；
- OQ-10。

Requirement：

- REQ-COMPAT-001 至 REQ-COMPAT-010；
- REQ-STABILITY-002；
- REQ-STABILITY-003。

依赖：

- WP0 accepted；
- Redis 8.8.1 exact tag 和 commit 身份保持固定；
- OQ-3 与 OQ-10 在进入 ready 前形成可执行选择。

交付：

- trusted Oracle independent rebuild；
- raw RESP2/RESP3 differential；
- TCL external-server runner；
- redis-rs test-only suite；
- partial-I/O property tests 和 fuzz；
- skip/difference registry；
- PR fast、nightly full、release gate 分层。

主要范围：

- tools/compat；
- tests/compat/redis-8.8.1；
- tests/tcl；
- tests/python；
- CI compatibility jobs；
- compatibility docs。

非目标：

- 不把 Redis 或 redis-rs 引入生产 server dependency；
- 不声明当前已实现全命令兼容；
- 不实现 Embedded Redis Hot Tier、Cluster Multi-Key 或业务存储格式。

退出门禁：

- Oracle provenance 不能由自报 metadata 证明。
- 基础命令 transcript 可重复。
- 差异均有 Issue、owner 和退出条件。

验证门禁：

- Oracle primary build 与 independent verifier rebuild hash 一致；
- `cargo test --manifest-path tools/compat/Cargo.toml`；
- raw RESP2/RESP3 differential、TCL external-server runner 和 Python integration；
- fast/nightly/release 三层门禁均输出 exact-ref、seed 和可回放 artifact。

### WP2：StorageManifest、CF、Comparator 和 Topology

状态：proposed。

Primary Issue：#342。

Child / Related：

- #343；
- #347；
- OQ-7；
- OQ-8。

Requirement：

- REQ-STORAGE-001 至 REQ-STORAGE-006。

依赖：

- WP0 accepted；
- D016 的 unknown-version fail-closed 决策；
- OQ-7 与 OQ-8 在进入 ready 前收敛为 manifest/topology 验收合同。

交付：

- StorageManifest；
- CF consumer closure；
- comparator 和 encoding identity；
- persisted instance topology；
- safe startup；
- upgrade/rollback；
- snapshot manifest pairing。

主要范围：

- src/storage/src/redis.rs；
- src/storage/src/format_base_key.rs；
- src/storage/src/format_base_value.rs；
- 适用 format_*；
- src/storage/src/custom_comparator.rs；
- src/storage/src/batch.rs；
- src/storage/src/checkpoint.rs；
- src/raft/src/lib.rs。

非目标：

- 不新增 Vector、Hot Tier 或其他 DataType/CF；
- 不实现 Multiple DB、Multi-Raft 或新的泛化 Engine 抽象；
- 不接受未知未来格式、Comparator 或 Snapshot 版本。

退出门禁：

- unknown format fail closed。
- old/new 双向矩阵明确。
- 缺失、额外、错序 CF 和 comparator mismatch 有测试。
- 新增 DataType 的所有消费者闭合。

验证门禁：

- `cargo test -p storage --all-targets --all-features`；
- `cargo test -p raft --all-targets --all-features`；
- storage fault-injection、真实 close/reopen、old→new、new→old 和拒绝矩阵；
- CF、Comparator、batch、compaction、checkpoint、snapshot 和 replay consumer-closure 扫描。

### WP3：有界 Runtime 与确定性生命周期

状态：proposed。

Primary Issue：#351。

Child / Related：

- #352；
- #408；
- #368；
- #347。

Requirement：

- REQ-OBS-001；
- REQ-PERF-001 至 REQ-PERF-003；
- REQ-STABILITY-002；
- REQ-STABILITY-003。

依赖：

- WP0 accepted；
- 重新取得 #350/#390 的 exact Linux/WSL baseline 证据；
- 与 WP2 的 Storage ownership/close 边界协调，但 WP2 和 WP3 可在接口冻结后有限并行。

交付：

- 保持 network/storage 数据面隔离；
- 统一 process supervisor；
- 有界 Storage 到 Raft bridge；
- absolute request deadline；
- admission-first shutdown；
- SIGINT、SIGTERM 和 service-stop；
- 所有长期 task 的 owner/cancel/join；
- 删除未接线脚手架。

主要范围：

- src/server/src/main.rs；
- src/common/runtime；
- src/net；
- cluster gRPC 和 Raft bridge 启动模块。

非目标：

- 不改变 Redis 命令语义、磁盘格式或 Raft durable-apply 合同；
- 不恢复旧的泛化 Runtime/Engine 脚手架；
- 不实现 Hot Tier、Multi-Raft 或新的无界队列。

退出门禁：

- in-flight write、queue full、leader unavailable、snapshot pause 和 shutdown 并发可重复。
- RocksDB 关闭后没有旧 task 访问。
- queue、deadline、drain 和 rejection 有真实指标。

验证门禁：

- common-runtime、net、server 的 all-target/all-feature 定向测试；
- WSL/Linux 的 SIGINT、SIGTERM、queue full、leader unavailable、snapshot pause 和 restart 测试；
- shutdown 顺序、JoinHandle/cancel ownership、absolute deadline 和 bounded-channel 静态闭包扫描。

### WP4：OpenRaft Durable Apply 合同

状态：proposed。

Primary Issue handling：进入 ready 前从 #332 拆出覆盖本工作包完整 durable-apply
验收的精确 Issue。#334 只覆盖 applied metadata，不能单独代表整个 WP4。

Parent / Epic：#332。

Existing child / related Issues：

- #334；
- #335；
- #336；
- #337。

Discussion：

- #330。

Requirement：

- REQ-RAFT-001；
- REQ-RAFT-002；
- REQ-RAFT-005；
- REQ-RAFT-008；
- REQ-STORAGE-001；
- REQ-STORAGE-004。

依赖：

- WP2 的 format/topology 边界；
- WP3 的 bounded bridge 和 lifecycle。

交付：

- vote/log/committed stable-storage policy；
- persisted applied metadata；
- per-log marker 或等价 typed outcome；
- 所有 cluster RESP write 经过 proposal；
- cluster linearizable read 由当前 Leader 提供，并经过 OpenRaft `ensure_linearizable`/ReadIndex，或经过批准且证明安全的 Lease read protocol；
- apply 时重验 slot、instance、generation；
- SUBMIT_UNKNOWN。

主要范围：

- src/raft/src/log_store_rocksdb.rs；
- src/raft/src/state_machine.rs；
- src/raft/src/leader_gate.rs；
- src/raft/src/node.rs；
- src/server/src/main.rs；
- src/net/src/executor_ext.rs；
- src/net/tests/storage_command_e2e_tests.rs；
- src/net/tests/network_integration_tests.rs；
- src/storage/src/batch.rs；
- storage/Raft fault tests。

非目标：

- 不把宽泛 Epic #332 当作可由单个 PR 自动关闭的 Primary Issue；
- 不实现 Multi-Raft、跨 Slot 原子性或 Hot Tier；
- 不把普通进程重启测试当作 power-loss durability 证明。

退出门禁：

- callback 与 WAL/sync 语义有明确证明。
- process crash 和 power-loss 模型分开。
- vote、append、truncate、purge、committed 和 reopen 单调性有故障测试。
- follower、leader transfer、partition 和 term 变化下的 read 不得绕过一致性门禁。

验证门禁：

- storage/Raft 定向测试、fault injection 和真实 RocksDB reopen；
- vote/log/committed callback 的 WAL/sync 证据与 OpenRaft 合同对账；
- 真实 RESP read 入口及只读命令分类全部经过同一读屏障；
- `ensure_linearizable`/ReadIndex 或批准的 Lease protocol 在 leader transfer、partition、term 变化、commit/apply lag 和 stale follower 场景的回归；
- process kill 与 power-loss 模型分别生成 exact-ref evidence；
- 每个实施 PR 使用精确 child 或新建精确 Issue 作为 Primary，#332 只作为 Parent/Epic。

### WP5：TTL、Generation、Compaction 和 Snapshot

状态：proposed。

Primary Issue：#338。

Child / Related：

- #339；
- #88；
- #138；
- #407 CLOSED；
- PR #409 MERGED。

Requirement：

- REQ-STORAGE-001 至 REQ-STORAGE-006；
- REQ-RAFT-001；
- REQ-RAFT-005。

依赖：

- WP2；
- WP4。

交付：

- persisted etime 和 generation 合同；
- ExpirationManager 去留决策及实现；
-真实 physical cleanup；
- collection meta/data reclaim；
- single applied frontier snapshot；
- streaming snapshot；
- strict version 和 marker recovery table。

主要范围：

- src/storage/src/expiration_manager.rs；
- src/storage/src/storage_impl.rs；
- src/storage/src/storage.rs；
- 各 DataType compaction filter；
- src/storage/src/checkpoint.rs；
- src/raft/src/state_machine.rs；
- src/raft/src/snapshot_archive.rs。

非目标：

- 不把 ExpirationManager 变成持久化权威；
- 不接受未知未来 Snapshot 或 storage format version；
- 不实现 Hot Tier、Multi-Key 或与本工作包无关的新 DataType。

退出门禁：

- restart、expire/persist race、同名 key 重建、cluster replay 和 snapshot restore 覆盖。
- Snapshot 每个 marker 阶段有 kill/restart 测试。
- 大 Snapshot 有内存上界。

验证门禁：

- storage/raft targeted tests、fault injection 和 WSL/Linux process kill matrix；
- restart、expire/persist race、same-key recreation、cluster replay 和 snapshot restore；
- archive byte/entry/path/size limits 与 streaming memory bound；
- marker 每个阶段的 fail/restart table 逐项产生可回放证据。

### WP6：Redis Core 语义审计与收敛

状态：proposed。

Primary Issue：#315。

Related：

- #325；
- #95、#117、#121、#127、#128、#129、#130、#131、#141、#142、#144、#195、#214、#215、#219、#220、#223、#252。

Requirement：

- REQ-COMPAT-001 至 REQ-COMPAT-007。

依赖：

- WP1；
- WP2。

主要范围：

- Redis 8.8.1 compatibility manifest 与 raw transcript；
- cmd、storage、RESP 和 client-visible error behavior；
- tests/compat、tests/tcl、tests/python 及对应回归夹具；
- 旧 Issue 的 current-main 复现、关闭或精确定界。

规则：

- 旧 Issue 不直接转成实现任务。
- 每个 Issue 先在当前 main 和 Redis 8.8.1 Oracle 上复现。
- 已实现则关闭旧 Issue。
- 仍存在则新建或更新精确到命令、错误、格式和验收的 Issue。
- Multi-Key 只做统一 pre-storage/pre-Raft reject，不实现跨 Slot 原子性。

非目标：

- 不按旧 Issue 标题直接补代码；
- 不实现真正 Multi-Key、2PC、跨 Slot 锁或原子 batch；
- 不把 redis-rs 或 Redis Oracle 放进生产依赖或请求路径。

退出门禁：

- required profile 在 Cache OFF 下可重复。
- error priority、WRONGTYPE、TTL、SCAN、Pipeline、binary payload 和连接行为有 raw transcript。

验证门禁：

- 每个命令在 exact Redis Oracle 与 Kiwi 上执行 raw frame/final-state differential；
- 变更命令的 success、error-priority、boundary、partial-I/O 和 binary payload 回归；
- 旧 Issue 的复现证据与 close/re-scope 结果写回 Primary/Related Issue；
- Cache OFF required profile、TCL/Python integration 和 changed-path Rust tests 通过。

### WP7：故障矩阵、真实 INFO 与 M6 Gate Review

状态：proposed。

Primary Issue：#340。

Related：

- #410；
- #325；
- #342；
- #343；
- #408。

Requirement：

- REQ-RAFT-005 至 REQ-RAFT-008；
- REQ-STABILITY-001 至 REQ-STABILITY-006；
- REQ-OBS-001；
- REQ-OBS-002。

依赖：

- WP1 至 WP6。

交付：

- deterministic Raft simulator；
- 3/5 node process harness；
- partition、delay、drop、duplicate、reorder；
- kill、pause、restart、disk fault；
- snapshot interruption；
- fixed-seed replay；
- Elle/Jepsen history；
-真实 INFO provider；
- G1-G7 evidence bundle。

主要范围：

- deterministic simulator、3/5 node process harness 和 fault controller；
- Raft/storage/snapshot/lifecycle/INFO 的跨模块验证；
- docs/quality/system-stability-gate.md 的 exact-ref evidence bundle。

非目标：

- 不实现 M7/M8 Hot Tier；
- 不用单元测试或普通 green CI 替代进程级、磁盘和一致性证明；
- 不启动 Multi-Raft、生产发行或容量扩展工作。

退出门禁：

- docs/quality/system-stability-gate.md 所有 required 项有 exact-ref 证据。
- 无未处理 P0/P1。
- M6 PASS 只允许提交 M7 授权请求，不自动解冻。

验证门禁：

- deterministic seed replay、3/5 node partition/kill/restart/disk-fault matrix；
- Elle/Jepsen history checker 与 Snapshot interruption；
- required Linux CI、sanitizers、soak、upgrade/rollback 和 operations drill；
- G1-G7 每项绑定 exact Head、环境、命令、artifact、结果和未覆盖风险。

<a id="wp8"></a>

### WP8：VectorSet 合并后生命周期、兼容性与门禁闭环

状态：in-progress。

Primary Issue handling：

- Primary Issue：[#421](https://github.com/arana-db/kiwi/issues/421)。
- [PR #356](https://github.com/arana-db/kiwi/pull/356) 是已合并事实和缺口来源，不作为当前开放 Issue。
- 只有 #415、#418、#421 的 required acceptance 与本工作包退出门禁全部闭合时，聚合 PR 才能使用 `Fixes`。

Implementation PR：[#422](https://github.com/arana-db/kiwi/pull/422)。

Parent / Related：

- [PR #356](https://github.com/arana-db/kiwi/pull/356)；
- [Issue #415](https://github.com/arana-db/kiwi/issues/415)；
- [Issue #418](https://github.com/arana-db/kiwi/issues/418)；
- [Issue #325](https://github.com/arana-db/kiwi/issues/325)；
- [Issue #340](https://github.com/arana-db/kiwi/issues/340)；
- [Issue #342](https://github.com/arana-db/kiwi/issues/342)；
- [Discussion #331](https://github.com/arana-db/kiwi/discussions/331)。

Requirement：

- REQ-VECTOR-001 至 REQ-VECTOR-005；
- REQ-COMPAT-001 至 REQ-COMPAT-003；
- REQ-COMPAT-007 至 REQ-COMPAT-010；
- REQ-STORAGE-001 至 REQ-STORAGE-006；
- REQ-RAFT-001；
- REQ-RAFT-002；
- REQ-RAFT-005；
- REQ-RAFT-008；
- REQ-STABILITY-002；
- REQ-STABILITY-003；
- REQ-OBS-001；
- REQ-OBS-002；
- REQ-WORK-005 至 REQ-WORK-007。

依赖：

- WP0 accepted；
- exact main 包含 PR #356，即 733888fc90ad8ef039947e87b08d7500a405954a 或其后继；
- D019 明确授权在一个 Draft PR 中聚合 #415 Trusted Oracle 和 WP8 产品闭环；
- 复用 WP1-WP7 已定义的 Oracle、manifest/topology、runtime、Raft、snapshot、Redis semantics 和 fault-gate 合同，并按本文限定范围逐项实施和验收；D019 不要求这些工作包的无关范围先行 accepted；
- 用户已确认 [VectorSet 合并后全量闭环设计](../docs/superpowers/specs/2026-08-06-vector-set-post-merge-remediation-design.md)；[VectorSet 合并后全量闭环实施总计划](../docs/superpowers/plans/2026-08-07-vector-set-post-merge-remediation.md) 和三个逐文件工作流计划已建立。

主要范围：

- Root/Instance StorageManifest v2、真实 Base 六 CF → 七 CF staged migration、已合并 Vector-v1 七 CF manifest v1 → v2 staged migration、故障恢复和与 source profile 对应的受控 rollback；
- Base v1/Head v2 snapshot 兼容、SnapshotInstallMarker 状态机、全量 Vector meta/member/incarnation 校验；
- VSIM key-scoped session、单一串行时刻和确定性并发 barrier；
- network runtime 深拷贝前的无分配 Vector admission，以及 VADD 类型化错误优先级；
- Trusted Redis 8.8.1 独立双构建、完整 artifact equality、runtime identity 和 cleanup-before-publish；
- raw RESP2/RESP3 differential、独立协议客户端、known difference/skip registry；
- 三节点 Leader/Follower cluster fail-closed required gate、capability 收敛；
- cargo-audit rkyv optional dependency reachability sentinel；
- exact-ref evidence、CI contract 和合并后写回。

非目标：

- 不实现 HNSW、IVF、Q8/BIN、VEMB RAW、新 Vector 命令、全文索引或其他 AI 主线扩展；
- 不启用 Vector Raft mutation 或把 cluster fail-closed 描述成 replicated support；
- 不把 Redis、redis-rs、Oracle controller 或 Redis-derived code 放进 production server dependency；
- 不用普通 green CI、端口 PING、版本字符串、strip 后 hash、被 skip 的 pytest 或抽样校验代替 required 证据；
- 不自动 merge、修改 branch protection、关闭 Issue 或 Resolve 历史评论。

退出门禁：

- 真实非空 Base 目录和 Base v1 snapshot 的 upgrade、每阶段 interruption、Head retry、reopen 和受控 Base rollback 通过；
- manifest、snapshot、incarnation、generation、compaction 和 consumer closure 无未解释分叉；
- VSIM 结果对应合法串行时刻，所有 Vector payload 在 StorageCommand 前完成 bounded admission；
- Trusted Oracle primary/rebuild 完整 artifact manifest 和 binary SHA-256 相等，正式 runtime 只来自 rebuild artifact，cleanup 成功后才发布 provenance；
- Vector RESP2/RESP3 differential 和三节点 cluster gate 非零执行且零 skip/xfail；
- rkyv reachability 变化会使 CI 失败；所有 required checks 绑定 exact Head；
- 无未处理 P0/P1，#415、#418、#421 和所有 known difference/skip 残留完成对账。

验证门禁：

- `git diff --check`、SDD validator normal/self-test、format、Clippy 和 changed-path Rust/Python tests；
- WSL/Linux Trusted Oracle 双构建、held tool identity、deadline/output cap/process cleanup 和 provenance mutation tests；
- Base/Head migration、snapshot v1/v2、multi-instance、close/reopen 和 fault-injection matrix；
- raw Vector differential、manifest/CI contract、network admission spy 和 deterministic VSIM concurrency tests；
- dedicated Linux three-node cluster gate、cargo-audit 和 rkyv feature-graph sentinel；
- final-Head GitHub checks、review threads、Issue state 和 exact-main verification 复核。

### 支持轨道

#### Block Cache

- Issue #143。
- PR #412 已于 2026-08-02 合并为 cbcbadc27068634d851ab0ed63989d2214ab2408，Issue #143 已关闭；其能力仍按共享预算、全实例/全 CF、table options、指标和基准证据评估。
- 只允许在共享预算、全实例/全 CF、table options、指标和基准均闭合后 accepted。

#### Build Performance

- Epic #353。
- 已合并 sccache 只是部分能力。
- 构建优化不得改变 arana-db/rust-rocksdb custom extension、ABI 或生产后端。
- prebuilt artifact identity 必须覆盖 fork ref、target、compiler/ABI、features、link mode、extension ref 和 checksum。

## 13. Issue、Requirement、工作包和 PR 追踪

### 13.1 强制关系

~~~text
Issue / Discussion
→ REQ
→ Decision
→ Work Package
→ Spec
→ Implementation Plan
→ PR
→ Merge SHA Verification
→ Accepted
~~~

### 13.2 Issue 规则

- 每个实施型工作包必须有一个 primary Issue。
- 没有合适 Issue 时，工作包进入 ready 前创建精确 Issue。
- 宽泛 Epic 必须拆 child Issue。
- Discussion 只作为设计来源。
- CLOSED Issue 只作为历史证据。
- 开始任务前重新确认 Issue state 和当前源码。

### 13.3 PR 关闭语义

完整满足 Issue 全部验收条件：

~~~markdown
Primary issue: Fixes #342
~~~

部分实现：

~~~markdown
Primary issue: Refs #334
Parent or Epic: Related #332
Related issues: Refs #335, Refs #336, Refs #337
Design context: Discussion #330
~~~

禁止对部分修复、Epic 或仍有 required 残留的 Issue 使用 Fixes/Closes；这些关系使用 Refs/Related。

### 13.4 PR 必填字段

- Work package；
- SDD baseline；
- Primary Issue；
- Parent/Epic 和 related Issues；
- Design Discussion；
- REQ-*；
- Decision；
- scope completion；
- verification environment、commands、results 和 uncovered risks。

原则上一个 PR 对应一个 primary Issue 和一个可共同验收的工作包目标。

### 13.5 合并后写回

~~~text
获取 merge commit
→ 在 exact main 上执行 required verification
→ 核对 Issue 自动关闭是否正确
→ 更新本文件中的 PR、merge SHA 和证据
→ implemented → verified
→ Requirement 和残留风险对账
→ verified → accepted
~~~

PR 合并不等于工作包 accepted。

## 14. SDD 工作方式

每个工作包独立执行：

~~~text
Issue validation
→ Specification
→ Design review
→ Implementation plan
→ Isolated worktree
→ TDD implementation
→ Targeted verification
→ PR
→ Exact-main verification
→ Acceptance
~~~

工作包下属材料可位于：

~~~text
docs/sdd/WP-N/
  spec.md
  plan.md
  verification.md
~~~

这些文件只负责该工作包的细节，不能修改项目级路线和状态。当前唯一计划由本文件 current_plan 指向。

实施计划必须：

- 列出精确文件；
- 将测试先于实现；
- 给出命令和预期；
- 区分 Windows、WSL/Linux 和 CI；
- 不包含占位词、回指式步骤或模糊错误处理；
- 每个 PR 保持单一目的；
- 每个提交和 PR 关联工作包、Issue 和 REQ。

## 15. 阶段退出门禁

### M0

- 本文件成为唯一入口。
- 旧实时状态文件不再维护状态副本。
- PR traceability 生效。
- recovery 与 SDD baseline 一致。

### M1

- trusted Oracle independent rebuild 通过。
- raw RESP differential 可重复。
- skip/difference 有 Issue 和 owner。

### M2

- StorageManifest、topology、format、comparator 和 migration 合同通过。
- 真实 close/reopen、corruption 和 disk fault 通过。

### M3

- required Redis Core profile Cache OFF 全绿。
- known differences 明确批准。

### M4

- public Raft profile 通过。
- commit/apply/durability 和 linearizable read 无绕行。
- membership、snapshot、transfer 和 reopen 可重复。

### M5

- deterministic 和 process-level 故障矩阵通过。
- safety 和线性一致性历史可检查、可回放。

### M6

- G1-G7 required 项全部通过。
- exact-ref evidence bundle 完整。
- 无未处理 P0/P1。
- 只能请求用户决定是否规划 M7。

## 16. Frozen 与 Deferred

### Frozen

- Embedded Redis Hot Tier；
- Cache ON；
- arana-db/redis 生产 fork 改造；
- native loader、FFI、发行接入；
- Vector Set Phase 2、HNSW、量化扩展和其他 AI 主线；PR #356 已合并的 Phase 1 表面只允许 WP8 做正确性、恢复、兼容性和门禁闭环；
- Multi-Raft。

### Deferred

- Multiple DB；
- MONITOR；
- TOML；
- StreamAppend RPC；
- Small Object Compact Encoding；
-跨 Slot atomicity；
-对象存储原生引擎。

### 当前 Multi-Key 决定

当前不支持真正 Multi-Key。命令必须在 storage/Raft 前统一拒绝。未来只有在 slot map、原子 batch、锁或事务协议和故障证明完成后重新立项。

## 17. 当前执行状态

| 字段 | 当前值 |
|---|---|
| Baseline | main@733888fc90ad8ef039947e87b08d7500a405954a |
| Current milestone | M0-M6 / WP8 |
| Current work package | WP8 |
| Status | in-progress |
| Current plan | [WP8 VectorSet 合并后全量闭环实施总计划](../docs/superpowers/plans/2026-08-07-vector-set-post-merge-remediation.md) |
| Current Issue | [#421](https://github.com/arana-db/kiwi/issues/421) |
| Current PR | [#422](https://github.com/arana-db/kiwi/pull/422) |
| WP0 exact-main verification | passed |
| Required mode | Cache OFF |
| M7/M8 | frozen |
| Next safe action | 提交并顺序集成 WP8 Runtime/Protocol Task 1-5，然后在独立 worktree 启动 Trusted Oracle/CI/Security 实施计划 |

PR #417 已修复 WP0 固定提交区间验证；main@688d905fec31b54aec76f36676f55efd8b5cfa17 的 ci run 30801285622 成功，当前 baseline commit 733888fc90ad8ef039947e87b08d7500a405954a 是其后继。WP0 已进入 accepted。PR #356 随后把 Vector Set Phase 1 合入主线，用户通过 D019 授权用 Draft PR #422 聚合 #415、#418、#421 的全量闭环。Storage/Recovery Task 1→7 已完成、集成并通过 exact-ref migration/rollback/snapshot 矩阵；独立 `codex/wp8-runtime-protocol` worktree 已从聚合 Head 建立。Runtime/Protocol Task 1 的无分配 `Bytes` admission 纯函数、Task 2 的 Cmd hook/双层 `GatedCmd` 顺序、Task 3 的 ParsedCommand Bytes 保留/Config 必填传播/真实 TCP storage spy/admission-before-copy 合同、Task 4 的 VADD typed parse outcome、Redis `arity=-5` dispatcher 边界、unknown trailing option 精确错误和 argv-shape heuristic 删除，以及 Task 5 的 function-scoped raw RESP2/RESP3 client、完整 frame reader、collection 零联网和五命令 Issue #421 operational-limit governance，均已完成 tests-first、变异验证和 changed-path 回归；正式双端 raw differential 仍由后续 verifier-supervised Trusted Oracle/CI runner 执行。下一安全动作是提交并顺序集成 Runtime/Protocol Task 1-5，然后启动 Trusted Oracle/CI/Security 实施计划。

## 18. 决策门禁

当前架构直接消费以下已批准 Decision：

| Decision | 对本 SDD 的约束 |
|---|---|
| D001 | Redis 8.8.1 exact commit 是唯一普通 Redis Oracle |
| D004 | RocksDB 是唯一完整权威存储 |
| D005 | RedisRaft 只定义公开兼容 Profile |
| D009 | M6 前 Hot Tier 生产实现冻结 |
| D011 | Oracle provenance 使用独立重建和 binary hash equality |
| D012 | 规划 task 与实施 task 分离 |
| D013 | SDD.md 是唯一项目权威入口 |
| D014 | 实施 PR 强制关联工作包、Issue 和 Requirement |
| D015 | 保持 network/storage 数据面隔离并建立统一生命周期 |
| D016 | 未知持久化版本默认 fail closed |
| D017 | 当前不支持真正 Multi-Key |
| D018 | 兼容性与故障验证使用分层门禁 |
| D019 | 一个 Draft PR 聚合 VectorSet 合并后全量闭环，但保持独立任务、worktree 和验收门禁 |

以下已批准 Decision 属于治理、测试来源或 M7-M10 冻结范围，同样受本 SDD
追踪，但不授权 WP0-WP8 增加对应生产能力：

| Decision | 映射 |
|---|---|
| D002 | M7-M10 组合发行许可证义务，frozen |
| D003 | M7-M10 热层术语，frozen |
| D006 | WP7 测试模型来源 |
| D007 | WP1/WP6 客户端测试边界 |
| D008 | WP0 工作恢复状态 |
| D010 | M7-M10 热层接口合同，frozen |

Deferred Requirement：

- `REQ-HOT-001` 至 `REQ-HOT-012`；
- `REQ-LICENSE-001` 至 `REQ-LICENSE-008`；
- `REQ-OBS-003`；
- `REQ-RAFT-003`；
- `REQ-RAFT-004`。

WP0-WP8 的 Requirement 字段覆盖当前 M0-M6 实施范围；以上范围映射到
M7-M10 或 frozen/deferred 合同，不构成当前实现授权。验证器只从各 WP 的
Requirement 字段和本 Deferred Requirement 字段计算全集，不接受注释或无关
段落中的偶然 ID 命中。

以下普通决定由工作包设计自行作出：

- 文件和模块内部拆分；
-测试分层和命令；
-Issue 子任务拆分；
-错误类型名称；
-有界队列和 deadline 的实现方式；
-文档和验证证据格式。

以下高影响决定必须由用户或指定维护者明确批准：

- M6 后是否解除 M7 冻结；
- 公开兼容性承诺的扩大或缩小；
-不可回滚磁盘格式变更；
- Multiple DB、Multi-Raft、跨 Slot 原子性；
- Embedded Redis Hot Tier 的 fork、许可证、ABI 和发行；
-首次 AGPL 组合发行；
-删除或迁移用户可见配置；
-merge、远端 Issue/Discussion 状态变更和其他外部写操作。

## 19. 文档维护和恢复协议

### 19.1 唯一入口

- 新会话先读 CLAUDE.md、CONTRIBUTING.md 和本文件。
- 本文件 current_work_package 和 current_plan 决定当前任务。
- STATE.md、KANBAN.md 和 ROADMAP.md 只保留迁移指针，不维护状态。

### 19.2 更新时机

- 工作包设计批准：accepted-design。
- Issue 和计划就绪：ready。
- 开始独立实施任务：in-progress。
- PR 合并：implemented。
- exact-main 验证通过：verified。
- Requirement、Issue 和残留风险对账：accepted。

### 19.3 漂移处理

发现 branch、HEAD、current plan、current PR、dirty ownership 或 GitHub state 不一致时：

1. 停止写操作；
2. 记录实际状态；
3. 区分用户工作、历史工作和当前工作；
4. 重新绑定 exact baseline；
5. 只有在不覆盖现有工作时继续。

### 19.4 历史材料

docs/superpowers/specs 和 docs/superpowers/plans 是历史材料。新文档不得把其中的 Rust 1.95、旧 nightly、Edition 2021 或旧 PR 状态当作当前标准。深链使用时必须先核对本文件和当前源码。

## 20. 规划文档验证

本文件或其控制面变更至少运行：

~~~powershell
git diff --check
python scripts/validate_sdd.py --self-test
python scripts/validate_sdd.py
~~~

验证者还必须：

- 保存验证器的确定性摘要，确认 REQ、Decision、当前状态和预期产物计数；
- 重新查询 main、Issue、Discussion 和 PR；
- 确认本文件是唯一项目状态入口；
- 确认没有把目标能力写成当前能力。

纯规划文档任务不运行昂贵 RocksDB 构建；若改动可执行脚本、CI、配置或源码，则按对应风险增加语法、构建、测试和 Linux/WSL 门禁。

## 21. 当前源码证据索引

以下位置绑定 baseline_ref，用于复核本文件的 Current 描述：

| 事实 | 源码位置 |
|---|---|
| Workspace、Rust 1.97.1、Edition 2024 和依赖 | [Cargo.toml](../Cargo.toml) |
| bootstrap runtime 和服务启动 | [server main](../src/server/src/main.rs#L119) |
| Storage 到 Raft append bridge | [server main](../src/server/src/main.rs#L307) |
| 当前 shutdown 顺序 | [runtime manager](../src/common/runtime/manager.rs#L224) |
| 有界 MessageChannel | [runtime message](../src/common/runtime/message.rs#L400) |
| StorageServer 执行入口 | [storage server](../src/common/runtime/storage_server.rs#L1353) |
| 网络侧执行扩展 | [executor ext](../src/net/src/executor_ext.rs#L51) |
| CF index 和 descriptor | [storage redis](../src/storage/src/redis.rs#L57) |
| TTL etime 权威读取 | [storage redis](../src/storage/src/redis.rs#L872) |
| stale 判断 | [storage implementation](../src/storage/src/storage_impl.rs#L638) |
| ExpirationManager 内存索引 | [expiration manager](../src/storage/src/expiration_manager.rs#L30) |
| CompactSpecificKey 当前 no-op | [storage](../src/storage/src/storage.rs#L462) |
| Binlog db/slot 隐含约束 | [storage batch](../src/storage/src/batch.rs#L445) |
| Raft log/vote/committed 写入 | [RocksDB Raft log store](../src/raft/src/log_store_rocksdb.rs#L105) |
| durable apply 后推进 last_applied | [Raft state machine](../src/raft/src/state_machine.rs#L470) |
| Snapshot install transaction | [Raft state machine](../src/raft/src/state_machine.rs#L548) |
| Snapshot archive 内存模型 | [snapshot archive](../src/raft/src/snapshot_archive.rs#L18) |
| Snapshot metadata version 处理 | [checkpoint](../src/storage/src/checkpoint.rs#L190) |
| 当前 Redis 8.8.1 manifest | [compatibility manifest](../tests/compat/redis-8.8.1/manifest.yaml#L18) |

源码移动或 baseline_ref 更新时，维护者必须重新定位这些证据；过期行号不能作为接受证据。
