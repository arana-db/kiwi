# VectorSet 合并后全量闭环设计

## 文档状态

- 日期：2026-08-06
- 状态：已确认，实施计划已建立
- 基线：`arana-db/kiwi main@733888fc90ad8ef039947e87b08d7500a405954a`
- 来源：[PR #356](https://github.com/arana-db/kiwi/pull/356) 合并后的代码事实
- Primary Issue：[Issue #421](https://github.com/arana-db/kiwi/issues/421)
- Related：[Issue #415](https://github.com/arana-db/kiwi/issues/415)、[Issue #418](https://github.com/arana-db/kiwi/issues/418)、Issue #325、Issue #340、Issue #342
- 用户授权：采用全量方案 C，在一个最终 GitHub PR 中闭合 VectorSet、Trusted Oracle、Storage、Runtime、协议、Cluster CI 和供应链门禁；不授权 merge。

本设计只固化目标、边界、状态机和验收合同。用户确认本文后，源码实现由新的独立 Codex 子任务和 worktree 执行；同一个 Draft PR 只作为跨任务的 GitHub 聚合边界。这个做法保留 D012 的任务隔离，不把“单 PR”解释为“单一未审计执行上下文”。

## 1. 背景与问题

PR #356 已把 VectorSet 合入 `main`，但合并后的主线仍存在十一类未闭环风险：

1. SDD 仍把 VectorSet 列为 frozen，缺少正式工作包、Requirement、Decision 和 PR 追踪。
2. Legacy RocksDB 目录新增 `VectorDataCF` 时没有可证明的中断恢复和 Base rollback 合同。
3. 当前 Head 拒绝已知 Base v1 snapshot，没有兼容恢复或明确的停止升级路径。
4. staged snapshot 校验会在确认来源合法前创建缺失 CF，并且只抽样验证 Vector member。
5. Vector 请求在资源上限判断前已经跨 runtime 多次深拷贝；`VALUES` 还没有统计 raw token 实际长度。
6. `VSIM ... ELE member` 的查询向量和候选成员来自两个 RocksDB snapshot，可能形成不可串行化的混合结果。
7. `VADD` 用 argv shape 猜测 WrongArity，把非法 vector 且缺 element 的请求错误改写为参数数量错误。
8. Vector differential 被正常测试目标排除，任何可 PING 的端口都可能被当作 Redis 8.8.1 Oracle。
9. 三节点 Vector cluster gate 默认 skip，绿色 CI 不证明 fail-closed 路由真实执行。
10. RESP3 测试通过共享 session 客户端执行 `HELLO 3`，污染后续 RESP2 测试。
11. `rkyv` advisory 使用全局 ignore，当前不可达事实一旦变化不会自动使 CI 失败。

这些问题横跨磁盘格式、恢复、公开协议、资源边界、并发一致性、供应链和 CI，不能靠补充注释或增加仅验证当前实现的单测关闭。

## 2. 目标与非目标

### 2.1 目标

本工作必须同时达到以下结果：

- 把 VectorSet 合并事实纳入唯一 SDD 控制面，建立 WP8、`REQ-VECTOR-001..005` 和批准 Decision。
- Legacy → Head migration 在每个持久化切换窗口都能继续、回滚或 fail closed。
- Base v1 与 Head v2 snapshot 有明确、可重复、不会猜测未来格式的兼容合同。
- VectorSet 的 incarnation、generation、meta、member、compaction 和 snapshot 身份闭合。
- `VSIM` 结果对应一个合法串行时刻。
- 所有 Vector payload 在跨 runtime 深拷贝前完成无分配、有溢出保护的 admission。
- 公共错误、RESP2/RESP3 frame、known difference 和 skip 都由 exact Redis 8.8.1 differential 约束。
- Trusted Oracle provenance 只在独立双构建完全相等、runtime 身份正确且 cleanup 成功后发布。
- Cluster fail-closed、differential 和 advisory exemption 都由实际执行且零 skip 的 Linux job 证明。

### 2.2 非目标

- 不实现 HNSW/IVF、Q8/BIN、VEMB RAW 或其他新 VectorSet 能力。
- 不启用 Raft Vector mutation；集群继续在路由和 storage 工作之前明确拒绝 Vector 命令。
- 不实现 Embedded Redis Hot Tier、Multi-Raft、跨 Slot 原子性或 2PC。
- 不把 Redis、redis-rs 或 Redis-derived 代码放入生产 server 依赖或请求路径。
- 不以版本字符串、端口 PING、strip 后 hash、`.text` 相等或允许列表代替完整 Oracle artifact equality。
- 不自动 merge、修改 branch protection、关闭 Issue 或 Resolve 历史评论。

## 3. 治理与追踪设计

### 3.1 WP8

新增 `WP8：VectorSet 合并后生命周期、兼容性与门禁闭环`。WP8 的 primary Issue 是 #421，正式 Requirement 是 `REQ-VECTOR-001..005`，并引用适用的 COMPAT、STORAGE、RAFT、STABILITY、OBS 和 WORK Requirement。

WP8 是对已经进入 `main` 的能力做合并后闭环，不授权新增 Vector 产品范围。VectorSet 从“全部 frozen”改为：

- WP8 列明的正确性、恢复、兼容性和门禁修复获准实施；
- 新索引、新量化、新公开命令和 AI 数据库扩张继续 frozen。

### 3.2 单 PR 与任务隔离

最终只创建一个 Draft PR，但工作按以下隔离边界顺序进入同一分支：

1. 治理和设计任务：只写 SDD、Requirement、Decision、validator 和本文。
2. 实施计划任务：把本文拆成精确文件和 TDD 步骤。
3. Oracle、Storage、Runtime/Protocol、CI/Security 使用独立实现子任务；任一时刻只有一个子任务修改共享分支。
4. 每个子任务先提交失败测试，再提交最小实现和验证证据。
5. Root 只在规格审查和代码质量审查通过后接收子任务提交。

单 PR 不免除每个 workstream 的独立测试、审查和 recovery checkpoint。

### 3.3 PR 关闭语义

Draft 阶段统一使用：

```text
Primary issue: Refs #421
Related issues: Refs #415, Refs #418, Refs #325, Refs #340, Refs #342
```

只有在 #421、#415 和 #418 的全部 required acceptance 同时满足后，才允许把对应关系改为 `Fixes`。PR 创建或普通 CI 绿色不构成关闭证据。

## 4. RocksDB migration 与 rollback

### 4.1 不允许原地半迁移

Legacy 目录不能采用“先在原目录创建 `VectorDataCF`，随后写 manifest”的顺序。该顺序在两步之间崩溃时会留下 Base 无法安全解释的目录，也无法区分合法升级和未知未来 schema。

迁移使用 sibling staged directory、root-level migration journal 和原子目录切换。所有 live RocksDB handle 必须在切换前释放。

### 4.2 Root/instance manifest v2

当前每实例 manifest 只保存 version、storage incarnation 和 next generation，不足以表达多实例 topology、CF role、comparator、codec、snapshot compatibility 和迁移权威。本次升级为“根 manifest + 每实例 manifest”：

```text
RootStorageManifestV2
- manifest_version / manifest_id / checksum
- storage_schema_version
- db_instance_num
- slot_mapping_version / slot_mapping_digest
- column_families[]
  - stable_id / name / role
  - comparator_id
  - key_codec_version / value_codec_version
- snapshot_read_min_version / snapshot_read_max_version / snapshot_write_version
- migration transaction
  - transaction_id / from_schema / to_schema / phase / current_instance
  - source_name / shadow_name / backup_name
- rollback_floor / features_used
- created_by / last_migrated_by

InstanceStorageManifestV2
- manifest_version / checksum
- instance_id
- root_manifest_id / root_manifest_digest
- storage_incarnation
- next_generation
```

路径字段只保存 storage root 下经过约束的相对 basename，不记录绝对路径。Root manifest 是 topology、CF 和 migration 的唯一权威；instance manifest 只保存运行期身份，不复制完整控制面。

### 4.3 Migration state machine

合法 phase：

```text
LegacyDetected
→ ShadowPrepared
→ InstanceCopied(i)
→ InstanceUpgraded(i)
→ AllInstancesVerified
→ SwitchPrepared
→ OldMovedToBackup(i)
→ ShadowPromoted(i)
→ NewStorageOpened
→ Committed
→ RollbackWindowClosed
```

合同：

1. `LegacyDetected` 只接受登记的真实 Base 六 CF fingerprint；未知 CF、comparator、部分 manifest 或未来 schema fail closed。
2. `ShadowPrepared` 为每个 instance 创建 sibling shadow，并 fsync root manifest；原目录保持不变。
3. `InstanceCopied(i)` 使用 RocksDB checkpoint/copy 创建 shadow，不能在原目录增加 CF。
4. `InstanceUpgraded(i)` 只在 shadow 中创建 `VectorDataCF`，写入绑定 root manifest digest 的 instance manifest。
5. `AllInstancesVerified` 关闭并重新打开全部 shadow，校验普通数据、TTL、精确 CF 集、checksum 和 instance identity；任一失败不启动服务。
6. `SwitchPrepared` 持久化切换意图。
7. `OldMovedToBackup(i)` 和 `ShadowPromoted(i)` 每次 rename 后都 fsync parent 并持久化 phase；在所有 instance 完成前不得发布 `Storage`。
8. `NewStorageOpened` 使用 Head 对正式目录做真实 reopen，并验证所有 instance 属于同一 transaction/root digest。
9. `Committed` 只说明新目录可启动；此时尚未接受客户端写入。
10. 开放网络 admission 前必须持久化 `RollbackWindowClosed`。从这一点开始，旧 backup 会与任何新写入分叉，不能再自动作为无损 Base rollback；需要逻辑导出/导入或显式降级工具。

恢复不能只根据目录存在与否猜测。root manifest phase、transaction ID、目录 identity、instance manifest digest 和实际目录组合必须一致。任一阶段必须至少保留一个经过验证、可启动的数据副本；不得删除唯一有效副本。

### 4.4 必需故障矩阵

真实非空 Base 目录必须覆盖：

- Base 写普通 String/Hash/ZSet 数据；
- Head 完整升级并读回；
- checkpoint 后崩溃；
- Vector CF 创建后崩溃；
- manifest 临时文件写入后崩溃；
- original → backup 后崩溃；
- stage → target 后崩溃；
- target reopen 失败；
- Head 重试；
- 网络 admission 尚未开放时恢复 backup，并由 Base reopen/read；
- `RollbackWindowClosed` 后拒绝把可能陈旧的 backup 当作无损回滚点。

每个故障点都要检查目录、journal、manifest、CF 和用户数据，不能只断言返回了 error。

## 5. Snapshot v1/v2 与 staged restore

### 5.1 Version 合同

- v2：必须携带每个 instance 的 storage incarnation，并与 staged manifest 一一相等。
- v1：只允许作为“不含 VectorSet schema/data”的 Legacy snapshot。restore 在独立 stage 内执行上一节的 Legacy migration，生成新的 incarnation；如果 v1 archive 已包含未知 CF、Vector meta/member 或无法分类的数据，fail closed。
- 未来 version：继续拒绝，不做 best-effort 解码。

### 5.2 Restore 顺序

```text
解包到 stage
→ 读取 archive metadata
→ 列出实际 CF，不创建任何缺失 CF
→ 按 v1/v2 分类
→ v1 显式迁移，或 v2 验证 manifest/incarnation
→ 全量校验 Vector meta/member
→ close/reopen stage
→ 原子安装
```

通用 restore 代码不得设置 `create_missing_column_families(true)`。只有被明确分类为 v1 Legacy migration 的路径可以在 stage 中创建 `VectorDataCF`。

### 5.3 SnapshotInstallMarker 恢复状态机

扩展现有 marker，不建立第二套平行恢复机制：

```text
StagedValidated
→ StoragePaused
→ MarkerPersisted
→ OldRenamedToBackup
→ NewRenamedToTarget
→ NewStorageOpened
→ RaftMetadataPersisted
→ CleanupPending
→ Complete
```

marker 绑定 version、phase、snapshot id/index/term、target/staged/backup 相对名称、root/instance manifest digest 和 snapshot metadata digest。启动恢复必须用 marker 和 digest 决定继续或恢复 backup；target/backup 都存在但无法判定权威时 fail closed，不能根据时间戳或目录存在猜测。cleanup 只能发生在新 storage reopen、Raft metadata 和 current snapshot 全部持久化之后。

### 5.4 全量一致性校验

移除固定 64 条抽样。校验必须遍历完整 `VectorDataCF`，对每条 member 检查：

- key codec version；
- storage incarnation；
- generation sequence；
- base meta 存在且类型为 VectorSet；
- meta generation 与 member generation 相等；
- vector value codec、dimension、metric 和 quantization 合法。

同时遍历所有 VectorSet meta，确认成员计数、generation、data revision 和数据范围一致。损坏数据不能通过“抽样未命中”进入 live storage。

## 6. VSIM 单一一致性视图

### 6.1 目标语义

`VSIM key ELE member ...` 和直接 vector 查询必须在一个 key-scoped read transaction 中完成。查询结果只能对应完整旧状态或完整新状态，不能把旧 query vector 与新成员集合混合。

### 6.2 Storage API

采用 key-scoped RAII `PreparedVsimSession`，复用 Vector mutation 已有的 record lock：

```rust
pub struct PreparedVsimSession<'a> {
    prepared: PreparedVectorQuery,
    logical_now_ms: i64,
    _key_guard: ScopeRecordLock<'a>,
}

impl PreparedVsimSession<'_> {
    pub fn search(self, query: VectorQuery, options: VectorSearchOptions)
        -> Result<Vec<VectorHit>>;
}
```

`prepare_vsim_session` 先获取与 VADD/VREM/DEL/recreate 相同的 key lock，再读取 missing-key、WRONGTYPE、dimension 和 ELE query。命令在 guard 存活期间解析 direct vector/options并执行 search，所有读取共用同一个捕获时间。writer 只能在 session drop 后提交，因此两个底层 snapshot 也不能跨 generation/recreate 混合。

该方案只阻塞同 key 写入，最长持锁时间仍受 FLAT query deadline、entry 和 byte budget 约束；不同 key 不受影响。底层未持锁的 `prepare_vsim`/`vsim` 入口收窄为 storage-internal，防止新 caller 绕过 session。仅携带 generation/data_revision 后重试不作为接受方案。

### 6.3 并发测试

测试使用确定性 barrier，在 query member 读取后阻塞 VSIM，然后分别执行：

- 更新 query member；
- 更新其他 member；
- VREM query member；
- DEL + 同名重建。

解除 barrier 后，结果必须等于完整旧状态或完整新状态。测试不得使用 sleep 猜测交错。

## 7. Network runtime admission

### 7.1 Admission 位置

RESP 解析后的 `ParsedCommand` 保留 `Bytes` 所有权，不立即把每个 bulk 转成 `Vec<u8>`。新增 cmd 层纯函数 `admit_vector_request(argv, limits)`，由 network runtime 在第一次 payload 深拷贝、构造 `StorageCommand::Execute` 或发送 storage channel 前调用。函数只读取 slice、长度和 option token，不分配、不解析浮点数、不访问 Storage。

配置必须从启动时同一份 `Config.vector` 传入 network server，不能在 network runtime 使用默认值，也不能为了 admission 跨 runtime 查询 `StorageOptions`。任何 `GatedCmd`/cluster wrapper 必须显式转发 admission，否则包装后的 Vector 命令会绕过限制。

覆盖命令：

- `VADD`；
- `VSIM` 的 direct vector 和 `ELE`；
- `VEMB`；
- `VREM`；
- `VISMEMBER`。

### 7.2 计数规则

- 所有长度和乘法使用 `checked_add`/`checked_mul`。
- FP32 同时检查 blob 实际长度和 dimension × 4。
- VALUES 累计每个 raw bulk 的实际 byte length，不能只计算 dimension × 4。
- element 使用 raw bulk length。
- 超限或整数溢出在进入 storage queue 前返回稳定错误。
- 对未超限请求，missing-key、WRONGTYPE、invalid-vector 和 WrongArity 的 Redis 语义优先级保持不变。
- 对超限请求，安全 admission 在 storage 语义之前返回本地资源错误；这属于有意的 operational-limit 差异，必须在 compatibility manifest 登记，不能伪装成 exact Redis error contract。

### 7.3 验证

使用很小的测试配置通过真实 TCP 路径发送超限 payload，并通过 storage spy/counter 证明请求没有构造或发送 `StorageCommand::Execute`。测试还必须覆盖 AUTH pipeline、cluster unsupported 和 `vector-enabled=false` 的静态 gate 顺序。禁止用数百 MiB fixture 制造测试资源压力。

## 8. VADD 解析与错误优先级

删除 `do_cmd` 中基于 argv 数量的 WrongArity heuristic。解析器返回可区分的内部结果：

- vector 编码非法；
- vector 已成功消费，但 element 缺失；
- element 存在但后续 option 非法；
- 完整请求。

只有“vector 已成功消费但 element 缺失”映射到 WrongArity。非法 VALUES token 或非法 FP32 blob 即使同时缺 element，也保留 invalid-vector 错误。

必需 raw differential：

- 完整合法 VALUES/FP32 缺 element；
- 非法 VALUES token 缺 element；
- 非法 FP32 长度缺 element；
- 零向量；
- 重复 option 和错误优先级。

## 9. Trusted Redis 8.8.1 Oracle

### 9.1 信任边界

Oracle 固定 source commit：`77b6c308396c9700672390a210143a8496fb4b10`。Controller 和 verifier 都必须在 Linux 上运行；Windows 入口只允许调用受控 WSL wrapper。

以下信息都不是单独信任根：调用者 metadata、build log、binary path、hash、size、版本字符串、`PING`、`INFO` 或 primary build。

### 9.2 Versioned build recipe

Recipe 固定：

- exact Redis commit 和 submodule/dependency状态；
- `LC_ALL=C`、`TZ=UTC`、固定 umask、隔离 HOME/TMPDIR；
- Git、shell、Make、CC、LD、AR、RANLIB 的绝对路径、版本、SHA-256 和 file identity；
- 无 ambient `PATH`、`PYTHONPATH`、`PYTHONHOME` 或 Git config 注入；
- 禁用 checkout-path 相关 DWARF 的 release Oracle build；Redis Makefile 的默认 `DEBUG=-g -ggdb` 必须由 versioned recipe 显式覆盖；
- 只有实际双构建证明完整 binary 和完整声明 artifact manifest 相等后，recipe 才能从候选变为 accepted。

如果无 DWARF recipe 仍产生差异，任务停在可复现性调查，不允许降低 equality。可使用 GCC 官方 `-fdebug-prefix-map`/`-ffile-prefix-map` 做补充实验，但路径映射参数自身进入构建 metadata 时也必须保持 artifact manifest 相等。

### 9.3 独立双构建

1. Primary 在 checkout A 构建候选 artifact 和 strict metadata。
2. Verifier 创建全新的 checkout B；不得使用 hardlink、alternates、shared object store、共享 HOME/TMPDIR 或 compiler cache。
3. Verifier 从自身受控工具目录执行同一 recipe。
4. A/B 的完整 artifact manifest 和 `redis-server` SHA-256 必须逐项相等。
5. 只从 held rebuild binary 启动正式 Oracle。
6. 严格解析唯一 `redis_version:8.8.1`，并绑定 runtime PID、file identity 和 hash。
7. Redis process group、临时 runtime、checkout B、logs 和 fallible handle 全部清理成功后，才原子发布 provenance。

任一失败不得留下最终 provenance 文件。

### 9.4 Bounded runner

所有外部命令必须具备：

- absolute wall-clock deadline；
- stdout/stderr byte cap；
- 独立 process group；
- TERM → grace period → KILL → wait 回收；
- held tool/file descriptor 或等价的 identity revalidation；
- 路径替换、进程遗留和 cleanup failure 回归测试。

## 10. Differential、RESP 隔离与 manifest

### 10.1 独立客户端

RESP2 和 RESP3 使用 function-scoped、显式 protocol 的独立客户端。测试不得对 session fixture 发送 `HELLO 3`。零向量和 missing-key + WITHSCORES 分别验证 RESP2 Array 与 RESP3 Map/Array 的 exact raw frame。

### 10.2 Required differential job

新增独立 Linux job：

1. 构建 Kiwi 当前 Head。
2. 运行 Trusted Oracle verifier 并启动 rebuild artifact。
3. 设置 `KIWI_COMPAT_REQUIRE_ORACLE=1`。
4. 显式收集 `test_vector_set_differential.py`，断言收集数大于零且等于注册表期望数。
5. 执行全部 RESP2/RESP3 参数化用例。
6. Oracle 不可达、身份不符、测试为零、skip、xfail、cleanup 失败均使 job 失败。

普通 `test-python` 不再静默 `--ignore` 该模块；fast job 可以按机器可读 skip registry 标明由 required Oracle job 接管，但必须由 CI contract test 证明接管 job 存在且执行相同 manifest scope。

### 10.3 Manifest

每个 V* 命令必须登记为：

- exact required；或
- `known_difference`，包含 owner、Issue、introduced_at、last_verified_ref、affected fields 和 remove_when；或
- temporary skip，包含 owner、Issue、reason、引入日期和解除条件。

未登记差异和不可达 Oracle 不得产生绿色结果。

## 11. Cluster fail-closed 与 capability

Vector 命令在 cluster mode 继续不受支持。pre-route gate 必须发生在 follower redirect、leader read barrier、Raft append 和 Storage 调用之前。

移除 `vector_set_raft_mutation_v1` capability 宣告；仅保留由当前 binary 和实际测试证明的 storage-format capability。以后只有 non-xfail 的三节点 mutation/replay/failover 测试通过后才能重新广告 Raft mutation capability。

新增 Linux cluster job：

- 固定并校验 `grpcurl` 版本和 checksum；
- 构建当前 Head 的 Kiwi binary；
- 设置 `KIWI_RUN_CLUSTER_TESTS=1`；
- 显式 collect 并运行 `test_vector_cluster.py`；
- 对 leader 和 follower 都验证同一 unsupported error；
- 通过 spy/log/metric 证明没有进入 Storage 或 Raft mutation；
- zero collected、skip、xfail 或遗留进程均失败。

## 12. rkyv advisory exemption

保留 cargo-audit ignore 的前提是 `rkyv@0.7.46` 不进入任何实际 target/feature graph。新增 fail-closed sentinel：

```text
cargo tree --locked --offline --target all --all-features -i rkyv@0.7.46
```

`cargo tree -i` 在无依赖链时也返回 exit code 0，因此 sentinel 必须检查 stdout 而不是只检查退出码。期望结果是空依赖链；cargo 执行失败或出现任何 inverse dependency 都立即失败并要求升级/移除 ignore。`.cargo/audit.toml` 记录 owner、Issue #421、当前真实路径 `openraft → byte-unit → rust_decimal`、rkyv 仅为当前不可达 optional dependency 的证据和 remove_when，不能再声称它用于当前 Raft wire serialization。

## 13. 验收矩阵

| 风险 | 实现证据 | 失败证明 |
|---|---|---|
| SDD 未授权 | WP8、REQ-VECTOR、Decision、validator | 删除 WP8/REQ/Decision 后 self-test 失败 |
| Legacy migration/rollback | 真实 Base 数据目录矩阵 | 每个 journal phase fault injection |
| v1 snapshot | v1 stage migration + reopen | v1 含 Vector/未知 CF 时拒绝 |
| staged 校验 | 完整 CF/member/meta 扫描 | 第 65 条及更后损坏仍被发现 |
| admission 太晚 | TCP 小限额 + storage spy | 超限请求 storage 调用数为零 |
| VSIM 两 snapshot | barrier 并发测试 | 旧 query + 新 members 混合结果不可能通过 |
| VADD 错误优先级 | raw Redis differential | 恢复 argv heuristic 时测试失败 |
| Oracle 未执行 | 双构建 provenance job | 端口错误、hash mismatch、zero tests、cleanup failure 均失败 |
| Cluster 测试 skip | required Linux cluster job | 未设置 env/grpcurl/zero collect 均失败 |
| RESP 污染 | 独立 RESP2/RESP3 客户端 | 单独运行每个测试结果一致 |
| rkyv ignore | feature-graph sentinel | 注入 rkyv feature 后 CI 失败 |

## 14. 验证层级

### 14.1 Fast/changed-path

- `git diff --check`
- `python scripts/validate_sdd.py --self-test`
- `python scripts/validate_sdd.py`
- `cargo fmt --check`
- 变更 crate 的 `cargo test`
- Vector parser、admission、storage、snapshot 和 capability targeted tests
- Python RESP2/RESP3 独立测试

### 14.2 Linux required

- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test --workspace`
- Trusted Oracle 双构建、artifact equality、runtime identity 和 cleanup
- Vector raw differential，非零收集且零 skip/xfail
- 三节点 cluster fail-closed gate
- cargo-audit 和 rkyv feature-graph sentinel

### 14.3 Recovery/compatibility

- Base → Head migration interruption/retry/Base rollback
- v1/v2 snapshot install、close/reopen 和损坏矩阵
- 多 instance manifest/incarnation 配对
- VSIM/VADD/VREM/DEL+recreate 确定性交错
- process cleanup、timeout、output cap 和路径替换故障

## 15. PR 生命周期

1. 本文、SDD、Requirement、Decision 和 validator 先进入 Draft PR。
2. 用户已确认本文；精确 implementation plan 由 [VectorSet 合并后全量闭环实施总计划](../plans/2026-08-07-vector-set-post-merge-remediation.md) 和三个下属工作流计划固定。
3. 每个 workstream 在独立子任务/worktree 中按 TDD 实施并经规格、质量双审查。
4. 每批提交更新 Draft PR 的验证矩阵，不提前改为 Ready。
5. 最终 Head 重跑全部 required Linux、Oracle、cluster、migration 和 recovery 门禁。
6. 无未解决 P0/P1、所有依赖 Issue 验收闭合、GitHub checks 和 merge policy 满足后，才能请求用户决定是否标记 Ready。
7. merge 始终需要用户另行授权。

## 16. 实施期精确合同

详细源码映射发现了设计中必须在编码前消除的多义性。以下条款是已确认方案 C 的实施级收窄，不扩大产品范围。

### 16.1 Manifest digest 与目录 identity

- Root/instance manifest v2 使用固定字段顺序的 Rust struct 序列化为 compact JSON。
- Digest 输入是不含 `digest` 字段的完整 JSON bytes，算法为 SHA-256，文本编码为 lowercase hexadecimal。
- Manifest 字段不使用序列化顺序不稳定的 map 承载 digest 内容；可变列表必须先按 stable ID 或 byte-order key 排序。
- 目录 identity 是 root manifest UUID、migration transaction UUID、instance UUID 和 manifest digest 的组合，不依赖 inode、Windows file ID 或目录路径本身。

### 16.2 Snapshot `RaftMetadataPersisted`

`RaftMetadataPersisted` 只在以下条件全部成立后写入 marker：

1. 新 storage 已 close/reopen 并通过 root/instance digest 和全量 Vector 一致性验证。
2. current snapshot metadata/data 已通过项目的 durable API 持久化。
3. state-machine applied index/membership 的 durable 状态已写入并在重新读取后与 snapshot metadata 一致。
4. 不把内存字段赋值或未 fsync 的文件当作 durable evidence。

### 16.3 Oracle v3 与 artifact kind

- 正式 schema 为 `kiwi-redis-oracle-build/v3` 和 `kiwi-redis-oracle-provenance/v3`。
- 固定 recipe ID 为 `redis-8.8.1-linux-release-v3`，变量包含 `BUILD_TLS=no`、`MALLOC=libc`、`DEBUG=`、`DEBUG_FLAGS=`、`ENABLE_LTO=`、`OPT=-O3 -fno-omit-frame-pointer` 和 `-j 1`。
- Artifact kind 只允许 `regular` 和受约束的 `symlink`。Symlink 目标必须是 source-relative path，不得绝对或逃出 source root，不得成环，解析深度不超过 8，最终必须指向 manifest 内的 regular file。
- Primary/rebuild 比较要求所有 entry 的 path、kind、mode、size、SHA-256 或 symlink target 完全相等。

### 16.4 Differential runtime lease

Differential 和 cleanup-before-publish 使用同一 verifier supervisor：

```text
primary/rebuild equality
→ start held rebuild Redis
→ validate INFO/PID/file identity/hash
→ run bounded differential callback
→ stop and reap callback/Kiwi/Redis process groups
→ remove runtime/checkout/log/temp resources
→ final identity revalidation
→ fsync/close/atomic provenance publish
```

不允许在 cleanup 前发布 provenance，也不允许 cleanup 后从未绑定复制品重新启动 Oracle。

### 16.5 Required collection registry

- `tests/compat/redis-8.8.1/vector-required-jobs.yaml` 只保存 required CI selection：job ID、test module、pytest marker、protocols、command scope、expected node IDs/item count、manifest profile 和 fast-job ownership。
- 产品 known difference 和 operational-limit difference 仍只保存在 Redis compatibility manifest，不在 required-jobs registry 复制一份。
- Rust contract test 必须证明 required-jobs command scope 与 manifest 的 Vector required/known-difference scope 完全相等，并证明实际 pytest node IDs/count 与 registry 一致。
