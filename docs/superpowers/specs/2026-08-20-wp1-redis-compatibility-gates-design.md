# WP1 Redis 8.8.1 兼容门禁设计

> 状态：approved planning contract
> 日期：2026-08-20
> Work package：WP1
> Planning baseline：`arana-db/kiwi@cb39927e44b84553f98ffee6ed1daa3f7388cf97`
> Redis baseline：tag `8.8.1`，commit `77b6c308396c9700672390a210143a8496fb4b10`
> Primary implementation child：[Issue #433](https://github.com/arana-db/kiwi/issues/433)
> Parent：[Issue #325](https://github.com/arana-db/kiwi/issues/325)
> Historical evidence：[Issue #415](https://github.com/arana-db/kiwi/issues/415)、[PR #422](https://github.com/arana-db/kiwi/pull/422)
> Decision：D020

## 1. 目标

本设计把 WP1 从抽象的兼容性方向冻结为可执行门禁合同，并为第一个独立 implementation slice 建立精确边界。完成本 planning task 后，WP1 可以进入 `ready`，但不能进入 `implemented`、`verified` 或 `accepted`。

第一个 implementation slice 是 Redis Core trusted raw smoke differential registry，覆盖：

- `PING`；
- `SET`；
- `GET`；
- `DEL` 的单 key 子集；
- `TYPE`；
- `PTTL`。

该 slice 的目标是建立第一个可重复、可回放、fail-closed 的 Redis Core raw 门禁，不是一次性完成 Redis Core、TCL、客户端生态、故障矩阵或系统稳定性。

## 2. 实时基线与已实现事实

### 2.1 Git 与 GitHub

- `origin/main` 精确 SHA 为 `cb39927e44b84553f98ffee6ed1daa3f7388cf97`。
- Issue #325 保持 OPEN，继续作为统一测试策略 umbrella。
- Issue #415 已 CLOSED，只作为 Trusted Oracle 历史实现证据。
- PR #422 已 MERGED，merge commit 为 `9a8a64aca12a825912f299450e10fc6043eca610`。
- `main@cb39927e` 的 Release Drafter、Security Audit、Benchmark、CodeQL 已成功；主 `ci` run `32340146786` 也已于 `2026-08-20T07:16:58Z` 成功，17 个 jobs 全部完成且无失败，最后完成的 `integration test` job `96344368042` 成功。
- open PR 只有 #424 和 #427。两者不修改本 planning task 的允许文件；其源码工作包分别仍处于 proposed/无精确实施计划或 conflict 状态，不能改变 WP1 的规划边界。
- 未发现对 `main` 生效的 required status-check 或 required-review rule。可见 checks 绿色只能记录为 visible evidence，不能表述为 required gate 已配置并通过。

### 2.2 Trusted Oracle

当前主线已经实现并必须复用：

```text
Redis tag:             8.8.1
Redis commit:          77b6c308396c9700672390a210143a8496fb4b10
Build schema:          kiwi-redis-oracle-build/v3
Provenance schema:     kiwi-redis-oracle-provenance/v4
Recipe:                redis-8.8.1-linux-release-v3
```

接受的信任合同包括：fresh disposable independent rebuild、primary/rebuild artifact manifest equality、`redis-server` SHA-256 equality、只运行 rebuild binary 取得正式 runtime identity、受控工具身份和 held FD、隔离 Python、deadline、stdout/stderr 上限、process-group cleanup、callback 后输入身份复核、cleanup-before-publish、evidence 先于 provenance 原子发布，以及任一失败回滚两个最终产物。

### 2.3 Vector required differential

当前 Vector registry 是独立且已进入 required visible CI 的现有消费者：

- schema：`kiwi-vector-required-jobs/v1`；
- job：1 个；
- commands：8 个，分别是 `VADD`、`VCARD`、`VDIM`、`VEMB`、`VINFO`、`VISMEMBER`、`VREM`、`VSIM`；
- expected pytest nodes：40；
- server-backed nodes：26；
- comparator/parser not-applicable nodes：14；
- RESP2/RESP3、raw request/response、final state、collection summary 和 cleanup 全部 fail closed。

本设计禁止通过改名、合并 registry、减少节点、改变 marker 或放宽 collection 规则来为 Core 腾位置。

### 2.4 当前兼容矩阵与测试来源

`tests/compat/redis-8.8.1/manifest.yaml` 当前只有 12 个命令：required 4、deferred 2、known difference 6。它不是完整 Redis Core matrix。六个 Core smoke 命令均未登记。

当前 `tests/tcl`：

- 不是 exact Redis 8.8.1 checkout 的权威来源；
- 只启用了少量历史 suite；
- 没有机器可读的完整 skip registry；
- 没有 required nightly evidence；
- 不能直接提升为 trust root。

普通 Python integration 是有效的多语言客户端和真实进程集成测试，但只运行 Kiwi，redis-py typed conversion 不能代替 Redis/Kiwi raw wire differential。

当前没有 redis-rs suite，也没有生产依赖；redis-rs 不是首个 implementation slice。

## 3. 方案比较与选择

### 3.1 方案 A：先建设大型统一 `kiwi-test-harness`

该方案可以统一 lifecycle、registry、日志和多类故障测试，但当前只有 Vector 和首个 Core smoke 两个稳定消费者。提前抽取大型框架会扩大接口、迁移现有 Vector gate，并把 WP1 的第一条 raw 证据线推迟到框架之后。

结论：不采用。

### 3.2 方案 B：先把现有 `tests/tcl` 接入 required CI

该方案表面上覆盖面更大，但现有复制树无法证明 exact upstream identity，skip/修改/覆盖也未闭合。直接接入会把历史候选误标为权威 Redis 8.8.1 evidence。

结论：不采用。

### 3.3 方案 C：复用 Oracle 安全内核，新增独立 Core registry 和最小 evidence profile

该方案先建立六命令 raw smoke，继续使用现有 independent rebuild、held tool、cleanup 和原子发布机制，只对 Vector-only evidence descriptor/collector 做窄参数化。Vector profile 保持原样，Core 使用独立 registry、marker、helper、evidence schema 和 CI job。

结论：采用。它是当前最小、可验证且不伪造覆盖面的方案。

## 4. 门禁分层

| 门禁 | 触发与目的 | Required 内容 | 不能推出的结论 |
|---|---|---|---|
| PR fast | 每个影响 Redis 公共行为或兼容基础设施的 PR | manifest/schema closure；受影响命令 trusted raw RESP2/RESP3 differential；受影响命令 final-state/TTL；changed-path Rust/Python tests；适用的 partial-I/O/parser 定向回归；zero collected、skip、xfail、xpass、deselect fail closed | 完整 TCL 通过；完整 Redis Core 兼容；系统稳定；Raft/cluster/sentinel 完成 |
| Nightly/full | 定期发现宽覆盖语义漂移和非确定性缺陷 | exact Redis 8.8.1 upstream TCL external-server runner；完整 required Redis Core raw differential；固定 seed/corpus 的 property/fuzz；exact source/Head/seed/log/artifact；known difference/skip closure | release/M6 稳定性完成；真实恢复/故障矩阵完成 |
| Release/M6 | 汇总发布和系统稳定性证据 | exact Kiwi Head；exact Redis source；Oracle binary/tool/provenance；Core raw differential；exact-upstream TCL；compat manifest；skip/difference owner/remove_when；恢复、故障和系统稳定证据 | 自动解冻 Embedded Redis Hot Tier；自动授权 Cache ON、Multi-Raft 或新功能实施 |

PR fast 不运行完整 TCL。普通 PR fast green 不得替代 nightly/full 或 release/M6。

## 5. 测试来源与职责

### 5.1 Raw RESP differential

Raw differential 是普通 Redis 可观察行为的第一权威门禁。请求和响应必须保存 exact bytes，不允许客户端 typed conversion、全局 normalization 或只存 hash。

Normalization 只允许命令专用、字段级、具备理由和测试的规则。首个 Core smoke 命令均使用 deterministic exact-frame 比较，不需要 normalization。

### 5.2 Exact-upstream TCL

权威 TCL 必须直接来自 exact Redis 8.8.1 checkout，以 external-server runner 分别连接 Redis 和 Kiwi。现有 `tests/tcl` 只能作为 legacy candidate；必须先完成 source、suite、修改、覆盖和 skip 对账，再决定迁移、替换或删除。

Standalone Cache OFF 是 WP1 首个 required profile。Sentinel/cluster suite 不默认 required；它们需要 WP4/WP7 的精确 Issue、拓扑和故障门禁。

### 5.3 Python

普通 Python suite 保留为多语言 client 和进程级 integration。Core raw module 可以使用 Python 管理 socket、进程输入和证据记录，但比较对象必须是 raw RESP bytes，不能经过 redis-py typed conversion。

### 5.4 redis-rs

redis-rs 只允许进入独立 test-only 客户端生态验收，不进入 production dependency，不作为 Redis 行为 Oracle，并排在 raw differential 和 exact-upstream TCL 之后。

## 6. 首个 implementation slice

### 6.1 命令与边界

首批命令为：

```text
PING
SET
GET
DEL  (single-key subset only)
TYPE
PTTL
```

`SET` 只覆盖当前已实现的三参数 `SET key value`。NX/XX/EX/PX/GET 等 option 不在该 slice 的完成声明中。

`DEL` 只覆盖单 key。即使当前实现可以接收多个 key，也不得借该 slice 宣称 Multi-Key 已获得批准或完成；D017 保持生效。

### 6.2 Required node inventory

Core registry 必须冻结 30 个 server-backed node：15 个 case × RESP2/RESP3。

| # | Case | 关键断言 |
|---:|---|---|
| 1 | PING no-arg | exact `PONG` frame |
| 2 | PING binary echo | NUL 与非 UTF-8 bytes 原样返回 |
| 3 | PING wrong arity | exact error frame/priority |
| 4 | SET binary success | binary key/value，exact `OK` |
| 5 | SET wrong arity | exact error frame/priority |
| 6 | GET existing binary | exact binary bulk/blob payload |
| 7 | GET missing | protocol-specific null frame exact match |
| 8 | GET wrong arity | exact error frame/priority |
| 9 | DEL existing single key | integer `1`，最终 missing |
| 10 | DEL missing single key | integer `0` |
| 11 | DEL wrong arity | exact error frame/priority |
| 12 | TYPE existing string | `string` |
| 13 | TYPE missing | `none` |
| 14 | PTTL persistent string | `-1` |
| 15 | PTTL missing | `-2` |

Comparator/parser unit tests不计入 30 个 required server-backed node；它们由 Rust/Python changed-path tests 单独执行，不能用来凑 collection count。

### 6.3 Final-state profile

每个 server-backed node 必须登记并输出一个 final-state profile：

- `persistent-string-v1`：`GET` exact bytes、`TYPE=string`、`PTTL=-1`；
- `all-missing-v1`：`GET=null`、`TYPE=none`、`PTTL=-2`；
- `no-owned-state-v1`：PING/arity error 场景仍使用唯一测试前缀并证明无残留 key。

清理必须在发布前验证：第一次 `DEL` 等于 profile 预期，第二次 `DEL=0`，随后 `TYPE=none`、`PTTL=-2`。任何 cleanup、残留或二次删除漂移都失败。

## 7. Registry 与 manifest closure

### 7.1 Core registry

新增：

```text
tests/compat/redis-8.8.1/core-required-jobs.yaml
```

schema：`kiwi-core-required-jobs/v1`。

registry 必须精确包含：

- 一个 job：`trusted-core-smoke-differential`；
- module：`tests/python/test_core_differential.py`；
- marker：`raw_core_protocol`；
- protocols：`resp2`、`resp3`；
- commands：六个固定命令；
- 15 个 case 的 request ownership；
- 30 个唯一 pytest node；
- 每个 node 的 final-state profile；
- `expected_item_count: 30`；
- manifest profile：`redis_8_8_1_standalone_cache_off`。

Rust loader/helper 必须拒绝 unknown field、空集合、重复 command/case/node、非大写 command、非固定协议/profile、node/module/marker 漂移、request ownership 缺失、case-command closure 不一致、final-state 缺失或多余、count 不一致。

### 7.2 Compatibility manifest

implementation 必须把 repository manifest 从 `kiwi-redis-compat/v1` 显式迁移到 `kiwi-redis-compat/v2`。v2 为 command contract 增加可选 `required_cases`，固定字段为：

```text
registry_path
registry_schema
case_ids
```

缺少 `required_cases` 时，classification 只描述整条命令；存在该字段时，classification 仍描述整条命令，而 registry 中列出的 case 形成独立 fail-closed required subset。不能无版本地改变 v1 的 `required` 或 `arguments: exact` 含义。

六个命令加入后，manifest 必须从 12 条增长为 18 条。现有 12 条只迁移 schema，classification 和 known differences 原样不变。新增六条在 command level 一律登记为 `known_difference`，通过 `required_cases` 绑定 Core registry/schema/case IDs，并同时绑定：

- exact Redis 8.8.1 identity；
- standalone Cache OFF；
- RESP2/RESP3；
- exact arguments/reply/error；
- binary-safe raw evidence；
- final-state/TTL evidence；
- 明确 owner；
- Issue #325 持有的 command-level coverage gap 和可测解除条件。

该 18 条 manifest 的分类计数必须是 4 `required`、2 `deferred`、12 `known_difference`。Core registry、manifest `required_cases` 和 evidence command/case/node 必须双向闭合；删除/增加 case、把 SET options 或 multi-key DEL 偷换为 covered、或提前把六条命令改为 command-level `required` 都失败。文档和 CI 名称不得暗示完整 Redis Core 覆盖。

## 8. Evidence schema 与回放

Core evidence 必须绑定：

- exact Kiwi Head 和 tree OID；
- callback-input manifest 及 SHA-256；
- exact Redis source、primary/rebuild binary、artifact/tool/provenance identity；
- Core required-jobs canonical JSON 和 helper identity；
- collection log/summary；
- pytest log/summary；
- raw request/response transcript；
- final-state transcript；
- Kiwi/Redis runtime logs；
- callback cleanup 和 controller cleanup；
- platform、命令、exit status、开始/结束时间；
- final artifact size/SHA-256 和 publication verification。

所有 raw bytes 使用 strict Base64，并附可重算 SHA-256。JSON/JSONL 必须 strict UTF-8、拒绝 duplicate key、non-finite number、空行、symlink、special file、额外文件、缺失文件和 size limit 违规。

回放入口固定为 `OracleProvenance::verify_external_bindings` 的 schema-aware evidence parser。现有 `kiwi-verify-oracle-evidence` 已把完整 sealed evidence bytes 传入该函数，不需要修改 binary 文件；`tools/compat/tests/oracle.rs` 另提供读取最终文件的 ignored offline replay test。在 Redis/Kiwi 进程和临时目录均已删除后，两条入口都必须只使用 final evidence、final provenance 和 exact Head/tree/source identity 重新校验 registry closure、collection、raw frame、final state、cleanup 和 binding。

Parser 必须使用严格 enum 区分 `VectorV1` 与 `CoreSmokeV1`，拒绝 duplicate/extra/missing key、non-finite number、非 canonical Base64、Base64/SHA 不一致、registry command/case/node 漂移、summary 不等于 30/30/0/0/0/0/0、raw Kiwi/Redis frame 不相等、final-state/TTL 漂移、cleanup second DEL 漂移和跨 profile helper/schema/file pairing。回放 mutant 必须先重算被篡改 evidence 的外层 size/SHA 并同步 provenance，确保失败来自内部语义，而不是旧 hash 不匹配。

## 9. Oracle 复用与最小泛化边界

### 9.1 必须保持不变的安全内核

以下逻辑不得复制、分叉或放宽：

- controller bootstrap 和隔离 Python；
- held executable/tool/source FD；
- controlled tool alias directory；
- primary/rebuild 两次构建；
- runtime identity；
- bounded process supervision；
- callback 前后输入身份复核；
- cleanup state machine；
- evidence-before-provenance 原子发布；
- post-publish sealed verification；
- 双输出 rollback。

### 9.2 允许泛化的 evidence profile

现有 Vector-only 耦合包括 runtime helper allowlist、`vector-required-jobs.json`、Vector canonical schema、Vector cleanup schema、Vector helper binding 和 `kiwi-vector-differential-evidence/v1`。

implementation 只允许把这些值收敛成两个固定 profile：

```text
vector-v1
core-smoke-v1
```

`vector-v1` 的文件、字段、40-node registry、schema 和 CI 行为必须保持现状。

`core-smoke-v1` 使用独立 helper、registry、cleanup schema 和 `kiwi-core-differential-evidence/v1`。Controller 根据 exact frozen callback argv 和 callback-input manifest 选择固定 profile；任意未知 callback/profile、helper/file/schema 组合必须 fail closed。

`kiwi-redis-oracle-provenance/v4` envelope、published-after-cleanup、evidence identity 和发布顺序保持不变。Rust verifier 增加第二个精确 evidence schema/file/profile pairing，并对两种 evidence document 做严格解析和语义回放，不允许任意字符串或弱通配。如果实施证明必须改变 v4 字段集合、已接受 Vector 文档含义或外部 sealed binding，必须停止该 slice 并提交新的 provenance schema Decision；不得静默把变更继续称为 v4。

## 10. Collection 与 failure semantics

required Core run 必须满足：

```text
collected = 30
passed = 30
failed = 0
skipped = 0
xfailed = 0
xpassed = 0
deselected = 0
```

`raw_core_protocol` marker 丢失、skip/skipif/xfail、endpoint 缺失、Oracle 缺失、pytest plugin 缺失、service startup、fixture、permission、namespace、build、identity、cleanup 或 artifact 错误必须归类为 `CORE_GATE_HARNESS_ERROR`，不能冒充目标 RED。

计划中的首个 RED 使用唯一 `CORE_GATE_TARGET_RED` marker，并要求：恰好一个目标 test 运行、exit 非零、目标 marker 恰好一次、无 harness marker。GREEN 要求相同 test exit 0 且两个 marker 都不存在。

最终判别性门禁必须保持 endpoint、runtime、collection 和 summary 合法，再只改变 Kiwi 一侧的可观察行为。至少执行：PING raw response 单字节翻转、GET binary payload 截断或 UTF-8 化、SET 返回 `OK` 但不把 mutation 转发到 Kiwi、PTTL persistent `-1` 改为 `-2`。这些 live behavior mutants 必须由 raw/final-state comparator 杀死，不能因 fixture、服务启动或外层 evidence hash 失败。

## 11. Known difference 与 skip 生命周期

任何 known difference 或 skip 必须包含：

```text
owner
issue
exact reason
profile
introduced_at
remove_when
last_verified_ref
affected cases
```

字段缺失、空值、Issue CLOSED 且无迁移 owner、remove_when 不可测量、case 未登记或 required case 被 skip 都失败。

首个 Core smoke 不吸收 #418 的 Vector differences。若 raw differential 暴露 Core 差异，implementation PR 必须在同一 child Issue 下给出修复，或在扩充 manifest 前创建独立 OPEN difference Issue；不得用 normalization 隐藏。

## 12. CI 与 changed-path 语义

新增 job 名称：`trusted Redis Core smoke differential`。

该 job 在以下变化时必须运行：

- Redis public command/RESP behavior；
- `tools/compat/**`；
- Core manifest/registry；
- Core Python module/marker/conftest；
- Core runner；
- Oracle controller/evidence verifier；
- compatibility workflow contract。

如果 workflow 使用 changed-path classifier，classifier 自身必须被 CI-contract tests 覆盖，未知/无法分类时 fail open to run，而不是跳过。job 不允许 `if: false`、`continue-on-error`、容忍缺失 artifact、未固定 action、非 exact expected Head 或普通 integration 替代。

普通 Python integration 必须显式排除 `raw_core_protocol`，避免在没有 trusted Oracle 的弱环境中误运行；CI-contract 同时禁止通过 `--ignore`、间接 Make 变量、环境 `PYTEST_ADDOPTS` 或 marker expression 把 required Core suite从专用 job 移除。

## 13. 与 #424、#427 的并行边界

PR #424 修改 INFO/runtime/server 相关 9 个文件；PR #427 修改 Raft durable metadata 相关 7 个文件。本 planning PR只修改 `.planning/`、compatibility 文档、spec/plan 和 scoped SDD validator，不与两者的源码文件重叠。

第一个 Core implementation slice 的共享热点是 `.github/workflows/ci.yml`，但 #424/#427 当前都不修改该文件。Core slice 不改 `src/**`，不会借 INFO 或 Raft PR 扩大 scope。

#424/#427 的可见 checks 绿色、mergeability 或 review 事件不构成 WP1 implementation acceptance。

## 14. Issue 与 PR 语义

Issue #433 是首个 implementation slice 的 Primary Issue。Issue #325 保持 OPEN 作为 Parent/Related。

planning PR 使用：

```text
Refs #433
Refs #325
```

并明确 planning-only，不关闭 #433。

只有完整满足 Issue #433 所有 required acceptance 的 implementation PR 才能使用：

```text
Fixes #433
```

不得 `Fixes #325`、`Fixes #415`、reopen #415、关闭 #418 或把 planning merge 当成 implementation acceptance。

## 15. WP1 ready 与下一安全动作

本 planning PR 合并前，WP1 仍只是 planning branch 上的 ready proposal。合并后，下一安全动作是：

1. 创建独立 WP1 implementation task；
2. 从 planning PR merge 后的 exact `origin/main` 创建新的 isolated worktree；
3. 创建新的 recovery checkpoint 和 Issue #433 dirty ownership；
4. 从 implementation plan 的 Task 1 marker-aware RED 开始；
5. 不复用本 planning worktree 执行源码、测试或 CI 修改。

SDD front matter 继续保留最后一个 current accepted 工作包 WP8；WP1 作为非 current work package 进入 `ready`。这符合现有 validator 的合法状态表示，也避免伪造尚不存在的 implementation PR。
