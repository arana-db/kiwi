# WP1 Redis 8.8.1 Core Raw Smoke Differential 实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 `subagent-driven-development`（推荐）或 `executing-plans` 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法跟踪进度。每个行为变化先使用 `test-driven-development`，提交、push、PR 和完成声明前使用 `verification-before-completion`。

**目标：** 为 `PING`、三参数 `SET`、`GET`、单 key `DEL`、`TYPE`、`PTTL` 建立 30-node、RESP2/RESP3、trusted Redis 8.8.1 raw differential、final-state/TTL 和 replayable evidence 的首个 WP1 required smoke gate。

**架构：** 新增独立 Core required-jobs registry、Rust canonical helper、Python raw module 和 Core runner；复用现有 Oracle independent rebuild、安全 controller 与 cleanup/publish transaction，只把 Vector-only evidence descriptor/collector 收敛成固定的 `vector-v1` 和 `core-smoke-v1` 两个 profile。普通 Python integration 与 required Core gate 保持分离，现有 40-node Vector gate 行为不变。

**技术栈：** Rust 2024、`yaml_serde`、Serde JSON、Python 3/pytest、raw TCP RESP、Bash、PowerShell/WSL、GitHub Actions、Trusted Redis 8.8.1 Oracle provenance v4。

**Work package：** WP1
**Primary Issue：** [#433](https://github.com/arana-db/kiwi/issues/433)
**Parent：** [#325](https://github.com/arana-db/kiwi/issues/325)
**Design：** [WP1 Redis 8.8.1 兼容门禁设计](../specs/2026-08-20-wp1-redis-compatibility-gates-design.md)
**Planning baseline：** `cb39927e44b84553f98ffee6ed1daa3f7388cf97`

---

## 文件结构

### 创建

- `tools/compat/src/bin/kiwi-required-core-jobs.rs`：读取 `kiwi-core-required-jobs/v1`，调用 Rust 权威 validator，输出 deterministic canonical JSON。
- `tests/compat/redis-8.8.1/core-required-jobs.yaml`：六命令、15 case、30 pytest node、raw request ownership 和 final-state profile 的权威 registry。
- `tests/python/test_core_differential.py`：管理两个 raw endpoint，发送 exact RESP2/RESP3 bytes，记录 raw transcript 和 final state。
- `scripts/compat/run-core-differential.sh`：build/freeze callback input、collection、execution、证据校验、cleanup 和 Oracle verifier callback 入口。

### 修改

- `tools/compat/src/manifest.rs`：显式迁移 `kiwi-redis-compat/v1`→`v2`，新增严格 `required_cases` 与 `RequiredCoreJobs` API；保留现有 12 条 classification 和 `RequiredVectorJobs` 行为。
- `tools/compat/src/oracle.rs`：在 provenance v4 envelope 内接受固定 Vector/Core evidence schema-file pairing，严格解析并离线重放 evidence document；拒绝未知或交叉配对。
- `tools/compat/tests/manifest.rs`：manifest v1→v2 migration、18-command classification/required-cases closure、Core registry success/failure mutants、Vector regression。
- `tools/compat/tests/oracle.rs`：Core/Vector evidence semantic replay、Vector v1 compatibility、schema/file/helper/cleanup mutants、双输出 rollback。
- `tools/compat/tests/ci_contract.rs`：marker-aware RED、Core runner/Make/conftest/workflow contract、Vector contract regression。
- `tests/compat/redis-8.8.1/manifest.yaml`：迁移到 v2；增加六个 command-level `known_difference` entries，并用 `required_cases` 精确绑定 Core subset。
- `tests/python/conftest.py`：注册 `raw_core_protocol`，参数化 RESP2/RESP3，保护 30 个 required node，生成 strict session summary。
- `tests/Makefile`：普通 Python/integration 入口显式排除 `raw_core_protocol`。
- `scripts/compat/oracle_controller.py`：只参数化 callback runtime/evidence descriptor、collector 和 binding；不改变安全执行/cleanup 内核。
- `.github/workflows/ci.yml`：新增唯一 `trusted Redis Core smoke differential` job 和严格 artifact upload。

### 明确不修改

- `src/**`：第一步先观察真实 differential，不预改命令实现。
- `tests/python/raw_resp_client.py`：复用现有 raw client。
- `scripts/compat/build-redis-8.8.1.sh`、`verify-redis-8.8.1.sh`、`verify-redis-8.8.1.ps1`：现有 wrapper 已提供通用 trusted callback 接口。
- `tools/compat/src/bin/kiwi-verify-oracle-evidence.rs`：不改文件；现有调用已把完整 sealed evidence bytes 传给 `OracleProvenance::verify_external_bindings`，semantic replay 在 `oracle.rs` 内完成。
- `Cargo.toml`、`Cargo.lock`、`tools/compat/Cargo.toml`：新增 `src/bin` 自动发现，使用现有依赖。
- `tests/tcl/**`、redis-rs、Raft/cluster/sentinel：不属于 Issue #433。

## Task 0：建立独立 implementation worktree 与基线

**文件：** 只创建 ignored `.codex/recovery/**`；不修改 tracked 文件。

- [ ] **步骤 1：重新获取 planning PR 合并后的 exact main**

运行：

```powershell
git fetch origin --prune
git rev-parse origin/main
git log -5 --oneline origin/main
```

预期：记录一个包含 planning PR 的 exact SHA；不得继续使用本计划中的历史 planning baseline 作为 implementation Head。

- [ ] **步骤 2：创建独立 worktree 和 branch**

运行前确认目标路径和 branch 不存在、`.worktrees/` 已 ignored。然后创建例如：

```powershell
git worktree add `
  'D:\test\github\kiwi\.worktrees\wp1-core-smoke-differential' `
  -b 'codex/wp1-core-smoke-differential' `
  origin/main
```

预期：新 worktree clean，`HEAD == origin/main`。不得复用 planning worktree 或两个 frozen Oracle worktree。

- [ ] **步骤 3：创建 implementation recovery checkpoint**

使用 `scripts/codex-workstate.ps1` 记录：Issue #433、exact base SHA、上述 14 个 exact files、commit/push/create PR 权限、merge 禁止、dirty ownership 和 Task 1 作为下一安全动作。

- [ ] **步骤 4：运行 relevant baseline**

Windows：

```powershell
python -I -B scripts/validate_sdd.py --self-test
python -I -B scripts/validate_sdd.py
cargo test -p kiwi-compat --test manifest -- --test-threads=1
cargo test -p kiwi-compat --test oracle -- --test-threads=1
cargo test -p kiwi-compat --test ci_contract -- --test-threads=1
```

预期：所有本地可执行检查通过。若 normal SDD validator 仅因 GitHub 网络失败，保留完整 URLError/exit code，并用 CI 的 planning validation 作独立证据；不得把网络失败改成 validator 忽略。

## Task 1：建立 marker-aware 总体 RED

**文件：** `tools/compat/tests/ci_contract.rs`

- [ ] **步骤 1：新增唯一总体合同 test**

新增 exact test：

```rust
#[test]
fn core_smoke_required_gate_contract_is_present() {
    let missing = required_core_contract_gaps(repository_root());
    assert!(
        missing.is_empty(),
        "CORE_GATE_TARGET_RED: {}",
        missing.join(", ")
    );
}
```

`required_core_contract_gaps` 只读检查以下 observable contract：

- Core registry 文件存在；
- Core runner 文件存在；
- Core Python module 存在；
- manifest schema 是 v2，六条 Core command-level classification 为 `known_difference`，且 `required_cases` 与 Core registry 双向闭合；
- workflow 含唯一 Core job；
-普通 Python Make 入口排除 `raw_core_protocol`。

fixture、repo root、文件读取失败必须 panic `CORE_GATE_HARNESS_ERROR:`，不能加入 `missing`。

- [ ] **步骤 2：运行 exact RED**

运行：

```powershell
cargo test -p kiwi-compat --test ci_contract `
  core_smoke_required_gate_contract_is_present `
  -- --exact --nocapture --test-threads=1
```

预期：

```text
running 1 test
CORE_GATE_TARGET_RED:
test result: FAILED. 0 passed; 1 failed
```

要求 exit 非零，`CORE_GATE_TARGET_RED:` 恰好一次，`CORE_GATE_HARNESS_ERROR:` 为零次。compile failure、fixture error、缺依赖或 zero tests 不是有效 RED。

- [ ] **步骤 3：保存 RED 证据但不提交独立失败 commit**

记录命令、exit code、marker 计数和完整 log 到 recovery checkpoint。保持 test 进入下一任务，最终由细粒度 positive/mutant tests 取代一次性 gap helper。

## Task 2：兼容 manifest 与 Core registry Rust 合同

**文件：**

- `tests/compat/redis-8.8.1/manifest.yaml`
- `tests/compat/redis-8.8.1/core-required-jobs.yaml`
- `tools/compat/src/manifest.rs`
- `tools/compat/tests/manifest.rs`
- `tools/compat/src/bin/kiwi-required-core-jobs.rs`

- [ ] **步骤 1：先写 manifest 和 registry failure tests**

在 `manifest.rs` tests 中先增加：

```text
repository_manifest_v2_contains_required_core_smoke_subsets
repository_core_registry_has_exact_contract
manifest_v2_preserves_the_existing_twelve_classifications
core_manifest_commands_remain_known_difference_at_command_scope
core_required_cases_are_bidirectionally_closed_with_the_registry
core_required_cases_reject_set_options_or_multi_key_del_scope_inflation
core_registry_rejects_unknown_field
core_registry_rejects_duplicate_node
core_registry_rejects_request_ownership_drift
core_registry_rejects_final_state_drift
core_registry_rejects_item_count_drift
core_registry_rejects_protocol_or_marker_drift
vector_registry_remains_exactly_40_nodes
```

第一条在旧 v1 manifest 上必须以 `CORE_GATE_TARGET_RED: manifest schema is not kiwi-redis-compat/v2` 失败。新 parser API 的 compile RED 只接受 expected missing symbol；先确认运行的是目标 test，不能把依赖下载或无关 compile error 当 RED。

- [ ] **步骤 2：运行细粒度 RED**

```powershell
cargo test -p kiwi-compat --test manifest `
  repository_manifest_v2_contains_required_core_smoke_subsets `
  -- --exact --nocapture --test-threads=1
```

预期：`running 1 test`，因 repository manifest 仍是 v1 且没有 Core `required_cases` 失败。

- [ ] **步骤 3：显式迁移 manifest v1→v2 并增加六个 entries**

把 `MANIFEST_SCHEMA` 和 repository YAML 显式升级为 `kiwi-redis-compat/v2`。现有 12 条 command classification/mode/known-difference 原样迁移；不得借 schema 升版改变 Vector 或其他现有条目。

新增六条命令全部使用 exact Redis 8.8.1 identity、standalone Cache OFF、RESP2/RESP3、command-level `classification: known_difference`、binary/raw/final-state evidence 和具体 owner。每条增加：

```yaml
required_cases:
  registry_path: tests/compat/redis-8.8.1/core-required-jobs.yaml
  registry_schema: kiwi-core-required-jobs/v1
  case_ids: [command-owned-case-ids]
```

每条 `known_differences` 使用 OPEN #325 持有尚未覆盖的完整 Redis 8.8.1 command surface，写明 exact reason、affected、introduced、last_verified_ref 和可测 remove_when。完成后总数必须是 18，分类严格为 4 required / 2 deferred / 12 known_difference；不得把三参数 SET、single-key DEL 或其他固定 cases 冒充整条命令 required。

- [ ] **步骤 4：编写权威 Core registry**

registry 固定：

```yaml
schema: kiwi-core-required-jobs/v1
jobs:
  - job_id: trusted-core-smoke-differential
    test_module: tests/python/test_core_differential.py
    pytest_marker: raw_core_protocol
    protocols: [resp2, resp3]
    commands: [PING, SET, GET, DEL, TYPE, PTTL]
    expected_item_count: 30
```

登记设计文档第 6.2 节的 15 个 case，并为 RESP2/RESP3 生成 30 个唯一 node ID、exact request Base64 ownership 和 final-state profile。Base64 必须由测试中使用的 deterministic request builder 生成，再由 Rust helper decode/re-encode equality test 锁定；不得手工录入无法复算的字符串。

- [ ] **步骤 5：实现最小 `RequiredCoreJobs` parser**

在 `manifest.rs` 中使用独立 raw/validated structs，复用现有 command canonicalization、Profile、Protocol 和 bounded string helper。`required_cases` 只允许固定 repository-relative registry path、exact Core schema、非空唯一 case IDs；仅在 command-level `known_difference` 且 tests 含 raw/final-state evidence 时允许。manifest tests 加载 Core registry 后必须证明 command/case 双向闭包，并拒绝缺失、多余、跨命令、SET option 或 multi-key DEL scope inflation。不修改 `RequiredVectorJobs` 的固定命令/marker/node 规则。

- [ ] **步骤 6：实现 canonical helper**

`kiwi-required-core-jobs` 只接受一个 registry path，成功时 stdout 只输出 canonical JSON，失败时 stderr 前缀固定为 `required Core jobs registry:` 且 exit 非零。

- [ ] **步骤 7：运行 GREEN 和 mutants**

```powershell
cargo test -p kiwi-compat --test manifest -- --test-threads=1
cargo build --locked -p kiwi-compat --bin kiwi-required-core-jobs
target\debug\kiwi-required-core-jobs.exe `
  tests\compat\redis-8.8.1\core-required-jobs.yaml `
  > $env:TEMP\kiwi-core-required-jobs.json
```

预期：manifest suite 全绿；repository schema=v2、commands=18、classification=4/2/12；六条 Core command 的 required case IDs 与 registry 双向相等；helper exit 0；canonical JSON schema 为 `kiwi-core-required-jobs/canonical-v1`，commands=6，expected nodes=30。

- [ ] **步骤 8：Commit**

```powershell
git add -- `
  tools/compat/src/manifest.rs `
  tools/compat/src/bin/kiwi-required-core-jobs.rs `
  tools/compat/tests/manifest.rs `
  tests/compat/redis-8.8.1/manifest.yaml `
  tests/compat/redis-8.8.1/core-required-jobs.yaml
git commit -s -m "test(compat): define Redis Core smoke registry" `
  -m "Constraint: keep the existing Vector registry and provenance contracts unchanged." `
  -m "Confidence: v2 migration and Rust mutants close command classification, required-case subset, node, request, and final-state ownership." `
  -m "Scope-risk: registry and manifest only; no command behavior or CI execution changes." `
  -m "Tested: cargo test -p kiwi-compat --test manifest -- --test-threads=1" `
  -m "Not-tested: trusted Redis/Kiwi process differential is introduced by the next tasks." `
  -m "Refs #433" `
  -m "Co-authored-by: OmX <omx@oh-my-codex.dev>"
```

## Task 3：Python raw module、marker ownership 与 final state

**文件：**

- `tests/python/test_core_differential.py`
- `tests/python/conftest.py`
- `tests/Makefile`
- `tools/compat/tests/ci_contract.rs`

- [ ] **步骤 1：写 marker/collection contract tests**

新增 CI-contract mutants，证明：

- 30 个 registry node 全部存在并拥有 `raw_core_protocol`；
- 任一 required node 丢 marker、增加 skip/skipif/xfail 都失败；
- 普通 Make targets 同时排除 `raw_vector_protocol`、`raw_core_protocol`、`required_vector_cluster`；
- `--ignore`、间接 Make 变量、`PYTEST_ADDOPTS` 或 marker-only bypass 都失败。

- [ ] **步骤 2：运行 marker RED**

```powershell
cargo test -p kiwi-compat --test ci_contract `
  core_differential_collection_contract_is_fail_closed `
  -- --exact --nocapture --test-threads=1
```

预期：旧 conftest/Makefile 不认识 Core marker，因此目标 test 失败。

- [ ] **步骤 3：实现 conftest ownership**

注册 `raw_core_protocol`，复用 raw endpoint fixture 和 RESP2/RESP3 function-scope 参数化。Core session summary必须写固定字段：`collected/passed/failed/skipped/xfailed/xpassed/deselected`，缺字段或非非负整数失败。

- [ ] **步骤 4：实现 15 case × 2 protocol**

Python module只使用 raw socket/client：

```text
test_ping_no_arg_raw_differential[resp2|resp3]
test_ping_binary_echo_raw_differential[resp2|resp3]
test_ping_wrong_arity_raw_differential[resp2|resp3]
test_set_binary_raw_differential[resp2|resp3]
test_set_wrong_arity_raw_differential[resp2|resp3]
test_get_binary_raw_differential[resp2|resp3]
test_get_missing_raw_differential[resp2|resp3]
test_get_wrong_arity_raw_differential[resp2|resp3]
test_del_existing_single_key_raw_differential[resp2|resp3]
test_del_missing_single_key_raw_differential[resp2|resp3]
test_del_wrong_arity_raw_differential[resp2|resp3]
test_type_string_raw_differential[resp2|resp3]
test_type_missing_raw_differential[resp2|resp3]
test_pttl_persistent_raw_differential[resp2|resp3]
test_pttl_missing_raw_differential[resp2|resp3]
```

每条 node 使用唯一 binary-safe key prefix；raw transcript 记录 request/response Base64+SHA；final state 按 `persistent-string-v1`、`all-missing-v1` 或 `no-owned-state-v1` 输出；cleanup double-DEL 后验证 TYPE/PTTL。

同一模块实现只在显式 `KIWI_CORE_TEST_MODE=1` 下启用的 loopback mutant proxy；proxy 只代理 Kiwi endpoint，Redis endpoint 保持原样，支持固定 mutant 名：

```text
ping-byte-flip
get-binary-truncate
set-ok-without-forward
pttl-minus-one-to-minus-two
```

mutant proxy 必须先完成真实 endpoint readiness，再只改变目标 reply/forward 行为，使 collection、runtime 和 pytest summary 仍合法。普通 module execution 和 required runner 检测到任一 test-mode/mutant env 时必须 fail closed；只有 runner 的显式 `--test-mutant NAME` 路径可以开启，且该路径禁止发布 final evidence/provenance。

- [ ] **步骤 5：只运行 collection，不把缺 endpoint 冒充目标失败**

在 WSL/ext4：

```bash
python3 -m pytest tests/python/test_core_differential.py \
  --collect-only -q -m raw_core_protocol
```

预期：列出恰好 30 个 node。若 conftest 在 collect-only 阶段要求真实 endpoint，应修复 fixture scope，使 collection 不启动服务；不得用环境变量伪造 endpoint。

- [ ] **步骤 6：运行 contract GREEN**

```powershell
cargo test -p kiwi-compat --test ci_contract -- --test-threads=1
```

预期：Core marker/Make mutants 全绿，现有 Vector marker tests 全绿。

- [ ] **步骤 7：Commit**

```powershell
git add -- tests/python/test_core_differential.py tests/python/conftest.py tests/Makefile tools/compat/tests/ci_contract.rs
git commit -s -m "test(compat): add raw Redis Core smoke cases" `
  -m "Constraint: compare raw RESP bytes and keep ordinary Python integration separate." `
  -m "Confidence: exact 30-node collection and marker mutation tests are fail closed." `
  -m "Scope-risk: test-only Python and collection contracts; no production command changes." `
  -m "Tested: cargo test -p kiwi-compat --test ci_contract -- --test-threads=1; pytest collect-only reports 30 nodes." `
  -m "Not-tested: Redis/Kiwi execution waits for the trusted runner and Oracle profile." `
  -m "Refs #433" `
  -m "Co-authored-by: OmX <omx@oh-my-codex.dev>"
```

## Task 4：Oracle Core evidence profile RED/GREEN

**文件：**

- `scripts/compat/oracle_controller.py`
- `tools/compat/src/oracle.rs`
- `tools/compat/tests/oracle.rs`

- [ ] **步骤 1：先写 profile/binding failure tests**

新增 exact tests：

```text
core_callback_profile_accepts_only_exact_frozen_argv
core_evidence_requires_core_helper_and_registry_schema
core_cleanup_schema_cannot_use_vector_schema
core_provenance_rejects_vector_core_schema_file_cross_pairing
core_evidence_semantic_replay_accepts_the_canonical_document
core_replay_rejects_registry_node_drift_after_outer_hash_rebind
core_replay_rejects_raw_response_byte_drift_after_outer_hash_rebind
core_replay_rejects_final_state_or_cleanup_drift_after_outer_hash_rebind
core_replay_rejects_duplicate_key_or_base64_sha_mismatch
vector_evidence_semantic_replay_fixture_remains_valid
unknown_differential_profile_is_rejected
vector_v1_evidence_fixture_remains_valid
vector_40_node_callback_fixture_remains_valid
core_publication_failure_rolls_back_evidence_and_provenance
```

Mutants 必须覆盖：helper 替换、file extra/missing、registry schema swap、cleanup schema swap、evidence schema swap、expected Head/tree drift、callback argv drift、publish/close/fsync/rename/post-verify fault；另对 registry node、raw response byte、final-state PTTL、cleanup second DEL、duplicate JSON key 和 Base64/SHA 做内部变异，并先重算 evidence size/SHA、同步 provenance，证明失败来自 semantic replay。

- [ ] **步骤 2：运行 Oracle RED**

```powershell
cargo test -p kiwi-compat --test oracle `
  core_callback_profile_accepts_only_exact_frozen_argv `
  -- --exact --nocapture --test-threads=1
```

预期：旧 controller 没有 Core profile，目标 test 失败；不得因 Linux-only fixture在 Windows提前 return 假 PASS。平台不支持的真实进程 test 使用明确 `#[ignore = "requires WSL/ext4 Linux"]`，静态 schema tests 在 Windows运行。

- [ ] **步骤 3：提取两个固定 evidence profile descriptor**

Descriptor 固定：

```text
vector-v1:
  callback argv = run-vector-differential.sh --callback
  helper = target/debug/kiwi-required-vector-jobs
  registry file/schema = vector-required-jobs.json / kiwi-vector-required-jobs/canonical-v1
  cleanup schema = kiwi-vector-callback-cleanup/v1
  evidence schema = kiwi-vector-differential-evidence/v1

core-smoke-v1:
  callback argv = run-core-differential.sh --callback
  helper = target/debug/kiwi-required-core-jobs
  registry file/schema = core-required-jobs.json / kiwi-core-required-jobs/canonical-v1
  cleanup schema = kiwi-core-callback-cleanup/v1
  evidence schema = kiwi-core-differential-evidence/v1
```

Descriptor 只选择 callback runtime/evidence allowlist、helper、registry、cleanup/evidence schema。不得移动或复制 `run_bounded`、rebuild、runtime identity、cleanup 和 publication transaction。

在 `oracle.rs` 增加 strict `DifferentialEvidenceDocument::{VectorV1, CoreSmokeV1}` parser。`OracleProvenance::verify_external_bindings` 在 file/name/size/SHA pairing 通过后必须解析 document，并验证 registry、collection、summary、raw transcript、final-state、cleanup 和 helper/schema pairing。现有 `kiwi-verify-oracle-evidence` 无需改文件，因为它已将完整 sealed evidence bytes 传入该函数。

- [ ] **步骤 4：保持 provenance v4 envelope**

`DifferentialEvidenceIdentity` 接受上述两个精确 schema 与文件名 pairing，禁止 wildcard。Vector fixture serialized fields和验证结果保持不变。新增 ignored test `replay_published_core_evidence_after_cleanup` 从 `KIWI_CORE_FINAL_PROVENANCE`、`KIWI_CORE_FINAL_EVIDENCE`、`KIWI_EXPECTED_HEAD`、`KIWI_EXPECTED_TREE` 读取最终文件，并调用同一 `verify_external_bindings` 路径。如果实现需要修改 provenance v4 field set 或 sealed external binding，停止实施并回到 planning；Issue #433 不授权 silent v5。

- [ ] **步骤 5：运行 Oracle GREEN**

Windows static/mutation tests：

```powershell
cargo test -p kiwi-compat --test oracle -- --test-threads=1
```

WSL/ext4 Linux ignored exact tests：

```bash
cargo test -p kiwi-compat --test oracle \
  core_ -- --ignored --nocapture --test-threads=1
```

预期：Windows 无 early-return 假 PASS；Linux 真实 profile/cleanup/publication/semantic replay mutants 全绿；所有外层 hash 已同步的内部 evidence mutants 仍被拒绝；现有 Vector Oracle tests 全绿。

- [ ] **步骤 6：Commit**

```powershell
git add -- scripts/compat/oracle_controller.py tools/compat/src/oracle.rs tools/compat/tests/oracle.rs
git commit -s -m "test(compat): generalize trusted differential evidence profiles" `
  -m "Constraint: preserve provenance v4 and the existing Vector evidence profile exactly." `
  -m "Confidence: schema, helper, callback, cleanup, and publication mutants reject cross-profile drift." `
  -m "Scope-risk: only callback evidence description and binding; the controller safety kernel is unchanged." `
  -m "Tested: cargo test -p kiwi-compat --test oracle -- --test-threads=1; WSL ignored Core profile tests." `
  -m "Not-tested: CI-host full Redis rebuild remains in Task 7." `
  -m "Refs #433" `
  -m "Co-authored-by: OmX <omx@oh-my-codex.dev>"
```

## Task 5：Core runner collection、execution、evidence 与 cleanup

**文件：**

- `scripts/compat/run-core-differential.sh`
- `tools/compat/tests/ci_contract.rs`

- [ ] **步骤 1：写 runner static/mutation tests**

要求 runner 包含并由 test 验证：

- `KIWI_COMPAT_REQUIRE_ORACLE=1`；
- exact `KIWI_EXPECTED_HEAD`；
- `cargo build --locked` 构建 Core helper、external verifier 和 Kiwi；
- authoritative registry 唯一输入；
- collect-only 后验证 exact 30 node IDs；
- execution summary 30/30/0/0/0/0/0；
- raw transcript 和 final-state 校验；
- fixed evidence allowlist 和逐文件大小；
- callback cleanup schema；
- callback stage；
- trap/process/data/log cleanup；
- evidence/provenance outputs distinct且预先不存在。
- ordinary/CI mode 拒绝 `KIWI_CORE_TEST_MODE`、`KIWI_CORE_TEST_MUTANT` 和 `--test-mutant`，显式 test-mutant mode 禁止发布 final outputs。

Mutants 覆盖：helper bypass、registry copy drift、summary 字段删除、30→29、skip/xfail/xpass/deselect、validation reordering、cleanup bypass、unexpected file、symlink、missing artifact、expected Head mismatch、required job 注入 test-mode env、test-mutant mode 意外发布 artifact。

- [ ] **步骤 2：运行 runner RED**

```powershell
cargo test -p kiwi-compat --test ci_contract `
  core_differential_runner_is_fail_closed `
  -- --exact --nocapture --test-threads=1
```

预期：runner 文件尚不存在，目标 marker RED；repo/path read error 使用 harness marker。

- [ ] **步骤 3：实现 callback mode**

`--callback` 内严格执行：

```text
validate frozen input
start Kiwi
wait raw readiness
collect 30 nodes
validate collection
run 30 nodes
validate raw transcript
validate final state
validate summary
stop/reap Kiwi
remove data/log dirs
write cleanup evidence
return callback status
```

任何 stage 失败仍执行 cleanup，最终 exit 保留首个业务错误或 cleanup 错误；cleanup 错误不能被原业务成功覆盖。

- [ ] **步骤 4：实现 outer mode**

Outer mode验证 clean exact Head，构建 Core helper/verifier/Kiwi，构建 primary Redis，再调用现有 verifier，以 exact Core callback argv 触发 `core-smoke-v1` profile。成功后只接受非空、互异的 final Core evidence 和 provenance。

- [ ] **步骤 5：在 WSL 运行真实 GREEN**

```bash
export KIWI_COMPAT_REQUIRE_ORACLE=1
export KIWI_REDIS_ORACLE_SOURCE=/absolute/ext4/redis-source
export KIWI_REDIS_ORACLE_PRIMARY_METADATA=/absolute/ext4/core-primary-build.json
export KIWI_REDIS_ORACLE_OUTPUT=/absolute/ext4/core-oracle-provenance.json
export KIWI_REDIS_ORACLE_EVIDENCE_OUTPUT=/absolute/ext4/core-differential-evidence.json
export KIWI_EXPECTED_HEAD="$(git rev-parse HEAD)"
bash scripts/compat/run-core-differential.sh
```

预期：独立 rebuild hash equality；30 collected/30 passed；两份 final JSON存在；work/runtime/checkout/process residue 为零。

- [ ] **步骤 6：受控 negative run**

复制 runner 输入到临时测试根并使用测试 seam 将 summary 的 `skipped` 改为 1。预期 exit 非零、无 final evidence/provenance、marker 为 collection/run contract error而非 harness setup error。

- [ ] **步骤 7：Commit**

```powershell
git add -- scripts/compat/run-core-differential.sh tools/compat/tests/ci_contract.rs
git commit -s -m "test(compat): run trusted Redis Core smoke differential" `
  -m "Constraint: use the accepted Oracle controller and publish no evidence before cleanup." `
  -m "Confidence: collection, transcript, final-state, cleanup, and residue mutants fail closed." `
  -m "Scope-risk: Core test runner only; no production command or Vector runner changes." `
  -m "Tested: ci_contract suite and WSL trusted Core runner, 30 collected and 30 passed." `
  -m "Not-tested: GitHub-hosted required job is introduced by the next task." `
  -m "Refs #433" `
  -m "Co-authored-by: OmX <omx@oh-my-codex.dev>"
```

## Task 6：Required visible CI job 与 bypass 防护

**文件：**

- `.github/workflows/ci.yml`
- `tools/compat/tests/ci_contract.rs`

- [ ] **步骤 1：先写 workflow RED/mutants**

验证：

- job id 唯一；
- name 精确 `trusted Redis Core smoke differential`；
- checkout/action SHA 固定；
- namespace/preflight 位于 runner 前；
- source checkout exact Redis commit；
- expected Head 使用 PR Head 或 push SHA；
- 运行 `bash scripts/compat/run-core-differential.sh`；
- evidence/provenance upload 严格固定两个 final files；
- `if-no-files-found: error`；
- retention 7 days；
- no `continue-on-error`、no conditional false、no premature upload；
- changed-path classifier 无法分类时运行 job。

- [ ] **步骤 2：运行 workflow RED**

```powershell
cargo test -p kiwi-compat --test ci_contract `
  trusted_core_smoke_required_job_is_unique_and_fail_closed `
  -- --exact --nocapture --test-threads=1
```

预期：旧 workflow 缺 job，exact test 失败。

- [ ] **步骤 3：新增独立 job**

复用 trusted Vector job 的 Linux trust boundary 和 source checkout步骤，但使用独立 `$RUNNER_TEMP/kiwi-core-oracle` 输出目录和 Core runner。不要把 Core 塞进 Vector job，也不要修改 Vector job 名称、registry、commands 或 upload paths。

- [ ] **步骤 4：替换总体 gap RED 为持久 positive/mutant tests**

删除 Task 1 一次性 `required_core_contract_gaps` helper；保留细粒度 manifest、registry、marker、runner、controller、workflow tests。再次运行 Task 1 exact test 名应不存在，完整 ci_contract suite 应执行所有持久 tests。

- [ ] **步骤 5：运行 GREEN**

```powershell
cargo test -p kiwi-compat --test ci_contract -- --test-threads=1
cargo test -p kiwi-compat --test manifest -- --test-threads=1
```

预期：Core workflow mutants 与现有 Vector workflow mutants 全绿。

- [ ] **步骤 6：Commit**

```powershell
git add -- .github/workflows/ci.yml tools/compat/tests/ci_contract.rs
git commit -s -m "ci: require trusted Redis Core smoke differential" `
  -m "Constraint: add a distinct Core gate without weakening or renaming the trusted Vector jobs." `
  -m "Confidence: workflow and bypass mutants cover Head identity, ordering, upload, markers, and path selection." `
  -m "Scope-risk: required CI time increases by one independent Redis rebuild and smoke run." `
  -m "Tested: cargo test -p kiwi-compat --test ci_contract -- --test-threads=1" `
  -m "Not-tested: GitHub-hosted execution is verified after push." `
  -m "Refs #433" `
  -m "Co-authored-by: OmX <omx@oh-my-codex.dev>"
```

## Task 7：完整回归、mutation proof 与跨平台验收

**文件：** 不新增范围；只验证上述 14 个文件。

- [ ] **步骤 1：Windows changed-path gates**

```powershell
cargo fmt --all -- --check
cargo test -p kiwi-compat --test manifest -- --test-threads=1
cargo test -p kiwi-compat --test oracle -- --test-threads=1
cargo test -p kiwi-compat --test ci_contract -- --test-threads=1
python -m pytest tests/python/test_core_differential.py --collect-only -q -m raw_core_protocol
git diff --check
```

预期：Rust suites 0 failure；pytest collection 30；diff check 0 error。Windows 不声明 Linux process/rebuild 语义已执行。

- [ ] **步骤 2：WSL/ext4 Linux trusted Core GREEN**

运行 Task 5 的真实 runner。记录：

```text
exact Head/tree
Redis primary/rebuild SHA equality
30 collected / 30 passed
0 failed/skipped/xfailed/xpassed/deselected
Core evidence SHA/size
provenance SHA/size
cleanup booleans
process/runtime/checkout residue
```

确认 Redis/Kiwi 进程、controller work root、verification checkout 和 callback 临时目录已经删除后，执行最终文件离线 replay：

```bash
export KIWI_CORE_FINAL_PROVENANCE=/absolute/ext4/core-oracle-provenance.json
export KIWI_CORE_FINAL_EVIDENCE=/absolute/ext4/core-differential-evidence.json
export KIWI_EXPECTED_HEAD="$(git rev-parse HEAD)"
export KIWI_EXPECTED_TREE="$(git rev-parse 'HEAD^{tree}')"
cargo test -p kiwi-compat --test oracle \
  replay_published_core_evidence_after_cleanup \
  -- --exact --ignored --nocapture --test-threads=1
```

预期：test 只读取两份 final files 和 expected Head/tree，内部 semantic replay 通过；不得访问 live work directory、Redis/Kiwi endpoint 或已删除 checkout。

- [ ] **步骤 3：Vector regression**

至少执行：

```bash
cargo test -p kiwi-compat --test manifest -- --test-threads=1
cargo test -p kiwi-compat --test oracle -- --test-threads=1
cargo test -p kiwi-compat --test ci_contract -- --test-threads=1

vector_output_root=$(mktemp -d /absolute/ext4/kiwi-vector-regression.XXXXXX)
export KIWI_COMPAT_REQUIRE_ORACLE=1
export KIWI_REDIS_ORACLE_SOURCE=/absolute/ext4/redis-source
export KIWI_REDIS_ORACLE_PRIMARY_METADATA="$vector_output_root/vector-primary-build.json"
export KIWI_REDIS_ORACLE_OUTPUT="$vector_output_root/vector-oracle-provenance.json"
export KIWI_REDIS_ORACLE_EVIDENCE_OUTPUT="$vector_output_root/vector-differential-evidence.json"
export KIWI_EXPECTED_HEAD="$(git rev-parse HEAD)"
test ! -e "$KIWI_REDIS_ORACLE_PRIMARY_METADATA"
test ! -e "$KIWI_REDIS_ORACLE_OUTPUT"
test ! -e "$KIWI_REDIS_ORACLE_EVIDENCE_OUTPUT"
bash scripts/compat/run-vector-differential.sh
```

预期：Vector 使用与 Core 不同的绝对、预先不存在的 metadata/provenance/evidence paths；registry 仍 40 nodes；trusted Vector evidence/provenance成功；既有 schema、helper、cleanup 和 CI job 未漂移。记录 artifact identity 后再删除 `vector_output_root`。

- [ ] **步骤 4：discriminative mutants**

在临时副本或 test seam 中依次变异：

- 30→29；
- `skipped=1`；
- Core helper 替换为 Vector helper；
- Core evidence schema 替换为 Vector schema；
- expected Head 改一位；
- published evidence 删除 registry node，再重算 size/SHA 并同步 provenance；
- published evidence 翻转 raw response byte，再重算 size/SHA 并同步 provenance；
- published evidence 删除 final-state PTTL，再重算 size/SHA 并同步 provenance；
- published evidence 把 cleanup 二次 DEL 改为 1，再重算 size/SHA 并同步 provenance；
- duplicate JSON key 与 transcript Base64/SHA 不一致；
- upload 移除 `if-no-files-found: error`；
- Vector expected node 40→39。

随后在真实 endpoint/readiness/collection/summary 均合法的 test-mutant mode 依次执行：

```bash
for mutant in \
  ping-byte-flip \
  get-binary-truncate \
  set-ok-without-forward \
  pttl-minus-one-to-minus-two
do
  if KIWI_CORE_TEST_MODE=1 \
    bash scripts/compat/run-core-differential.sh --test-mutant "$mutant"
  then
    echo "mutant unexpectedly passed: $mutant" >&2
    exit 1
  fi
done
```

每个 run 必须显示目标 raw/final-state comparison marker，不能显示 `CORE_GATE_HARNESS_ERROR`，且不能产生 final evidence/provenance。普通 runner、GitHub required job 或未带 `--test-mutant` 的执行若看到 test-mode env 必须立即拒绝。

预期：每个 structural/replay/behavior mutant 至少杀死一个精确 gate；外层 hash 已同步的 artifact mutants仍因内部 semantic replay 失败；behavior mutants 由真实 raw/final-state comparator 失败；没有 mutant 只靠 unrelated parser/setup failure 失败。

- [ ] **步骤 5：final changed-file audit**

```powershell
git diff --name-only origin/main...HEAD
git status --porcelain=v2 --branch --untracked-files=all
git diff --check origin/main...HEAD
```

预期：tracked changed-file set 恰好是 Issue #433 的 14 个 files，worktree 除 ignored recovery 外 clean。

- [ ] **步骤 6：记录 verification checkpoint**

Recovery 记录每条命令、exit code、实际 test count、未运行项和环境边界。任何真实 differential failure先分类为 Kiwi behavior difference、test contract error 或 harness error，不删除断言、不降级 required case。

## Task 8：push、Ready implementation PR 与关闭语义

**文件：** 不新增 tracked 文件。

- [ ] **步骤 1：检查 commit 叙事与 Signed-off-by**

```powershell
git log --format=fuller origin/main..HEAD
git diff --stat origin/main...HEAD
```

预期：每个 commit 使用 `git commit -s`，包含 Constraint、Confidence、Scope-risk、Tested、Not-tested、`Refs #433` 和 `Co-authored-by: OmX <omx@oh-my-codex.dev>`。

- [ ] **步骤 2：push 非强制更新**

```powershell
git push -u origin codex/wp1-core-smoke-differential
```

禁止 force-push、rebase、reset 或 merge。

- [ ] **步骤 3：创建 Ready implementation PR**

PR body 必须包含：

- Work package WP1；
- exact SDD/planning baseline 和 implementation base；
- Primary Issue `Fixes #433`，仅当所有 required acceptance 已满足；
- Parent `Related #325`；
- Historical #415、PR #422；
- Related Vector registry #418；
- REQ-COMPAT-001/002/003/006/007/008/009/010、REQ-STABILITY-003、REQ-WORK-005/007；
- D020；
- exact 14 files；
- scope/non-goals；
- Windows/WSL/Linux/CI evidence；
- 未执行项与原因；
- 明确不关闭 #325/#415/#418。

如果任何 required acceptance 仍缺失，PR 必须使用 `Refs #433`，不得 `Fixes #433`。

- [ ] **步骤 4：等待 exact Head visible checks**

复核 final Head 没变化，列出所有 visible checks、run/job IDs、artifact identity、mergeability 和 review threads。`no required checks reported` 必须作为治理 evidence gap 单独报告，不能改写为 required checks pass。

- [ ] **步骤 5：停止在 merge 前**

本计划不包含 merge 命令。把 PR URL、Issue URL、branch、Head、changed files、验证结果、remaining risk 和下一安全动作交付给用户。
