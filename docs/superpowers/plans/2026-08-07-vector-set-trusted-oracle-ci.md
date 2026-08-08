# Redis 8.8.1 Trusted Oracle、Vector CI 与 Security 实施计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development 逐任务执行，使用 superpowers:test-driven-development 先观察 RED，每个提交前使用 superpowers:verification-before-completion。步骤使用复选框（`- [ ]`）语法跟踪进度。

**目标：** 从 exact Redis 8.8.1 commit 建立 fail-closed Trusted Oracle v3，要求 primary/rebuild 独立构建的完整 artifact manifest 和 binary SHA-256 完全相等，正式 runtime 只从 held rebuild binary 启动，raw differential 在 verifier 监管的 runtime lease 中运行，所有进程/临时资源清理成功后才原子发布 provenance。同时建立非零、零 skip/xfail 的 Vector differential 和三节点 cluster required jobs，收敛 dormant Raft mutation capability，并让 `rkyv` advisory ignore 在依赖可达时自动失效。

**架构：** Rust `kiwi-compat` 是 Oracle v3 schema 和 CI contract 的唯一 normative consumer。`oracle_controller.py` 是受控外部进程、held FD、source/build/runtime/cleanup 状态机和 JSON producer 的唯一实现。Shell/PowerShell 只负责平台参数校验和调度。Workflow YAML 只安装前置、调用版本化 runner 和上传已完成 cleanup 的审计证据。

**技术栈：** Rust、serde JSON、Python 3 isolated mode、Bash、PowerShell/WSL、Linux `/proc/self/fd`、Git、GNU Make、CC/LD/AR/RANLIB、SHA-256、pytest、GitHub Actions Ubuntu、cargo-audit。

---

## Oracle v3 固定合同

- Schema：`kiwi-redis-oracle-build/v3` 和 `kiwi-redis-oracle-provenance/v3`。
- Recipe ID：`redis-8.8.1-linux-release-v3`。
- Source：tag `8.8.1`，commit `77b6c308396c9700672390a210143a8496fb4b10`。
- Build variables：`BUILD_TLS=no`、`MALLOC=libc`、`DEBUG=`、`DEBUG_FLAGS=`、`ENABLE_LTO=`、`OPT=-O3 -fno-omit-frame-pointer`、`-j 1`。
- Controller 以 isolated HOME/TMPDIR、清除 Python/Git/compiler cache 和受控 PATH 执行。
- Git、shell、Make、CC、LD、AR、RANLIB 的 path、version、SHA-256 和 file identity 全部记录；通过 held FD 执行或 fail closed。
- Artifact manifest 扫描 source tree 内全部 build 产物，按 source-relative byte order 排序。Kind 只允许 `regular` 和 `symlink`。
- `regular` 记录 path、mode、size、SHA-256；`symlink` 记录 path、mode、relative target。Target 不得绝对/逃出，不得成环，解析深度不超过 8，最终必须指向 manifest 内 regular file。
- Primary/rebuild 必须完整 manifest 相等且 final Redis binary SHA-256 相等。
- Runtime 只能从 held rebuild binary 启动；`INFO server` 必须唯一解析为 `redis_version:8.8.1`，PID、executable FD/file identity/hash 与 rebuild evidence 一致。
- Differential 通过 verifier `--run-after-ready` callback argv 在 runtime lease 内运行。
- 顺序固定为：equality → held rebuild runtime → identity → bounded callback → TERM/grace/KILL/wait → 删除 runtime/checkout/log/temp → final identity revalidation → fsync/close/atomic provenance publish。
- output 目标已存在时 fail closed；清理前不得存在最终 provenance。

## Task 1：Oracle v3 DTO、schema 和 validated Rust API

**Requirement：** `REQ-COMPAT-008`、`REQ-COMPAT-009`、`REQ-COMPAT-010`

**文件：**

- Create: `tools/compat/src/oracle.rs`
- Create: `tools/compat/tests/oracle.rs`
- Modify: `tools/compat/src/lib.rs`
- Modify: `tools/compat/Cargo.toml`
- Modify: `Cargo.lock`

- [ ] **步骤 1：写入 schema mutation 失败测试**

  测试必须拒绝：

  - 非 exact commit/tag/recipe/schema version；
  - duplicate/unknown/missing key；
  - 错误 integer width/range、SHA、timestamp、relative path、collection bound；
  - primary/rebuild artifact path/kind/mode/size/SHA/target 任一差异；
  - symlink 绝对目标、逃出、环、深度超过 8、最终非 regular；
  - comparison 任一 false；
  - runtime 未绑定 rebuild binary 或 INFO 版本不唯一；
  - cleanup 任一 false；
  - cleanup completion 前 published；
  - canonical fixture 中每一个 nested DTO/type mutation。

  ```bash
  cargo test -p kiwi-compat --test oracle -- --nocapture
  ```

  预期 RED：`oracle` module 和 v3 类型不存在。

- [ ] **步骤 2：实现 normative schema**

  `oracle.rs` 定义 source、recipe、tool identity、artifact entry、artifact comparison、runtime identity、bounded callback result、cleanup result、build evidence 和 final provenance 的完整 struct/enum，所有字段使用 `deny_unknown_fields`。无可选“略过安全证据”字段。

- [ ] **步骤 3：验证 ignored security mutant 调度**

  ```bash
  cargo test -p kiwi-compat --test oracle oracle_rejects_runtime_not_bound_to_rebuild -- --exact --ignored --test-threads=1 --nocapture
  ```

  命令必须输出 `running 1 test`，没有 `ORACLE_HARNESS_ERROR`，并且未修复 fixture 返回非零。

## Task 2：受控 primary builder 与完整 artifact manifest

**Requirement：** `REQ-COMPAT-008`、`REQ-COMPAT-009`

**文件：**

- Create: `scripts/compat/oracle_controller.py`
- Create: `scripts/compat/build-redis-8.8.1.sh`
- Modify: `tools/compat/tests/oracle.rs`

- [ ] **步骤 1：写入 process/tool/artifact 失败测试**

  - ambient PATH/PYTHONPATH/PYTHONHOME/Git config 不能替换 controller/tool。
  - tool directory 填充后 no-follow 扫描并冻结为 `0500`，recipe 不能替换 alias。
  - held FD 后替换原路径时执行原文件或 fail closed，replacement marker 不出现。
  - 每条命令有 deadline、stdout/stderr cap、独立 process group、TERM→grace→KILL→wait。
  - isolated HOME/TMPDIR 开始和结束均为空且 A/B 不共享。
  - build 前 artifact manifest 为空。
  - build 后拒绝 device/socket/path escape/非法 symlink。
  - manifest byte-order 排序、无重复、数量和单文件/总 bytes 上限受控。
  - recipe 明确清空 DEBUG/LTO 并禁止 checkout path 进入 release DWARF。
  - primary 只能写临时候选 metadata，不得写 final provenance。

  ```bash
  cargo test -p kiwi-compat --test oracle oracle_build_ -- --include-ignored --test-threads=1 --nocapture
  bash -n scripts/compat/build-redis-8.8.1.sh
  ```

- [ ] **步骤 2：实现 controller primary-build 状态机**

  `oracle_controller.py` 用 Python isolated mode 实现受控 tool registry、held FD、bounded runner、source identity、build recipe、artifact scan、canonical JSON producer 和候选 metadata 原子写入。Shell wrapper 只校验参数并 `exec` controller。

- [ ] **步骤 3：Linux feasibility**

  ```bash
  scripts/compat/build-redis-8.8.1.sh \
    --source /tmp/kiwi-oracle/source-a \
    --metadata /tmp/kiwi-oracle/run/primary-build.json
  ```

  输出必须包含 exact source、recipe、tool identities、完整 artifact manifest 和 primary binary evidence，但不得生成 final provenance。

## Task 3：Independent verifier、runtime lease 与 cleanup-before-publish

**Requirement：** `REQ-COMPAT-008`、`REQ-COMPAT-009`、`REQ-COMPAT-010`

**文件：**

- Modify: `scripts/compat/oracle_controller.py`
- Create: `scripts/compat/verify-redis-8.8.1.sh`
- Create: `scripts/compat/verify-redis-8.8.1.ps1`
- Modify: `tools/compat/tests/oracle.rs`

- [ ] **步骤 1：写入 verifier/runtime/cleanup 失败测试**

  - checkout B 必须 fresh disposable exact checkout，禁止 hardlink、alternates、shared object store、共享 HOME/TMPDIR/compiler cache。
  - source A rename-replace 后 clone 只使用 held source identity 或 fail closed。
  - verifier 重新解析 tools，不信任 primary metadata 的 tool 选择。
  - binary/artifact 任一不等时在启动 Redis 前失败。
  - runtime 从 held rebuild binary 启动，INFO/PID/file identity/hash 必须一致。
  - callback timeout/output flood/child leak 必须终止整个 process group 并返回非零。
  - Redis、callback、checkout B、log、temp root、fallible handle 任一清理失败时 final provenance 不存在。
  - output parent/source/tool/primary evidence 最终 identity 复验失败时不发布。
  - existing output target 失败；成功路径使用同目录 temp + fsync + close + atomic rename。

  ```bash
  cargo test -p kiwi-compat --test oracle oracle_verifier_ -- --include-ignored --test-threads=1 --nocapture
  bash -n scripts/compat/verify-redis-8.8.1.sh
  ```

- [ ] **步骤 2：实现 verifier-supervised callback**

  `verify-redis-8.8.1.sh` 支持：

  ```text
  --run-after-ready <callback argv...>
  ```

  controller 把 Oracle host/port、exact runtime evidence path 通过明确 environment 传给 callback；callback 不能修改 verifier 资源目录。PowerShell 入口只做 Windows→WSL 路径和 argv 安全转换。

- [ ] **步骤 3：真实 Linux 双构建验收**

  ```bash
  scripts/compat/verify-redis-8.8.1.sh \
    --source /tmp/kiwi-oracle/source-a \
    --primary-metadata /tmp/kiwi-oracle/run/primary-build.json \
    --output /tmp/kiwi-oracle/run/oracle-provenance.json \
    --run-after-ready /bin/true
  ```

  必须验证 manifest/binary equality、runtime binding、cleanup 全 true、最终无遗留进程/目录，然后才出现 provenance。

## Task 4：manifest-driven raw Vector differential required gate

**Requirement：** `REQ-COMPAT-001`、`REQ-COMPAT-002`、`REQ-COMPAT-003`、`REQ-VECTOR-003`、`REQ-VECTOR-005`

**文件：**

- Modify: `tools/compat/src/manifest.rs`
- Modify: `tools/compat/tests/manifest.rs`
- Modify: `tests/compat/redis-8.8.1/manifest.yaml`
- Create: `tests/compat/redis-8.8.1/vector-required-jobs.yaml`
- Modify: `tests/python/test_vector_set_differential.py`
- Modify: `tests/python/conftest.py`
- Modify: `tests/Makefile`
- Create: `scripts/compat/run-vector-differential.sh`
- Create: `tools/compat/tests/ci_contract.rs`
- Modify: `.github/workflows/ci.yml`

- [ ] **步骤 1：写入 collection/ownership/skip 失败测试**

  - `--collect-only` 不在 import 时连接 endpoint。
  - required-jobs registry 固定 job ID、module、marker、protocols、command scope、expected node IDs/item count、manifest profile 和 fast-job ownership。
  - registry command set 与 manifest 中全部 Vector required/known-difference scope 完全相等。
  - node ID/item count 增删未同步 registry 时失败。
  - Oracle/Kiwi 不可达、runtime identity mismatch、collected=0、skip、xfail/xpass、unexpected deselection、cleanup failure 均非零。
  - `tests/Makefile` 不再用路径级 `--ignore=test_vector_set_differential.py`；fast job 只能用 registry 定义的 marker ownership 显式 deselect。
  - CI contract 验证 `trusted-vector-differential` 存在、Ubuntu、无 `continue-on-error`、调用唯一 runner 并设置 required mode。

  ```bash
  python3 -m pytest tests/python/test_vector_set_differential.py --collect-only -q
  cargo test -p kiwi-compat --test manifest
  cargo test -p kiwi-compat --test ci_contract vector_differential
  ```

- [ ] **步骤 2：实现 required runner 与 workflow**

  `run-vector-differential.sh` 在 verifier callback 内：

  1. 启动当前 Head Kiwi 并验证 endpoint identity。
  2. 运行 `--collect-only`，与 registry expected node IDs/count 完全比较。
  3. 以 `-v -ra --strict-markers` 运行全部 RESP2/RESP3 用例。
  4. 解析 pytest totals，要求 collected > 0、failed=0、skipped=0、xfail=0、xpass=0、deselected=0。
  5. 清理 Kiwi，将 callback 结果交回 verifier；verifier 再清理 Redis 并发布 provenance。

- [ ] **步骤 3：真实 required 命令**

  ```bash
  KIWI_COMPAT_REQUIRE_ORACLE=1 bash scripts/compat/run-vector-differential.sh
  ```

## Task 5：capability 收敛与三节点 cluster fail-closed required gate

**Requirement：** `REQ-RAFT-008`、`REQ-VECTOR-005`

**文件：**

- Modify: `src/raft/src/capabilities.rs`
- Modify: `src/raft/src/grpc/admin.rs`
- Modify: `tests/python/test_vector_cluster.py`
- Modify: `tests/python/conftest.py`
- Create: `scripts/ci/run-vector-cluster.sh`
- Modify: `tools/compat/tests/ci_contract.rs`
- Modify: `.github/workflows/ci.yml`
- Verify: `src/cmd/src/table.rs`
- Verify: `src/net/tests/storage_command_e2e_tests.rs`

- [ ] **步骤 1：写入 capability/collection/process 失败测试**

  - literal `vector_set_raft_mutation_v1` 不再出现在 node advertisement 或 current required capability set。
  - `node_capabilities()` exact set 只包含当前真实 storage/snapshot format capability。
  - leader/follower 对 8 个 Vector 命令都返回同一稳定 unsupported error。
  - follower redirect、leader barrier、Raft append、StorageCommand 之前拒绝。
  - cluster module 不使用模块级 `skipif`；缺 binary/grpcurl/env 由 required runner 显式失败。
  - shutdown 使用 TERM→grace→KILL→wait，最终验证所有 PID/process group 消失。
  - grpcurl version/checksum 不符失败。
  - CI contract 验证 `vector-cluster-fail-closed` Ubuntu job、当前 Head build、required mode、唯一 runner、无 `continue-on-error`。

  ```bash
  cargo test -p raft capabilities::tests -- --nocapture
  cargo test -p net --test storage_command_e2e_tests storage_command_e2e_disabled_cluster -- --nocapture
  python3 -m pytest tests/python/test_vector_cluster.py --collect-only -q
  cargo test -p kiwi-compat --test ci_contract vector_cluster
  ```

- [ ] **步骤 2：收敛 capability 并实现 runner**

  - 删除 Raft mutation capability 常量和 dormant required set 语义，不引入任何 cluster-enable 路径。
  - 把 cluster 测试参数化为 leader/follower × 8 command 的明确 node IDs，registry 固定 expected count。
  - runner 验证 pinned grpcurl，显式 collect，运行三节点，要求 zero skip/xfail 和 zero process residue。

- [ ] **步骤 3：真实 required 命令**

  ```bash
  KIWI_RUN_CLUSTER_TESTS=1 bash scripts/ci/run-vector-cluster.sh
  ```

## Task 6：`rkyv` fail-closed reachability sentinel

**Requirement：** `REQ-STABILITY-003`

**文件：**

- Create: `scripts/ci/check-rkyv-reachability.sh`
- Create: `scripts/tests/test-rkyv-reachability-sentinel.sh`
- Modify: `.cargo/audit.toml`
- Modify: `.github/workflows/ci.yml`
- Modify: `.github/workflows/security.yml`
- Modify: `tools/compat/tests/ci_contract.rs`

- [ ] **步骤 1：用 fake cargo 写入三分支失败测试**

  1. cargo 非零退出 → sentinel 失败。
  2. cargo exit 0 但 stdout 有 inverse dependency → sentinel 失败。
  3. cargo exit 0、stdout 空、stderr 为 `nothing to print` → sentinel 成功。

  测试同时断言 exact command：

  ```text
  cargo tree --locked --offline --target all --all-features -i rkyv@0.7.46
  ```

  ```bash
  bash scripts/tests/test-rkyv-reachability-sentinel.sh
  ```

- [ ] **步骤 2：实现 sentinel 和 audit owner 合同**

  - sentinel 检查 cargo exit code 和 stdout，不把 stderr warning 当作依赖链。
  - `.cargo/audit.toml` 记录 owner、Issue #421、潜在路径 `openraft -> byte-unit -> rust_decimal`、当前为不可达 optional dependency、remove_when，删除“当前 Raft wire serialization 正在使用”的错误说明。
  - PR blocking `ci.yml` 在 `cargo fetch --locked` 后运行 offline sentinel 和 `cargo audit`。
  - `security.yml` 保留 scheduled visibility，不作为 PR blocking gate 的替代。

- [ ] **步骤 3：真实命令**

  ```bash
  cargo fetch --locked
  bash scripts/ci/check-rkyv-reachability.sh
  cargo audit
  cargo test -p kiwi-compat --test ci_contract rkyv
  ```

## Task 7：最终 CI contract、Linux acceptance 与 exact-Head 证据

**Requirement：** `REQ-VECTOR-003`、`REQ-VECTOR-005`、`REQ-STABILITY-002`、`REQ-STABILITY-003`、`REQ-OBS-002`

**文件：**

- Modify: `tools/compat/tests/ci_contract.rs`
- Modify: `.github/workflows/ci.yml`
- Verify: Task 1-6 所有文件

- [ ] **步骤 1：锁定 workflow 合同**

  CI contract 必须断言：

  - job IDs `trusted-vector-differential`、`vector-cluster-fail-closed`、`static-analysis` 存在；
  - required jobs 使用 Ubuntu，无 `continue-on-error`，只调用版本化 runner；
  - differential 从 verifier supervisor 获得 rebuild runtime，不 PING 任意端口；
  - cluster 显式验证 grpcurl version/checksum；
  - fast integration 不 path-ignore differential；
  - static-analysis 在准备 locked dependencies 后运行 rkyv sentinel 和 cargo audit；
  - runner 对 zero collection、skip、xfail、identity mismatch、cleanup failure 都非零；
  - 上传的 provenance/log 已经 cleanup 成功，不上传存活 runtime 或候选 provenance。

  ```bash
  cargo test -p kiwi-compat --test ci_contract
  ```

- [ ] **步骤 2：最终 Linux 命令顺序**

  ```bash
  git diff --check
  python3 scripts/validate_sdd.py --self-test
  python3 scripts/validate_sdd.py
  cargo fmt --check
  cargo clippy --all-targets --all-features -- -D warnings
  cargo test -p kiwi-compat --test manifest
  cargo test -p kiwi-compat --test oracle -- --include-ignored --test-threads=1 --nocapture
  cargo test -p kiwi-compat --test ci_contract
  cargo test -p raft capabilities::tests
  cargo test -p net --test storage_command_e2e_tests storage_command_e2e_disabled_cluster
  bash scripts/tests/test-rkyv-reachability-sentinel.sh
  cargo fetch --locked
  bash scripts/ci/check-rkyv-reachability.sh
  cargo audit
  KIWI_COMPAT_REQUIRE_ORACLE=1 bash scripts/compat/run-vector-differential.sh
  KIWI_RUN_CLUSTER_TESTS=1 bash scripts/ci/run-vector-cluster.sh
  cargo test --workspace
  ```

- [ ] **步骤 3：保存 exact-Head 审计证据**

  记录 Kiwi Head、Redis commit、distro/kernel/arch、tool path/version/SHA/file identity、primary/rebuild manifests、binary hashes、runtime PID/file identity/hash、pytest collected/passed/skipped/xfail totals、cleanup totals和最终无遗留进程证明。

## 工作流最终门禁

- [ ] Oracle 真实双构建在 Linux filesystem 运行，A/B 不共享 HOME/TMPDIR/cache/object store。
- [ ] 完整 artifact manifest 和 binary SHA-256 相等，runtime 绑定 rebuild binary。
- [ ] Differential collected > 0、skip=0、xfail=0，RESP2/RESP3 raw frame 非零执行。
- [ ] Cluster expected node IDs 全部执行，leader/follower 同样 fail closed，无遗留进程。
- [ ] `rkyv` sentinel 的 fake-cargo 三分支和真实 offline graph 都通过，`cargo audit` 成功。
- [ ] CI contract 能在删除 required job、添加 `continue-on-error`、恢复 path-ignore、绕过 verifier runtime 或不检查 stdout 时失败。
- [ ] 规格 reviewer 确认 cleanup-before-publish、held rebuild runtime、完整 artifact equality 和 required zero-skip 都未降级。
- [ ] 代码质量 reviewer 确认无 shell/Python 并行 schema、无安全关键 YAML 状态机、无未绑定 runtime copy、无过早 provenance publish。
