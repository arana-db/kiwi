# VectorSet 合并后全量闭环实施总计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development 按任务执行，使用 superpowers:test-driven-development 观察每个失败用例，并在提交前使用 superpowers:verification-before-completion。步骤使用复选框（`- [ ]`）语法跟踪进度。

**目标：** 在 Draft PR #422 中完整闭合 PR #356 合并后的 VectorSet Storage 生命周期、snapshot 恢复、VSIM 一致性、network admission、VADD 错误优先级、Redis 8.8.1 Trusted Oracle、raw differential、三节点 cluster fail-closed 和 `rkyv` 依赖可达性门禁。

**架构：** WP8 使用一个 GitHub Draft PR 聚合，但每个实现工作流在独立 worktree 和分支中串行执行。每个任务先提交能够在未修复实现上失败的测试，再提交最小实现；Root 只在规格复核和代码质量复核都通过后把工作流提交顺序集成到 `codex/fix-vector-set-post-merge`。

**技术栈：** Rust 1.97.1 / Edition 2024、RocksDB、OpenRaft、Tokio、RESP2/RESP3、Python/pytest、Bash、PowerShell/WSL、GitHub Actions、Redis 8.8.1 exact commit `77b6c308396c9700672390a210143a8496fb4b10`。

---

## 权威、基线与边界

- 权威 SDD：`.planning/SDD.md`，当前工作包为 WP8。
- 书面设计：`docs/superpowers/specs/2026-08-06-vector-set-post-merge-remediation-design.md`。
- 主线事实基线：`main@733888fc90ad8ef039947e87b08d7500a405954a`。
- 真实 Base 兼容基线：`688d905fec31b54aec76f36676f55efd8b5cfa17`。
- 聚合分支：`codex/fix-vector-set-post-merge`。
- 聚合 PR：Draft PR #422。
- Primary Issue：#421；Related Issues：#415、#418、#325、#340、#342。
- 授权包含修改源码、测试、文档和 CI，运行验证，提交并 push 到 Draft PR #422。
- 不授权 merge、rebase、`reset --hard`、`clean`、修改 branch protection、关闭 Issue 或 Resolve 历史评论。
- 不把 Redis、redis-rs 或 Oracle controller 引入生产 server/storage/raft 依赖或请求路径。

## 计划分解

| 工作流 | 详细计划 | 负责的主要合同 |
|---|---|---|
| Storage / Snapshot / VSIM | [2026-08-07-vector-set-storage-recovery.md](2026-08-07-vector-set-storage-recovery.md) | Root/Instance Manifest v2、Base 六 CF 与已合并 Vector-v1 七 CF staged migration、对应 source rollback、v1/v2 snapshot、install marker 恢复、全量 Vector 一致性、VSIM 单一串行视图 |
| Runtime / Protocol | [2026-08-07-vector-set-runtime-protocol.md](2026-08-07-vector-set-runtime-protocol.md) | `Bytes` 无拷贝 admission、Config 真实传递、gate 顺序、VADD 类型化错误、RESP2/RESP3 独立 raw 客户端 |
| Trusted Oracle / CI / Security | [2026-08-07-vector-set-trusted-oracle-ci.md](2026-08-07-vector-set-trusted-oracle-ci.md) | Oracle v3 schema、受控双构建、artifact equality、runtime lease、cleanup-before-publish、required differential/cluster jobs、capability 收敛、rkyv sentinel |

## 文件所有权与集成顺序

- Storage 工作流拥有 `src/storage/**`、snapshot 恢复所需的 `src/raft/src/state_machine.rs`、`src/raft/src/snapshot_install.rs`、Storage 兼容脚本与对应 Rust 测试。
- Runtime/Protocol 工作流拥有 `src/net/**`、`src/cmd/src/vector/admission.rs`、`src/cmd/src/vector/vadd.rs`、`src/cmd/src/table.rs`、`src/server/src/main.rs` 的 network limits 传递，以及 raw RESP 客户端。
- Oracle/CI 工作流拥有 `tools/compat/**`、`scripts/compat/**`、`scripts/ci/**`、`scripts/tests/**`、`.github/workflows/ci.yml`、`.cargo/audit.toml`、cluster/differential required runner。
- `src/cmd/src/vector/vsim.rs`、`src/server/src/main.rs`、`tests/python/test_vector_set_differential.py`、`tests/python/conftest.py` 是明确的交叉文件；必须按 Storage → Runtime/Protocol → Oracle/CI 的顺序集成，后续工作流保留前序工作流的已验证行为。
- 任一时刻只有一个实现 worker 修改文件；规格复核和代码质量复核可以在 worker 提交后进行，但不得与同一任务的写入并发。

## 任务 0：使 WP8 进入 ready

**Requirement：** `REQ-WORK-005`、`REQ-WORK-006`、`REQ-WORK-007`、`D012`、`D019`

**文件：**

- Modify: `docs/superpowers/specs/2026-08-06-vector-set-post-merge-remediation-design.md`
- Create: 本总计划和三个工作流计划
- Modify: `.planning/SDD.md`
- Modify: `scripts/validate_sdd.py`

- [ ] **步骤 1：先增加 current-plan 失败路径自测**

  在 `scripts/validate_sdd.py` 增加正向 WP8 external-plan 用例和以下反向变异：

  - WP0 指向 external plan 必须失败；
  - WP1-WP8 指向 `.planning/SDD.md#wpN` 必须失败；
  - current plan 不存在必须失败；
  - current plan 逃出仓库或不在 `docs/superpowers/plans/` 必须失败；
  - current-state table 的 plan link 与 front matter 不一致必须失败；
  - current plan 中断链或 fence 不成对必须失败。

  运行：

  ```powershell
  python scripts/validate_sdd.py --self-test
  ```

  预期 RED：新的正向 WP8 external-plan 用例因现有 validator 强制 `.planning/SDD.md#wp8` 而失败。

- [ ] **步骤 2：实现 WP0 例外和 WP1-WP8 external-plan 合同**

  `scripts/validate_sdd.py` 必须执行：

  - WP0 唯一允许 `current_plan: .planning/SDD.md#wp0`；
  - WP1-WP8 的 `current_plan` 必须是存在的 repo-relative `docs/superpowers/plans/*.md`；
  - current-state table 从 `.planning/SDD.md` 出发的相对链接必须解析到同一文件；
  - normal validation 自动把 current plan 加入 Markdown 链接和 fence 校验；
  - self-test 临时契约拷贝必须包含 current plan。

  重跑自测，预期 GREEN：所有新旧变异均通过。

- [ ] **步骤 3：冻结实现期合同**

  修改设计文档：

  - 状态改为“已确认，实施计划已建立”；
  - Manifest digest 固定为 SHA-256 lowercase hex，输入是不含 digest 字段的固定字段顺序 JSON bytes，不允许 map 影响序列化顺序；
  - 目录 identity 是 root/transaction/instance UUID 与 manifest digest 的组合，不依赖 inode 或 Windows file ID；
  - `RaftMetadataPersisted` 表示 current snapshot metadata/data 和 state-machine applied/membership 已按当前 durable API 持久化并在 reopen 后复验，不以内存赋值作为证据；
  - Oracle 正式 schema 固定为 v3，recipe 固定为 `redis-8.8.1-linux-release-v3`；
  - artifact kind 只允许 regular 和受约束 symlink，symlink 目标必须相对、不逃出、无环，最终指向 regular file；
  - differential 在 verifier 监管的 rebuild-runtime lease 内运行，cleanup 成功后才发布 provenance。

- [ ] **步骤 4：提升 SDD 当前工作包**

  `.planning/SDD.md` 修改为：

  ```text
  current_work_package: WP8
  current_work_package_status: ready
  current_plan: docs/superpowers/plans/2026-08-07-vector-set-post-merge-remediation.md
  current_issue: 421
  current_pr: 422
  next_safe_action: create-wp8-storage-implementation-worktree
  ```

  WP8 块增加且只增加一行 `Implementation PR：[#422](https://github.com/arana-db/kiwi/pull/422)。`，状态改为 `ready`，current-state table 同步指向本总计划、Issue #421 和 PR #422。

- [ ] **步骤 5：验证并提交规划里程碑**

  ```powershell
  git diff --check
  rg -n "TO[D]O|TB[D]|待[定]|以后[补]|类似上[文]" `
    docs/superpowers/plans/2026-08-07-vector-set-post-merge-remediation.md `
    docs/superpowers/plans/2026-08-07-vector-set-storage-recovery.md `
    docs/superpowers/plans/2026-08-07-vector-set-runtime-protocol.md `
    docs/superpowers/plans/2026-08-07-vector-set-trusted-oracle-ci.md
  python scripts/validate_sdd.py --self-test
  python scripts/validate_sdd.py
  ```

  全部成功后使用 Lore 约定、`git commit -s`、`Co-authored-by: OmX <omx@oh-my-codex.dev>` 提交并 push 到 PR #422。

## 任务 1：建立 Storage 独立实现边界

- [ ] 从包含本计划的 clean 聚合分支 commit 创建 `codex/wp8-storage-recovery` 和 `D:\test\github\kiwi\.worktrees\wp8-storage-recovery`。
- [ ] 用 `scripts/codex-workstate.ps1` 建立 TaskId `wp8-vector-storage-recovery-implementation`，mode `implementation`，禁止 merge/rebase/reset/clean/resolve-comments。
- [ ] 把 SDD WP8 状态从 `ready` 改为 `in-progress`，不改 current plan/Issue/PR。
- [ ] 按 Storage 计划 Task 1→7 执行；每个 Task 的 RED、GREEN、规格复核、质量复核和 checkpoint 都完成后才进入下一 Task。
- [ ] 完成后将 Storage 分支提交顺序集成到聚合分支，在聚合分支重跑 Storage 计划的 changed-path 门禁后 push。

## 任务 2：建立 Runtime/Protocol 独立实现边界

- [ ] 从已集成 Storage 的 clean 聚合分支 commit 创建 `codex/wp8-runtime-protocol` 和 `D:\test\github\kiwi\.worktrees\wp8-runtime-protocol`。
- [ ] 用 `scripts/codex-workstate.ps1` 建立 TaskId `wp8-vector-runtime-protocol-implementation`。
- [ ] 按 Runtime/Protocol 计划 Task 1→5 执行，保留 Storage 工作流在 `VSimCmd::do_cmd()` 和 server startup 建立的合同。
- [ ] 完成后顺序集成、重跑 cmd/net/server/storage VSIM changed-path 门禁并 push。

## 任务 3：建立 Oracle/CI/Security 独立实现边界

- [ ] 从已集成 Runtime/Protocol 的 clean 聚合分支 commit 创建 `codex/wp8-trusted-oracle-ci` 和 `D:\test\github\kiwi\.worktrees\wp8-trusted-oracle-ci`。
- [ ] 用 `scripts/codex-workstate.ps1` 建立 TaskId `wp8-vector-trusted-oracle-ci-implementation`。
- [ ] 按 Oracle/CI 计划 Task 1→7 执行，先建立 Oracle v3 和 supervisor lease，再接 differential、cluster、capability 和 security gate。
- [ ] 完成后顺序集成，在 Ubuntu/WSL Linux 运行真实 Oracle 双构建、raw differential、cluster 和 rkyv sentinel，然后 push。

## 任务 4：聚合分支最终验证与 PR 对账

- [ ] **本地/WSL 基础门禁**

  ```powershell
  git diff --check
  python scripts/validate_sdd.py --self-test
  python scripts/validate_sdd.py
  wsl bash -lc 'cd /mnt/d/test/github/kiwi/.worktrees/vector-set-post-merge-remediation && cargo fmt --check'
  wsl bash -lc 'cd /mnt/d/test/github/kiwi/.worktrees/vector-set-post-merge-remediation && cargo clippy --all-targets --all-features -- -D warnings'
  wsl bash -lc 'cd /mnt/d/test/github/kiwi/.worktrees/vector-set-post-merge-remediation && cargo test --workspace'
  ```

- [ ] **Storage/Recovery 必需门禁**

  运行 Storage 计划列出的 manifest、migration、snapshot compatibility、snapshot install recovery、full Vector consistency、VSIM concurrency 和 exact Base/Head 矩阵。任一 fault phase 未执行、Base binary 未 reopen/read 或测试为零均失败。

- [ ] **Runtime/Protocol 必需门禁**

  运行 Runtime/Protocol 计划列出的 cmd admission、GatedCmd 转发、network storage spy、Config 传递、VADD typed outcome、RESP session isolation 和 raw frame 测试。删除 `Config.vector` 连接点、GatedCmd 转发或某个命令 admission 时对应测试必须失败。

- [ ] **Oracle/CI/Security 必需门禁**

  运行 Oracle/CI 计划列出的 schema mutation、process/security ignored tests、真实 Linux 双构建、artifact equality、runtime identity、raw differential、cluster、CI contract、cargo audit 和 rkyv sentinel。必须记录 collected/passed/skipped/xfail，required job 要求 collected > 0 且 skipped=0、xfail=0。

- [ ] **GitHub final-Head 复核**

  - 重新读取 PR #422 `headRefOid`、base、mergeable、mergeStateStatus、reviewDecision 和全部 checks；
  - 确认当前 Head 与本地验证提交一致；
  - 未完成全部 required Linux 门禁时保持 Draft；
  - 不 merge、不关闭 Issue、不 Resolve 旧评论。

- [ ] **更新 PR #422 描述**

  PR 描述必须列出 WP8、SDD baseline、Issue #421、Related #415/#418、`REQ-VECTOR-001..005`、所有实际命令和结果、Oracle provenance 产物、collected/skip 统计、未覆盖风险，并且在全部验收未完成前使用 `Refs #421` 而不是 `Fixes #421`。

## 完成定义

WP8 只在以下条件同时成立时可以从 `in-progress` 进入 `implemented`：

1. 三个工作流计划的所有复选项完成，每个 RED 均有未修复失败证据。
2. 真实 Base 目录/v1 snapshot、Oracle 双构建、raw differential 和三节点 cluster 都在 Linux 上非零执行。
3. 没有未处理 P0/P1，没有 required skip/xfail，没有幸存 Oracle/Kiwi/cluster 进程或未清理临时目录。
4. 聚合分支最终 Head 与 GitHub PR #422 Head 一致，指定 required checks 全部成功。
5. 只把状态改为 `implemented`；`verified`、`accepted` 和 Issue 关闭需要合并后 exact-main 验证和另行授权。
