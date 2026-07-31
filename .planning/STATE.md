# Kiwi 当前状态

> 更新时间：2026-07-30
>
> 当前 task 类型：implementation
>
> 当前 PR：`#388`（`feat/rocksdb-build-accel-and-prd`）
>
> 已验证远端快照：Base `main` at `0f8d96238860a5c29a5582e461e4cdeb974431b3`；Head `b47cb1eebe098e2d4d2d784020dc283a8d026d28`
>
> 状态：PR `#388` 在 2026-07-30 复检时为 OPEN；本轮 review fix 已形成提交，发布状态、checks 和 review threads 必须实时查询
>
> 当前范围：修复 C/C++ sccache 的 Windows 编译器回归和重复 `target` 缓存，补充自动回归探针，并统一版本化 PRD、用户故事与项目状态
>
> Requirement 边界：`REQ-STABILITY-003`、`REQ-WORK-001`、`REQ-WORK-003`、`REQ-WORK-005`；跨平台构建修复只维护 G6 CI 的可执行性与证据条件，不构成任何稳定性 Gate 已通过的证据

## 当前目标

PR `#388` 是与 PR `#383` 规划任务分离的 implementation task。其既有范围是在 CI 与 `scripts/dev.sh` 中为 RocksDB C/C++ 编译接入 sccache，并新增由权威规划文档综合出的 PRD 与用户故事。本轮 review fix 处理 Windows compiler wrapper 回归、重复缓存、自动回归探针、文档 Requirement 映射、Hot Tier 授权边界和当前项目状态问题。

本 task 不实现 Redis Oracle provenance，不接受旧六文件 Oracle 草稿，也不启动 Embedded Redis Hot Tier。PR `#388` 的合并、CI 通过或 M7 Ready 均不能替代 Oracle implementation task 或 Hot Tier implementation task 的单独授权。

## 当前授权边界

允许：

- 在本轮 review fix 中修改 `.github/workflows/ci.yml`、`scripts/dev.sh`、`scripts/tests/test-dev-sccache-env.sh`、`docs/prd.md`、`docs/personas-and-user-stories.md`、`.planning/STATE.md` 和 `.planning/KANBAN.md`。
- 执行 Markdown、链接、Requirement/Decision ID、一致性和 Git diff 等只读检查。
- 记录已经发生并可验证的 PR/Base/Head 历史；瞬时 checks、threads 和 dirty ownership 继续实时查询。

禁止：

- 修改 Cargo、生产源码、Oracle provenance 实现、Hot Tier 实现或其他未授权路径。
- 在旧 `redis-8.8.1-stability-foundation` worktree 继续、暂存、提交、push、清理或回退六文件实现草稿。
- 把旧草稿的绿色测试、审查或真实构建准备表述为方案 A 已实现。
- 扩大到 Embedded Redis Hot Tier、Redis fork、动态库、loader、Cache ON 或组合发行实现。
- 在未获授权时 commit、push、merge、rebase、Resolve 或回复 GitHub review thread。

## 已确认决定

- PR `#383` 已于 2026-07-28 合并：final Head `42c16bef899385bd2e1b1e16e2e0202d4a614590`，merge commit `58030e1331655546ea4547a9a94efc493534ef7d`；它只完成 Oracle 方案 A 的规划闭环。
- PR `#388` 是独立 implementation task，不得继承或隐式扩大 PR `#383` 的 Oracle 实施授权。
- `D011`：Redis Oracle required provenance 采用 verifier fresh-checkout independent rebuild 和 exact binary hash equality。
- `D012`：规划 task 与实施 task 分离；规划批准不授权源码实现，提前产生的实现草稿冻结。
- Redis 8.8.1 tag `8.8.1` / commit `77b6c308396c9700672390a210143a8496fb4b10` 是唯一兼容和 Oracle 基线。
- Cache OFF、RocksDB 权威存储、OpenRaft 实现和 Embedded Redis Hot Tier 冻结边界均未改变。

## Oracle provenance 历史状态（不属于当前 task）

### Task 1：exact compatibility manifest

- PR：`#372`。
- Final Head：`6a692bc195f96327296296977a100af301deaf01`。
- Merge commit：`9e91707d774ad367d682e23677dcef79ecb14338`。
- 状态：已合并到 `main`。

### Task 2：trusted Oracle provenance

- 设计：已采用方案 A。
- 规划状态：方案 A 的 15 路径 planning-only Diff 已由 PR `#383` 合并；final Head `42c16bef899385bd2e1b1e16e2e0202d4a614590`，merge commit `58030e1331655546ea4547a9a94efc493534ef7d`。
- 实施状态：未在已接受边界中开始。
- 实施入口：`docs/superpowers/plans/2026-07-28-redis-8.8.1-trusted-oracle-provenance.md`。
- 启动条件：另开专用于 Oracle provenance 的 implementation task，从包含本规划的 clean commit 创建新 worktree，保存新 TaskId 和 recovery checkpoint；当前 PR `#388` 不满足或替代该条件。

## 冻结实现草稿

相关但不属于当前 PR `#388` dirty ownership：

```text
Worktree:
D:\test\github\kiwi\.worktrees\redis-8.8.1-stability-foundation

Branch:
codex/redis-8.8.1-stability-foundation

Frozen base HEAD:
2507a0c7b47707ef0f29ad360f51676976ffb483

Frozen recovery checkpoint:
D:\test\github\kiwi\.worktrees\redis-8.8.1-stability-foundation\.codex\recovery\checkpoints\20260728-152408-829-redis-8.8.1-m1-002-oracle-provenance.md
```

该 worktree 存在六文件未提交草稿。它没有被方案 A 接受，不能在本 task 处理。后续 implementation task 可以只读分析其中的测试思想，但所有复用必须对照 `D011`、`REQ-COMPAT-008`、`REQ-COMPAT-009` 和 `REQ-COMPAT-010` 重新审计。

## PR `#383` 规划交付（历史）

1. `D011`：独立重建信任模型。
2. `D012`：规划/实施 task 分离。
3. `REQ-COMPAT-008`、`REQ-COMPAT-009`、`REQ-COMPAT-010`：anti-splice、controlled bootstrap/tool、cleanup-before-publish。
4. `REQ-WORK-005`：规划与实施的恢复边界。
5. 可信 Oracle 详细设计规格。
6. 新 implementation task 可直接执行的 TDD 计划。
7. 旧总计划 Task 2 的 superseded 指针。
8. Compatibility contract、system boundary 和 stability gate 的 required 条件同步。

## 当前验证证据

- PR `#372` 的合并状态、final Head 和 `main` merge commit 已实时确认。
- PR `#383` 已合并；其规划提交只涉及 15 个 planning/docs 路径，并从 Task 1 合并后的 `main` 重放，未带入 Cargo、脚本、CI 或实现路径。
- 旧六文件草稿 worktree 已只读核对，并继续冻结。
- PR `#388` 的远端审查快照为 Base `0f8d96238860a5c29a5582e461e4cdeb974431b3`、Head `b47cb1eebe098e2d4d2d784020dc283a8d026d28`；本轮 review fix 已形成提交，当前发布状态须通过 GitHub 实时查询。
- `wsl.exe --cd /mnt/d/test/github/review/kiwi-pr-388/source -- bash scripts/tests/test-dev-sccache-env.sh`：7 个 Windows/Unix/compiler 场景全部 PASS。
- WSL Python/PyYAML 解析 `.github/workflows/ci.yml`：8 个 job 可解析，手写 `actions/cache` 的 `target` owner 为 0，compiler regression probe 恰有 1 个 CI step。
- 文档一致性探针：59 个使用中的 `REQ-*`、4 个 `D*` 均能在权威文件解析，Markdown 表格结构通过。
- `git diff --check`：通过；暂存区为空。当前环境没有 `shellcheck` 和 `actionlint`，未执行这两项。
- checks、review threads 和 PR 状态不在本文件中缓存；任何当前结论必须重新查询 GitHub。

PR `#383` 的结果只证明 Oracle 规划闭环，不证明方案 A 已实现；PR `#388` 也不改变该结论。

## 下一条安全动作

1. 对本轮七个 task-owned 路径执行 shell 回归、workflow YAML、Markdown、引用、Requirement/Decision ID、Git diff 和路径边界检查。
2. 本轮 push 已获得单独授权；发布后重新查询新 Head 的 checks。Resolve 或回复 review thread 仍须对应的单独授权，不得把已发布提交表述为 CI 已验证内容。
3. PR `#383` 的规划历史保持不变，旧六文件 Oracle 草稿继续冻结。
4. 只有用户另开 Oracle provenance implementation task 后，才从包含方案 A 的 clean `main` 创建新 worktree、TaskId 和 recovery checkpoint，并先执行真实 Redis 双 checkout reproducibility 门禁。
5. Hot Tier 继续 Frozen；Gate PASS 后仍须用户明确批准一个单独的 implementation task。

## 恢复检查

```powershell
Get-Content -Raw AGENTS.md
Get-Content -Raw .planning\PROJECT.md
Get-Content -Raw .planning\STATE.md
Get-Content -Raw .planning\KANBAN.md
Get-Content -Raw docs\prd.md
Get-Content -Raw docs\personas-and-user-stories.md
if (Test-Path .codex\recovery\ACTIVE.md) { Get-Content -Raw .codex\recovery\ACTIVE.md }
git status --porcelain=v2 --branch --untracked-files=all
git diff --cached --name-only
gh pr view 388 -R arana-db/kiwi --json state,baseRefOid,headRefOid,statusCheckRollup,reviewDecision
```

如果 branch、HEAD、task type 或 dirty ownership 与 recovery 记录不同，先报告差异，不得自动 checkout、restore、reset、stash、clean 或覆盖文件。
