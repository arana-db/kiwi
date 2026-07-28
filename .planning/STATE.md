# Kiwi 当前状态

> 更新时间：2026-07-28
>
> 当前 task 类型：planning-only
>
> 规划 PR：`#383`（`codex/redis-8.8.1-oracle-provenance-plan`）
>
> 规划 Base：`main` at `9e91707d774ad367d682e23677dcef79ecb14338`
>
> 状态：Task 1 已由 PR `#372` 合并；方案 A 已由规划 PR `#383` 固化；恢复时必须实时确认该 PR 是否已合并，implementation 只能在规划合并后另开 Codex task
>
> 当前设计：`docs/superpowers/specs/2026-07-28-redis-8.8.1-trusted-oracle-provenance-design.md`
>
> 当前实施计划：`docs/superpowers/plans/2026-07-28-redis-8.8.1-trusted-oracle-provenance.md`

## 当前目标

本 task 只把 Redis 8.8.1 可信 Oracle provenance 的方案 A 固化到项目文档：primary build 产生候选和审计 metadata，verifier 在 fresh disposable exact checkout 中独立重建，要求 primary/rebuild binary SHA-256 完全一致，只运行 rebuild binary 取得正式 `INFO server` evidence，并在全部 cleanup 成功后发布 provenance v2。

本 task 不继续任何 Rust、test、Bash、PowerShell 或 CI 实现。Kiwi 底层兼容、RocksDB 和 OpenRaft 工作按既有 Roadmap 保持原节奏，方案 A 只替换 `M1-001` Task 2 的 provenance 信任模型。

## 当前授权边界

允许：

- 更新 `AGENTS.md` 实际指向的 `CLAUDE.md`、`.planning/`、兼容/系统边界/稳定门禁合同、设计规格和实施计划。
- 执行 Markdown、链接、Requirement/Decision ID、一致性和 Git diff 检查。
- 在独立 planning branch 保存、提交和发布纯规划成果；不得带入实现草稿。

禁止：

- 修改 `tools/compat`、`scripts/compat`、Cargo、CI 或生产源码。
- 在旧 `redis-8.8.1-stability-foundation` worktree 继续、暂存、提交、push、清理或回退六文件实现草稿。
- 把旧草稿的绿色测试、审查或真实构建准备表述为方案 A 已实现。
- 扩大到 Embedded Redis Hot Tier、Redis fork、动态库、loader、Cache ON 或组合发行实现。
- 在本 task 从 planning mode 静默切换到 implementation mode。

## 已确认决定

- `D011`：Redis Oracle required provenance 采用 verifier fresh-checkout independent rebuild 和 exact binary hash equality。
- `D012`：规划 task 与实施 task 分离；规划批准不授权源码实现，提前产生的实现草稿冻结。
- Redis 8.8.1 tag `8.8.1` / commit `77b6c308396c9700672390a210143a8496fb4b10` 是唯一兼容和 Oracle 基线。
- Cache OFF、RocksDB 权威存储、OpenRaft 实现和 Embedded Redis Hot Tier 冻结边界均未改变。

## M1-001 当前分解

### Task 1：exact compatibility manifest

- PR：`#372`。
- Final Head：`6a692bc195f96327296296977a100af301deaf01`。
- Merge commit：`9e91707d774ad367d682e23677dcef79ecb14338`。
- 状态：已合并到 `main`。

### Task 2：trusted Oracle provenance

- 设计：已采用方案 A。
- 规划状态：方案 A 的 15 路径 planning-only Diff 由 PR `#383` 发布；本文件不缓存其易漂移的 Head、checks 或 merge 状态，恢复时必须实时查询。
- 实施状态：未在已接受边界中开始。
- 实施入口：`docs/superpowers/plans/2026-07-28-redis-8.8.1-trusted-oracle-provenance.md`。
- 启动条件：PR `#383` 合并后另开 Codex task，从包含本规划的 clean commit 创建新 worktree，保存新 TaskId 和 recovery checkpoint。

## 冻结实现草稿

相关但不属于本 planning task dirty ownership：

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

## 本规划 task 的交付

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
- PR `#383` 的规划提交只涉及 15 个 planning/docs 路径，并从 Task 1 合并后的 `main` 重放；禁止带入 Cargo、脚本、CI 或实现路径。
- 旧六文件草稿 worktree 已只读核对，并继续冻结。
- 本规划 PR 只执行与文档风险相匹配的 Diff、链接、ID、Markdown 和 GitHub 状态检查；不运行 Kiwi、RocksDB 或 Redis 编译测试。

这些结果只证明规划闭环，不证明方案 A 已实现。

## 下一条安全动作

1. 实时查询 PR `#383` 的 state、Head、Base、checks、review threads 和文件列表，不从本文件推断 GitHub 当前状态。
2. 若 PR `#383` 仍为 OPEN，保持 Task 2 六文件草稿冻结；只有用户另行明确授权后才能 merge。
3. 若 PR `#383` 已 MERGED，确认 `main` 包含本规划，并继续保持旧六文件草稿冻结。
4. 只有用户另开 implementation task 后，才从包含本规划的 clean `main` 创建新 worktree、TaskId 和 recovery checkpoint。
5. 新 implementation task 先执行真实 Redis 双 checkout reproducibility 门禁；门禁通过前不复用或提交旧草稿。

## 恢复检查

```powershell
Get-Content -Raw AGENTS.md
Get-Content -Raw .planning\PROJECT.md
Get-Content -Raw .planning\STATE.md
Get-Content -Raw .planning\KANBAN.md
Get-Content -Raw docs\superpowers\specs\2026-07-28-redis-8.8.1-trusted-oracle-provenance-design.md
Get-Content -Raw docs\superpowers\plans\2026-07-28-redis-8.8.1-trusted-oracle-provenance.md
if (Test-Path .codex\recovery\ACTIVE.md) { Get-Content -Raw .codex\recovery\ACTIVE.md }
git status --porcelain=v2 --branch --untracked-files=all
git diff --cached --name-only
```

如果 branch、HEAD、task type 或 dirty ownership 与 recovery 记录不同，先报告差异，不得自动 checkout、restore、reset、stash、clean 或覆盖文件。
