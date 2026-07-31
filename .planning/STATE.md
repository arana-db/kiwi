# Kiwi 当前状态

> 更新时间：2026-07-31
>
> 当前 task 类型：implementation
>
> 当前 PR：待创建（`codex/fix-resp-parser-limits`）
>
> 实现基线：`main` at `cbc28958f261ae049d67a8b4a9d904d794b37726`
>
> 状态：设计已批准，独立 worktree 与恢复 checkpoint 已建立；正在执行 parser TDD
>
> 当前范围：拒绝超出 Redis 8.8.1 整数边界的 RESP 聚合长度，并限制 Array/Map/Set/Push 的初始预分配
>
> Requirement 边界：`REQ-COMPAT-002`、`REQ-COMPAT-006`、`REQ-WORK-003`

## 当前目标

本 task 修复 Issue #395 B1 中已经由源码确认的未认证 RESP 聚合类型无界预分配问题。客户端声明长度不得直接控制 `Vec` 的初始容量；超出 Redis 8.8.1 `INT_MAX` 边界的声明返回协议错误，合法声明的初始容量最多为 1024。

本 task 不处理实际流入的超大 bulk 或连接累计 buffer 限额，不修改 PR #402 的文档，不处理 Issue #395 的其他条目，也不实现 Redis Oracle provenance 或 Embedded Redis Hot Tier。

## 当前授权边界

允许：

- 修改 `src/resp/src/parse.rs`、本 task 的设计/计划、`.planning/STATE.md` 和 `.planning/KANBAN.md`。
- 运行 resp crate 单测、Clippy、格式检查、Git diff 检查及与 changed surface 对应的验证。
- commit、push 并创建以 `main` 为 base 的独立 PR；发布后实时查询 checks 和 review threads。

禁止：

- 修改 Cargo、网络/认证逻辑、其他生产源码、PR #402 文档、Oracle provenance 或 Hot Tier 实现。
- 在旧 `redis-8.8.1-stability-foundation` worktree 继续、暂存、提交、push、清理或回退六文件实现草稿。
- 把旧草稿的绿色测试、审查或真实构建准备表述为方案 A 已实现。
- 扩大到 Embedded Redis Hot Tier、Redis fork、动态库、loader、Cache ON 或组合发行实现。
- merge、rebase、Resolve 或回复 GitHub review thread。

## 已确认决定

- PR `#383` 已于 2026-07-28 合并：final Head `42c16bef899385bd2e1b1e16e2e0202d4a614590`，merge commit `58030e1331655546ea4547a9a94efc493534ef7d`；它只完成 Oracle 方案 A 的规划闭环。
- PR `#388` 已于 2026-07-30 合并；本 task 是从最新 `main` 创建的独立 implementation task。
- RESP 聚合长度上限采用 Redis 8.8.1 的 `INT_MAX`，初始预分配上限采用 1024。
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
- 启动条件：另开专用于 Oracle provenance 的 implementation task，从包含本规划的 clean commit 创建新 worktree，保存新 TaskId 和 recovery checkpoint；当前 RESP parser task 不满足或替代该条件。

## 冻结实现草稿

相关但不属于当前 RESP parser task dirty ownership：

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
- PR `#388` 已于 2026-07-30 合并，final Head `1ee8c916a55d03d02a250ed95af83712fa14a742`。
- 本 task 已确认 `RespParse` 在认证前可达；Array/Map/Set/Push 均把未受信任的 `i64` 长度直接传给 `Vec::with_capacity`。
- Redis 8.8.1 exact tag 的 multibulk parser 拒绝大于 `INT_MAX` 的声明值，并把初始 argv 分配限制为 1024。
- `wsl.exe --cd /mnt/d/test/github/review/kiwi-pr-388/source -- bash scripts/tests/test-dev-sccache-env.sh`：7 个 Windows/Unix/compiler 场景全部 PASS。
- WSL Python/PyYAML 解析 `.github/workflows/ci.yml`：8 个 job 可解析，手写 `actions/cache` 的 `target` owner 为 0，compiler regression probe 恰有 1 个 CI step。
- 文档一致性探针：59 个使用中的 `REQ-*`、4 个 `D*` 均能在权威文件解析，Markdown 表格结构通过。
- `git diff --check`：通过；暂存区为空。当前环境没有 `shellcheck` 和 `actionlint`，未执行这两项。
- checks、review threads 和 PR 状态不在本文件中缓存；任何当前结论必须重新查询 GitHub。

PR `#383` 的结果只证明 Oracle 规划闭环，不证明方案 A 已实现；PR `#388` 也不改变该结论。

## 下一条安全动作

1. 先运行超限 frame 回归并保留预期红灯，再实现统一容量 helper。
2. 运行 `cargo test -p resp`、目标 Clippy、`cargo fmt --check` 和 `git diff --check`。
3. 本轮 push 和创建独立 PR 已获得授权；发布后重新查询新 Head 的 checks。不得把未完成的 CI 表述为通过。
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
gh pr list --repo arana-db/kiwi --head codex/fix-resp-parser-limits --json number,state,baseRefName,headRefOid,statusCheckRollup,reviewDecision
```

如果 branch、HEAD、task type 或 dirty ownership 与 recovery 记录不同，先报告差异，不得自动 checkout、restore、reset、stash、clean 或覆盖文件。
