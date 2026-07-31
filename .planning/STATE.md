# Kiwi 当前状态

> 更新时间：2026-07-31
>
> 当前 task 类型：implementation
>
> 当前 PR：`#406`（`codex/fix-resp-aggregate-allocation`）
>
> 实现基线：`main` at `cbc28958f261ae049d67a8b4a9d904d794b37726`
>
> 状态：独立 PR `#406` 已创建；零声明预分配修复已通过本地双平台验证和独立复审，最终 Head/checks 以 GitHub 实时查询为准
>
> 当前范围：拒绝超出 Redis 8.8.1 整数边界的 RESP 聚合长度，消除 Array/Map/Set/Push 的声明驱动预分配，并限制递归嵌套深度
>
> Requirement 边界：`REQ-COMPAT-002`、`REQ-COMPAT-006`、`REQ-WORK-003`

## 当前目标

本 task 修复 Issue #395 B1 中已经由源码确认的未认证 RESP 聚合类型无界预分配问题。客户端声明长度不得触发 `Vec` 预分配；超出 Redis 8.8.1 `INT_MAX` 边界的声明返回协议错误，合法聚合容器只随成功解析出的元素增长，聚合递归最多为 128 层。

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
- RESP 聚合长度上限采用 Redis 8.8.1 的 `INT_MAX`；合法声明采用零预分配，容量只随成功解析出的元素增长。
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
- TDD 红灯：原实现解析 `i64::MAX` 聚合头时发生 `capacity overflow`，回归断言失败。
- 首版 Windows/WSL `cargo test -p resp`：62 个单元测试、20 个集成测试通过。
- Windows 与 WSL `cargo clippy -p resp --all-targets -- -D warnings -D clippy::unwrap_used`：通过。
- `cargo fmt --all -- --check` 与 `git diff --check`：通过。
- 首版实现提交：`b10c85cd694032ae86f7a07a02d192142ab32d7f`；PR：`#404`。独立 review 前的远端 Head 为该提交，checks 当时仍在运行。
- 独立 review 在 `94694d81173ad9443f903bf44881efcbdaae4700` 发现：重复最大合法聚合头仍可叠加每层 1024 槽位的预分配；测试侧分配探针实测单个合法头申请 73,754 字节，128 层未完成头申请 9,440,512 字节，后者每增加 10 个单字节分片会累计申请约 94 MB。
- 分配 TDD 红灯：四种最大合法声明头的 allocation regression 在首个 Array 头以 73,754 字节失败；128 层 regression 以 9,440,512 字节失败。
- 分配 TDD 绿灯：四种聚合声明改为 `Vec::new()` 后，同一组 2 个 allocation regression 通过，容量只由成功解析元素的 `push` 增长。
- 深度 TDD 红灯：旧实现把 129 层完整 Array 嵌套解析为成功结果；exact 测试实际运行 1 个用例并按预期失败。
- 深度 TDD 绿灯：统一 128 层门禁后，同一 exact 测试实际运行 1 个用例并通过。
- 当前工作区 Windows 与 WSL `cargo test -p resp`：各 66 个单元测试、20 个集成测试通过；WSL 使用任务专属 Linux target。
- 当前工作区 Windows 与 WSL `cargo clippy -p resp --all-targets -- -D warnings -D clippy::unwrap_used`：通过。
- 当前工作区 `cargo fmt --all -- --check` 与 `git diff --check`：通过。
- 零声明预分配修复已完成独立规格复审：生产实现无 Critical/Important；测试阈值最初可能放过小容量预分配的 Minor 已改为预留输入 buffer 后严格断言解析阶段零分配，并由同一审查者确认闭环。
- 最终实现提交：`e82c4496484ee8d42694d950436b517bfe2669da`。原 PR `#404` 在本轮 push 前被外部更新为宽范围 Head `334a235a95c50ca1cdd71927e459a2c6ac5e5bb0`；未 force-push 覆盖，窄范围修复改由独立 PR `#406` 发布。
- checks、review threads 和 PR 状态不在本文件中缓存；任何当前结论必须重新查询 GitHub。

PR `#383` 的结果只证明 Oracle 规划闭环，不证明方案 A 已实现；PR `#388` 也不改变该结论。

## 下一条安全动作

1. 提交并 push PR #406 编号和分支状态对账。
2. push 后重新查询最终 Head 的 checks、评论和 review threads；不得把 #404 或旧 Head 的 CI 结果作为 #406 的最终状态。
3. 若最终 Head checks 未完成，只报告 pending，不给可 Merge 结论。
4. 不 Resolve 或回复 #402/#404/#406 review thread，不 merge PR。
5. PR `#383` 的规划历史保持不变，旧六文件 Oracle 草稿继续冻结。
6. 只有用户另开 Oracle provenance implementation task 后，才从包含方案 A 的 clean `main` 创建新 worktree、TaskId 和 recovery checkpoint，并先执行真实 Redis 双 checkout reproducibility 门禁。
7. Hot Tier 继续 Frozen；Gate PASS 后仍须用户明确批准一个单独的 implementation task。

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
gh pr list --repo arana-db/kiwi --head codex/fix-resp-aggregate-allocation --json number,state,baseRefName,headRefOid,statusCheckRollup,reviewDecision
```

如果 branch、HEAD、task type 或 dirty ownership 与 recovery 记录不同，先报告差异，不得自动 checkout、restore、reset、stash、clean 或覆盖文件。
