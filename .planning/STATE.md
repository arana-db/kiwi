# Kiwi 当前状态

> 更新时间：2026-07-31
>
> 当前 task 类型：implementation
>
> 当前 PR：`#404`（`codex/fix-resp-parser-limits`）
>
> 实现基线：`main` at `cbc28958f261ae049d67a8b4a9d904d794b37726`
>
> 状态：PR 已创建；嵌套深度及广义资源边界缺口已在本地修复并验证，待发布后复检最终 Head/checks
>
> 当前范围：限制 RESP 首行、payload、buffer、递归、解码节点和重复解析工作；限制未认证连接 buffer；清理 parser 历史副本；为 optional pipeline 建立真实背压
>
> Requirement 边界：`REQ-COMPAT-002`、`REQ-COMPAT-006`、`REQ-WORK-003`

## 当前目标

本 task 修复 Issue #395 B1 中已经由源码确认的未认证 RESP 资源耗尽路径，并处理 Issue #398 的 optional pipeline 无界 channel。客户端声明长度不得直接控制分配；解析器和未认证连接具有内存、深度、对象数与累计工作预算；`command_queue_size` 形成真实背压。

经调用链复核，Issue #395 的 `DEL`、`MSETNX` 和 expiration P0 描述与当前 executor gate、单写者 Raft apply 及 no-op compaction 不符。本 PR 不添加可能与 Raft apply 死锁的无效 record lock，也不修改 PR #402 的文档、Redis Oracle provenance 或 Embedded Redis Hot Tier。

## 当前授权边界

允许：

- 修改 `src/resp/src/parse.rs`、`src/net` 下 parser consumer、pipeline 和定向测试、本 task 的设计/计划、`.planning/STATE.md` 和 `.planning/KANBAN.md`。
- 运行 resp/net 定向测试、workspace 测试、Clippy、格式检查、Git diff 检查和 WSL/Linux TCP 验证。
- commit 并 fast-forward push 到现有 PR #404 的 head 分支；发布后实时查询 checks 和 review threads。

禁止：

- 修改 Cargo、storage/Raft/命令语义、PR #402 文档、Oracle provenance 或 Hot Tier 实现。
- 在旧 `redis-8.8.1-stability-foundation` worktree 继续、暂存、提交、push、清理或回退六文件实现草稿。
- 把旧草稿的绿色测试、审查或真实构建准备表述为方案 A 已实现。
- 扩大到 Embedded Redis Hot Tier、Redis fork、动态库、loader、Cache ON 或组合发行实现。
- merge、rebase、Resolve 或回复 GitHub review thread。

## 已确认决定

- PR `#383` 已于 2026-07-28 合并：final Head `42c16bef899385bd2e1b1e16e2e0202d4a614590`，merge commit `58030e1331655546ea4547a9a94efc493534ef7d`；它只完成 Oracle 方案 A 的规划闭环。
- PR `#388` 已于 2026-07-30 合并；本 task 是从最新 `main` 创建的独立 implementation task。
- RESP 聚合长度上限采用 Redis 8.8.1 的 `INT_MAX`，初始预分配上限采用 1024。
- 所有 RESP 首行上限为 64 KiB，bulk payload 上限为 512 MiB，通用 parser buffer 上限为 1 GiB，未认证连接 buffer 上限为 1 MiB。
- 单 frame 最多物化 65,536 个 RESP 节点；分片 aggregate 重放最多累计 1,000,000 次节点访问。
- optional pipeline 使用容量至少为 1 的 bounded Tokio channel，queue admission 纳入现有 30 秒 timeout。
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
- 独立 review 在该 Head 发现：重复最大合法聚合头仍可叠加每层 1024 槽位的预分配并持续增长调用栈；原最大合法长度测试也未直接证明 1024 容量上限。
- 深度 TDD 红灯：旧实现把 129 层完整 Array 嵌套解析为成功结果；exact 测试实际运行 1 个用例并按预期失败。
- 深度 TDD 绿灯：统一 128 层门禁后，同一 exact 测试实际运行 1 个用例并通过。
- 最终工作区 Windows 与 WSL `cargo test -p resp`：各 65 个单元测试、20 个集成测试通过；WSL 使用 Rust/Cargo 1.97.1 和任务专属 Linux target。
- 最终工作区 Windows 与 WSL `cargo clippy -p resp --all-targets -- -D warnings -D clippy::unwrap_used`：通过。
- 最终工作区 `cargo fmt --all -- --check` 与 `git diff --check`：通过。
- 二次独立 review 未发现 Critical、Important 或 Minor；确认 128/129 深度边界、四类聚合错误传播和容量 helper 测试与设计一致。
- 后续广义审计新增 3 个 Important：非 inline 首行绕过、分片 aggregate O(N²) 重放、协议 count 与实际对象预算混淆；均已通过统一首行、65,536 节点和 1,000,000 node-visits 预算修复。
- 最终 consolidation worktree Windows `cargo test -p resp --all-features --locked`：71 unit + 20 integration + doc tests 全部通过。
- 最终 consolidation worktree Windows `cargo test -p net --lib --all-features --locked`：32/32 通过；严格 resp/net Clippy 以 `-D warnings -D clippy::unwrap_used` 通过。
- WSL/Linux RESP 结果同为 71 + 20 + doc tests；新增未认证超限 TCP 与既有 protocol-error 控制用例均为 1/1 通过。
- Windows workspace 已运行；到达的 unit suites 全部通过，19 个 TCP 用例统一复现仓库基线 `server did not become connectable`，不作为本分支回归。
- checks、review threads 和 PR 状态不在本文件中缓存；任何当前结论必须重新查询 GitHub。

PR `#383` 的结果只证明 Oracle 规划闭环，不证明方案 A 已实现；PR `#388` 也不改变该结论。

## 下一条安全动作

1. 提交并 fast-forward push PR #404 的 consolidation 修复后，重新查询最终 Head 的 checks、评论和 review threads；不得把旧 Head 的 CI 结果作为最终状态。
2. 若最终 Head checks 未完成，只报告 pending，不给可 Merge 结论。
3. 不 Resolve 或回复 #402/#404 review thread，不 merge PR。
4. PR `#383` 的规划历史保持不变，旧六文件 Oracle 草稿继续冻结。
5. 只有用户另开 Oracle provenance implementation task 后，才从包含方案 A 的 clean `main` 创建新 worktree、TaskId 和 recovery checkpoint，并先执行真实 Redis 双 checkout reproducibility 门禁。
6. Hot Tier 继续 Frozen；Gate PASS 后仍须用户明确批准一个单独的 implementation task。

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
