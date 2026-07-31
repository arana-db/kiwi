# Kiwi 当前状态

> 更新时间：2026-07-31
>
> 当前 task 类型：implementation
>
> 当前 PR：`#404`（`codex/fix-resp-parser-limits`）
>
> 最新集成基线：`main` at `ed49ab4c3c362ba77111dbcd9791f93ebfce74a4`
>
> 状态：PR #402、#403、#405、#406 已合并；PR #404 的最新 base 冲突已在独立 v4 worktree 中解决并完成验证，远端发布与 CI 状态必须从 PR 实时查询
>
> 当前范围：在 #406 的零声明驱动分配合同上保留 #404 的 RESP 首行、payload、buffer、递归、节点和重复解析工作预算；保留未认证连接 buffer、parser 历史副本清理和 optional pipeline 背压，并验证 #403 的 admission/response 共享 30 秒总预算实现已接入真实 `submit_command` 入口
>
> Requirement 边界：`REQ-COMPAT-002`、`REQ-COMPAT-006`、`REQ-WORK-003`

## 当前目标

本 task 修复 Issue #395 B1 中已经由源码确认的未认证 RESP 资源耗尽路径，并处理 Issue #398 的 optional pipeline 无界 channel。客户端声明长度不得直接控制分配；解析器和未认证连接具有内存、深度、对象数与累计工作预算；`command_queue_size` 形成真实背压。

PR #406 已定义并合入零声明驱动分配合同：聚合容器从空向量开始，容量只能由已经成功解析的元素触发。PR #404 只能扩展该合同，不能重新引入按声明长度保留容量。

PR #403 已合入 bounded pipeline 和共享 deadline helper。本轮保留其实现，并补充真实 `submit_command` 回归，避免冲突误选旧入口后 helper 测试仍假绿。

经调用链复核，Issue #395 的 `DEL`、`MSETNX` 和 expiration P0 描述与当前 executor gate、单写者 Raft apply 及 no-op compaction 不符。本 PR 不添加可能与 Raft apply 死锁的无效 record lock，也不修改 PR #402 的文档、Redis Oracle provenance 或 Embedded Redis Hot Tier。

## 当前授权边界

允许：

- 在 v4 worktree 修改 `src/resp/src/parse.rs`、`src/net/src/pipeline.rs`、`src/net/Cargo.toml`、本 task 的设计/计划、`.planning/STATE.md` 和 `.planning/KANBAN.md`。
- 在未提交状态集成最新 `origin/main`，运行 resp/net 定向测试、workspace 关联测试、Clippy、格式检查、Git diff 检查和 WSL/Linux TCP 验证。

禁止：

- 未获得单独授权时 commit、push、force-push、merge PR、Resolve 或回复 GitHub review thread。
- 修改 storage/Raft/命令语义、PR #402/#403/#405 文档、Oracle provenance 或 Hot Tier 实现。
- 在旧 `redis-8.8.1-stability-foundation` worktree 继续、暂存、提交、push、清理或回退六文件实现草稿。
- 把旧草稿的绿色测试、审查或真实构建准备表述为方案 A 已实现。
- 扩大到 Embedded Redis Hot Tier、Redis fork、动态库、loader、Cache ON 或组合发行实现。
- merge、rebase、Resolve 或回复 GitHub review thread。

## 已确认决定

- PR `#383` 已于 2026-07-28 合并：final Head `42c16bef899385bd2e1b1e16e2e0202d4a614590`，merge commit `58030e1331655546ea4547a9a94efc493534ef7d`；它只完成 Oracle 方案 A 的规划闭环。
- PR `#388` 已于 2026-07-30 合并；本 task 是从最新 `main` 创建的独立 implementation task。
- PR `#402`、`#403`、`#405`、`#406` 已合并；本 task 的最新组合基线为 `ed49ab4c3c362ba77111dbcd9791f93ebfce74a4`。
- RESP 聚合长度上限采用 Redis 8.8.1 的 `INT_MAX`；合法声明采用零预分配，容量只随成功解析出的元素增长。
- 所有 RESP 首行上限为 64 KiB，bulk payload 上限为 512 MiB，通用 parser buffer 上限为 1 GiB，未认证连接 buffer 上限为 1 MiB。
- 单 frame 最多物化 65,536 个 RESP 节点；分片 aggregate 重放最多累计 1,000,000 次节点访问。
- optional pipeline 使用容量至少为 1 的 bounded Tokio channel，queue admission 与 response 共享一个 30 秒总 deadline。
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
- 零声明预分配修复已完成独立规格复审：生产实现无 Critical/Important；测试阈值最初可能放过小容量预分配的 Minor 已改为预留输入 buffer 后严格断言解析阶段零分配，并由同一审查者确认闭环。
- 最终实现提交：`e82c4496484ee8d42694d950436b517bfe2669da`。原 PR `#404` 在本轮 push 前被外部更新为宽范围 Head `334a235a95c50ca1cdd71927e459a2c6ac5e5bb0`；未 force-push 覆盖，窄范围修复改由独立 PR `#406` 发布。
- PR `#406` 已于 2026-07-31 合并，final Head `236073fcd499b57c7d808da7b904341551b40bce`，merge commit `7886fee82bf5f95f2291c4e2e4720248f56c0211`。
- PR `#402` 和 `#405` 随后合并，产生上一组合基线 `cfda82939afc5c332f3cdeaea2072be37c38177c`。
- PR `#403` 于 2026-07-31 合并，merge commit `ed49ab4c3c362ba77111dbcd9791f93ebfce74a4`；它已实现 bounded pipeline 和单一外层 timeout，并新增虚拟时间回归。
- PR #404 原 Head `334a235a95c50ca1cdd71927e459a2c6ac5e5bb0` 的 clean Windows MSVC 基线：`resp` 71 unit + 20 integration，`net --lib` 32/32，通过且 0 failed。
- 广义审计新增 3 个 Important：非 inline 首行绕过、分片 aggregate O(N²) 重放、协议 count 与实际对象预算混淆；PR #404 已通过统一首行、65,536 节点和 1,000,000 node-visits 预算修复。
- PR #404 旧 Head 的 review 线程指出 admission 和 response 使用两个独立 30 秒 timeout，最坏总等待接近 60 秒；PR #403 已在最新 main 中改为共享 deadline，本轮额外覆盖真实 `submit_command` 接线。
- v2 草稿完成两项红灯证据：大声明在相同已解析元素数下额外分配 73,728 字节；真实 `submit_command` 在虚拟时间 31 秒仍未完成。v2 继续冻结，不作为最终组合 Head 证据。
- v3 曾合并旧基线 `cfda8293`，但在 #403 合并后已冻结；v4 从 PR #404 Head 重新合并 `ed49ab4c`，并在该组合源码上完成 Windows/WSL 验证。
- v4 clean Head Windows 基线：`resp` 71 unit + 20 integration、`net --lib` 32/32，均为 0 failed。
- v4 冲突专项 Windows exact：parser 声明长度无关分配、真实 `submit_command` 共享 30 秒 deadline、helper 共享 deadline 和 bounded channel 满载测试均实际运行 1 个测试并通过。
- v4 最终 Windows MSVC：`resp` 80 unit + 20 integration + doc tests、`net --lib` 35/35、严格 Clippy 和最终真实 `submit_command` exact 回归全部通过；Clippy 的唯一额外输出是其明确不受 `-D warnings` 控制的 MSVC `linker_messages`。
- v4 最终 WSL/Linux：`resp` 80 unit + 20 integration + doc tests、`net --lib` 35/35、两个 TCP exact 回归和严格 Clippy 全部通过；两个 TCP 命令均实际输出 `running 1 test`。
- v4 已清除全部 conflict marker，并暂存 6 个原冲突路径以完成索引解析；`cargo fmt --all -- --check` 和 staged diff check 通过。本条验证记录形成时尚未发布，当前 commit/push 状态必须从 Git 和 PR #404 实时查询。
- checks、review threads 和 PR 状态不在本文件中缓存；任何当前结论必须重新查询 GitHub。

PR `#383` 的结果只证明 Oracle 规划闭环，不证明方案 A 已实现；PR `#388` 也不改变该结论。

## 下一条安全动作

1. 发布前确认没有 conflict marker、未合并索引项或 whitespace error，并核对远端 PR #404 Head 与 `origin/main` 租约未漂移。
2. 只使用普通 fast-forward push 发布同一已验证组合 Head，不使用 force-push 覆盖远端变更。
3. 等待并复核新 Head CI、GitHub merge 状态和 review thread；CI 失败时先区分 PR 回归、Base 基线与环境问题。
4. 只有评论对应问题已在新 Head 上修复并具备验证证据时才 Resolve；不自动 merge PR #404。
5. PR `#383` 的规划历史保持不变，旧六文件 Oracle 草稿以及 v2/v3 过期 merge worktree 继续冻结。
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
gh pr view 404 --repo arana-db/kiwi --json state,baseRefName,headRefName,headRefOid,statusCheckRollup,reviewDecision,mergeable,mergeStateStatus
```

如果 branch、HEAD、task type 或 dirty ownership 与 recovery 记录不同，先报告差异，不得自动 checkout、restore、reset、stash、clean 或覆盖文件。
