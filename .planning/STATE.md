# Kiwi 当前状态

> 更新时间：2026-07-28
>
> 发布分支：`codex/redis-8.8.1-doc-foundation`
>
> 发布 Base：`main`（PR #371）
>
> 状态：文档基线由 PR #371 发布；`M1-001` 已获授权并处于 In Progress
>
> 当前设计：`docs/superpowers/specs/2026-07-26-redis-8.8.1-stability-first-design.md`
>
> 当前实现计划：`docs/superpowers/plans/2026-07-26-redis-8.8.1-stability-foundation.md`

## 当前目标

通过 PR #371 发布 Redis 8.8.1 的项目基线、兼容合同、系统边界、许可证方案、未来动态库 ABI 和稳定性门禁，并在恢复时实时确认该 PR 与 `main` 的当前关系；同时只在独立工作边界内推进已获授权的 `M1-001`，以 Cache OFF 为唯一 required 运行模式建立 exact Oracle、机器可读 manifest 和 provenance。

Embedded Redis Hot Tier 当前只做文档、方案和接口设计。系统稳定门禁通过且用户重新明确批准之前，不实现、不编译、不接入、不加载，也不进入生产依赖图。

## 当前实施授权边界

允许：

- 在 PR #371 范围内维护项目宪法、需求、决定、Roadmap、State、Kanban 和已批准计划，并完成该文档基线的审查修正。
- 在独立工作树 `codex/redis-8.8.1-stability-foundation` 内执行 `M1-001` 的 Task 1 和 Task 2：Redis 8.8.1 exact manifest、Oracle provenance、对应的 compatibility 测试与受控验证脚本。
- 运行与上述范围直接对应的测试、Clippy、格式检查、脚本语法检查和真实 WSL/Linux Oracle 构建验证，并保存 recovery checkpoint。
- 按当前实现任务的显式授权和 `.codex/recovery/` 记录执行暂存、提交或 push；版本化状态文件不扩大 Git 权限边界。

禁止：

- 在 `M1-001` 验收前启动 `M1-002` 或其他后续实现卡。
- 修改 Kiwi 生产源码、生产依赖、通用构建路径或 CI 行为；`M1-001` 只允许 compatibility 工具、测试、manifest 和受控 Oracle 脚本。
- 创建 Redis 生产 fork 改动、动态库实现、FFI binding、loader 或 Cache ON 路径。
- 覆盖、续写、清理或回退此前冻结的实现工作树。

PR #371 是当前文档基线的远端发布记录。PR #372 基于该文档分支，只包含 `M1-001` Task 1；截至 2026-07-28，它为 Draft 且尚未合并。恢复工作时必须实时复检两个 PR 的 state、base、Head 和 merge 状态，不能把本文件中的快照当作 GitHub 当前状态。Task 2 已启动但尚未提交、尚未验收。各工作树的精确 branch、HEAD、index 和 dirty ownership 只以对应 `.codex/recovery/` 的当前记录为准，不在版本化状态文件中复制易漂移的本机路径清单。

## 已确认决定

- Redis 8.8.1 是唯一当前 Redis 兼容、接口设计、测试 Oracle 和未来热层源码基线。
- 当前主线固定为 Cache OFF；热层不得成为兼容、持久化或 Raft 正确性的前置条件。
- RocksDB 是全量、权威、可恢复的数据真相层。
- OpenRaft 是 Kiwi 的共识实现；RedisRaft 只定义公开行为参考。
- redis-rs 只作为测试客户端，不进入生产 server 依赖图。
- Kiwi 自有源码维持 Apache-2.0；未来官方组合发行物履行适用的 AGPL-3.0-only 义务。
- 热层生产实现必须同时满足系统稳定门禁和用户重新明确批准，自动化通过不能替代授权。

## 本轮已验证的文档成果

1. 统一 Redis 8.8.1 exact tag、commit 和可观察语义口径。
2. 将开发顺序调整为 Cache OFF 兼容 → RocksDB 正确性 → OpenRaft 稳定与故障证明 → 系统稳定门禁。
3. 记录组合发行许可证、Corresponding Source 和 Redis fork provenance 方案。
4. 设计未来动态库的版本化 C ABI、安全加载、pairing manifest 和失效不变量。
5. 建立 `docs/quality/system-stability-gate.md`，把热层实现变成门禁后的延期里程碑。
6. 保留此前冻结实现工作树及其 Git/验证证据，不覆盖、不续写，也不把其中改动混入当前文档任务。

上述内容以 PR #371 作为远端发布记录；恢复时先查询该 PR 的实时状态，并确认 `main` 是否已经包含对应文档基线。PR 之外的本机运行状态继续保存在被忽略的 `.codex/recovery/`。

## M1-001 当前进度

- `M1-001` 已获得实现授权并处于 In Progress，不得再恢复为“等待授权”。
- Task 1 已提交至 PR #372；截至 2026-07-28，该 PR 为 Draft，Head 为 `2507a0c7b47707ef0f29ad360f51676976ffb483`，尚未合并；恢复时必须实时复检。
- Task 2 已在对应隔离工作树启动，但仍是未提交、未验收的本机工作；不得把绿色局部测试、未提交 Diff 或 PR #372 的 Task 1 状态表述为 Task 2 或 `M1-001` 完成。
- Task 2 的精确 dirty paths、验证结果和剩余问题以该工作树对应的 `.codex/recovery/ACTIVE.md` 与 checkpoint 为准。

## 本轮验证证据

- 21 个当前入口、项目真相、设计、计划和门禁文档完成一致性检查。
- 58 个 Requirement ID 唯一；当前文档引用未发现未知 ID。
- 10 个 Decision ID 唯一；当前设计引用未发现失效 ID。
- 当前实施计划包含 8 个有序任务，热层生产实现不在计划范围内。
- 当前文档的本地 Markdown 链接、代码围栏和行尾空白检查通过。
- 文档候选集执行 `git diff --check` exit 0；验证时 Git index 为空。
- 发布工作树最终重放到 `main` commit `3c55165d1c76f656b4a4d9f576e5ed7d2274086e`，发布分支为 `codex/redis-8.8.1-doc-foundation`；PR Head 由 Git 历史和 GitHub 实时状态记录。

## 当前实施顺序

文档基线以 PR #371 作为发布记录；`M1-001` 已在独立工作边界内启动。后续工作仍按以下顺序推进，同一时间不得把下一张实现卡隐式转为 In Progress：

1. `M1-001`：Redis 8.8.1 exact Oracle、manifest 和 provenance（In Progress；Task 1 在 Draft PR #372，Task 2 尚未验收）。
2. `M1-002`：RESP2/RESP3 持久连接级 raw wire differential。
3. `M2-001`：RocksDB authority/durability contract。
4. `M2-002`：审计并扩展现有 close/reopen 回归，补齐全部 handle、TTL/metadata、Snapshot 和故障恢复门禁。
5. `M4-001`：`kiwi_redisraft_public_v1` 机器可读合同。
6. `M4-002`：OpenRaft deterministic simulator、现有 LogStore reopen 缺口与单 Group 正确性。
7. `M5-001`：3/5 节点进程、网络、磁盘故障和 Elle/Jepsen history。
8. `M6-001`：系统稳定 Gate Review。

在第 8 项通过并取得用户新授权之前，不启动任何 M7/M8 实现卡。

## 本机工作树与恢复边界

每个实现工作树的精确路径、分支、HEAD、index、dirty paths、验证证据和授权边界以该工作树自己的 `.codex/recovery/ACTIVE.md` 为准。版本化 `.planning/` 只记录项目级里程碑和已验收结果，不替代本机恢复记录。

恢复或继续工作时：

- 先核对任务身份、工作树、branch、HEAD、index 和 dirty ownership；任何漂移都必须停止写操作并报告。
- 不跨工作树混入 PR #371 文档修正、PR #372 Task 1 或尚未验收的 Task 2 改动。
- 不暂存、提交、清理、回退或删除不属于当前任务授权的改动。
- 不把 PR、未提交 Diff、局部测试或规格审查表述为 `M1-001` 已完成；只有任务要求的真实构建、验证和验收全部闭环后才能更新为 Done。

## 下一条安全动作

1. 实时查询 PR #371：若为 OPEN，完成修复和复审并等待明确 merge 授权；若为 MERGED，确认 `main` 已包含对应文档基线；若为 CLOSED 且未合并，停止并报告新的发布边界。
2. 实时查询 PR #372 的 state、base、Head、Draft 和 merge 状态，并与 `M1-001` recovery checkpoint 对账。
3. 继续在 `M1-001` 的既有隔离工作树内修复和验证 Task 2；以对应 `.codex/recovery/` 恢复精确本机状态，不从本文件推断 dirty ownership。
4. Task 2 通过代码质量复核、完整测试和真实 WSL/Linux Oracle 构建验证后，再按授权提交到 PR #372 并复审；在 `M1-001` 验收前不启动 `M1-002`。
5. 不启动任何 Embedded Redis Hot Tier 实现卡。

## 恢复检查

```powershell
Get-Content -Raw AGENTS.md
Get-Content -Raw .planning\PROJECT.md
Get-Content -Raw .planning\STATE.md
Get-Content -Raw .planning\KANBAN.md
Get-Content -Raw docs\quality\system-stability-gate.md
if (Test-Path .codex\recovery\ACTIVE.md) { Get-Content -Raw .codex\recovery\ACTIVE.md }
git status --porcelain=v2 --branch --untracked-files=all
git diff --cached --name-only
```

如果 branch、HEAD 或 dirty ownership 与恢复记录不同，先报告差异，不得自动 checkout、restore、reset、stash、clean 或覆盖文件。
