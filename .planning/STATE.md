# Kiwi 当前状态

> 更新时间：2026-07-26
>
> 发布分支：`codex/redis-8.8.1-doc-foundation`
>
> 发布 Base：`main` at `3c55165d1c76f656b4a4d9f576e5ed7d2274086e`
>
> 状态：M0 文档与架构基线已验证；等待 Cache OFF 实现授权
>
> 当前设计：`docs/superpowers/specs/2026-07-26-redis-8.8.1-stability-first-design.md`
>
> 当前实现计划：`docs/superpowers/plans/2026-07-26-redis-8.8.1-stability-foundation.md`

## 当前目标

先把 Redis 8.8.1 的项目基线、兼容合同、系统边界、许可证方案、未来动态库 ABI 和稳定性门禁写入本地权威文档；随后以 Cache OFF 为唯一 required 运行模式，推进 Redis 8.8.1 兼容、RocksDB 正确性和 OpenRaft 稳定性证明。

Embedded Redis Hot Tier 当前只做文档、方案和接口设计。系统稳定门禁通过且用户重新明确批准之前，不实现、不编译、不接入、不加载，也不进入生产依赖图。

## 当前实施授权边界

允许：

- 更新项目宪法、需求、决定、Roadmap、State 和 Kanban。
- 创建设计、兼容、许可证、ABI、稳定门禁和后续实施计划文档。
- 执行只读检查、Markdown 一致性检查和 Git diff 检查。

禁止：

- 修改生产源码、测试代码、构建脚本或 CI 行为。
- 创建 Redis 生产 fork 改动、动态库实现、FFI binding、loader 或 Cache ON 路径。
- 覆盖、续写、清理或回退此前冻结的实现工作树。

项目真相和恢复机制可以通过独立文档 PR 发布，但 PR 必须使用显式文件 allowlist，不得带入无关 dirty 文件。PR 之外的源码实现、暂存、提交或 push 仍需要与对应实现卡匹配的明确授权。

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

上述内容通过独立文档 PR 形成远端恢复锚点；PR 之外的本机运行状态继续保存在被忽略的 `.codex/recovery/`。

## 本轮验证证据

- 21 个当前入口、项目真相、设计、计划和门禁文档完成一致性检查。
- 58 个 Requirement ID 唯一；当前文档引用未发现未知 ID。
- 10 个 Decision ID 唯一；当前设计引用未发现失效 ID。
- 当前实施计划包含 8 个有序任务，热层生产实现不在计划范围内。
- 当前文档的本地 Markdown 链接、代码围栏和行尾空白检查通过。
- 文档候选集执行 `git diff --check` exit 0；验证时 Git index 为空。
- 发布工作树最终重放到 `main` commit `3c55165d1c76f656b4a4d9f576e5ed7d2274086e`，发布分支为 `codex/redis-8.8.1-doc-foundation`；PR Head 由 Git 历史和 GitHub 实时状态记录。

## 当前实施顺序

文档迁移通过一致性检查后，下一项实现工作必须新建清晰的隔离工作边界，并按以下顺序推进：

1. `M1-001`：Redis 8.8.1 exact Oracle、manifest 和 provenance。
2. `M1-002`：RESP2/RESP3 持久连接级 raw wire differential。
3. `M2-001`：RocksDB authority/durability contract。
4. `M2-002`：审计并扩展现有 close/reopen 回归，补齐全部 handle、TTL/metadata、Snapshot 和故障恢复门禁。
5. `M4-001`：`kiwi_redisraft_public_v1` 机器可读合同。
6. `M4-002`：OpenRaft deterministic simulator、现有 LogStore reopen 缺口与单 Group 正确性。
7. `M5-001`：3/5 节点进程、网络、磁盘故障和 Elle/Jepsen history。
8. `M6-001`：系统稳定 Gate Review。

在第 8 项通过并取得用户新授权之前，不启动任何 M7/M8 实现卡。

## 冻结工作树保护

此前实现工作树的精确路径、分支、HEAD、dirty paths 和验证证据以 `.codex/recovery/ACTIVE.md` 的冻结记录为准。

恢复或继续工作时：

- 不在冻结工作树上叠加 Redis 8.8.1 实现。
- 不暂存、提交、清理、回退或删除冻结改动。
- 不把冻结成果表述为当前 Redis 8.8.1 基线已经实现。
- 需要复用思路时，只能重新审计并在新的隔离工作边界中实现。

## 下一条安全动作

1. 发布并复核本独立文档 PR，不触碰此前冻结实现工作树。
2. PR 建立远端恢复锚点后，为 `M1-001` 创建新的隔离工作边界并保存独立 recovery checkpoint。
3. 先实现 Redis 8.8.1 exact Oracle、机器可读 manifest 和 provenance；不启动任何热层实现卡。

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
