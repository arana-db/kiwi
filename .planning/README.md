# Kiwi 项目工作台

本目录是 Kiwi 的长期项目事实源。它解决两个问题：

1. 项目在多年演进中不偏离 Redis 兼容、混合存储和强一致三条主线。
2. Codex Desktop、CLI 或其他代理崩溃、重启、清空会话后，可以只依赖仓库文件恢复工作。

## 新会话启动顺序

每个新会话在修改文件前必须按顺序完整读取：

1. `AGENTS.md`
2. `CLAUDE.md`
3. `CONTRIBUTING.md`
4. `.planning/PROJECT.md`
5. `.planning/STATE.md`
6. `.planning/KANBAN.md`
7. `.planning/DECISIONS.md` 中最近的相关决定
8. `.planning/STATE.md` 指向的当前实现计划
9. `.codex/recovery/ACTIVE.md`，如果该本机文件存在
10. 当前 `git status --porcelain=v2 --branch --untracked-files=all`

恢复时的优先级是：

```text
当前文件系统和 Git 状态
  > .codex/recovery/ACTIVE.md 的当前任务、授权和 dirty 归属
  > .planning/STATE.md 的项目级当前状态
  > .planning/DECISIONS.md 的已批准决定
  > .planning/ROADMAP.md
  > 旧 .ai-team/.omx 产物
  > 会话记忆
```

如果 branch、HEAD 或 dirty 文件集合与 `ACTIVE.md` 不一致，新会话必须先报告差异，不得自动执行 checkout、restore、reset、stash、clean、格式化或覆盖。

## 文件职责

| 文件 | 唯一职责 |
|---|---|
| `PROJECT.md` | 项目北极星、产品边界和不可妥协原则 |
| `REQUIREMENTS.md` | 带编号的可验收需求 |
| `ROADMAP.md` | 唯一阶段路线、依赖和退出门禁 |
| `STATE.md` | 当前里程碑、最后完成事项、阻塞和下一步 |
| `KANBAN.md` | 当前可执行工作项状态，不重复架构正文 |
| `DECISIONS.md` | 当前有效的已批准架构决定及其理由；失效方案由 Git 历史保留，不进入当前启动口径 |
| `REFERENCES.md` | 固定上游版本、commit、许可证和研究来源 |

## 更新纪律

- 架构决定确认后，先更新 `DECISIONS.md` 和对应规范，再更新 Roadmap、State 和 Kanban；替换现行决定时必须在提交说明和评审证据中保留变更原因。
- 完成一个可独立验证的工作项后，立即更新 `STATE.md` 和 `KANBAN.md`。
- 运行长测试、开始大规模编辑、等待外部任务、需要用户授权或准备结束会话前，运行：

```powershell
scripts\codex-workstate.ps1 `
  -Title '<current task>' `
  -TaskId '<stable-id>' `
  -Mode '<planning|implementation|verification|awaiting-user|blocked>' `
  -TaskPath '<path1>','<path2>'
```

- `.codex/recovery/` 是本机运行状态，已被精确忽略；`.planning/` 是应纳入版本控制的项目事实源。
- `.ai-team/` 和 `.omx/` 只能作为历史资料，不能覆盖本目录中的当前状态。
