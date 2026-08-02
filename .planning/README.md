# Kiwi 项目工作台

项目唯一权威入口是 [SDD.md](SDD.md)。

新会话按以下顺序恢复：

1. 阅读仓库根目录 CLAUDE.md 和 CONTRIBUTING.md。
2. 完整阅读 [SDD.md](SDD.md)，确认 baseline、current_work_package、current_plan、current_pr 和 next_safe_action。
3. 阅读 SDD 当前工作包引用的 Requirement、Decision、Issue、Discussion、spec、plan 和 verification。
4. 对照 .codex/recovery/ACTIVE.md、实际 branch、HEAD、dirty ownership 和 GitHub 实时状态。
5. 任何身份或状态漂移先停止写操作并报告。

下属注册表：

| 文件 | 职责 |
|---|---|
| PROJECT.md | 项目北极星、范围和不可妥协原则 |
| REQUIREMENTS.md | REQ 注册表和批准优先级 |
| DECISIONS.md | 已批准 Decision 注册表 |
| OPEN_QUESTIONS.md | 尚未闭合的高影响问题 |
| REFERENCES.md | exact 上游、许可证和研究来源 |

兼容入口：

- STATE.md、KANBAN.md 和 ROADMAP.md 只保留迁移指针。
- 它们不得维护独立状态、路线或看板。
- 历史 specs/plans 只提供背景，不是当前计划。

更新项目状态、工作包、路线和门禁时只修改 SDD.md；下属注册表仅在其记录类型发生变化时更新。
