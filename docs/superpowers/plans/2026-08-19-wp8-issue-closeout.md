# WP8 与 Issue #421 合并后收口实施计划

> **执行要求：** 使用 `subagent-driven-development` 隔离治理与存储诊断工作流；
> 本计划的每个行为变更先用 `test-driven-development` 观察 RED，并在提交/PR/合并前
> 使用 `verification-before-completion`。不得修改另一个 worktree 或根工作区。

**目标：** 把已经合并并通过 exact-main CI 的 WP8 提升为可机器验证的
`accepted`，把 active #421 owner 引用迁移到仍开放的 #418/#430，并在治理 PR
合并及新 exact-main 验证后关闭 #421。

**架构：** `.planning/SDD.md` 记录 WP8 immutable PR 与 exact-main evidence；
`scripts/validate_sdd.py` 对 accepted evidence、投影和 GitHub Actions run fail closed；
compat manifest 只迁移 issue owner，audit config 只迁移 advisory owner，不改变产品或
安全合同。

**基线：** `main@9a8a64aca12a825912f299450e10fc6043eca610`。

**设计：**
`docs/superpowers/specs/2026-08-19-wp8-issue-closeout-design.md`。

---

## 文件所有权

- `.planning/SDD.md`：WP8 accepted 状态和 evidence projection。
- `scripts/validate_sdd.py`：WP8 evidence normal/self-test 合同。
- `tests/compat/redis-8.8.1/manifest.yaml`：五条 operational-limit issue 迁移。
- `tools/compat/tests/manifest.rs`：迁移后的语义与回退 mutant。
- `.cargo/audit.toml`：`RUSTSEC-2026-0235` owner 迁到 #430。
- `tools/compat/tests/ci_contract.rs`：审计 owner 和回退 mutant。
- `.planning/STATE.md`、`.planning/KANBAN.md`：只读确认继续作为 SDD 跳转页；没有
  独立状态时不修改。
- 本计划和对应设计文档：批准范围与执行证据。

## 已建立的基线

- [x] `python -I -B scripts/validate_sdd.py --self-test`：39 个失败路径变异通过。
- [x] `python -I -B scripts/validate_sdd.py`：`errors=0`。
- [x] `cargo test -p kiwi-compat --test manifest`：49 passed。
- [x] `cargo test -p kiwi-compat --test ci_contract`：26 passed。

## Task 1：WP8 evidence RED self-tests

**文件：** `scripts/validate_sdd.py`

- [ ] 新增构造“WP8 accepted + 完整 evidence”的纯文本 helper，避免每个 mutant 手写
  不一致状态。
- [ ] 新增固定 run payload loader，成功 payload 必须是：`ci` workflow、`main`
  branch、`push` event、completed/success、Head 等于 recorded exact-main ref。
- [ ] 添加以下失败变异：
  - accepted 但 exact-main status 不是 passed；
  - PR Base/Head/merge parent/merge 任一 SHA 漂移；
  - Base→Head ancestry、merge 唯一 parent、Head tree=merge tree 或 merge subject 漂移；
  - exact-main ref/run 缺失或格式错误；
  - run 的 workflow/path/event/branch/head/conclusion 任一漂移；
  - WP8 block 或当前状态表 evidence projection 与 front matter 漂移；
  - baseline_ref 仍停留在 PR #422 之前。
- [ ] 运行 `python -I -B scripts/validate_sdd.py --self-test`。

预期 RED：至少一个 accepted-without-evidence mutant 被旧 validator 接受。fixture 或
Markdown 链接错误不是有效 RED。

## Task 2：实现 WP8 immutable/exact-main validator

**文件：** `scripts/validate_sdd.py`

- [ ] 增加 WP8 identity/evidence field constants 和精确 immutable values：PR 422、
  Base/merge parent `733888fc...`、Head `2b03219...`、merge `9a8a64a...`。
- [ ] 校验 SHA/decimal/status 格式；当 WP8 是 verified/accepted/released 时要求
  exact-main `passed`。
- [ ] 复用现有 GitHub run loader 模式增加 WP8 run 校验；normal validation 在线
  读取 recorded run，self-test 注入 payload，不访问网络。
- [ ] 校验 Base→Head ancestry、merge 唯一 parent=Base、Head tree=merge tree、merge
  subject 含 `(#422)`；merge→verification 允许 equality，verification→baseline 必须是
  ancestry；不改变 WP0 immutable evidence。
- [ ] 校验 WP8 block 和当前状态表的 evidence projection 恰好一次且与 front matter
  相同。
- [ ] 重跑 self-test，预期新增和既有失败变异全部 GREEN。

## Task 3：写回 accepted SDD 状态

**文件：** `.planning/SDD.md`

- [ ] 更新 `updated_at` 和 `baseline_ref`。
- [ ] 添加设计规定的七个 WP8 evidence 字段及 `wp8_pr_number`。
- [ ] 把 front matter 和 WP8 block 状态改为 `accepted`。
- [ ] `current_plan` 指向本计划；保留 current Issue 421 / PR 422 作为历史身份。
- [ ] 把实施前 `next_safe_action` 改为选择下一个经批准工作包。
- [ ] 在 WP8 block 增加 post-merge evidence 段，精确投影 PR Base/Head、merge、
  exact-main run 和 accepted 结论。
- [ ] 调整退出门禁的 Issue 对账文本：#415/#421 required acceptance 已完成；#418
  保持开放并拥有 residual differences；不得声称 #418 已关闭。
- [ ] 更新第 17 节当前表与叙述，删除“下一步提交 Task 1-5/启动 Oracle”的过期事实。
- [ ] scoped 更新实时快照、Issue registry、INV-12/INV-13/INV-17 和“normal CI 排除
  differential”等陈旧描述，只写 PR #422 已证明的事实，不把 WP2/WP6 未来范围伪装
  为完成；新增 open #430 owner。
- [ ] `next_safe_action` 精确使用 `await-next-authorized-work-package`，表中对应“等待下
  一个经明确批准的工作包；M7/M8 继续 frozen”。
- [ ] 不修改 `.planning/STATE.md` / `KANBAN.md`，除非 validator 证明跳转页本身漂移。
- [ ] 运行 normal validator，预期 `current.status=accepted`、`baseline_ref=9a8a64a...`、
  `errors=0`。

## Task 4：Vector operational-limit owner RED/GREEN

**文件：**

- `tools/compat/tests/manifest.rs`
- `tests/compat/redis-8.8.1/manifest.yaml`

- [ ] 先把权威测试改为：VADD、VEMB、VISMEMBER、VREM、VSIM 各有一条 issue=#418
  且 reason 含对应 admission term 的 operational-limit difference；全 manifest 不得
  有 issue=#421。
- [ ] 加回退 mutant：把其中一条 #418 operational-limit URL 改回 #421，validator/
  断言必须失败。对 VADD/VEMB/VSIM 要用 reason 前缀定位，不能误匹配同命令已有的
  其他 #418 difference。
- [ ] 运行精确测试，预期 RED：旧 manifest 仍把五条指向 #421。
- [ ] 只改五个 issue URL 为 #418；reason、remove_when、owner、affected、introduced、
  last_verified_ref 不变。
- [ ] 重跑 `cargo test -p kiwi-compat --test manifest`，预期 GREEN。

## Task 5：rkyv advisory owner RED/GREEN

**文件：**

- `tools/compat/tests/ci_contract.rs`
- `.cargo/audit.toml`

- [ ] 先让 `validate_rkyv_audit_governance` 只检查 advisory 紧邻 comment block，要求
  唯一 `owner: security-deps / Issue #430`，并增加把 #430 退回 WP8/#421、删除/重复
  owner，以及在无关位置追加伪 #430 owner 的 mutants。
- [ ] 运行精确 test，预期 RED：旧 owner 仍是 WP8 / #421。
- [ ] 只改 owner comment；advisory ignore、potential path、unreachable status 和
  remove_when 保持不变。
- [ ] 重跑 `cargo test -p kiwi-compat --test ci_contract`，预期 GREEN。

## Task 6：本地验证与 Test Guard

- [ ] `python -I -B scripts/validate_sdd.py --self-test`
- [ ] `python -I -B scripts/validate_sdd.py`
- [ ] `cargo test -p kiwi-compat --test manifest`
- [ ] `cargo test -p kiwi-compat --test ci_contract`
- [ ] `cargo fmt --all -- --check`
- [ ] `cargo clippy -p kiwi-compat --all-targets -- -D warnings`
- [ ] `git diff --check origin/main...HEAD`
- [ ] `git grep -n -E 'issues/421|Issue #421' -- tests/compat/redis-8.8.1/manifest.yaml
  .cargo/audit.toml` 预期无输出（退出码 1）；历史文档、WP8 Primary Issue 身份和负向
  mutants 不要求全仓零匹配。
- [ ] 使用 Test Guard 检查新增 self-test/mutant 是否真实执行权威入口、能杀死旧实现、
  没有仅 substring 的伪绑定。

## Task 7：独立复审、提交和 PR

- [ ] 规格复审：状态/证据/owner 迁移与批准设计一致，P0/P1/P2 全零。
- [ ] 质量复审：validator fail-closed、mutant 区分度、无生产行为变化，P0/P1/P2 全零。
- [ ] 仅提交计划列出的文件；检查 `git status --short` 与 committed diff。
- [ ] push `codex/wp8-issue-closeout`，创建独立 PR，body 使用 `Refs #421`，不得让 PR
  merge 自动提前关闭 Issue。
- [ ] 等待 exact Head 全部 required/visible checks 成功，复核 mergeability 和 review
  threads，再合并。

## Task 8：exact-main 与 #421 关闭

- [ ] 获取治理 PR merge SHA，等待该 SHA 或其 exact main 后继的 `ci` push run 成功；
  该新 SHA/run 只写入 #421 closeout 评论，不覆盖 SDD 中 PR #422 的 immutable run。
- [ ] 复核 main 上 active manifest/audit owner 已分别为 #418/#430，且两 Issue OPEN。
- [ ] 在 #421 留一条证据评论：PR #422、治理 PR、两次 exact-main run、WP8 accepted
  SDD、五条差异到 #418、advisory 到 #430、验收逐项映射。
- [ ] 以 completed 关闭 #421，并重新读取 Issue 状态确认。
- [ ] 不关闭或改变 #418、#430、#325、#340、#342。
