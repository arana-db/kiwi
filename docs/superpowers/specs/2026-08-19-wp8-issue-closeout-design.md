# WP8 与 Issue #421 合并后收口设计

## 文档状态

- 日期：2026-08-19
- 状态：用户已批准，待实施与独立复审
- 仓库：`arana-db/kiwi`
- 实施基线：`main@9a8a64aca12a825912f299450e10fc6043eca610`
- 实现 PR：[PR #422](https://github.com/arana-db/kiwi/pull/422)
- PR Base：`733888fc90ad8ef039947e87b08d7500a405954a`
- PR Head：`2b03219cdd5e452e08c1b2144c3c90516190d41f`
- Merge / exact-main SHA：`9a8a64aca12a825912f299450e10fc6043eca610`
- Exact-main CI：run `32129266046`
- Primary Issue：[Issue #421](https://github.com/arana-db/kiwi/issues/421)
- 保持开放的后续追踪：[Issue #418](https://github.com/arana-db/kiwi/issues/418)、[Issue #430](https://github.com/arana-db/kiwi/issues/430)

本文只收口 PR #422 合并后的治理事实，不修改 VectorSet 生产行为、兼容边界、
存储格式、CI 执行逻辑或依赖图。Issue #421 只有在本设计对应的治理 PR 合并、
新 exact-main 验证成功且追踪引用全部迁移后才允许关闭。

## 1. 问题

PR #422 已合并且 exact-main CI 成功，但唯一 SDD 控制面仍把 WP8 标记为
`in-progress`，并保留实施前的下一动作。与此同时：

1. 五条仍有效的 Vector operational-limit known differences 继续把 #421 当作
   owner；关闭 #421 会使 active `remove_when` 指向已关闭 Issue。
2. `RUSTSEC-2026-0235` 的临时审计豁免继续把 WP8 / #421 当作 owner；该豁免的
   生命周期独立于已经完成的 VectorSet 工作包。
3. 当前 validator 只对 WP0 的 immutable PR 和 exact-main 证据做机器校验；WP8
   即使被手工改成 `accepted`，也没有防止缺失或漂移证据的失败路径。

因此，代码已经完成并不等于 Issue 可以安全关闭。必须先把“完成事实”“残留差异
owner”“长期安全豁免 owner”拆开，并让 SDD validator 对 WP8 的接受状态 fail
closed。

## 2. 目标与非目标

### 2.1 目标

- 把 WP8 状态从 `in-progress` 提升为 `accepted`，并记录 PR #422 immutable
  Base/Head、merge SHA、exact-main SHA、CI run 和 `passed` 结论。
- 让 normal validator 和 self-test 约束 WP8 接受证据；删除、篡改或把成功 run
  指向非 main/non-push/non-CI/non-success 时必须失败。
- 把 VADD、VEMB、VISMEMBER、VREM、VSIM 的五条 operational-limit differences
  从 #421 迁移到仍开放的 #418，不改变 reason、`remove_when`、owner、affected 或
  Redis exact-ref。
- 把 `RUSTSEC-2026-0235` 临时豁免的 owner 从 WP8 / #421 迁移到专门且开放的
  #430，并让 CI contract 固定该 owner。
- 更新当前状态和下一安全动作，使仓库不再声称 WP8 仍在实施。
- 在治理 PR 合并并完成新的 exact-main 验证后，为 #421 留下可核查证据并关闭。

### 2.2 非目标

- 不关闭 #418、#430、#325、#340 或 #342。
- 不删除任何 known difference 或审计豁免。
- 不修改 Vector 命令、资源上限、raw differential、Oracle、cluster gate 或
  migration 生产实现。
- 不把 #418 的剩余差异伪装成 WP8 未完成；#418 是长期兼容追踪，不是 #421 的
  关闭阻塞，只要 active 引用已经迁移并且 Issue 保持开放。
- 不改写 WP0 immutable evidence。

## 3. 状态与证据合同

### 3.1 SDD front matter

新增以下 WP8 evidence 字段：

```text
wp8_pr_number: 422
wp8_pr_base_ref: 733888fc90ad8ef039947e87b08d7500a405954a
wp8_pr_head_ref: 2b03219cdd5e452e08c1b2144c3c90516190d41f
wp8_merge_parent_ref: 733888fc90ad8ef039947e87b08d7500a405954a
wp8_merge_ref: 9a8a64aca12a825912f299450e10fc6043eca610
wp8_exact_main_verification_ref: 9a8a64aca12a825912f299450e10fc6043eca610
wp8_exact_main_verification_run: 32129266046
wp8_exact_main_verification_status: passed
```

同时更新：

- `updated_at: 2026-08-19`；
- `baseline_ref` 为上述 exact-main SHA；
- `current_work_package: WP8`；
- `current_work_package_status: accepted`；
- `current_plan` 指向本设计对应的收口计划；
- `current_issue: 421` 与 `current_pr: 422` 保留为已完成工作包的历史身份；
- `next_safe_action` 改为选择下一个经批准的工作包，而不是继续执行 PR #422。

保留 current Issue/PR 并不表示它们仍开放；它们用于把 accepted WP8 绑定到完成它
的 GitHub 对象。开放/关闭状态以 GitHub 实时证据为准。

### 3.2 Validator

当 WP8 状态为 `verified`、`accepted` 或 `released` 时，validator 必须要求：

- 所有 WP8 SHA 字段是完整 lowercase Git SHA；PR/run 是正十进制；
- PR number、Base、Head 和 merge SHA 与本次已合并事实精确相等；
- Base 是 Head 的祖先；merge 的唯一 parent 等于 Base；PR Head tree 等于 merge
  tree；merge subject 包含 `(#422)`；
- exact-main status 是 `passed`，merge 是 verification ref 的祖先（允许二者相等），
  verification ref 是 baseline ref 的祖先，且当前仓库可解析这些提交；
- recorded run 是 `ci` workflow 的 `main` push，`head_sha` 等于 recorded exact-main
  ref，状态 completed 且 conclusion success；
- WP8 block 与当前状态表中的 evidence projection 与 front matter 一致。

self-test 必须从有效快照分别变异 status、SHA、Git ancestry/tree/subject、run identity
和投影文本，证明这些失败路径真实由 validator 拒绝。测试不能联网依赖真实 GitHub；
run loader 使用注入的固定 payload。normal validation 可以沿用现有 GitHub run 在线
复核路径。manifest 与 audit owner mutants 分别属于 Rust contract tests，Python SDD
validator 不读取或复制这两个合同。

## 4. Active 追踪迁移

### 4.1 Vector compatibility

manifest 中 issue 等于 `https://github.com/arana-db/kiwi/issues/421` 的五条
operational-limit difference 全部改为 #418。只允许改 issue URL；以下字段必须
保持字节语义不变：

- command 和 classification；
- owner；
- reason；
- `remove_when`；
- introduced / affected；
- Redis exact `last_verified_ref`。

`tools/compat/tests/manifest.rs` 必须查找 #418，并额外证明五条迁移后的 difference
仍是 operational-limit 条目，而不是误匹配同一命令下原有的其他 #418 difference。
测试还应加入回退到 #421 的 mutant，确保关闭后不会重新引入 active 引用。

### 4.2 Advisory exemption

`.cargo/audit.toml` 的 owner 精确改为 `security-deps / Issue #430`。potential path、
unreachable 状态和 `remove_when` 不变。`ci_contract` 必须在 advisory 的紧邻治理
comment block 内要求该唯一 owner，且拒绝恢复成 WP8 / #421 或在无关位置追加伪 owner。
#430 保持 OPEN，直到豁免按其验收条件真正移除。

### 4.3 STATE / KANBAN

`.planning/STATE.md` 与 `.planning/KANBAN.md` 已经是指向 SDD 的历史兼容跳转页。本次
只读验证两者继续满足 pointer 合同，不复制 accepted、SHA、run 或 Issue 状态；真实
writeback 只发生在唯一权威 `.planning/SDD.md`。

## 5. Issue 关闭顺序

1. 治理 PR 通过本地验证与双重独立复审。
2. 提交、push、创建 PR；等待 exact Head 的 required/visible checks 成功。
3. 合并治理 PR。
4. 等待新 main 的 `ci` workflow 成功，并记录新的 main SHA 与 run。
5. 实时复核仓库不再存在 active #421 owner 引用；历史文档中的 Issue 链接允许保留。
6. 在 #421 评论 PR #422、治理 PR、两个 exact-main run、#418/#430 迁移去向和
   验收映射，然后以 completed 关闭。

`wp8_exact_main_verification_*` 永久记录 PR #422 实现验收的 merge/run；治理 PR
合并后的新 SHA/run 在合并前不可预知，因此记录在 #421 最终 closeout 评论，不覆盖
SDD 中的 PR #422 immutable evidence，也不引入第二个 writeback PR。

任何一步失败都保持 #421 OPEN；不得用旧的 run `32129266046` 代替治理 PR 合并后的
新 exact-main 验证。

## 6. 验收标准

- SDD normal/self-test、manifest、CI contract 和 diff checks 全部通过。
- WP8 accepted 状态缺任一 immutable/exact-main 字段都会失败。
- 五条 operational-limit difference 全部且仅迁到 #418；manifest/audit 等 active
  owner 不再引用 #421。历史设计、WP8 Primary Issue 身份和负向 mutant 可以保留 #421。
- rkyv 豁免 owner 是开放的 #430，reachability sentinel 与 remove condition 未放宽。
- 独立规格复审与质量/Test Guard 复审均为 P0=0/P1=0/P2=0。
- 治理 PR exact Head 和合并后的 exact main 均为绿色。
- #421 最终关闭；#418、#430、#325、#340、#342 状态不因本工作改变。
