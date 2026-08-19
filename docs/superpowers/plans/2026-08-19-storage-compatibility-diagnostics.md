# Storage 兼容拒绝诊断契约实施计划

> **执行要求：** 使用 `test-driven-development` 先证明真实 `Storage::open` 在旧实现上
> 缺少诊断字段，再做最小实现；提交/PR/合并前使用
> `verification-before-completion`。不得修改治理 worktree 或根工作区。

**目标：** 闭合 Issue #342 剩余验收项，使生产 `Storage::open` 的每个格式兼容拒绝
稳定包含 `current`、`on_disk`、`action` 和原始 `cause`，且不改变任何磁盘格式或迁移
状态机。

**架构：** 在 Storage admission 边界只包装格式/严格 RocksDB open 错误；一个只读、
有界 descriptor helper 描述失败时落盘格式；内部 manifest/migration helper 继续返回
精确 cause。状态发布顺序不变。

**基线：** `main@9a8a64aca12a825912f299450e10fc6043eca610`。

**设计：**
`docs/superpowers/specs/2026-08-19-storage-compatibility-diagnostics-design.md`。

---

## 文件所有权

- `src/storage/src/storage_manifest.rs`：current/on-disk descriptor 的常量与只读 helper。
- `src/storage/src/storage.rs`：生产 admission 边界的一次性兼容诊断包装。
- `src/storage/tests/storage_manifest_v2_test.rs`：future/corrupt/topology/缺失实例的真实
  `Storage::open` RED/GREEN。
- `src/storage/tests/storage_migration_test.rs`：unknown legacy/comparator 严格 open 与已知
  migration 正向回归；仅确有需要时修改。
- `src/storage/tests/redis_vector_test.rs`：只读确认旧单实例入口语义；不以它代替生产
  证据。
- `src/storage/src/storage_migration.rs`：默认不修改；只有 descriptor/action 必须消费
  已有公开分类且无法在 manifest/storage 层完成时才做外科式改动。
- 本计划和对应设计文档：批准范围与执行证据。

## 环境基线

- [x] Windows 共享 target 中 `redis_vector_test` 编译成功，但进程启动返回
  `STATUS_ENTRYPOINT_NOT_FOUND (0xc0000139)`；这不是测试 RED/GREEN 证据。
- [ ] 后续 Windows 复验使用任务专属 `CARGO_TARGET_DIR`；若 native DLL 仍阻塞，使用
  WSL/Linux 作为 RocksDB 行为权威，并记录 Windows 环境缺口。不得 clean 共享缓存。

## Task 1：定义权威断言 helper

**文件：** `src/storage/tests/storage_manifest_v2_test.rs`

- [ ] 增加 `assert_storage_compatibility_refusal`，要求错误 Display：
  - `storage compatibility refusal:` 恰好一次；
  - `current=`、`on_disk=`、`action=`、`cause=` 各恰好一次且非空；
  - caller 指定的 on-disk/cause 关键证据存在；
  - `Storage::insts` 为空且 `is_opened` 为 false。
- [ ] helper 不接受字段仅出现在 Debug/stack trace；断言真实用户可见 Display。

## Task 2：真实 `Storage::open` RED fixtures

**文件：**

- `src/storage/tests/storage_manifest_v2_test.rs`
- `src/storage/tests/storage_migration_test.rs`（unknown legacy/comparator fixture 如需）

- [ ] future Root manifest：把合法 root JSON 的 `manifest_version` 改为 99，写到真实
  root 路径，调用 `Storage::open`，要求 `on_disk` 体现 v99 且 cause 保留 unsupported
  version。
- [ ] corrupt Root manifest：写损坏 digest 或非 JSON，要求
  `on_disk=root-manifest-present-unreadable` 且 cause 保留 digest/JSON 详情。
- [ ] topology mismatch：合法 Root 的 `db_instance_num=2`，用 `Storage::new(3, ...)`
  打开，要求 descriptor 和可执行 action。
- [ ] missing/invalid instance：合法 Root 下缺少实例或 instance manifest，要求 envelope
  且不重建目录/不发布 Storage。
- [ ] unregistered legacy CF 或 comparator mismatch：通过现有 legacy fixture/真实
  RocksDB 创建，要求原始 RocksDB comparator/CF 详情出现在 `cause`，而不是当前只有
  `RocksDB error` 的丢失文本。
- [ ] 运行每个精确测试。预期 RED：旧实现只含局部 cause，缺统一字段；comparator
  fixture 还应证明当前 Display 丢失 RocksDB source 详情。

fixture/setup、文件锁、`STATUS_ENTRYPOINT_NOT_FOUND` 或 cleanup 失败都不是有效 RED。

## Task 3：实现有界只读 on-disk descriptor

**文件：** `src/storage/src/storage_manifest.rs`

- [ ] 从现有 Root/Instance/schema 常量组合唯一 `current` descriptor，不复制版本魔法数。
- [ ] 增加 crate-private descriptor helper：
  - 不存在/空目录返回 `empty`；
  - Root manifest 存在时只读固定上限字节，超过上限或解析失败返回
    `root-manifest-present-unreadable`；
  - 在不要求 digest/完整 schema 合法的前提下提取 numeric `manifest_version` 和
    `storage_schema_version`；
  - Root manifest 缺失但目录非空返回 `legacy-without-root-manifest`；
  - metadata/read 失败返回 `unavailable`，不覆盖原 open error。
- [ ] 用 `symlink_metadata` 拒绝明显 manifest symlink；descriptor 只用于诊断，不能
  创建目录、打开 RocksDB 或调用 migration。
- [ ] 单元/集成断言读取上限、unreadable fallback 和版本提取；不要为诊断实现第二套
  manifest validator。

## Task 4：实现 production admission envelope

**文件：** `src/storage/src/storage.rs`

- [ ] 增加单一 wrapper，把 `InvalidFormat` 与严格 RocksDB open 的兼容拒绝格式化为：

  ```text
  storage compatibility refusal: current=...; on_disk=...; action=...; cause=...
  ```

- [ ] 对 RocksDB variant 显式使用内部 source 的 Display，保留 comparator/CF 具体错误；
  不得只嵌入外层 `RocksDB error`。
- [ ] action 根据 descriptor/cause 做最小分类；只引用已有 staged migration、匹配版本、
  离线检查或备份恢复，不发明 CLI。
- [ ] wrapper 应用于以下每个 `Storage::open` admission step：migration prepare/resume、
  Root load、instance validation、strict instance open、migration finalize 后的第二次验证/
  reopen。
- [ ] 内部 helper 不包装，避免 `current=` 等字段嵌套两次。
- [ ] 普通 I/O variant 原样返回；背景任务启动后的非兼容错误不进入本 wrapper。
- [ ] `self.insts`、`db_path`、background/expiration publication 行保持原顺序。

## Task 5：GREEN 与正向不回归

- [ ] 重跑 Task 2 的每个精确测试，预期 GREEN。
- [ ] 增加/保留正向测试：empty root 创建成功；current v2 reopen 成功；Base-v1 和
  Vector-v1 已知 staged migration 成功；rollback/finalize 关键状态用例成功。
- [ ] mutant：分别删除 current、on_disk、action、cause，或只包装第一次 Root load；
  权威测试必须失败。
- [ ] comparator mutant 把 Rocks source 退回外层 `RocksDB error`，测试必须失败。

## Task 6：风险匹配验证与 Test Guard

- [ ] 任务专属 Windows target 或 WSL：
  `cargo test -p storage --test storage_manifest_v2_test`
- [ ] WSL/Linux：
  `cargo test -p storage --features test-fault-injection --test storage_migration_test`
  （至少精确 diagnostic + migration 正向集；资源允许时全文件）。
- [ ] `cargo test -p storage --test redis_vector_test` 的相关生产入口/manifest 回归；若
  Windows DLL 阻塞，记录并由 Linux gate覆盖。
- [ ] `cargo fmt --all -- --check`
- [ ] `cargo clippy -p storage --all-targets --features test-fault-injection -- -D warnings`
- [ ] `git diff --check origin/main...HEAD`
- [ ] Test Guard 检查 fixtures 真实调用 `Storage::open`、旧实现确实 RED、没有仅测试
  helper 自己、cleanup 失败不会被吞掉。

## Task 7：独立复审、提交和 PR

- [ ] 规格复审：四字段、真实生产边界、非目标和 #342 验收一致，P0/P1/P2 全零。
- [ ] 质量复审：错误分类、Rocks source、bounded inspection、状态不发布和 tests/mutants
  均闭合，P0/P1/P2 全零。
- [ ] 只提交计划列出的文件；检查 worktree ownership 和 committed diff。
- [ ] push `codex/storage-diagnostic-contract`，创建独立 PR，使用 `Refs #342`，避免在
  exact-main 证据前自动关闭。
- [ ] 等待 exact Head checks 成功，复核 mergeability/threads 后合并。

## Task 8：exact-main 与 #342 关闭

- [ ] 获取诊断 PR merge SHA，等待该 SHA 或其 exact main 后继的 CI push run 成功。
- [ ] 在 main 上确认生产 `Storage::open` tests 和四字段合同仍存在。
- [ ] 在 #342 留下九项验收逐项映射、PR、merge SHA、exact-main run、测试命令和典型
  future/corrupt/topology/comparator 诊断。
- [ ] 以 completed 关闭 #342 并重新读取 Issue 状态确认。
- [ ] 不修改 #325、#340、#418、#421 或 #430 的状态（#421 由另一治理计划独立收口）。
