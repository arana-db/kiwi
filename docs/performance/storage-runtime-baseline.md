# Storage Runtime Baseline Harness

本目录与 `tools/runtime-baseline` 一起，提供 kiwi storage 执行链路的性能与背压基线
（对应 epic #347 → #350 → #351 → #352）。

## 设计约束（必须守）

- **冻结阈值只能来自固定 Linux/WSL 主机。** 设计规格明确禁止用 Windows / GitHub
  hosted runner 的跑分数字冻结阈值——矩阵的可重现性依赖固定的内核、文件系统与
  CPU 拓扑。因此本仓的 CI 只在固定 Linux 环境执行完整矩阵；本地/Windows 仅用于
  开发迭代与 `cargo check` / `clippy` / `cargo test`。
- **不在基线未稳前伪造数字。** `tools/runtime-baseline/src/thresholds.rs` 中的常量
  当前为占位哨兵值（`f64::NAN` / `usize::MAX` / `u64::MAX` / `i64::MAX`），并标注
  `TODO(#351)`。只要任一仍为占位值，`thresholds::all_frozen()` 返回 `false`，
  `verify_outcome` 会据此把任何 case 判为 `NonPublishable`，杜绝「用占位值假装通过」。

## 本次 PR 交付了什么（#390）

建立 #351 量化验收门禁的**阈值冻结机制**本身：

| 文件 | 作用 |
|------|------|
| `tools/runtime-baseline/src/thresholds.rs` | #351 验收所需的冻结基线阈值常量 + `all_frozen()` 哨兵检测 |
| `tools/runtime-baseline/src/verify.rs` | `Outcome` 比对器：`verify_outcome` / `verify_outcome_path`，返回 `Publishable` / `NonPublishable` / `Fail` |
| `tools/runtime-baseline/schema/outcome.schema.json` | verifier 期望的 outcome JSON 结构 |
| 本文件 | 方法论与回填流程说明 |

`verify.rs` 的单元测试 `placeholder_thresholds_make_every_case_non_publishable` 是「不伪造
数字」的硬保证：占位状态下任何真实数值都判 `NonPublishable`。

## 为什么 observer 接线与真实数字是 follow-up

- **Wiring & Smoke（Tasks 6–12）**：把 `baseline` observer 状态机接入 `runtime` crate 的
  `manager` / `message` / `storage_server` 生产路径，并实现 harness 运行时、控制面、Python
  控制器与 `cases.yaml`。这部分改动进入 `src/common/runtime/`，会拉入 `storage` → `rocksdb`，
  必须在能编译/运行 RocksDB 的环境验证——因此作为独立 PR 在 Linux 主机推进。
- **Baseline Results（真实数字冻结）**：在固定 Linux/WSL 主机按版本化 `cases.yaml` 跑完整
  矩阵后，把真实数值回填进 `thresholds.rs`，再让 verifier 全量判 `Publishable`。仅此 PR 验收
  通过后才关闭 #350。

## 未来回填流程

1. 固定 Linux/WSL 主机，按版本化 `cases.yaml` 跑完整矩阵（基础吞吐 + value 变体 + 单因素扫描
   + offered-load 4 档 + lifecycle 3 类；每 case 重复 5 次、CV<=5%）。
2. 将实测值回填 `thresholds.rs`（替换 `TODO(#351)` 占位）。
3. `python3 tools/runtime-baseline/run_baseline.py verify ...` 跑 `verify_outcome` 全量
   判 `Publishable`。
4. 提交原始 JSON/CSV + 环境清单（内核、CPU、文件系统）+ 摘要；冻结 #351 量化验收阈值。
5. 关闭 #350。

## 本地验证（不依赖 RocksDB）

```bash
cargo check   -p runtime-baseline
cargo test    -p runtime-baseline
cargo clippy  -p runtime-baseline --all-targets -- -D warnings -D clippy::unwrap_used
```
