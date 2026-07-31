# Storage Runtime Baseline Harness

本目录与 `tools/runtime-baseline` 一起，提供 kiwi storage 执行链路的性能与背压基线
（对应 epic #347 → #350 → #351 → #352）。

## 设计约束（必须守）

- **冻结阈值只能来自固定 Linux/WSL 主机。** 设计规格明确禁止用 Windows / GitHub
  hosted runner 的跑分数字冻结阈值——矩阵的可重现性依赖固定的内核、文件系统与
  CPU 拓扑。当前 hosted CI 只编译 harness 并执行代码检查；完整矩阵未来只能通过
  `workflow_dispatch` 在固定环境运行，或由维护者在规定的固定 Linux/WSL 主机执行。
- **不在基线未稳前伪造数字。** `tools/runtime-baseline/src/thresholds.rs` 中的常量
  当前为占位哨兵值（`f64::NAN` / `usize::MAX` / `u64::MAX` / `i64::MAX`），并标注
  `TODO(#351)`。只要任一仍为占位值，`thresholds::all_frozen()` 返回 `false`，
  `verify_outcome` 会据此返回 `PolicyUnavailable`，杜绝「用占位值假装通过」。

## 本次 PR 的准备工作（Refs #390）

建立 #351 量化验收门禁的**阈值冻结机制**本身：

| 文件 | 作用 |
|------|------|
| `tools/runtime-baseline/src/thresholds.rs` | #351 验收所需的冻结基线阈值常量 + `all_frozen()` 哨兵检测 |
| `tools/runtime-baseline/src/verify.rs` | 严格的单 case threshold checker；返回 `Pass` / `PolicyUnavailable` / `Fail` |
| `tools/runtime-baseline/schema/outcome.schema.json` | 单 case threshold input 的 JSON Schema 2020-12 合同 |
| 本文件 | 方法论与回填流程说明 |

单 case 的 `Pass` 只证明该 case 满足冻结阈值，不代表完整 baseline run 可发布。最终发布
结论还必须由 controller verifier 核对 manifest case 集守恒、重复轮次、CV、来源身份和
完整 run outcome。占位状态下 checker 只返回 `PolicyUnavailable`。

## 为什么 observer 接线与真实数字是 follow-up

- **Wiring & Smoke（Tasks 6–12）**：把 `baseline` observer 状态机接入 `runtime` crate 的
  `manager` / `message` / `storage_server` 生产路径，并实现 harness 运行时、控制面、Python
  控制器与 `cases.yaml`。这部分改动进入 `src/common/runtime/`，会拉入 `storage` → `rocksdb`，
  必须在能编译/运行 RocksDB 的环境验证——因此作为独立 PR 在 Linux 主机推进。
- **Baseline Results（真实数字冻结）**：在固定 Linux/WSL 主机按版本化 `cases.yaml` 跑完整
  矩阵后，把真实数值回填进 `thresholds.rs`，再由完整 verifier 聚合全部 case、稳定性和
  provenance 证据。只有原始结果、报告和真实阈值满足 #390 Acceptance 后才关闭 #390。

## 未来回填流程

1. 固定 Linux/WSL 主机，按版本化 `cases.yaml` 跑完整矩阵（基础吞吐 + value 变体 + 单因素扫描
   + offered-load 4 档 + lifecycle 3 类；性能 case 重复 5 次、lifecycle case 重复 3 次，
   CV<=5%）。
2. 将实测值回填 `thresholds.rs`（替换 `TODO(#351)` 占位）。
3. 由后续 `run_baseline.py verify` 对每个 case 执行 threshold check，并核对完整 case 集、
   重复轮次、CV 和来源身份后产生 run 级发布结论。
4. 提交原始 JSON/CSV + 环境清单（内核、CPU、文件系统）+ 摘要；冻结 #351 量化验收阈值。
5. 满足 #390 Acceptance 后关闭 #390，并在 #350 / #351 中引用结果。

## 本地验证（不依赖 RocksDB）

```bash
cargo check   -p runtime-baseline
cargo test    -p runtime-baseline
cargo clippy  -p runtime-baseline --all-targets -- -D warnings -D clippy::unwrap_used
```
