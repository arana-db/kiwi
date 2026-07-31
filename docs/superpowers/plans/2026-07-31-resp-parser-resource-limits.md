# RESP 聚合类型资源限额实现计划

> PR #406 已合并，本文件作为零声明预分配与 nesting 阶段计划保留；PR #404 的
> 广义资源边界计划见 `2026-07-31-bounded-request-processing.md`。

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。

**目标：** 阻止未认证客户端通过 RESP 聚合类型声明长度或递归嵌套触发无界初始分配、capacity-overflow panic 或栈耗尽。

**架构：** 在 `resp` crate 内增加统一的聚合长度和嵌套深度校验 helper，按 Redis 8.8.1 的 `INT_MAX` 边界拒绝超限声明，聚合容器从空向量开始并只随成功解析元素增长，在第 129 层聚合解析前返回错误。四种聚合解析器共享门禁，网络层继续复用现有协议错误关闭连接路径。

**技术栈：** Rust 2024、`nom` streaming parser、`bytes`、Cargo tests、Clippy

---

## 文件结构

- 修改：`src/resp/src/parse.rs`，包含聚合长度校验、Array/Map/Set/Push 调用点和回归测试。
- 创建：`docs/superpowers/specs/2026-07-31-resp-parser-resource-limits-design.md`，冻结问题、边界与验收合同。
- 创建：`docs/superpowers/plans/2026-07-31-resp-parser-resource-limits.md`，记录 TDD 实施步骤。
- 修改：`.planning/STATE.md`，记录当前实现任务、授权边界和验证结果。
- 修改：`.planning/KANBAN.md`，把已合并 PR #388 归档并登记唯一进行中的 `RESP-LIMITS-001`。

### 任务 1：建立会安全失败的超限回归测试

**文件：**
- 修改：`src/resp/src/parse.rs` 的 `tests` 模块

- [x] **步骤 1：编写失败测试**

```rust
#[test]
fn test_reject_oversized_aggregate_lengths_without_panicking() {
    for frame in [
        "*9223372036854775807\r\n",
        "%9223372036854775807\r\n",
        "~9223372036854775807\r\n",
        ">9223372036854775807\r\n",
    ] {
        let mut parser = RespParse::new(RespVersion::RESP3);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            parser.parse(Bytes::copy_from_slice(frame.as_bytes()))
        }));

        assert!(matches!(result, Ok(RespParseResult::Error(_))), "{frame:?}");
    }
}
```

- [x] **步骤 2：运行测试验证失败**

运行：`cargo test -p resp parse::tests::test_reject_oversized_aggregate_lengths_without_panicking -- --exact --nocapture`

预期：FAIL；现有实现发生 `capacity overflow`，`catch_unwind` 返回 `Err`，断言失败。

### 任务 2：实现统一长度校验和零声明预分配

**文件：**
- 修改：`src/resp/src/parse.rs` 的常量、helper 和四个聚合解析函数

- [x] **步骤 1：增加常量和 helper**

```rust
const MAX_AGGREGATE_LENGTH: i64 = i32::MAX as i64;
fn validate_aggregate_length(
    input: &[u8],
    len: i64,
) -> Result<(), nom::Err<nom::error::Error<&[u8]>>> {
    if len > MAX_AGGREGATE_LENGTH {
        return Err(nom::Err::Failure(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )));
    }

    Ok(())
}
```

- [x] **步骤 2：在四个调用点使用 helper**

各解析函数完成负数语义处理后调用 `Self::validate_aggregate_length(input, len)?`，并使用 `Vec::new()`。Array/Map/Set/Push 的容量只由完整元素的 `push` 增长，不由声明头触发。

- [x] **步骤 3：运行红灯测试并确认转绿**

运行：`cargo test -p resp parse::tests::test_reject_oversized_aggregate_lengths_without_panicking -- --exact --nocapture`

预期：PASS；四种 frame 均返回 `RespParseResult::Error`，没有 panic。

### 任务 3：限制聚合递归深度

**文件：**
- 修改：`src/resp/src/parse.rs` 的递归入口和 `tests` 模块

- [x] **步骤 1：编写并运行第 129 层失败测试**

运行：`cargo test -p resp parse::tests::test_reject_aggregate_nesting_beyond_limit -- --exact --nocapture`

预期红灯：旧实现返回 `Complete` 或 `Incomplete`，没有拒绝第 129 层。

- [x] **步骤 2：增加统一深度门禁**

`parse_resp_data` 传递当前聚合深度；Array、Map、Set、Push 在进入解析和分配前调用统一 helper。允许 128 层，拒绝第 129 层。

- [x] **步骤 3：确认深度测试转绿**

同一步骤 1 的 exact 命令实际运行 1 个测试并 PASS；另补 128 层成功边界和四种聚合前缀覆盖。

### 任务 4：覆盖分配和长度边界并验证兼容行为

**文件：**
- 修改：`src/resp/src/parse.rs` 的 `tests` 模块

- [x] **步骤 1：增加最大合法边界测试**

```rust
#[test]
fn test_maximum_aggregate_lengths_do_not_preallocate_declared_size() {
    for frame in [
        "*2147483647\r\n",
        "%2147483647\r\n",
        "~2147483647\r\n",
        ">2147483647\r\n",
    ] {
        let mut parser = RespParse::new(RespVersion::RESP3);
        assert_eq!(
            parser.parse(Bytes::copy_from_slice(frame.as_bytes())),
            RespParseResult::Incomplete,
            "{frame:?}"
        );
    }
}
```

使用测试侧线程局部分配计数器直接测量真实解析行为：在计量前构造输入并预留 parser buffer，四种最大合法声明头和 128 层未完成 Array 头在解析阶段都必须为零分配。首版 1024 容量上限在这两个测试中分别实测约 73 KB 和 9.4 MB，并按预期红灯失败。

- [x] **步骤 2：运行 resp crate 完整测试**

运行：`cargo test -p resp`

预期：全部 PASS，现有正常、空、null、分片和 RESP3 聚合解析行为不变。

- [x] **步骤 3：运行静态检查**

运行：`cargo clippy -p resp --all-targets -- -D warnings -D clippy::unwrap_used`、`cargo fmt --all -- --check`、`git diff --check`

预期：三条命令退出码均为 0，无 warning、格式或 whitespace error。

### 任务 5：同步项目状态并交付 PR

**文件：**
- 修改：`.planning/STATE.md`
- 修改：`.planning/KANBAN.md`

- [x] **步骤 1：记录验证证据**

把任务 3、4 的 exact 命令、环境、结果和最终 commit 写入 STATE/KANBAN；不得把尚未完成的 GitHub checks 写成通过。

- [x] **步骤 2：检查范围**

运行：`git status --short`、`git diff --stat origin/main...HEAD`、`git diff origin/main...HEAD`

预期：只有本计划“文件结构”列出的五个路径；无 Cargo、网络、认证、#402 文档或 Hot Tier 改动。

- [x] **步骤 3：提交并推送 review 修复**

首版实现提交：`fix(resp): bound aggregate parser allocations`。独立 review 修复必须在二次审查和最终验证后另行提交并 push。

- [x] **步骤 4：创建独立 PR**

PR base 必须是 `main`，正文引用 Issue #395 和 PR #402，但不得使用 `Closes #395`，因为本 PR 只处理其中 B1。
