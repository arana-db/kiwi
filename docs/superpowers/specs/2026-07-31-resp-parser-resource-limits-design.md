# RESP 聚合类型资源限额设计

> 状态：已批准
> 日期：2026-07-31
> 基线：`main` at `cbc28958f261ae049d67a8b4a9d904d794b37726`
> Requirement：`REQ-COMPAT-002`、`REQ-COMPAT-006`、`REQ-WORK-003`
> 跟踪：Issue #395 B1；来源审查 PR #402

## 问题

`RespParse` 在认证检查之前解析客户端输入。Array、Map、Set 和 Push
把客户端声明的 `i64` 长度直接传给 `Vec::with_capacity`。攻击者只需发送
聚合类型头部，就能让进程尝试按未受信任的声明值预分配内存；超过布局上限时
还会触发 capacity-overflow panic。

单层容量上限仍不足以封闭该路径。聚合元素会递归进入同一解析器，攻击者可以用
重复的最大合法聚合头让每层都保留 1024 个槽位的容量，并在收到完整 frame 之前
持续增加调用栈和累计预分配。因此递归深度也必须在分配前受限。

PR #402 中“固定载荷约分配 80GB”的表述不准确，因为实际元素类型是
`RespData`，而且分配结果取决于平台分配器和 overcommit。本设计只依赖已经由
源码证明的根因：未认证输入直接控制初始分配大小。

## 目标

- 客户端声明值不得直接决定聚合容器的初始分配大小。
- 超过 Redis 8.8.1 可接受整数范围的聚合长度必须返回协议解析错误。
- 最大合法声明值只有头部、尚无元素时，应快速返回 `Incomplete`，不能尝试巨额分配。
- 聚合嵌套超过固定解析深度时必须返回错误，不能继续增长调用栈和累计预分配。
- 正常 Array、Map、Set、Push 及负数 null 语义保持不变。

## 非目标

- 本 PR 不引入完整的 `proto-max-bulk-len` 配置链路。
- 本 PR 不限制实际流入的 bulk payload 或连接累计 parser buffer；这些需要独立的
  请求大小和连接资源合同。
- 本 PR 不修改 #402 的文档内容，也不处理 Issue #395 的其他条目。
- 本 PR 不改变 RESP 编码、命令执行、认证或网络生命周期。

## 方案

在 `src/resp/src/parse.rs` 增加一个私有 helper：

1. 拒绝大于 `i32::MAX` 的聚合长度。Redis 8.8.1 的 multibulk 解析同样拒绝
   超过 `INT_MAX` 的声明值，因此该边界不会扩大 Kiwi 与目标 Oracle 的差异。
2. 使用 `usize::try_from` 做 checked conversion。
3. 初始容量取 `min(declared_len, 1024)`。1024 与 Redis 8.8.1 的初始 argv
   分配上限一致；实际收到更多元素时由 `Vec` 正常增长。
4. Array、Map、Set、Push 共用 helper，避免四条路径以后再次漂移。
5. 四种聚合类型共用 128 层嵌套门禁，并在解析头部和分配容器前拒绝第 129 层。
   该值把单次解析的递归栈和同时存活的单层预分配限制在确定范围内。
6. 超限通过 `nom::Err::Failure` 返回，现有网络层会把
   `RespParseResult::Error` 转为 `InvalidData` 并关闭当前连接。

## 测试

- 红灯：四种聚合类型输入 `i64::MAX`，当前实现会 panic；测试要求捕获调用结果并
  断言解析器返回 `RespParseResult::Error`。
- 绿灯：应用 helper 后，上述四种输入均返回错误且不 panic。
- 容量：直接断言 helper 对 `0/1/1024/1025/i32::MAX` 返回
  `0/1/1024/1024/1024`，确定性固定预分配上限。
- 长度边界：`i32::MAX` 只有头部时返回 `Incomplete`；`i32::MAX + 1` 返回错误。
- 深度红灯：第 129 层聚合在旧实现中仍会继续递归；测试要求返回解析错误。
- 深度边界：128 层完整嵌套可解析，第 129 层对 Array、Map、Set、Push 均返回错误。
- 兼容：保留并运行现有正常、空、null、分片输入及 RESP3 聚合类型测试。

## 验收

- `cargo test -p resp`
- `cargo clippy -p resp --all-targets -- -D warnings -D clippy::unwrap_used`
- `cargo fmt --all -- --check`
- `git diff --check`
- exact Head 的 GitHub CI 通过后，才能把远端验证状态表述为完成。
