# RESP 聚合类型资源限额设计

> PR #406 已合并，本文件作为零声明预分配与 nesting 阶段记录保留；PR #404 的
> 广义资源边界合同见 `2026-07-31-bounded-request-processing-design.md`。
>
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

单层容量上限仍不足以封闭该路径。首版修复把每层初始容量限制为 1024，但分配
探针确认单个最大合法 Array 头仍申请约 73 KB，128 层未完成头会在一次解析中申请
约 9.4 MB；后续每个分片还会从保留 buffer 根部重解析并重复申请。因此声明头必须
采用零预分配，递归深度也必须在解析下一层前受限。

PR #402 中“固定载荷约分配 80GB”的表述不准确，因为实际元素类型是
`RespData`，而且分配结果取决于平台分配器和 overcommit。本设计只依赖已经由
源码证明的根因：未认证输入直接控制初始分配大小。

## 目标

- 客户端声明值不得触发聚合容器预分配；容量只能随成功解析出的元素增长。
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
2. Array、Map、Set、Push 共用长度校验 helper，避免四条路径以后再次漂移。
3. 聚合容器从 `Vec::new()` 开始，只有元素完整解析后才由 `push` 驱动增长；仅有
   声明头或未完成的下一元素时不为声明长度保留槽位。
4. Map 的 pair 容器使用同一策略，不按声明 pair 数预分配。
5. 四种聚合类型共用 128 层嵌套门禁，并在解析头部和分配容器前拒绝第 129 层。
   该值把单次解析的递归栈限制在确定范围内。
6. 超限通过 `nom::Err::Failure` 返回，现有网络层会把
   `RespParseResult::Error` 转为 `InvalidData` 并关闭当前连接。

## 测试

- 红灯：四种聚合类型输入 `i64::MAX`，当前实现会 panic；测试要求捕获调用结果并
  断言解析器返回 `RespParseResult::Error`。
- 绿灯：应用 helper 后，上述四种输入均返回错误且不 panic。
- 分配：测试在计量前构造输入并预留 parser buffer，再用线程局部分配计数器证明
  四种最大合法声明头和 128 层未完成 Array 头在解析阶段均为零分配；任何重新引入
  的声明驱动容器容量都会使测试失败。
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
