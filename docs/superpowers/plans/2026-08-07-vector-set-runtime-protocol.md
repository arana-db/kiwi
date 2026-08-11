# VectorSet Runtime Admission 与协议语义实施计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development 逐任务执行，使用 superpowers:test-driven-development 先观察 RED，提交前使用 superpowers:verification-before-completion。步骤使用复选框（`- [ ]`）语法跟踪进度。

**目标：** 在 RESP payload 第一次 `Bytes -> Vec<u8>` 深拷贝和 `StorageCommand::Execute` 构造之前，用启动时同一份 `Config.vector` 对所有 Vector payload 命令完成无分配、checked arithmetic admission；同时删除 VADD argv-shape 错误猜测，建立 function-scoped RESP2/RESP3 raw differential 客户端。

**架构：** `RespData::BulkString(Bytes)` 在 `ParsedCommand` 中保持 `Bytes`。Network handler 执行 unknown-command lookup、AUTH、generic arity、feature gate、cluster gate 和 Vector resource admission；只有通过后才构造现有 `Client.argv: Vec<Vec<u8>>` 并进入 storage runtime。Cmd trait 的 admission hook 由双层 `GatedCmd` 显式转发，防止 wrapper 绕过。

**技术栈：** Rust `bytes::Bytes`、Tokio network runtime、RESP parser、cmd table、bounded storage channel、Python socket、pytest、Redis 8.8.1 raw RESP2/RESP3 Oracle。

---

## 固定顺序与边界

Network 处理顺序固定为：

```text
RESP parse to Bytes
-> command lookup
-> authentication / NOAUTH
-> generic arity
-> vector-enabled gate
-> cluster unsupported gate
-> vector resource admission
-> Bytes -> Vec<u8> conversion
-> follower redirect / leader read barrier
-> do_initial
-> StorageCommand::Execute
```

- 本工作流不修改 `StorageCommand::Execute` 的 serde/wire 类型。
- 不把全局 `ClientContext.argv` 改为 `Bytes`。
- 不删除 storage-side `VectorParseLimits`；它继续保护非 network caller。
- 不在 net runtime 使用 `VectorConfig::default()` 或查询 `StorageOptions`。
- 不在 admission 中解析 `f32`、构造 `CanonicalVector`、访问 storage 或判定 missing/WRONGTYPE/member existence。
- 超限请求允许在 storage 语义之前返回本地 resource error，但必须在 compatibility manifest 登记 operational-limit difference。

## Task 1：无分配 Vector admission 纯函数

**Requirement：** `REQ-VECTOR-005`

**文件：**

- Create: `src/cmd/src/vector/admission.rs`
- Modify: `src/cmd/src/vector/mod.rs`

- [x] **步骤 1：写入失败测试**

  - `vector::admission::tests::admission_checks_fp32_actual_bytes_and_dimension_bytes`
  - `vector::admission::tests::admission_counts_values_raw_token_bytes`
  - `vector::admission::tests::admission_checks_all_element_payload_commands`
  - `vector::admission::tests::admission_rejects_checked_arithmetic_overflow`
  - `vector::admission::tests::admission_defers_under_limit_invalid_syntax_to_command_parser`
  - `vector::admission::tests::admission_checks_partial_values_before_parser_error`

  ```powershell
  cargo test -p cmd vector::admission::tests -- --nocapture
  ```

  预期 RED：当前没有 `VectorAdmissionLimits` 和 `admit_vector_request`。

- [x] **步骤 2：实现限额类型和纯函数**

  ```rust
  pub struct VectorAdmissionLimits {
      pub max_dimension: usize,
      pub max_element_bytes: usize,
      pub max_vector_bytes: usize,
  }

  pub fn admit_vector_request(
      argv: &[Bytes],
      limits: VectorAdmissionLimits,
  ) -> Result<(), VectorAdmissionError>;
  ```

  命令覆盖：`VADD`、`VSIM`、`VEMB`、`VREM`、`VISMEMBER`。

  计数规则：

  - FP32：检查 blob 实际 byte length、`len / 4`、element raw length；`len % 4 != 0` 且未超限时交给 parser。
  - VALUES：检查 dimension 十进制 checked parse、`dimension.checked_mul(4)`、所有已提供 value token raw length 的 `checked_add`；两种 byte 计数都必须不超 `max_vector_bytes`。
  - dimension 语法非法时返回 `Ok(())` 交给 parser；整数溢出、乘法溢出或累加溢出返回稳定 resource error。
  - element 按 raw bulk length 检查，不解码 UTF-8。
  - 不计 key、command name 和 option token，不新增未批准的全 argv 限额。

- [x] **步骤 3：GREEN 与 mutant 检查**

  重跑 Task 1 测试；删除 VALUES raw-token 累加、把 `checked_add` 改为普通加法或删除任一 element command 分支时对应测试必须失败。

  实际证据：clean baseline 的现有 Vector 命令测试 13/13 通过；tests-first 运行因 `VectorAdmissionLimits`、`VectorAdmissionError`、`admit_vector_request` 和 checked-sum helper 尚不存在而以编译错误 RED。实现后 admission 6/6、完整 `cmd` 167/167 通过，`cargo clippy -p cmd --all-targets --all-features -- -D warnings`、`cargo fmt --all -- --check` 与 `git diff --check` 通过。删除 VALUES raw-byte 限额使 `admission_counts_values_raw_token_bytes` 失败，普通加法使 overflow 测试 panic，删除 `VISMEMBER` element 分支使全 element 命令表格测试失败；恢复实现后全部回归重新通过。规格复审发现畸形 FP32 在 byte limit 内但 `len / 4` 超 dimension 时曾错误 defer；新增精确测试先以 `Ok(()) != DimensionLimit` RED，再移除余数对 dimension 检查的屏蔽。最终规格复审 `RUNTIME_TASK1_SPEC_PASS`，质量复审 `RUNTIME_TASK1_QUALITY_PASS`。

## Task 2：Cmd admission hook 与双层 `GatedCmd` 转发

**Requirement：** `REQ-VECTOR-005`、`REQ-RAFT-008`

**文件：**

- Modify: `src/cmd/src/lib.rs`
- Modify: `src/cmd/src/table.rs`
- Modify: `src/cmd/src/vector/vadd.rs`
- Modify: `src/cmd/src/vector/vsim.rs`
- Modify: `src/cmd/src/vector/vemb.rs`
- Modify: `src/cmd/src/vector/vrem.rs`
- Modify: `src/cmd/src/vector/vismember.rs`

- [x] **步骤 1：写入 wrapper 绕过失败测试**

  - `table::tests::gated_vector_commands_forward_network_admission`
  - `table::tests::network_admission_feature_gate_precedes_cluster_gate`
  - `table::tests::network_admission_cluster_gate_precedes_resource_limit`
  - `table::tests::non_vector_commands_use_noop_network_admission`

  ```powershell
  cargo test -p cmd table::tests::gated_vector_commands_forward_network_admission -- --exact
  cargo test -p cmd table::tests::network_admission_feature_gate_precedes_cluster_gate -- --exact
  cargo test -p cmd table::tests::network_admission_cluster_gate_precedes_resource_limit -- --exact
  ```

- [x] **步骤 2：实现 hook 和显式转发**

  - `Cmd` 增加默认 no-op `admit_network_request(&[Bytes], VectorAdmissionLimits)`。
  - 五个 payload command 覆盖 hook 并调用 Task 1 纯函数。
  - `GatedCmd::admit_network_request` 先执行当前 wrapper 的 `allowed/error`，允许时再转发 inner。
  - 保留现有 `check_pre_route` cluster 防御，不把所有 command `do_initial` 前移。

- [x] **步骤 3：回归**

  ```powershell
  cargo test -p cmd table::tests -- --nocapture
  cargo test -p cmd vector::admission::tests -- --nocapture
  ```

  实际证据：四个 tests-first 用例先因 `dyn Cmd` 不存在 `admit_network_request` 而编译 RED；实现默认 no-op hook、Vector feature/cluster 双层 `GatedCmd` 显式转发和五个 payload 命令 override 后全部转绿，完整 `cmd` 171/171 与 all-targets/all-features clippy 通过。删除 wrapper 转发、删除 `VISMEMBER` override、让 inner 在 outer 前执行时对应测试均失败。质量复审发现通用 `GatedCmd` 还包装 FLUSH，若无区分会提前改变非 Vector 顺序；真实 disabled-FLUSH admission 测试先 RED，随后增加仅 Vector 两层启用的 `reject_during_network_admission` 私有标志，FLUSH 继续由原 `do_initial` 门禁处理。最终规格复审 `RUNTIME_TASK2_SPEC_PASS`，质量复审 `RUNTIME_TASK2_QUALITY_PASS`。

## Task 3：`ParsedCommand<Bytes>`、Config 传递与真实 TCP storage spy

**Requirement：** `REQ-VECTOR-005`、`REQ-OBS-001`

**文件：**

- Modify: `src/net/src/network_handle.rs`
- Modify: `src/net/src/handle.rs`
- Modify: `src/net/src/network_server.rs`
- Modify: `src/net/src/lib.rs`
- Modify: `src/server/src/main.rs`
- Modify: `src/net/tests/storage_command_e2e_tests.rs`

- [x] **步骤 1：写入 copy-boundary 和 dispatch 失败测试**

  - `network_handle::tests::parsed_command_keeps_bulk_bytes_until_admission`
  - `network_handle::tests::parsed_command_admission_precedes_client_payload_copy`
  - `network_handle::tests::non_bulk_arguments_preserve_existing_empty_argument_semantics`
  - `storage_command_e2e_vector_admission_rejects_before_storage_dispatch`
  - `storage_command_e2e_vector_admission_uses_values_raw_byte_count`
  - `storage_command_e2e_vector_admission_checks_all_element_commands`
  - `storage_command_e2e_vector_admission_preserves_auth_pipeline_order`
  - `storage_command_e2e_vector_admission_preserves_static_gate_order`
  - `storage_command_e2e_vector_admission_dispatches_under_limit_once`
  - `server::tests::vector_admission_limits_follow_config`
  - `net::tests::server_factory_forwards_vector_admission_limits`

  ```powershell
  cargo test -p net network_handle::tests::parsed_command_keeps_bulk_bytes_until_admission -- --exact
  cargo test -p net --test storage_command_e2e_tests storage_command_e2e_vector_admission_rejects_before_storage_dispatch -- --exact
  cargo test -p server tests::vector_admission_limits_follow_config -- --exact
  ```

  预期 RED：当前 `ParsedCommand` 已深拷贝，network 不持有 limits，超限请求会进入 storage channel。

- [x] **步骤 2：实现 `Bytes` 保留和必填 limits 连接**

  ```rust
  struct ParsedCommand {
      cmd_name: Bytes,
      argv: Vec<Bytes>,
  }
  ```

  `extract_command_from_data` 消费 `RespData` 并 move bulk `Bytes`。`VectorAdmissionLimits` 作为必填参数沿以下路径传递：

  ```text
  server::start_server
  -> ServerFactory::create_server
  -> create_network_server
  -> NetworkServer::new / field
  -> process_connection_with_storage_client_until_cancelled
  -> process_network_connection_until_cancelled
  -> process_command_batch
  ```

  `process_command_batch` 在 admission 成功后才执行 `Bytes::to_vec()` 和 `Client::set_argv`。

  storage spy 使用现有 `ChannelStats.requests_sent`：超限前后不变，under-limit control 恰好增加 1。AUTH pipeline 覆盖同一 TCP write 中的 `AUTH` + over-limit VADD；未认证时 `NOAUTH` 优先。

- [x] **步骤 3：生产连接变异检查**

  删除 `Config.vector -> VectorAdmissionLimits -> NetworkServer` 任一连接点、恢复 `ParsedCommand: Vec<Vec<u8>>` 或把 admission 移到 `executor_ext.rs` 时对应测试必须失败。

- [x] **步骤 4：回归**

  ```powershell
  cargo test -p net network_handle::tests -- --nocapture
  cargo test -p net --test storage_command_e2e_tests storage_command_e2e_vector_admission -- --nocapture
  cargo test -p server -- --nocapture
  ```

  实际证据：copy-boundary 测试先因 `ParsedCommand` 仍为 `Vec<Vec<u8>>` 编译 RED；真实 TCP storage spy 随后观察到超限 VADD 返回 `Integer(1)` 并进入 storage channel。实现把 bulk payload 作为 `Bytes` 保持到 AUTH、generic arity、Vector feature/cluster/resource admission 全部通过之后，再进行唯一一次 `Bytes -> Vec<u8>` 转换，并把 `Config.vector` limits 作为必填值沿完整 production chain 传入。六个 admission E2E、完整 33 项 storage E2E、全 `net` 和全 `server` 回归通过。断开 Config 映射、在 factory 中替换 limits、绕过 admission、在 admission 前覆盖 `Client.argv` 时对应测试均 RED；独立复审补充的 direct-batch sentinel/pointer 测试和 factory full-path 测试闭合了“把 admission 移到 executor”与 factory 转发盲区。

## Task 4：VADD 类型化 parse outcome 和错误优先级

**Requirement：** `REQ-VECTOR-003`

**文件：**

- Modify: `src/cmd/src/vector/vadd.rs`

- [x] **步骤 1：写入能杀死 argv-shape heuristic 的失败测试**

  - `complete_values_without_element_returns_wrong_arity`
  - `complete_fp32_without_element_returns_wrong_arity`
  - `invalid_values_token_without_element_stays_invalid_vector`
  - `invalid_fp32_length_without_element_returns_wrong_arity`
  - `invalid_fp32_length_with_fifth_argument_stays_invalid_vector`
  - `invalid_trailing_option_after_element_stays_typed`

  ```powershell
  cargo test -p cmd vector::vadd::tests -- --nocapture
  ```

  关键 RED：`VADD key VALUES 1 not-a-float` 在 Redis 的五参数入口内必须保持 invalid vector，不能再由 argv-shape heuristic 改写为 WrongArity。Redis 8.8.1 在 parser 前对全部四参数请求执行 `argc < 5` WrongArity，因此 `VADD key FP32 <3 bytes>` 也必须是 WrongArity；使用第五个 placeholder 才能证明非法 FP32 进入 parser 后保持 invalid vector。

- [x] **步骤 2：实现 `VAddParseError`**

  - 保持 Redis 8.8.1 dispatcher 最小 arity `-5`；四参数 FP32 请求在 parser 前统一 WrongArity。
  - `parse_vadd_with_limits` 返回 `VAddParseError::{InvalidVector, MissingElement, InvalidOption, ResourceLimit}`。
  - 对已经通过 dispatcher 的请求，只有 vector 完整消费后缺 element 映射为 standard WrongArity。
  - element 后未知 option 返回 Redis 8.8.1 的 `ERR invalid option after element`。
  - 删除 `do_cmd` 中基于 `argv.len()` 的 heuristic。
  - storage-side limits 保留为 defense-in-depth。

- [x] **步骤 3：明确 raw differential 由 Task 5 交付**

  Task 4 只交付 Rust typed outcome；正式 frame-aware raw client 由 Task 5 创建，避免用 redis-py typed conversion 或临时单次 `recv()` 冒充 raw 证据。Task 5 的 `test_vadd_typed_error_precedence_raw` 分别在 RESP2/RESP3 覆盖：合法 VALUES 缺 element、四参数 FP32 WrongArity、非法 VALUES token 缺 element、带第五参数的非法 FP32 length、重复 option、element 后非法 option。

- [x] **步骤 4：回归**

  ```powershell
  cargo test -p cmd vector::vadd::tests -- --nocapture
  ```

  Task 5 建立 raw client 且 Oracle runtime lease 可用后执行：

  ```bash
  python3 -m pytest tests/python/test_vector_set_differential.py::test_vadd_typed_error_precedence_raw -vv
  ```

  实际证据：对照 exact Redis 8.8.1 commit `77b6c308396c9700672390a210143a8496fb4b10` 的 `commands.json` 与 `modules/vector-sets/vset.c` 后，纠正了原计划把 VADD arity 改为 `-4` 的错误前提；public dispatcher 保持 `-5`，unknown trailing option 对齐为 `ERR invalid option after element`。tests-first Vector 运行得到 19/23 通过、4 个预期失败，分别命中 arity、四参数 malformed FP32 和 trailing-option 语义；最小 typed outcome 实现后 Vector 23/23、完整 `cmd` 175/175 通过。恢复 argv-shape heuristic、把 `MissingElement` 合并为 `InvalidVector`、把 unknown option 降级为 generic invalid-vector 时对应回归均失败；`cargo clippy -p cmd --all-targets --all-features -- -D warnings`、`cargo fmt --all -- --check`、`git diff --check` 和 SDD validator self-test/正向校验通过。test-guard 删除了两个生产不可达的四参数私有 parser 断言，并保证 wire 文本不复用生产常量自证；独立复核结论为 `SPEC_PASS` 和 `QUALITY_PASS`。正式 raw RESP2/RESP3 节点仍由 Task 5 的 frame-aware client 交付，本 Task 没有用 redis-py typed value 代替 raw evidence。

## Task 5：function-scoped RESP2/RESP3 raw 客户端与 manifest difference

**Requirement：** `REQ-COMPAT-001`、`REQ-COMPAT-002`、`REQ-COMPAT-003`、`REQ-VECTOR-003`

**文件：**

- Create: `tests/python/raw_resp_client.py`
- Modify: `tests/python/conftest.py`
- Modify: `tests/python/test_vector_error_matrix.py`
- Modify: `tests/python/test_vector_set_differential.py`
- Modify: `tests/compat/redis-8.8.1/manifest.yaml`
- Modify: `tools/compat/tests/manifest.rs`

- [ ] **步骤 1：先写协议隔离和 raw frame 失败测试**

  - `test_protocol_clients_are_function_scoped_and_explicit`
  - `test_resp2_client_is_not_polluted_by_resp3_negotiation`
  - `test_vsim_missing_key_withscores_raw_frame`
  - `test_zero_vector_values_raw_differential`
  - `test_zero_vector_fp32_raw_differential`
  - `test_vadd_typed_error_precedence_raw`

  删除 `test_vector_error_matrix.py` 对 session-scoped `redis_binary_client` 发送 `HELLO 3` 的路径。

- [ ] **步骤 2：实现 frame-aware raw client**

  `raw_resp_client.py` 实现 `encode_command`、`RawRespConnection.connect`、`execute_raw`、`close`；reader 递归消费 Bulk/Array/Map 的完整 frame，不用单次 `recv()` 作为 frame 边界。

  - 每个测试函数、endpoint、protocol 使用独立 socket。
  - RESP3 在自己的 socket 发送 `HELLO 3` 并完整读取 handshake map。
  - binary FP32 和 NUL element 按 bulk length 编码。
  - required raw 断言比较 Kiwi/Redis 完整 frame，不只比较 redis-py typed value。

- [ ] **步骤 3：登记 operational-limit difference**

  对 `VADD`、`VSIM`、`VEMB`、`VREM`、`VISMEMBER` 登记 owner `cmd-vector`、Issue #421、affected `standalone_cache_off; resp2/resp3`、exact Redis source ref 和 remove_when。Manifest validator 必须拒绝缺失 owner/Issue/affected/ref/remove_when 的 difference。

- [ ] **步骤 4：回归**

  ```powershell
  cargo test -p kiwi-compat --test manifest
  python -m pytest tests/python/test_vector_error_matrix.py -vv
  python -m pytest tests/python/test_vector_set_differential.py --collect-only -q
  ```

  真实 raw differential 由 Oracle/CI 工作流的 verifier-supervised runner 执行；本 Task 交付的测试不得在 import/collection 时连接 endpoint。

## 工作流最终门禁

- [ ] `git diff --check`
- [ ] `cargo fmt --check`
- [ ] `cargo clippy -p cmd -p net -p server --all-targets --all-features -- -D warnings`
- [ ] `cargo test -p cmd`
- [ ] `cargo test -p net --test storage_command_e2e_tests`
- [ ] `cargo test -p server`
- [ ] `cargo test -p kiwi-compat --test manifest`
- [ ] `python -m pytest tests/python/test_vector_error_matrix.py -vv`
- [ ] `python -m pytest tests/python/test_vector_set_differential.py --collect-only -q`
- [ ] 规格 reviewer 确认 admission 位于第一次 payload 深拷贝前，feature/cluster gate 显式转发，under-limit 错误优先级不变。
- [ ] 代码质量 reviewer 确认无 net-side 默认 limits、无大 payload fixture、无一次 `recv()` frame parser、无全局 Client/StorageCommand ABI 重写。
