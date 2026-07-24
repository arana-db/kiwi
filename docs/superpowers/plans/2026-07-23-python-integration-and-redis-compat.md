# Python 集成测试与 Redis 兼容修复实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。

**目标：** 让 Ubuntu CI 真实运行 Kiwi Python 集成测试，并修复由真实运行暴露的二进制、RESP3 null、KEYS 和多实例并发可见原子性问题。

**架构：** Bash 脚本只负责专用 Kiwi 的生命周期，pytest fixture 只负责严格连接和测试隔离。存储与协议修复分别由字节接口、版本感知 null 编码、统一多键锁和 MetaCF glob 扫描完成；跨 RocksDB 崩溃事务明确不在本阶段范围内。

**技术栈：** Rust、RocksDB、RESP2/RESP3、pytest、redis-py、GitHub Actions、Bash、WSL Ubuntu。

---

## 文件职责

- 创建 `tests/run_python_integration.sh`：Ubuntu 专用服务启动、readiness、pytest、日志和清理。
- 修改 `.github/workflows/ci.yml`：调用真实集成测试脚本。
- 修改 `tests/python/conftest.py`：严格服务门禁和专用 FLUSHDB 隔离。
- 修改 `src/resp/src/encode.rs`：协议版本感知的 null 编码和测试。
- 修改 `src/storage/src/redis_strings.rs`：二进制 GET/MGET、KEYS glob/复合类型扫描和单元测试。
- 修改 `src/storage/src/storage_impl.rs`：字节接口及 MSET/MGET/DEL 统一多键锁。
- 修改 `src/cmd/src/get.rs`、`src/cmd/src/mget.rs`：直接返回原始字节。
- 修改 `src/net/tests/storage_command_e2e_tests.rs`：真实网络二进制和 RESP null 回归测试。
- 修改 `tests/python/test_mset.py`、`tests/python/test_mset_concurrent.py`、`tests/python/test_wrongtype_errors.py`、`tests/python/test_list_commands.py`：修正测试语义和线程错误传播。

### 任务 1：真实 Python CI 生命周期和测试隔离

- [ ] 在当前实现下顺序运行两个 list 测试，记录第二个因数据残留失败的红灯。
- [ ] 为脚本添加可执行文件存在、端口占用、PING readiness、pytest 退出码、服务退出和 trap 清理逻辑。
- [ ] 在 conftest 中增加 `KIWI_TEST_REQUIRE_SERVER`、`KIWI_TEST_ISOLATED_SERVER` 和 autouse availability/isolation fixtures。
- [ ] 将 workflow 的 integration test 步骤改为 `bash tests/run_python_integration.sh`。
- [ ] 运行两个 list 测试，预期 `2 passed`；运行不存在 binary 的脚本负路径，预期非零且 pytest 未启动。
- [ ] 提交 `ci(test): run Python integration tests against Kiwi`。

### 任务 2：RESP3 null 合法编码

- [ ] 在 `src/resp/src/encode.rs` 添加测试：RESP3 编码 `BulkString(None)` 和 `Array(None)` 均为 `_\r\n`；先运行并确认当前得到 `$-1`/`*-1` 的红灯。
- [ ] 最小修改 `encode_resp_data`，按 `self.version` 选择 RESP2/RESP3 null。
- [ ] 添加网络 E2E：RESP3 连接对不存在键执行 LINDEX/MGET，不得超时且解析为 null。
- [ ] 运行 resp 和 net 相关测试，确认绿灯。
- [ ] 提交 `fix(resp): encode nulls for negotiated protocol`。

### 任务 3：GET/MGET 二进制安全

- [ ] 添加 storage 和网络回归测试，写入 `[0, 1, 2, 3, 255]` 后 GET/MGET 必须逐字节相同；确认当前红灯显示 replacement bytes。
- [ ] 将 Redis/Storage 的 get 和 mget 返回类型改为字节容器，命令层直接构造 bulk string。
- [ ] 修复所有编译调用方，不改变数值命令的显式文本解析。
- [ ] 运行 storage、cmd、net 测试和 Python binary MSET 测试，确认绿灯。
- [ ] 提交 `fix(storage): preserve binary string values`。

### 任务 4：KEYS glob 与复合类型扫描

- [ ] 添加 storage 测试：`test_*` 匹配 string/list/hash/set/zset，非匹配键不返回；确认当前红灯。
- [ ] 提取/复用 glob matcher，修复重复的不可达 ParsedBaseKey 分支，并按实际 data type 判断存活。
- [ ] 覆盖 `*`、`?`、字符类、转义和过期/空复合类型。
- [ ] 运行 storage KEYS 测试和相关 Python 测试，确认绿灯。
- [ ] 提交 `fix(storage): support Redis key glob matching`。

### 任务 5：多实例命令的进程内原子可见性

- [ ] 添加多实例 storage 并发回归测试：writer 在同一 MSET 更新跨实例键，reader 使用 MGET 时只能看到完整旧批次或完整新批次；DEL 竞态也只能返回全有或全无。
- [ ] 确认当前测试可复现混合结果。
- [ ] 提取二进制安全的锁标识并让 MSET、MGET、DEL 在实例分组前获取同一批确定顺序锁。
- [ ] 避免与 Redis 内部已持有的同一 LockMgr 重入；仅包围当前无内部多键锁的入口。
- [ ] 运行 storage 并发测试和 Python `test_concurrent_mset_and_get`、`test_race_condition_delete_and_set`。
- [ ] 提交 `fix(storage): serialize multi-instance multi-key commands`。

### 任务 6：修正 Python Redis 语义与线程断言

- [ ] 将 MSET-on-composite 测试改为断言覆盖为 string，而不是 WRONGTYPE。
- [ ] 将 empty dict 测试改为断言 arity ResponseError。
- [ ] 将两次 GET 的并发观察改为一次 MGET，并把工作线程中的断言/异常汇总回主线程。
- [ ] 先在未完成产品修复的对应 commit 上确认测试能捕获二进制/null/原子性缺陷，再在最终实现上运行绿灯。
- [ ] 提交 `test: align Python integration tests with Redis semantics`。

### 任务 7：完整验证、审查、push 和 PR

- [ ] Windows 运行 `make fmt-check`、workspace check、workspace Clippy 和受影响 crate 测试。
- [ ] WSL 使用现有 Linux target 运行 workspace build、CI 同款 Clippy、全量 `cargo test`。
- [ ] WSL 运行 `bash tests/run_python_integration.sh`，确认 55 个测试无 server skip、无失败、无线程 warning，并确认 Kiwi PID 和临时目录被清理。
- [ ] 用独立子代理先做规格符合性审查，修复后复审通过。
- [ ] 用独立子代理做代码质量审查，修复后复审通过。
- [ ] 检查最终 diff、状态和 commit 历史，push `codex/issue-349-remove-engine`。
- [ ] 创建 conventional-commit 标题的 PR，描述方案 A、测试证据以及不包含跨 RocksDB 崩溃事务的明确边界。
