# Python 集成测试与 Redis 兼容修复设计

## 背景

方案 A 的 RocksDB-only 实现已在 Windows 和 WSL 的 Rust 门禁中通过，但 WSL 真实启动 Kiwi 后，Python 集成测试得到 `32 passed, 23 failed`。现有 GitHub integration job 没有启动 Kiwi；连接失败被 fixture 转换为 skip，因此历史 CI 出现过 `55 skipped` 但 job 成功的假绿。

失败可归纳为四类：

1. CI 没有管理真实 Kiwi 进程，且测试连接失败时不阻断。
2. `KEYS test_*` 不支持 glob，复合类型扫描分支不可达，导致 fixture 清理完全失效。
3. GET/MGET 使用 lossy UTF-8，RESP3 null 仍按 RESP2 编码，造成二进制损坏和客户端超时。
4. Python 测试包含错误的 Redis 语义假设；同时默认多 RocksDB 实例下，MSET/MGET/DEL 缺少统一的多键并发门禁。

## 目标

- Ubuntu CI 启动一台使用临时配置和临时数据目录的真实 Kiwi，并以 PING 作为 readiness。
- CI 模式下服务不可用必须失败，不能 skip；每个测试使用 FLUSHDB 隔离。
- GET/MGET 对任意字节二进制安全。
- RESP2 和 RESP3 分别使用各自合法的 null 编码。
- Python 测试符合 Redis MSET 的覆盖、参数和并发观察语义。
- MSET、MGET、DEL 在 Kiwi 进程运行期间使用一致的确定顺序多键锁，避免并发客户端观察到跨实例部分更新。
- 修复 KEYS 的 glob 匹配和复合类型枚举，使已确认的兼容问题不继续遗留。

## 非目标

- 本阶段不实现多个独立 RocksDB 数据库之间的崩溃原子事务。进程崩溃或掉电时的跨数据库部分提交需要 WAL/事务协调器或存储布局调整，另行设计。
- 不把 Python 进程管理逻辑放入跨平台 Rust 单元测试或 Windows CI。
- 不改变普通开发者直接运行 pytest 时“服务器不存在则 skip”的便利行为；严格模式只由专用脚本显式开启。

## 设计

### CI 生命周期

新增 `tests/run_python_integration.sh`：

- 使用 `set -Eeuo pipefail`。
- 检查 `KIWI_BIN`（默认 `target/debug/kiwi`）存在且可执行。
- 在启动前确认 `127.0.0.1:6379` 没有被占用。
- 在 `${RUNNER_TEMP:-/tmp}` 下创建临时目录、配置、data-dir 和日志。
- 启动 Kiwi 后同时检查 PID 存活和 redis-py PING，readiness 有界等待。
- 以 `KIWI_TEST_REQUIRE_SERVER=1`、`KIWI_TEST_ISOLATED_SERVER=1` 运行现有 `make -C tests test-python`。
- pytest 返回后确认 Kiwi 未意外退出。
- EXIT/INT/TERM trap 优先发送 SIGINT，超时后 TERM/KILL，保留 pytest 原始退出码；失败时输出服务日志并删除临时目录。

`.github/workflows/ci.yml` 的 Ubuntu integration job 调用该脚本，不再直接调用 pytest。

### pytest 严格模式与隔离

`tests/python/conftest.py` 增加：

- session autouse 服务可用性门禁：严格模式连接失败调用 `pytest.fail`，普通模式保留 skip。
- function autouse 隔离 fixture：仅专用隔离模式下，在每个测试前后调用 FLUSHDB。
- `redis_clean` 在隔离模式直接复用 client；普通模式保留 best-effort 清理，以免误清理开发者外部 Redis。
- binary client 也由 session availability 和 function isolation 覆盖。

### 二进制 GET/MGET

存储读取接口统一返回 `Vec<u8>` / `Option<Vec<u8>>`，命令层直接构造 bulk string，不再经过 `String::from_utf8_lossy`。数值命令仍在各自需要解析文本时显式转换。

### RESP null

`RespEncoder` 根据 negotiated version 编码空值：

- RESP2：bulk null 为 `$-1\r\n`，array null 为 `*-1\r\n`。
- RESP3：所有 null 语义编码为 `_\r\n`，包括 `BulkString(None)` 和 `Array(None)`。

### KEYS

MetaCF 的 key 解码只执行一次，然后根据 value 的 data type/parser 判断存活。pattern 使用 Redis glob 语义，至少覆盖 `*`、`?`、字符类和转义；测试覆盖字符串及 list/hash/set/zset。

### 多实例并发可见原子性

在 Storage 层提供统一的二进制 key 到锁标识转换，并让 MSET、MGET、DEL 在分组访问实例之前获取同一批确定顺序锁。这样这些多键命令之间不会互相穿插。单实例继续使用 RocksDB WriteBatch；多实例仍逐实例 commit，但进程内读写可见性被锁住。

### 测试语义修正

- MSET 覆盖 list/hash/set/zset 后断言 TYPE 为 string、GET 返回新值，旧类型命令返回 WRONGTYPE。
- 空 MSET 断言参数数量错误。
- 并发原子性读取使用一次 MGET，不再使用两次独立 GET。
- 线程内断言必须把异常传回主测试线程，禁止仅产生 pytest warning。

## 验证

- 每项生产修复先运行对应回归测试并确认红灯，再实现绿灯。
- Windows：fmt-check、workspace check、workspace Clippy、相关 Rust 测试。
- WSL：workspace build、CI 同款 Clippy、全量 cargo test。
- WSL：脚本负路径、启动/清理路径和完整 55 个 Python 测试。
- 最终由独立子代理依次做规格符合性审查和代码质量审查。
