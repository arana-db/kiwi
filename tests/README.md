# Kiwi 测试目录

本目录包含 Kiwi 项目的各类测试用例，参考 [kiwi-cpp](https://github.com/OpenAtomFoundation/kiwi) 项目的测试组织结构。

## 📁 目录结构

```text
tests/
├── README.md                # 本文件
├── fix_client_requests.py   # 历史修复脚本（已完成任务）
├── python/                  # Python 集成测试
│   ├── test_mset.py         # MSET 命令测试
│   └── ...                  # 其他 Python 测试
├── integration/             # 集成测试文档和脚本
│   ├── test_mset.md         # MSET 测试指南
│   └── ...                  # 其他集成测试文档
├── tcl/                     # Redis 官方 TCL 测试用例（待添加）
│   └── ...                  # Redis 官方测试套件
└── go/                      # Go 语言测试用例（待添加）
    └── ...                  # Go 测试文件
```

## 🧪 测试类型

### 1. 单元测试 (Unit Tests)
位于各个模块的 `tests/` 子目录中：
- `src/storage/tests/` - 存储层单元测试
- `src/cmd/tests/` - 命令层单元测试（如需要）
- 其他模块的单元测试

**运行方式：**
```bash
# 运行所有单元测试
cargo test

# 运行特定模块的测试
cargo test --package storage

# 运行特定测试用例
cargo test test_redis_mset
```

### 2. Python 集成测试
位于 `tests/python/` 目录：
- 使用 `redis-py` 客户端
- 测试命令的实际行为
- 验证与 Redis 的兼容性

**运行方式：**
```bash
# 安装依赖
pip install redis pytest

# 运行所有 Python 测试
pytest tests/python/

# 运行特定测试文件
python tests/python/test_mset.py

# 使用 pytest
pytest tests/python/test_mset.py -v
```

### 3. 集成测试文档
位于 `tests/integration/` 目录：
- 测试指南和手册
- 手动测试步骤
- 测试用例规范

### 4. TCL 测试（计划中）
位于 `tests/tcl/` 目录：
- Redis 官方测试套件
- 确保与 Redis 完全兼容
- 自动化回归测试

**参考：** [Redis TCL 测试](https://github.com/redis/redis/tree/unstable/tests)

### 5. Go 测试（计划中）
位于 `tests/go/` 目录：
- 自定义 Go 语言测试
- 性能测试
- 并发测试

## 🚀 快速开始

### 前置要求

1. **Rust 测试**：
   - Rust 工具链已安装
   - 项目已编译

2. **Python 测试**：
   ```bash
   pip install redis pytest
   ```

3. **服务器运行**：
   ```bash
   # 启动 Kiwi 服务器
   cargo run --bin server --release
   
   # 或使用 Makefile
   make run
   ```

### 运行测试

```bash
# 1. 单元测试（无需启动服务器）
make test
# 或
cargo test

# 2. Python 集成测试（需要先启动服务器）
# 终端 1：启动服务器
cargo run --bin server --release

# 终端 2：运行测试
pytest tests/python/ -v

# 3. 特定命令测试
python tests/python/test_mset.py
```

## 📝 添加新测试

### 添加单元测试

在对应模块的 `tests/` 目录添加：

```rust
// src/storage/tests/redis_string_test.rs
#[test]
fn test_new_feature() {
    // 测试代码
}
```

### 添加 Python 集成测试

在 `tests/python/` 创建新文件：

```python
# tests/python/test_newcmd.py
import redis
import pytest

def test_newcmd_basic():
    r = redis.Redis(host='localhost', port=6379)
    # 测试代码
    assert r.newcmd() == expected
```

### 添加集成测试文档

在 `tests/integration/` 创建 Markdown 文档：

```text
# NEWCMD 测试指南

## 测试步骤
1. ...
2. ...
```

## 🎯 测试覆盖目标

- [ ] **字符串命令**：SET, GET, MSET, MGET, APPEND, STRLEN, etc.
- [ ] **哈希命令**：HSET, HGET, HMSET, HGETALL, etc.
- [ ] **列表命令**：LPUSH, RPUSH, LPOP, RPOP, LRANGE, etc.
- [ ] **集合命令**：SADD, SREM, SMEMBERS, SINTER, etc.
- [ ] **有序集合**：ZADD, ZREM, ZRANGE, ZRANK, etc.
- [ ] **键命令**：DEL, EXISTS, EXPIRE, TTL, etc.
- [ ] **事务**：MULTI, EXEC, DISCARD, WATCH, etc.
- [ ] **持久化**：SAVE, BGSAVE, etc.
- [ ] **集群**：分布式操作测试

## 📊 测试报告

测试结果和覆盖率报告（待实现）：

```bash
# 生成覆盖率报告
cargo tarpaulin --out Html

# 查看报告
open tarpaulin-report.html
```

## 🛠️ 历史工具脚本

### fix_client_requests.py

这是一个历史修复脚本，用于自动修复 `src/raft/src/integration_tests.rs` 文件中的 `ClientRequest` 构造。

**功能**：
- 自动为 `ClientRequest` 结构体添加缺失的 `consistency_level` 字段
- 将数字 ID 包装为 `RequestId` 类型

**状态**：✅ 已完成任务，所有 `ClientRequest` 构造都已修复

**使用方法**（仅供参考，无需再次运行）：
```bash
cd tests/
python fix_client_requests.py
```

**注意**：此脚本已完成其历史使命，保留在此处仅供参考和文档记录。

## 🔗 相关资源

- [Redis 命令参考](https://redis.io/commands/)
- [Redis 测试套件](https://github.com/redis/redis/tree/unstable/tests)
- [redis-py 文档](https://redis-py.readthedocs.io/)
- [Cargo 测试指南](https://doc.rust-lang.org/cargo/guide/tests.html)

## 🤝 贡献指南

添加新测试时，请确保：

1. ✅ 测试文件放在正确的目录
2. ✅ 测试用例命名清晰
3. ✅ 包含正面和负面测试
4. ✅ 添加必要的文档说明
5. ✅ 所有测试都能通过

## 📞 问题反馈

如有测试相关问题，请提交 Issue 或 PR。

---

**最后更新**: 2025-10-24  
**版本**: 1.0.0
