# 快速测试参考卡片 🚀

## 一键运行所有新增测试

### Windows
```cmd
tests\run_new_tests.bat
```

### Linux/Mac
```bash
chmod +x tests/run_new_tests.sh
./tests/run_new_tests.sh
```

---

## 单独运行测试

### 1. WRONGTYPE 错误测试 (10 个用例)
```bash
pytest tests/python/test_wrongtype_errors.py -v
```

### 2. MSET 并发测试 (8 个用例)
```bash
# 快速测试（排除慢速）
pytest tests/python/test_mset_concurrent.py -v -m "not slow"

# 所有测试
pytest tests/python/test_mset_concurrent.py -v

# 只运行慢速测试
pytest tests/python/test_mset_concurrent.py -v -m "slow"
```

### 3. Raft 网络分区测试 (1 个可运行)
```bash
cargo test --test raft_network_partition_tests test_network_simulator
```

---

## 前置条件

### 启动服务器
```bash
cargo run --bin server --release
```

### 安装 Python 依赖
```bash
pip install redis pytest pytest-timeout
```

---

## 测试标记

```bash
# 运行特定标记的测试
pytest tests/python/ -v -m concurrent    # 并发测试
pytest tests/python/ -v -m wrongtype     # 类型错误测试
pytest tests/python/ -v -m slow          # 慢速测试
pytest tests/python/ -v -m "not slow"    # 排除慢速测试
```

---

## 常用命令

```bash
# 显示详细输出
pytest tests/python/test_*.py -v -s

# 只运行失败的测试
pytest tests/python/ --lf

# 生成覆盖率报告
pytest tests/python/ --cov=tests/python --cov-report=html

# 并行运行测试（需要 pytest-xdist）
pytest tests/python/ -n auto
```

---

## 故障排查

### 连接错误
```
redis.ConnectionError: Error connecting to localhost:6379
```
**解决**: 启动 Kiwi 服务器

### 依赖缺失
```
ModuleNotFoundError: No module named 'redis'
```
**解决**: `pip install redis pytest`

### 测试超时
```
FAILED tests/python/test_mset_concurrent.py::test_high_concurrency_stress
```
**解决**: 增加超时时间或降低并发度

---

## 文档链接

- 📖 [详细测试指南](../tests/NEW_TESTS_GUIDE.md)
- 📋 [测试目录说明](../tests/README.md)
- 📊 [问题检查报告](问题检查报告.md)
- ✅ [完成总结](测试补充完成总结.md)

---

**快速提示**: 
- 所有 Python 测试需要服务器运行
- Raft 测试大部分需要集群环境（标记为 `#[ignore]`）
- 使用 `-v` 查看详细输出，`-s` 查看 print 输出
