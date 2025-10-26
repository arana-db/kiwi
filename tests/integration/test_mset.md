# MSET 命令测试指南

## 测试步骤

### 1. 启动服务器
```bash
cargo run --bin server
```

### 2. 使用 Redis CLI 连接并测试

#### 基础功能测试
```redis
# 测试 1: 基本的 MSET 操作
MSET key1 "value1" key2 "value2" key3 "value3"
# 期望: OK

# 验证所有键都已设置
GET key1
# 期望: "value1"
GET key2
# 期望: "value2"
GET key3
# 期望: "value3"

# 测试 2: MGET 验证
MGET key1 key2 key3
# 期望: ["value1", "value2", "value3"]

# 测试 3: 覆盖已存在的键
MSET key1 "new_value1" key4 "value4"
# 期望: OK

GET key1
# 期望: "new_value1"
GET key4
# 期望: "value4"

# 测试 4: 单个键值对
MSET single_key "single_value"
# 期望: OK

GET single_key
# 期望: "single_value"
```

#### 错误情况测试
```redis
# 测试 5: 参数数量错误（缺少值）
MSET key1 "value1" key2
# 期望: ERR wrong number of arguments for MSET

# 测试 6: 没有参数
MSET
# 期望: ERR wrong number of arguments for 'mset' command

# 测试 7: 只有命令名
MSET key1
# 期望: ERR wrong number of arguments for MSET
```

#### 原子性测试
```redis
# 测试 8: 多个键同时设置（原子性）
MSET atomic1 "a1" atomic2 "a2" atomic3 "a3"
# 期望: OK

# 立即查询所有键，都应该存在
MGET atomic1 atomic2 atomic3
# 期望: ["a1", "a2", "a3"]
```

#### 与其他命令的集成测试
```redis
# 测试 9: MSET 后使用 STRLEN
MSET str1 "hello" str2 "world"
STRLEN str1
# 期望: 5
STRLEN str2
# 期望: 5

# 测试 10: MSET 后使用 APPEND
MSET base "hello"
APPEND base " world"
GET base
# 期望: "hello world"

# 测试 11: MSET 后使用 INCR (应该失败，因为值不是整数)
MSET counter "not_a_number"
INCR counter
# 期望: ERR value is not an integer or out of range

# 测试 12: MSET 设置数字字符串
MSET num1 "10" num2 "20"
INCR num1
# 期望: 11
GET num1
# 期望: "11"
```

## 性能测试

### 使用 redis-benchmark 测试（如果可用）
```bash
redis-benchmark -t mset -n 10000 -r 100000
```

## 预期行为

1. **成功情况**: 
   - MSET 总是返回 "OK"
   - 所有键值对原子性地被设置
   - 已存在的键会被新值覆盖

2. **错误情况**:
   - 参数数量必须是奇数（命令 + 键值对）
   - 至少需要一个键值对
   - 参数数量不正确时返回错误信息

3. **特性**:
   - 原子操作：所有键同时设置
   - 覆盖现有值
   - 不影响键的过期时间（如果之前设置了TTL，会被清除）
   - 兼容 Redis MSET 命令的所有行为
