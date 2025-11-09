# Openraft 集成故障排除指南

本文档提供 Openraft 集成过程中常见问题的诊断和解决方案。

## 编译错误

### 错误 1: "trait cannot be implemented because it is sealed"

**错误信息**:
```
error[E0277]: the trait bound `MyStorage: Sealed` is not satisfied
```

**原因**: 尝试直接实现 Openraft 的 sealed trait（如 `RaftStateMachine`、`RaftLogStorage`）。

**解决方案**:
1. 使用 Adaptor 模式，不要直接实现 sealed traits
2. 实现非 sealed 的 traits：`RaftLogReader`、`RaftSnapshotBuilder`
3. 参考 `src/raft/src/adaptor.rs` 中的实现

**示例**:
```rust
// ❌ 错误 - 直接实现 sealed trait
impl RaftStateMachine<TypeConfig> for MyStateMachine { }

// ✅ 正确 - 使用 Adaptor
pub struct StateMachineAdaptor {
    inner: Arc<MyStateMachine>,
}

impl RaftStateMachine<TypeConfig> for StateMachineAdaptor {
    // 实现方法
}
```

### 错误 2: 类型不匹配

**错误信息**:
```
error[E0308]: mismatched types
expected `openraft::Entry<TypeConfig>`
found `CustomEntry`
```

**原因**: Openraft 类型和自定义类型不兼容。

**解决方案**:
1. 实现类型转换 trait（`From`、`Into`、`TryFrom`）
2. 使用转换层统一处理类型转换
3. 参考 `src/raft/src/conversion.rs`

**示例**:
```rust
impl From<CustomEntry> for openraft::Entry<TypeConfig> {
    fn from(entry: CustomEntry) -> Self {
        openraft::Entry {
            log_id: entry.log_id.into(),
            payload: entry.payload.into(),
        }
    }
}
```

### 错误 3: 生命周期错误

**错误信息**:
```
error[E0597]: `storage` does not live long enough
```

**原因**: 异步函数中的引用生命周期问题。

**解决方案**:
1. 使用 `Arc` 包装共享数据
2. 在异步块中 clone Arc
3. 避免在异步函数签名中使用引用

**示例**:
```rust
// ❌ 错误
async fn read_data(&self) -> Result<Data> {
    self.storage.read().await
}

// ✅ 正确
async fn read_data(&self) -> Result<Data> {
    let storage = self.storage.clone();
    tokio::task::spawn_blocking(move || {
        storage.read()
    }).await?
}
```

## 运行时错误

### 错误 4: "task panicked"

**错误信息**:
```
thread 'tokio-runtime-worker' panicked at 'called `Result::unwrap()` on an `Err` value'
```

**原因**: 
- 未正确处理错误
- 在异步上下文中使用 `unwrap()`
- 存储操作失败

**解决方案**:
1. 使用 `?` 操作符传播错误
2. 添加适当的错误处理
3. 检查存储层的错误日志

**示例**:
```rust
// ❌ 错误
let data = storage.read().unwrap();

// ✅ 正确
let data = storage.read()
    .map_err(|e| {
        error!("Failed to read from storage: {}", e);
        StorageError::ReadFailed(e)
    })?;
```

### 错误 5: 异步运行时阻塞

**症状**: 
- 应用程序挂起
- 高 CPU 使用率
- 请求超时

**原因**: 在异步上下文中执行同步 I/O 操作。

**解决方案**:
1. 使用 `tokio::task::spawn_blocking` 执行同步操作
2. 配置专用的阻塞线程池
3. 监控线程池使用情况

**示例**:
```rust
// ❌ 错误 - 阻塞异步运行时
async fn read_from_disk(&self) -> Result<Data> {
    self.db.get(&key) // 同步操作
}

// ✅ 正确 - 使用 spawn_blocking
async fn read_from_disk(&self) -> Result<Data> {
    let db = self.db.clone();
    let key = key.clone();
    tokio::task::spawn_blocking(move || {
        db.get(&key)
    }).await?
}
```

### 错误 6: 快照传输失败

**错误信息**:
```
InstallSnapshotError: timeout waiting for snapshot
```

**原因**:
- 快照过大
- 网络超时
- 内存不足

**解决方案**:
1. 增加快照超时配置
2. 实现流式快照传输
3. 压缩快照数据
4. 增加快照分块大小

**配置示例**:
```rust
let config = Config {
    snapshot_max_chunk_size: 1024 * 1024, // 1MB
    install_snapshot_timeout: 60000, // 60 seconds
    max_in_snapshot_log_to_keep: 1000,
    ..Default::default()
};
```

## 数据一致性问题

### 问题 7: 日志条目丢失

**症状**:
- 节点重启后数据丢失
- 日志索引不连续
- 状态不一致

**原因**:
- 写入未正确持久化
- RocksDB 配置不当
- 崩溃恢复失败

**解决方案**:
1. 启用 RocksDB 同步写入
2. 配置 WAL（Write-Ahead Log）
3. 实现崩溃恢复逻辑

**示例**:
```rust
// 配置同步写入
let mut write_options = rocksdb::WriteOptions::default();
write_options.set_sync(true);
db.write_opt(batch, &write_options)?;

// 配置 WAL
let mut db_options = rocksdb::Options::default();
db_options.set_wal_recovery_mode(rocksdb::DBRecoveryMode::AbsoluteConsistency);
```

### 问题 8: 状态机不一致

**症状**:
- 不同节点返回不同结果
- 读取到旧数据
- 应用日志失败

**原因**:
- 日志应用顺序错误
- 快照恢复不完整
- 并发控制问题

**解决方案**:
1. 确保日志按顺序应用
2. 验证快照完整性
3. 使用适当的锁机制

**示例**:
```rust
impl StateMachine {
    pub fn apply_entries(&mut self, entries: Vec<Entry>) -> Result<()> {
        // 验证日志连续性
        for (i, entry) in entries.iter().enumerate() {
            if i > 0 && entry.log_id.index != entries[i-1].log_id.index + 1 {
                return Err(StorageError::LogGap);
            }
        }
        
        // 按顺序应用
        for entry in entries {
            self.apply_single(entry)?;
        }
        
        Ok(())
    }
}
```

## 性能问题

### 问题 9: 高延迟

**症状**:
- 写入延迟高
- 读取缓慢
- 吞吐量低

**诊断步骤**:
1. 检查 RocksDB 性能指标
2. 分析日志中的慢操作
3. 使用性能分析工具（perf、flamegraph）

**优化方案**:
```rust
// 1. 启用批量写入
let mut batch = rocksdb::WriteBatch::default();
for entry in entries {
    batch.put_cf(cf, key, value);
}
db.write(batch)?;

// 2. 配置 RocksDB 缓存
let mut db_options = rocksdb::Options::default();
db_options.set_write_buffer_size(64 * 1024 * 1024); // 64MB
db_options.set_max_write_buffer_number(3);
db_options.set_target_file_size_base(64 * 1024 * 1024);

// 3. 使用 LRU 缓存
let cache = LruCache::new(1000);
```

### 问题 10: 内存使用过高

**症状**:
- 内存持续增长
- OOM 错误
- 频繁 GC

**原因**:
- 缓存过大
- 快照未释放
- 日志积压

**解决方案**:
```rust
// 1. 限制缓存大小
let cache = LruCache::new(NonZeroUsize::new(1000).unwrap());

// 2. 定期清理旧日志
async fn compact_logs(&self) -> Result<()> {
    let last_applied = self.get_last_applied_log()?;
    let keep_logs = 1000;
    
    if last_applied > keep_logs {
        let purge_upto = last_applied - keep_logs;
        self.purge_logs_upto(purge_upto).await?;
    }
    
    Ok(())
}

// 3. 配置快照策略
let config = Config {
    snapshot_policy: SnapshotPolicy::LogsSinceLast(10000),
    max_in_snapshot_log_to_keep: 1000,
    ..Default::default()
};
```

## 网络问题

### 问题 11: 节点无法通信

**症状**:
- 选举超时
- 心跳失败
- 日志复制失败

**诊断步骤**:
1. 检查网络连接
2. 验证节点配置
3. 查看防火墙规则

**解决方案**:
```bash
# 检查端口是否开放
netstat -an | grep 7379

# 测试节点连接
telnet node2 7379

# 检查防火墙
sudo iptables -L
```

**配置示例**:
```rust
let config = Config {
    heartbeat_interval: 500,
    election_timeout_min: 1500,
    election_timeout_max: 3000,
    ..Default::default()
};
```

## 调试技巧

### 启用详细日志

```rust
use tracing_subscriber;

tracing_subscriber::fmt()
    .with_max_level(tracing::Level::TRACE)
    .with_target(true)
    .with_thread_ids(true)
    .with_line_number(true)
    .init();
```

### 使用 Openraft 指标

```rust
// 获取 Raft 指标
let metrics = raft.metrics().borrow().clone();
println!("Current leader: {:?}", metrics.current_leader);
println!("Last log index: {}", metrics.last_log_index);
println!("Last applied: {}", metrics.last_applied);
```

### 检查存储状态

```rust
// 验证日志完整性
pub fn verify_log_integrity(&self) -> Result<()> {
    let last_index = self.get_last_log_index()?;
    
    for i in 1..=last_index {
        if self.get_log_entry(i)?.is_none() {
            error!("Missing log entry at index {}", i);
            return Err(StorageError::LogGap);
        }
    }
    
    Ok(())
}
```

## 获取帮助

如果以上方案无法解决问题：

1. 查看 [Openraft 官方文档](https://docs.rs/openraft/)
2. 搜索 [Openraft GitHub Issues](https://github.com/datafuselabs/openraft/issues)
3. 查看项目的集成测试：`src/raft/src/tests/`
4. 启用 TRACE 级别日志并分析输出
5. 提交 Issue 并附上详细的错误信息和日志

## 相关文档

- [Openraft 集成指南](./OPENRAFT_INTEGRATION.md)
- [性能优化指南](./performance_optimizations.md)
- [设计文档](../../../.kiro/specs/openraft-sealed-traits-fix/design.md)
