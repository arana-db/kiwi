# Raft 与存储层真正集成需求文档

## 介绍

本文档定义了将 Kiwi 的 Raft 共识层与现有存储层真正集成的需求，实现强一致性的分布式 Redis 兼容数据库。当前实现中，Raft 模块使用内存存储（simple_mem_store），存储层（src/storage/src）独立运行，两者没有实际集成，导致无法实现强一致性目标。

## 术语表

- **Raft_Layer**: Raft 共识层，负责日志复制和一致性
- **Storage_Layer**: 存储层，实现 Redis 协议到 RocksDB 的转换
- **State_Machine**: Raft 状态机，应用已提交的日志条目
- **Log_Storage**: Raft 日志存储，持久化 Raft 日志
- **Adaptor_Pattern**: Openraft 要求的适配器模式，用于桥接自定义存储实现
- **Write_Path**: 写操作路径，必须经过 Raft 共识
- **Read_Path**: 读操作路径，支持强一致性读和最终一致性读
- **Storage_Engine**: 存储引擎接口，连接状态机和实际存储

## 需求

### 需求 1: 实现 Openraft Adaptor 集成

**用户故事**: 作为系统架构师，我希望使用 Openraft 的 Adaptor 模式将我们的 RaftStorage 和 KiwiStateMachine 集成到 openraft 中。

#### 验收标准

1. THE Adaptor_Pattern SHALL 正确包装 RaftStorage 以满足 openraft 的 sealed traits
2. THE RaftStorage SHALL 使用 RocksDB 持久化 Raft 日志而非内存存储
3. THE Adaptor SHALL 实现 openraft 要求的所有存储接口方法
4. THE 日志条目 SHALL 正确序列化和反序列化
5. THE 快照功能 SHALL 通过 Adaptor 正确工作

### 需求 2: 连接状态机到实际存储层

**用户故事**: 作为开发者，我希望 KiwiStateMachine 能够调用 src/storage/src 中的 Redis 实现来真正执行命令。

#### 验收标准

1. THE KiwiStateMachine SHALL 实现 StorageEngine trait 连接到 Redis 存储
2. THE Redis 命令 SHALL 通过状态机应用到 RocksDB
3. THE 状态机 SHALL 支持所有基础 Redis 命令（GET, SET, DEL, EXISTS 等）
4. THE 命令执行 SHALL 是原子性的
5. THE 错误处理 SHALL 正确传播到 Raft 层

### 需求 3: 实现写操作通过 Raft 路由

**用户故事**: 作为系统架构师，我希望所有写操作都经过 Raft 共识，以保证强一致性。

#### 验收标准

1. THE 写操作 SHALL 通过 RaftNode.propose() 提交到 Raft
2. THE 写操作 SHALL 在多数节点确认后才返回成功
3. THE 非 Leader 节点 SHALL 将写请求转发到 Leader
4. THE 写操作 SHALL 按照 Raft 日志顺序应用
5. THE 写操作失败 SHALL 正确回滚或重试

### 需求 4: 实现读操作一致性保证

**用户故事**: 作为应用开发者，我希望能够选择强一致性读或最终一致性读。

#### 验收标准

1. THE 强一致性读 SHALL 通过 Leader 确认后返回
2. THE 最终一致性读 SHALL 可以从任意节点读取
3. THE 读操作 SHALL 根据一致性级别正确路由
4. THE Leader 确认 SHALL 使用 read_index 机制
5. THE 读操作 SHALL 不阻塞写操作

### 需求 5: 修改 RaftNode 使用真实存储

**用户故事**: 作为系统架构师，我希望 RaftNode 使用我们实现的 RaftStorage 而不是 simple_mem_store。

#### 验收标准

1. THE RaftNode SHALL 使用 RaftStorage 通过 Adaptor 模式
2. THE simple_mem_store SHALL 被移除或仅用于测试
3. THE Raft 日志 SHALL 持久化到 RocksDB
4. THE 节点重启 SHALL 能够恢复 Raft 状态
5. THE 快照 SHALL 正确保存和恢复

### 需求 6: 集成到网络层请求处理

**用户故事**: 作为系统架构师，我希望网络层接收到的 Redis 命令能够正确路由到 Raft 层。

#### 验收标准

1. THE 网络层 SHALL 识别读写操作类型
2. THE 写操作 SHALL 路由到 RaftNode.propose()
3. THE 读操作 SHALL 根据一致性级别路由
4. THE 命令解析 SHALL 保持与现有 Redis 协议兼容
5. THE 响应格式 SHALL 符合 Redis 协议

### 需求 7: 实现集群数据复制

**用户故事**: 作为运维工程师，我希望数据能够自动复制到集群中的所有节点。

#### 验收标准

1. THE Raft 日志 SHALL 复制到所有节点
2. THE 状态机 SHALL 在所有节点上应用相同的日志
3. THE 数据一致性 SHALL 在所有节点间保持
4. THE 复制延迟 SHALL 被监控和记录
5. THE 落后节点 SHALL 通过快照追赶

### 需求 8: 实现故障转移和恢复

**用户故事**: 作为运维工程师，我希望 Leader 节点故障时能够自动选举新 Leader。

#### 验收标准

1. THE Leader 故障 SHALL 触发新的选举
2. THE 新 Leader SHALL 在多数节点同意后产生
3. THE 客户端请求 SHALL 自动重定向到新 Leader
4. THE 数据 SHALL 在故障转移后保持一致
5. THE 故障节点恢复 SHALL 能够重新加入集群

### 需求 9: 实现快照和日志压缩

**用户故事**: 作为系统管理员，我希望系统能够定期创建快照以压缩日志。

#### 验收标准

1. THE 快照 SHALL 在日志达到阈值时自动创建
2. THE 快照 SHALL 包含完整的数据库状态
3. THE 旧日志 SHALL 在快照后被清理
4. THE 快照 SHALL 用于新节点或落后节点的状态传输
5. THE 快照创建 SHALL 不阻塞正常操作

### 需求 10: 实现配置变更管理

**用户故事**: 作为集群管理员，我希望能够动态添加或移除节点。

#### 验收标准

1. THE 节点添加 SHALL 通过 Raft 配置变更协议
2. THE 节点移除 SHALL 安全地从集群中移除
3. THE 配置变更 SHALL 不影响正在进行的操作
4. THE 新节点 SHALL 通过快照快速同步数据
5. THE 配置变更 SHALL 持久化到所有节点

### 需求 11: 实现性能优化

**用户故事**: 作为性能工程师，我希望系统性能接近单机 Redis 的性能。

#### 验收标准

1. THE 批量写入 SHALL 使用 Raft 批处理优化
2. THE 读操作 SHALL 支持本地读以减少延迟
3. THE 日志复制 SHALL 使用流水线优化
4. THE 快照传输 SHALL 使用增量传输
5. THE 性能指标 SHALL 被监控和记录

### 需求 12: 实现测试和验证

**用户故事**: 作为质量工程师，我希望有完整的测试来验证强一致性。

#### 验收标准

1. THE 单元测试 SHALL 覆盖所有核心组件
2. THE 集成测试 SHALL 验证端到端的写入和读取
3. THE 一致性测试 SHALL 验证数据在所有节点上一致
4. THE 故障测试 SHALL 验证故障转移和恢复
5. THE 性能测试 SHALL 验证系统性能指标
