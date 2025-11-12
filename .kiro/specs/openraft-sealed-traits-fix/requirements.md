# Openraft Sealed Traits 集成修复需求文档

## 介绍

本文档定义了解决 Kiwi Raft 模块与 Openraft 0.9.21 sealed traits 集成问题的需求。Openraft 使用密封 trait（sealed traits）限制外部实现，导致 Kiwi 无法直接实现 `RaftLogStorage` 和 `RaftStateMachine` 等核心接口。本需求旨在通过"适配层 + 委托模式"实现合规集成，既遵守 Openraft 的接口限制，又保持 Kiwi 业务逻辑的独立性。

## 术语表

- **Sealed_Trait**: Rust 中的密封 trait，通过私有内部 trait 或 `#[sealed]` 宏限制外部实现
- **Openraft**: 基于 Raft 共识算法的 Rust 库（版本 0.9.21）
- **Adapter_Layer**: 适配层，用于桥接 Kiwi 业务逻辑与 Openraft sealed traits
- **KiwiStateMachine**: Kiwi 自定义的状态机实现，处理 Redis 命令
- **RaftStorage**: Kiwi 自定义的 Raft 日志和状态存储实现
- **Delegation_Pattern**: 委托模式，适配层将接口调用转发给内部业务组件
- **TypeConfig**: Openraft 的类型配置 trait，定义 Raft 使用的数据类型
- **RaftLogStorage**: Openraft 的日志存储 sealed trait
- **RaftStateMachine**: Openraft 的状态机 sealed trait

## 需求

### 需求 1: 识别 Openraft Sealed Traits 限制

**用户故事**: 作为系统架构师，我需要明确 Openraft 中哪些 trait 是密封的，以便设计正确的集成方案。

#### 验收标准

1. THE 开发团队 SHALL 确认 Openraft 0.9.21 中 `RaftLogStorage` 是否为 sealed trait
2. THE 开发团队 SHALL 确认 Openraft 0.9.21 中 `RaftStateMachine` 是否为 sealed trait
3. THE 文档 SHALL 记录所有 sealed traits 的关联类型和方法签名
4. THE 文档 SHALL 记录 Openraft 提供的基础实现类型（如 `Adaptor`）
5. THE 开发团队 SHALL 验证当前实现是否违反 sealed trait 限制

### 需求 2: 解耦 Kiwi 业务组件

**用户故事**: 作为开发者，我希望 Kiwi 的核心业务逻辑（状态机、存储）与 Openraft 接口完全解耦，以便独立测试和维护。

#### 验收标准

1. THE KiwiStateMachine SHALL 不直接依赖 Openraft 的 trait 定义
2. THE RaftStorage SHALL 不直接依赖 Openraft 的 trait 定义
3. THE 业务组件 SHALL 仅关注核心功能（数据读写、快照生成）
4. THE 业务组件 SHALL 定义自己的错误类型，不依赖 Openraft 错误类型
5. THE 业务组件 SHALL 可以独立编译和单元测试

### 需求 3: 实现状态机适配层

**用户故事**: 作为系统集成工程师，我需要一个适配层来桥接 KiwiStateMachine 与 Openraft 的 sealed `RaftStateMachine` trait。

#### 验收标准

1. THE KiwiStateMachineAdapter SHALL 组合 KiwiStateMachine 实例
2. THE KiwiStateMachineAdapter SHALL 实现 Openraft 要求的 `RaftStateMachine` trait（或使用 Openraft 提供的 Adaptor）
3. THE 适配层 SHALL 将 `apply` 方法调用委托给 KiwiStateMachine
4. THE 适配层 SHALL 将 `snapshot` 方法调用委托给 KiwiStateMachine
5. THE 适配层 SHALL 将 `restore` 方法调用委托给 KiwiStateMachine
6. THE 适配层 SHALL 正确转换 Openraft 类型与 Kiwi 内部类型
7. THE 适配层 SHALL 处理类型转换错误并返回 Openraft 兼容的错误类型

### 需求 4: 实现存储适配层

**用户故事**: 作为系统集成工程师，我需要一个适配层来桥接 RaftStorage 与 Openraft 的 sealed `RaftLogStorage` trait。

#### 验收标准

1. THE RaftStorageAdapter SHALL 组合 RaftStorage 实例
2. THE RaftStorageAdapter SHALL 实现 Openraft 要求的 `RaftLogStorage` trait（或使用 Openraft 提供的 Adaptor）
3. THE 适配层 SHALL 将日志追加操作委托给 RaftStorage
4. THE 适配层 SHALL 将日志读取操作委托给 RaftStorage
5. THE 适配层 SHALL 将状态持久化操作委托给 RaftStorage
6. THE 适配层 SHALL 将快照存储操作委托给 RaftStorage
7. THE 适配层 SHALL 正确处理 Openraft 的 `LogId` 和 `Entry` 类型

### 需求 5: 实现类型转换层

**用户故事**: 作为开发者，我需要在 Openraft 类型与 Kiwi 内部类型之间进行无缝转换。

#### 验收标准

1. THE 类型转换层 SHALL 将 Openraft 的 `Entry<TypeConfig>` 转换为 Kiwi 的 `ClientRequest`
2. THE 类型转换层 SHALL 将 Kiwi 的 `ClientResponse` 转换为 Openraft 的响应类型
3. THE 类型转换层 SHALL 将 Openraft 的 `RaftError` 转换为 Kiwi 的 `RaftError`
4. THE 类型转换层 SHALL 将 Kiwi 的错误类型转换为 Openraft 的 `RaftError`
5. THE 类型转换 SHALL 保持数据完整性，不丢失信息

### 需求 6: 验证适配层兼容性

**用户故事**: 作为质量保证工程师，我需要确保适配层正确对接 Openraft，且不违反 sealed trait 限制。

#### 验收标准

1. THE 适配层实现 SHALL 通过编译，无 sealed trait 违规错误
2. THE 单元测试 SHALL 验证状态机适配层的 `apply` 方法正确性
3. THE 单元测试 SHALL 验证状态机适配层的 `snapshot` 和 `restore` 方法正确性
4. THE 单元测试 SHALL 验证存储适配层的日志操作正确性
5. THE 集成测试 SHALL 验证 Raft 核心流程（日志复制、状态机应用、快照生成）通过适配层正常工作

### 需求 7: 处理异步逻辑适配

**用户故事**: 作为开发者，我需要适配层正确处理 Kiwi 业务组件中的异步操作。

#### 验收标准

1. THE 适配层 SHALL 使用 `async/await` 正确转发异步调用
2. THE 适配层 SHALL 处理异步操作中的错误传播
3. THE 适配层 SHALL 不阻塞 Raft 线程或事件循环
4. THE 适配层 SHALL 支持并发的日志应用和快照操作
5. THE 适配层 SHALL 正确处理异步操作的取消和超时

### 需求 8: 实现错误处理统一

**用户故事**: 作为开发者，我需要在适配层中统一转换错误类型，避免错误类型不兼容。

#### 验收标准

1. THE 适配层 SHALL 将 Kiwi 内部错误转换为 Openraft 的 `RaftError`
2. THE 错误转换 SHALL 保留原始错误信息和上下文
3. THE 错误转换 SHALL 正确映射错误类别（存储错误、网络错误、状态机错误）
4. THE 适配层 SHALL 记录错误转换日志，便于调试
5. THE 错误处理 SHALL 不导致 panic 或未定义行为

### 需求 9: 优化适配层性能

**用户故事**: 作为性能工程师，我需要确保适配层不成为系统性能瓶颈。

#### 验收标准

1. THE 适配层 SHALL 避免不必要的数据拷贝
2. THE 适配层 SHALL 使用零拷贝技术（如 `Bytes`）传递数据
3. THE 适配层 SHALL 支持批量日志应用以减少转发开销
4. THE 性能测试 SHALL 验证适配层的延迟开销小于 1ms
5. THE 性能测试 SHALL 验证适配层不降低 Raft 吞吐量超过 5%

### 需求 10: 文档化适配层设计

**用户故事**: 作为新加入的开发者，我需要清晰的文档来理解适配层的设计和使用方法。

#### 验收标准

1. THE 设计文档 SHALL 解释 sealed trait 问题及解决方案
2. THE 设计文档 SHALL 包含适配层的架构图和数据流图
3. THE 代码注释 SHALL 解释每个适配方法的转换逻辑
4. THE 示例代码 SHALL 展示如何使用适配层初始化 Raft 节点
5. THE 文档 SHALL 说明未来 Openraft 接口变更时的适配策略
