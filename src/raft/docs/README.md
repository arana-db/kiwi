# Raft 模块文档

本目录包含 Kiwi Raft 模块的技术文档。

## 核心文档

### [OPENRAFT_INTEGRATION.md](./OPENRAFT_INTEGRATION.md)
Openraft 集成的完整指南，包括：
- Adaptor 模式架构说明
- 快速开始指南
- 详细的代码示例
- API 参考
- 已知限制和解决方案
- 性能优化建议
- 测试指南和最佳实践

### [TROUBLESHOOTING.md](./TROUBLESHOOTING.md)
故障排除指南，涵盖：
- 编译错误及解决方案
- 运行时错误诊断
- 数据一致性问题
- 性能问题分析
- 网络问题排查
- 调试技巧

### [QUICK_REFERENCE.md](./QUICK_REFERENCE.md)
快速参考手册，提供：
- 常用代码片段
- 配置参考
- 错误处理模式
- 类型转换示例
- 测试工具
- 监控和指标
- 命令行工具

## 研究和设计文档

### [openraft_adaptor_research.md](./openraft_adaptor_research.md)
Openraft Adaptor 模式的研究文档，记录了：
- Sealed traits 问题的发现
- 各种解决方案的探索
- 最终方案的选择理由

### [openraft_sealed_traits_solution.md](./openraft_sealed_traits_solution.md)
Sealed traits 问题的详细分析和解决方案

### [openraft_breakthrough.md](./openraft_breakthrough.md)
Openraft 集成的突破性进展记录

## 实现笔记

### [task_6_integration_notes.md](./task_6_integration_notes.md)
任务 6（集成 Adaptor 到 RaftNode）的实现笔记

### [task_10_3_trace_logs_summary.md](./task_10_3_trace_logs_summary.md)
任务 10.3（添加 trace 日志）的总结

### [performance_optimizations.md](./performance_optimizations.md)
性能优化的详细说明和实现

## 文档使用指南

### 新手入门
1. 先阅读 [OPENRAFT_INTEGRATION.md](./OPENRAFT_INTEGRATION.md) 的"快速开始"部分
2. 查看 [QUICK_REFERENCE.md](./QUICK_REFERENCE.md) 了解常用代码片段
3. 遇到问题时参考 [TROUBLESHOOTING.md](./TROUBLESHOOTING.md)

### 深入理解
1. 阅读 [openraft_adaptor_research.md](./openraft_adaptor_research.md) 了解设计决策
2. 查看 [OPENRAFT_INTEGRATION.md](./OPENRAFT_INTEGRATION.md) 的"详细示例"部分
3. 研究 [performance_optimizations.md](./performance_optimizations.md) 进行性能调优

### 问题排查
1. 首先查看 [TROUBLESHOOTING.md](./TROUBLESHOOTING.md) 中的常见问题
2. 启用详细日志并分析输出
3. 参考 [QUICK_REFERENCE.md](./QUICK_REFERENCE.md) 中的调试技巧

## 相关资源

### 外部文档
- [Openraft 官方文档](https://docs.rs/openraft/)
- [Openraft GitHub](https://github.com/datafuselabs/openraft)
- [Raft 论文](https://raft.github.io/raft.pdf)

### 项目文档
- [项目架构文档](../../../docs/ARCHITECTURE.md)
- [Raft 实现状态](../../../docs/RAFT_IMPLEMENTATION_STATUS.md)
- [设计文档](../../../.kiro/specs/openraft-sealed-traits-fix/design.md)

### 代码位置
- Adaptor 实现: `src/raft/src/adaptor.rs`
- 存储层: `src/raft/src/storage/`
- 类型转换: `src/raft/src/conversion.rs`
- 集成测试: `src/raft/src/tests/`

## 贡献指南

### 更新文档
1. 保持文档与代码同步
2. 添加新功能时更新相关文档
3. 修复 bug 时更新故障排除指南
4. 使用清晰的中文和代码示例

### 文档风格
- 使用 Markdown 格式
- 代码示例要完整可运行
- 包含错误处理
- 添加注释说明关键点
- 提供实际的使用场景

## 文档维护

最后更新: 2024-11
维护者: Kiwi 开发团队

如有问题或建议，请提交 Issue 或 Pull Request。
