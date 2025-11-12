# Kiwi 项目文档

本目录包含 Kiwi 项目的所有技术文档和设计文档。

## 文档索引

### 架构与设计

- [ARCHITECTURE.md](ARCHITECTURE.md) - 项目整体架构设计
- [DUAL_RUNTIME_PROJECT_COMPLETION.md](DUAL_RUNTIME_PROJECT_COMPLETION.md) - 双运行时项目完成报告

### Raft 实现

- [RAFT_IMPLEMENTATION_STATUS.md](RAFT_IMPLEMENTATION_STATUS.md) - Raft 实现状态和进度
- [RAFT_CONSISTENCY_TEST_FIX.md](RAFT_CONSISTENCY_TEST_FIX.md) - Raft 集群一致性测试修复说明
- **[raft/](raft/)** - Raft 模块详细文档目录
  - [raft/README.md](raft/README.md) - Raft 文档索引
  - [raft/COMPLETE_FIX_SUMMARY.md](raft/COMPLETE_FIX_SUMMARY.md) - ⭐ 完整测试修复总结（270 passed, 0 failed）
  - [raft/OPENRAFT_INTEGRATION.md](raft/OPENRAFT_INTEGRATION.md) - Openraft 集成完整指南
  - [raft/TROUBLESHOOTING.md](raft/TROUBLESHOOTING.md) - Openraft 故障排除指南
  - [raft/QUICK_REFERENCE.md](raft/QUICK_REFERENCE.md) - Openraft 快速参考

### 测试相关

- [INTEGRATION_TESTS_SUMMARY.md](INTEGRATION_TESTS_SUMMARY.md) - 集成测试总结
- [MSET_TESTING_SUMMARY.md](MSET_TESTING_SUMMARY.md) - MSET 命令测试总结
- [MSET_FINAL_SUMMARY.md](MSET_FINAL_SUMMARY.md) - MSET 最终总结
- [MSET_FINAL_UPDATES.md](MSET_FINAL_UPDATES.md) - MSET 最终更新

### Bug 修复

- [BUGFIX_MESSAGE_CHANNEL.md](BUGFIX_MESSAGE_CHANNEL.md) - 消息通道 Bug 修复

### CI/CD 与合规性

- [CI_CD_UPDATES.md](CI_CD_UPDATES.md) - CI/CD 更新说明
- [LICENSE_COMPLIANCE.md](LICENSE_COMPLIANCE.md) - 许可证合规性说明
- [LICENSE_COMPLIANCE_VERIFICATION.md](LICENSE_COMPLIANCE_VERIFICATION.md) - 许可证合规性验证

## 其他文档位置

- **测试计划**: `tests/raft_test_plan.md` - Raft 测试计划
- **测试说明**: `tests/README.md` - 测试目录说明
- **项目 README**: 根目录的 `README.md` 和 `README_CN.md`

## 文档贡献指南

1. 所有技术文档和设计文档应放在 `docs/` 目录下
2. 测试相关的具体文档可以放在 `tests/` 目录下
3. 项目根目录只保留 README、CHANGELOG 和 LICENSE 等核心文件
4. 文档应使用 Markdown 格式
5. 文档命名应清晰明确，使用大写字母和下划线分隔
