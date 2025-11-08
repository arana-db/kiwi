# 文档整理说明

## 整理日期
2024-11-08

## 整理内容

### 从根目录移动到 docs/
- `DUAL_RUNTIME_PROJECT_COMPLETION.md`
- `INTEGRATION_TESTS_SUMMARY.md`
- `RAFT_CONSISTENCY_TEST_FIX.md`
- `RAFT_IMPLEMENTATION_STATUS.md`

### 从 tests/ 移动到 docs/
- `CI_CD_UPDATES.md`
- `LICENSE_COMPLIANCE.md`
- `LICENSE_COMPLIANCE_VERIFICATION.md`
- `MSET_FINAL_SUMMARY.md`
- `MSET_FINAL_UPDATES.md`
- `MSET_TESTING_SUMMARY.md`

### 保留在原位置的文档
- `README.md` (根目录) - 项目主要说明
- `README_CN.md` (根目录) - 项目中文说明
- `CHANGELOG.md` (根目录) - 变更日志
- `tests/README.md` - 测试目录说明
- `tests/raft_test_plan.md` - Raft 测试计划

## 文档结构

```
kiwi/
├── README.md                    # 项目主要说明
├── README_CN.md                 # 项目中文说明
├── CHANGELOG.md                 # 变更日志
├── LICENSE                      # 许可证
├── docs/                        # 所有技术文档
│   ├── README.md               # 文档索引
│   ├── ARCHITECTURE.md         # 架构设计
│   ├── RAFT_*.md              # Raft 相关文档
│   ├── MSET_*.md              # MSET 相关文档
│   ├── *_COMPLIANCE.md        # 合规性文档
│   └── ...                     # 其他技术文档
└── tests/                       # 测试代码和测试相关文档
    ├── README.md               # 测试说明
    ├── raft_test_plan.md       # Raft 测试计划
    └── *.rs                    # 测试代码
```

## 整理原则

1. **根目录简洁**: 只保留核心文件（README、LICENSE、CHANGELOG、配置文件）
2. **文档集中**: 所有技术文档和设计文档放在 `docs/` 目录
3. **测试独立**: 测试代码和测试计划放在 `tests/` 目录
4. **便于查找**: 在 `docs/README.md` 中提供完整的文档索引

## 好处

- ✅ 项目结构更清晰
- ✅ 文档更容易查找和维护
- ✅ 符合开源项目的最佳实践
- ✅ 便于新贡献者快速了解项目
