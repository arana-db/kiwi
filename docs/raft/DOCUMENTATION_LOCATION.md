# Raft 文档位置说明

## 文档位置

所有 Raft 相关的文档现在统一存放在项目根目录的 `docs/raft/` 目录下。

## 目录结构

```
kiwi/
├── docs/                           # 项目文档根目录
│   ├── README.md                   # 项目文档索引
│   ├── raft/                       # Raft 模块文档（所有 Raft 文档都在这里）
│   │   ├── README.md               # Raft 文档索引
│   │   ├── COMPLETE_FIX_SUMMARY.md # ⭐ 完整测试修复总结
│   │   ├── OPENRAFT_INTEGRATION.md # 集成指南
│   │   ├── TROUBLESHOOTING.md      # 故障排除
│   │   ├── QUICK_REFERENCE.md      # 快速参考
│   │   └── ... (其他 21 个文档)
│   └── ... (其他项目文档)
├── src/
│   ├── raft/                       # Raft 模块源代码
│   │   ├── src/                    # 源代码
│   │   ├── tests/                  # 测试
│   │   ├── Cargo.toml
│   │   └── build.rs
│   └── ... (其他源代码)
└── ...
```

## 为什么这样组织？

### 优点
1. **清晰分离**：文档和代码分离，便于管理
2. **统一位置**：所有项目文档都在 `docs/` 目录下
3. **易于查找**：文档集中在一个地方，不需要在源代码目录中查找
4. **版本控制**：文档变更和代码变更可以独立追踪
5. **构建优化**：文档不会影响代码编译

### 文档分类
- **核心文档**：集成指南、故障排除、快速参考
- **研究文档**：Adaptor 研究、Sealed traits 解决方案
- **实现笔记**：任务实现记录、性能优化
- **修复文档**：测试修复总结、问题解决记录
- **解决方案**：各种技术方案的详细说明

## 快速访问

### 从项目根目录
```bash
cd docs/raft
ls *.md
```

### 从源代码目录
```bash
cd ../../docs/raft
ls *.md
```

### 在浏览器中
- GitHub: `https://github.com/your-org/kiwi/tree/main/docs/raft`
- 本地: `file:///path/to/kiwi/docs/raft/README.md`

## 文档索引

详细的文档索引请查看：
- [docs/raft/README.md](./README.md) - Raft 文档完整索引
- [docs/README.md](../README.md) - 项目文档总索引

## 重要文档推荐

### 新手必读
1. [OPENRAFT_INTEGRATION.md](./OPENRAFT_INTEGRATION.md) - 从这里开始
2. [QUICK_REFERENCE.md](./QUICK_REFERENCE.md) - 常用代码片段
3. [TROUBLESHOOTING.md](./TROUBLESHOOTING.md) - 遇到问题时查看

### 了解最新进展
1. [COMPLETE_FIX_SUMMARY.md](./COMPLETE_FIX_SUMMARY.md) - ⭐ 最重要的修复文档

### 深入理解
1. [openraft_adaptor_research.md](./openraft_adaptor_research.md) - 设计决策
2. [OPENRAFT_ADAPTOR_SOLUTION.md](./OPENRAFT_ADAPTOR_SOLUTION.md) - 技术方案
3. [performance_optimizations.md](./performance_optimizations.md) - 性能优化

## 维护指南

### 添加新文档
1. 在 `docs/raft/` 目录下创建新的 `.md` 文件
2. 更新 `docs/raft/README.md` 添加索引
3. 如果是重要文档，也更新 `docs/README.md`

### 更新现有文档
1. 直接编辑 `docs/raft/` 目录下的文件
2. 如果文档标题或用途变化，更新相关索引

### 文档命名规范
- 使用大写字母和下划线：`COMPLETE_FIX_SUMMARY.md`
- 或使用小写字母和下划线：`openraft_adaptor_research.md`
- 保持命名清晰、描述性强

## 历史记录

- **2024-11-11**: 将所有文档从 `src/raft/docs/` 移动到 `docs/raft/`
- **2024-11-11**: 完成所有 9 个测试失败的修复（270 passed, 0 failed）
- **2024-11**: 完成 OpenRaft 集成和 Adaptor 模式实现

## 相关链接

- [项目 README](../../README.md)
- [Raft 实现状态](../RAFT_IMPLEMENTATION_STATUS.md)
- [项目架构](../ARCHITECTURE.md)
