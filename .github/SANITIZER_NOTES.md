# Sanitizer 和 Benchmark 检查说明

## 概述

PR #260 引入了新的代码质量检查，包括 Sanitizers 和 Benchmark 编译检查。这些检查可能会在现有代码上失败，这是正常的。

## Sanitizer 检查

### 什么是 Sanitizers？

Sanitizers 是编译器提供的运行时检查工具，用于检测：

- **AddressSanitizer (ASAN)**: 内存访问错误（越界、use-after-free 等）
- **LeakSanitizer (LSAN)**: 内存泄漏
- **ThreadSanitizer (TSAN)**: 数据竞争和线程安全问题

### 为什么会失败？

Sanitizers 非常严格，可能会检测到：
1. 现有代码中的潜在问题
2. 依赖库中的问题
3. 测试代码中的问题
4. 误报（false positives）

### 当前状态

这些检查已设置为 **非阻塞** (`continue-on-error: true`)，意味着：
- ✅ 不会阻止 PR 合并
- ✅ 提供有价值的诊断信息
- ✅ 帮助识别潜在问题
- ✅ 可以逐步修复

### 如何处理失败？

1. **短期**: 忽略这些失败，它们不会阻止开发
2. **中期**: 查看失败日志，识别真正的问题
3. **长期**: 逐步修复检测到的问题

### 查看详细日志

访问 GitHub Actions 页面查看具体的 sanitizer 报告：
- 点击失败的 sanitizer 检查
- 下载 artifacts 查看完整报告
- 搜索 "ERROR" 或 "WARNING" 关键字

## Benchmark 编译检查

### 什么是 Benchmark 检查？

确保项目的性能基准测试代码能够正常编译。

### 为什么会失败？

可能的原因：
1. Benchmark 代码依赖的 API 发生变化
2. 缺少必要的 feature flags
3. 依赖版本不兼容

### 当前状态

同样设置为 **非阻塞**，不影响 PR 合并。

### 如何修复？

```bash
# 本地测试 benchmark 编译
cargo bench --no-run

# 查看具体错误
cargo bench --no-run --verbose
```

## 最佳实践

### 对于开发者

1. **不要被吓到**: 这些检查是帮助工具，不是障碍
2. **逐步改进**: 可以在后续 PR 中修复问题
3. **优先级**: 先关注功能开发，再优化代码质量

### 对于维护者

1. **定期审查**: 每月查看一次 sanitizer 报告
2. **分类问题**: 区分真实问题和误报
3. **创建 Issues**: 为重要问题创建跟踪 issue
4. **文档化**: 记录已知的误报和抑制规则

## 配置 Sanitizer 抑制

如果遇到已知的误报，可以创建抑制文件：

### AddressSanitizer 抑制

创建 `.github/asan.supp`:
```
# 抑制已知的误报
leak:known_leak_function
```

### ThreadSanitizer 抑制

创建 `.github/tsan.supp`:
```
# 抑制已知的数据竞争
race:known_race_function
```

然后在 workflow 中使用：
```yaml
env:
  ASAN_OPTIONS: suppressions=.github/asan.supp
  TSAN_OPTIONS: suppressions=.github/tsan.supp
```

## 禁用特定检查

如果某个 sanitizer 检查持续产生噪音，可以临时禁用：

```yaml
# 在 .github/workflows/ci-enhanced.yml 中
sanitizers:
  strategy:
    matrix:
      include:
        # - sanitizer: address  # 临时禁用
        - sanitizer: leak
        - sanitizer: thread
```

## 参考资源

- [AddressSanitizer 文档](https://github.com/google/sanitizers/wiki/AddressSanitizer)
- [ThreadSanitizer 文档](https://github.com/google/sanitizers/wiki/ThreadSanitizerCppManual)
- [Rust Sanitizers 指南](https://doc.rust-lang.org/beta/unstable-book/compiler-flags/sanitizer.html)

## 总结

这些新增的检查是为了提高代码质量，但不应该阻碍开发进度。它们提供了有价值的诊断信息，可以帮助我们逐步改进代码库的健壮性和安全性。
