# Compilation FAQ - 编译常见问题

[English](#english) | [中文](#中文)

---

## English

### Q: Why does librocksdb-sys recompile every time?

**A:** There are several possible reasons:

1. **First-time compilation** - Normal behavior, wait for it to complete
2. **No sccache installed** - Install it for caching across builds
3. **Different build profiles** - Debug and release builds are separate
4. **Dependency changes** - Modified Cargo.toml or features

**Quick fix:**
```bash
# Install sccache and cargo-watch
# Windows:
scripts\quick_setup.cmd

# Linux/macOS:
./scripts/quick_setup.sh
```

### Q: How can I speed up development?

**A:** Use `cargo check` instead of `cargo build`:

```bash
# 5-10x faster than cargo build
cargo check

# Or use the dev script
# Windows:
scripts\dev check

# Linux/macOS:
./scripts/dev.sh check
```

### Q: How do I verify if caching is working?

**A:** Run the diagnostic script:

```bash
# Windows:
scripts\diagnose.cmd

# Linux/macOS:
./scripts/diagnose.sh
```

Look for "Fresh librocksdb-sys" in the output - this means caching is working.

### Q: What's the difference between cargo build and cargo check?

**A:**

| Command | What it does | Speed | Use case |
|---------|-------------|-------|----------|
| `cargo build` | Compiles and generates executable | Slow (~18 min first time) | When you need to run the program |
| `cargo check` | Only checks syntax and types | Fast (~5 min first time, ~10-30s incremental) | During development |

### Q: How do I use watch mode?

**A:** Watch mode automatically checks your code when you save files:

```bash
# Windows:
scripts\dev watch

# Linux/macOS:
./scripts/dev.sh watch
```

This gives you instant feedback (5-10 seconds) when you save a file.

### Q: I installed sccache but it's still slow?

**A:** sccache needs to build the cache first. After the first compilation:

1. Check if it's working: `sccache --show-stats`
2. Clean and rebuild to test: `cargo clean && cargo build`
3. Second build should be much faster (~1-2 minutes vs ~18 minutes)

### Q: Error: "incremental compilation is prohibited"?

**A:** This happens when both sccache and incremental compilation are enabled. They conflict with each other.

**Solution:** Use the updated scripts - they automatically handle this:
- With sccache: Sets `CARGO_INCREMENTAL=0`
- Without sccache: Sets `CARGO_INCREMENTAL=1`

Or manually:
```bash
export CARGO_INCREMENTAL=0
cargo build
```

See [sccache vs Incremental Compilation](SCCACHE_VS_INCREMENTAL.md) for details.

### Q: Should I ever run cargo clean?

**A:** Only when:
- You have build errors that won't go away
- You're switching Rust versions
- You're troubleshooting build issues

Otherwise, avoid it - it deletes all your cached builds.

---

## 中文

### 问：为什么 librocksdb-sys 每次都重新编译？

**答：** 可能有以下几个原因：

1. **首次编译** - 正常现象，等待完成即可
2. **未安装 sccache** - 安装后可以跨构建缓存
3. **不同的构建配置** - Debug 和 release 构建是分开的
4. **依赖变化** - 修改了 Cargo.toml 或 features

**快速解决：**
```bash
# 安装 sccache 和 cargo-watch
# Windows:
scripts\quick_setup.cmd

# Linux/macOS:
./scripts/quick_setup.sh
```

### 问：如何加速开发？

**答：** 使用 `cargo check` 代替 `cargo build`：

```bash
# 比 cargo build 快 5-10 倍
cargo check

# 或使用开发脚本
# Windows:
scripts\dev check

# Linux/macOS:
./scripts/dev.sh check
```

### 问：如何验证缓存是否工作？

**答：** 运行诊断脚本：

```bash
# Windows:
scripts\diagnose.cmd

# Linux/macOS:
./scripts/diagnose.sh
```

查看输出中的 "Fresh librocksdb-sys" - 这表示缓存正在工作。

### 问：cargo build 和 cargo check 有什么区别？

**答：**

| 命令 | 作用 | 速度 | 使用场景 |
|------|------|------|---------|
| `cargo build` | 编译并生成可执行文件 | 慢（首次约 18 分钟） | 需要运行程序时 |
| `cargo check` | 只检查语法和类型 | 快（首次约 5 分钟，增量 10-30 秒） | 开发过程中 |

### 问：如何使用监视模式？

**答：** 监视模式会在你保存文件时自动检查代码：

```bash
# Windows:
scripts\dev watch

# Linux/macOS:
./scripts/dev.sh watch
```

这样可以在保存文件后立即获得反馈（5-10 秒）。

### 问：我安装了 sccache 但还是很慢？

**答：** sccache 需要先建立缓存。首次编译后：

1. 检查是否工作：`sccache --show-stats`
2. 清理并重建测试：`cargo clean && cargo build`
3. 第二次构建应该快很多（约 1-2 分钟 vs 约 18 分钟）

### 问：我应该运行 cargo clean 吗？

**答：** 只在以下情况：
- 遇到无法解决的构建错误
- 切换 Rust 版本
- 排查构建问题

否则避免使用 - 它会删除所有缓存的构建。

---

## Quick Reference - 快速参考

### Fastest Development Workflow - 最快的开发流程

```bash
# 1. One-time setup - 一次性设置
# Windows:
scripts\quick_setup.cmd
# Linux/macOS:
./scripts/quick_setup.sh

# 2. Daily development - 日常开发
# Terminal 1 - 终端 1:
scripts\dev watch  # or ./scripts/dev.sh watch

# Terminal 2 - 终端 2: Edit code, save, see results in seconds
# 编辑代码，保存，几秒钟内看到结果

# 3. When you need to run - 需要运行时:
scripts\dev run  # or ./scripts/dev.sh run
```

### Performance Comparison - 性能对比

| Operation - 操作 | Time - 时间 |
|-----------------|------------|
| First `cargo build` - 首次构建 | ~18 minutes - 约 18 分钟 |
| Incremental `cargo build` - 增量构建 | ~2-5 minutes - 约 2-5 分钟 |
| With sccache - 使用 sccache | ~1-2 minutes - 约 1-2 分钟 |
| `cargo check` - 快速检查 | ~10-30 seconds - 约 10-30 秒 |
| Watch mode (small changes) - 监视模式（小改动） | ~5-10 seconds - 约 5-10 秒 |

---

## More Information - 更多信息

- [Why Recompiling?](WHY_RECOMPILING.md) - Detailed diagnosis
- [Build Optimization](BUILD_OPTIMIZATION.md) - Complete guide
- [Quick Start](QUICK_START.md) - Getting started
- [编译加速方案总结](编译加速方案总结.md) - 中文详细指南
