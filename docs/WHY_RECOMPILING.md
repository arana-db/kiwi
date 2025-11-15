# 为什么每次都重新编译？

## 问题现象

运行 `cargo build` 或 `cargo run` 时，看到 librocksdb-sys 等大型依赖重新编译。

## 可能的原因

### 1. 首次编译（正常）

如果是第一次编译或清理了 target 目录，所有依赖都需要编译。

**解决方案**：等待首次编译完成，之后会使用缓存。

### 2. 依赖 features 变化

不同的编译配置（debug/release）或 features 会导致重新编译。

**检查方法**：
```bash
# 查看编译详情
cargo build --verbose 2>&1 | grep -E "Fresh|Compiling"
```

如果看到 "Fresh librocksdb-sys"，说明使用了缓存。
如果看到 "Compiling librocksdb-sys"，说明需要重新编译。

### 3. 时间戳问题

文件系统时间戳不一致可能导致 Cargo 认为文件已更改。

**解决方案**：
```bash
# 清理并重建
cargo clean
cargo build
```

### 4. 构建脚本（build.rs）变化

librocksdb-sys 有复杂的 build.rs，某些环境变量或系统状态变化会触发重新编译。

**常见触发因素**：
- 环境变量变化
- 系统库版本变化
- 编译器版本变化

### 5. 没有安装 sccache

sccache 可以缓存编译结果，即使清理了 target 目录也能快速恢复。

**解决方案**：
```bash
# 快速安装和配置
# Windows:
scripts\quick_setup.cmd

# Linux/macOS:
chmod +x scripts/quick_setup.sh
./scripts/quick_setup.sh
```

## 如何验证是否使用了缓存

### 方法 1：使用诊断脚本

```bash
# Windows:
scripts\diagnose.cmd

# Linux/macOS:
chmod +x scripts/diagnose.sh
./scripts/diagnose.sh
```

### 方法 2：查看编译输出

```bash
cargo build --verbose 2>&1 | grep librocksdb
```

输出示例：
- ✅ `Fresh librocksdb-sys v0.17.1+9.9.3` - 使用缓存
- ❌ `Compiling librocksdb-sys v0.17.1+9.9.3` - 重新编译

### 方法 3：检查 target 目录

```bash
# Windows:
dir target\debug\deps\*rocksdb*.rlib

# Linux/macOS:
ls -lh target/debug/deps/*rocksdb*.rlib
```

如果文件存在且时间戳较旧，说明有缓存。

## 优化建议

### 1. 使用 cargo check（最重要）

开发时不需要生成可执行文件，只需要检查语法：

```bash
# 比 cargo build 快 5-10 倍
cargo check

# 或使用脚本
# Windows:
scripts\dev check

# Linux/macOS:
./scripts/dev.sh check
```

### 2. 安装 sccache

sccache 可以缓存编译结果，即使在不同项目间也能共享：

```bash
# 一键安装和配置
# Windows:
scripts\quick_setup.cmd

# Linux/macOS:
./scripts/quick_setup.sh
```

安装后，首次编译仍需时间，但之后会快很多：
- 首次：~18 分钟
- 之后：~1-2 分钟（使用 sccache）

### 3. 使用 watch 模式

自动监视文件变化，保存时自动检查：

```bash
# Windows:
scripts\dev watch

# Linux/macOS:
./scripts/dev.sh watch
```

### 4. 只编译修改的模块

如果只修改了某个模块，只编译该模块：

```bash
# 只编译 server 模块
cargo build -p server

# 只编译 runtime 模块
cargo build -p runtime
```

### 5. 避免 cargo clean

除非必要，不要运行 `cargo clean`，这会删除所有缓存。

## 性能对比

| 操作 | 时间（首次） | 时间（增量） | 时间（sccache） |
|------|-------------|-------------|----------------|
| cargo build | ~18 分钟 | ~2-5 分钟 | ~1-2 分钟 |
| cargo check | ~5 分钟 | ~10-30 秒 | ~5-15 秒 |
| cargo check（小改动） | - | ~5-10 秒 | ~5-10 秒 |

## 常见问题

### Q: 我已经编译过了，为什么还要重新编译？

A: 检查以下几点：
1. 是否切换了编译模式（debug ↔ release）
2. 是否修改了 Cargo.toml 中的依赖或 features
3. 是否运行了 `cargo clean`
4. 运行诊断脚本查看详情

### Q: sccache 安装后还是很慢？

A: sccache 需要首次编译来建立缓存。首次编译后，清理 target 目录再编译会快很多。

验证 sccache 是否工作：
```bash
sccache --show-stats
```

### Q: 如何完全避免重新编译 librocksdb-sys？

A: 
1. 安装 sccache（最有效）
2. 不要运行 `cargo clean`
3. 开发时使用 `cargo check` 而不是 `cargo build`
4. 使用 watch 模式自动检查

### Q: 为什么 Linux 下编译比 Windows 慢？

A: 可能的原因：
1. 没有安装 sccache
2. 系统资源限制
3. 文件系统性能差异

解决方案：
1. 安装 sccache
2. 使用 `cargo check` 代替 `cargo build`
3. 增加并行编译任务数：`export CARGO_BUILD_JOBS=8`

## 立即行动

### 最快的解决方案

```bash
# 1. 运行快速设置（安装 sccache 和 cargo-watch）
# Windows:
scripts\quick_setup.cmd

# Linux/macOS:
chmod +x scripts/quick_setup.sh
./scripts/quick_setup.sh

# 2. 使用 check 代替 build
# Windows:
scripts\dev check

# Linux/macOS:
./scripts/dev.sh check

# 3. 使用 watch 模式
# Windows:
scripts\dev watch

# Linux/macOS:
./scripts/dev.sh watch
```

这样可以将开发时的编译时间从 **18 分钟降低到 10-30 秒**！
