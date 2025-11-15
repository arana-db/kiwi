# Kiwi 编译优化指南

## 问题
每次 `cargo build` 或 `cargo run` 都会重新编译 `librocksdb-sys`，耗时很长（通常需要几分钟）。

## 解决方案

### 方案 1: 使用 sccache（最推荐）

sccache 是一个编译缓存工具，可以缓存编译结果，大幅加速重复编译。

#### 安装和配置

```powershell
# 1. 安装 sccache
cargo install sccache

# 2. 配置 Cargo 使用 sccache
# 编辑 ~/.cargo/config.toml 或运行：
.\setup_sccache.ps1

# 3. 验证配置
sccache --show-stats
```

#### 使用效果
- 首次编译：正常时间
- 后续编译：可节省 50-90% 的时间

### 方案 2: 优化 Cargo 配置（已应用）

已经在 `.cargo/config.toml` 和 `Cargo.toml` 中添加了优化配置：

```toml
[build]
incremental = true  # 启用增量编译
jobs = 8           # 并行编译任务数

[profile.dev]
opt-level = 0      # 降低优化级别，加快编译
debug = 1          # 减少调试信息
incremental = true # 增量编译
```

### 方案 3: 使用 cargo check 代替 cargo build

在开发过程中，如果只是想检查代码是否能编译通过，使用 `cargo check`：

```powershell
# 只检查，不生成可执行文件（快 5-10 倍）
cargo check

# 检查特定包
cargo check -p server
```

### 方案 4: 使用 cargo-watch 自动编译

安装 cargo-watch 实现自动增量编译：

```powershell
# 安装
cargo install cargo-watch

# 监视文件变化，自动运行 check
cargo watch -x check

# 监视文件变化，自动运行 build
cargo watch -x build

# 监视文件变化，自动运行测试
cargo watch -x test
```

### 方案 5: 分离依赖编译

如果你经常修改某些模块，可以只编译这些模块：

```powershell
# 只编译 server 模块
cargo build -p server

# 只编译 runtime 模块
cargo build -p runtime

# 编译多个模块
cargo build -p server -p runtime -p net
```

### 方案 6: 使用 mold 链接器（Linux/WSL）

如果在 WSL 或 Linux 环境下，可以使用 mold 链接器加速链接过程：

```bash
# 安装 mold
sudo apt install mold

# 配置 Cargo 使用 mold
# 在 .cargo/config.toml 中添加：
[target.x86_64-unknown-linux-gnu]
linker = "clang"
rustflags = ["-C", "link-arg=-fuse-ld=mold"]
```

## 为什么 librocksdb-sys 总是重新编译？

可能的原因：

1. **构建脚本变化**：librocksdb-sys 有复杂的 build.rs，某些环境变量或系统状态变化会触发重新编译
2. **时间戳问题**：文件系统时间戳不一致
3. **依赖特性变化**：不同的 feature 组合会导致重新编译
4. **缓存失效**：target 目录被清理或损坏

## 快速构建脚本

使用提供的 `fast_build.ps1` 脚本：

```powershell
# 开发模式构建
.\fast_build.ps1

# Release 模式构建
.\fast_build.ps1 -Release

# 只检查不构建
.\fast_build.ps1 -Check

# 清理构建缓存
.\fast_build.ps1 -Clean
```

## 最佳实践

1. **开发时使用 cargo check**：快速验证代码
2. **使用 sccache**：缓存编译结果
3. **启用增量编译**：已在配置中启用
4. **避免 cargo clean**：除非必要，不要清理 target 目录
5. **使用 cargo-watch**：自动增量编译
6. **分模块编译**：只编译修改的模块

## 性能对比

| 操作 | 时间（首次） | 时间（增量） | 时间（sccache） |
|------|-------------|-------------|----------------|
| cargo build | ~18 分钟 | ~2-5 分钟 | ~1-2 分钟 |
| cargo check | ~5 分钟 | ~10-30 秒 | ~5-15 秒 |
| cargo build -p server | ~3 分钟 | ~10-30 秒 | ~5-15 秒 |

## 故障排除

### 如果编译仍然很慢

1. 检查是否启用了增量编译：
```powershell
$env:CARGO_INCREMENTAL = "1"
```

2. 检查 sccache 是否工作：
```powershell
sccache --show-stats
```

3. 清理并重建缓存：
```powershell
cargo clean
sccache --stop-server
sccache --start-server
cargo build
```

4. 检查磁盘空间：
```powershell
# target 目录可能很大
Get-ChildItem target -Recurse | Measure-Object -Property Length -Sum
```

## 推荐工作流

```powershell
# 1. 安装工具
cargo install sccache cargo-watch

# 2. 配置 sccache
.\setup_sccache.ps1

# 3. 开发时使用 cargo-watch
cargo watch -x check -x test

# 4. 需要运行时
cargo run

# 5. 发布构建
cargo build --release
```
