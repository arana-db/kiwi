# Kiwi 快速开发指南

## 🚀 快速开始

### 开发模式（推荐）

**Windows:**
```cmd
# 快速检查代码（不生成可执行文件，速度快 5-10 倍）
scripts\dev check
```

**Linux/macOS:**
```bash
# 首次使用需要添加执行权限
chmod +x scripts/*.sh

# 快速检查代码
./scripts/dev.sh check
```

**跨平台（使用 cargo）:**
```bash
cargo check
```

### 构建和运行

**Windows:**
```cmd
# 构建（开发模式）
scripts\dev build

# 构建并运行
scripts\dev run

# Release 模式构建
scripts\dev build --release
```

**Linux/macOS:**
```bash
# 构建（开发模式）
./scripts/dev.sh build

# 构建并运行
./scripts/dev.sh run

# Release 模式构建
./scripts/dev.sh build --release
```

### 自动监视模式（最推荐）

**Windows:**
```cmd
# 自动监视文件变化，实时检查代码
scripts\dev watch
```

**Linux/macOS:**
```bash
# 自动监视文件变化，实时检查代码
./scripts/dev.sh watch
```

这会在你保存文件时自动运行 cargo check，大大提高开发效率！

## ⚡ 加速编译

### 方法 1: 使用 sccache（最有效）

```bash
# 1. 安装 sccache
cargo install sccache

# 2. 配置（运行一次即可）
# Windows:
scripts\setup_sccache.ps1
# Linux/macOS:
./scripts/setup_sccache.sh

# 3. 查看缓存统计
# Windows:
scripts\dev stats
# Linux/macOS:
./scripts/dev.sh stats
```

**效果**：首次编译后，后续编译可节省 50-90% 时间！

### 方法 2: 使用 cargo check 代替 cargo build

```powershell
# 开发时只检查语法，不生成可执行文件
cargo check          # 快 5-10 倍

# 需要运行时才 build
cargo build
```

### 方法 3: 只编译修改的模块

```powershell
# 只编译 server 模块
cargo build -p server

# 只编译 runtime 模块  
cargo build -p runtime

# 编译多个模块
cargo build -p server -p net -p runtime
```

## 📊 查看编译统计

```powershell
# 查看构建统计、缓存大小、sccache 状态
.\dev.ps1 stats
```

## 🛠️ 常用命令

| 命令 | 说明 | 速度 |
|------|------|------|
| `scripts\dev check` (Win) / `./scripts/dev.sh check` (Unix) | 快速检查代码 | ⚡⚡⚡ 最快 |
| `scripts\dev watch` (Win) / `./scripts/dev.sh watch` (Unix) | 自动监视并检查 | ⚡⚡⚡ 最快 |
| `cargo check` | 检查代码 | ⚡⚡⚡ 快 |
| `cargo build -p server` | 只编译 server | ⚡⚡ 较快 |
| `scripts\dev build` (Win) / `./scripts/dev.sh build` (Unix) | 完整构建 | ⚡ 正常 |
| `cargo build` | 完整构建 | ⚡ 正常 |
| `scripts\dev run` (Win) / `./scripts/dev.sh run` (Unix) | 构建并运行 | ⚡ 正常 |

## 🎯 推荐工作流

### 日常开发

**Windows:**
```cmd
# 1. 启动自动监视（在一个终端）
scripts\dev watch

# 2. 编辑代码，保存后自动检查

# 3. 需要运行时（在另一个终端）
scripts\dev run
```

**Linux/macOS:**
```bash
# 1. 启动自动监视（在一个终端）
./scripts/dev.sh watch

# 2. 编辑代码，保存后自动检查

# 3. 需要运行时（在另一个终端）
./scripts/dev.sh run
```

### 首次设置

```bash
# 1. 安装加速工具
cargo install sccache cargo-watch

# 2. 配置 sccache
# Windows:
scripts\setup_sccache.ps1
# Linux/macOS:
./scripts/setup_sccache.sh

# 3. 首次完整构建（会比较慢）
cargo build

# 4. 之后的构建会快很多！
```

## 💡 为什么 librocksdb-sys 编译慢？

librocksdb-sys 是一个 C++ 库的 Rust 绑定，需要：
1. 编译整个 RocksDB C++ 库
2. 生成 Rust 绑定代码
3. 编译绑定代码

**解决方案**：
- ✅ 使用 sccache 缓存编译结果
- ✅ 使用 cargo check 避免重复编译
- ✅ 启用增量编译（已配置）
- ✅ 避免 `cargo clean`

## 🔧 故障排除

### 编译仍然很慢？

```powershell
# 1. 检查 sccache 是否工作
sccache --show-stats

# 2. 重启 sccache
sccache --stop-server
sccache --start-server

# 3. 清理并重建（最后手段）
cargo clean
cargo build
```

### 磁盘空间不足？

```powershell
# 查看 target 目录大小
.\dev.ps1 stats

# 清理旧的构建产物
cargo clean

# 只保留 release 构建
Remove-Item target\debug -Recurse -Force
```

## 📈 性能对比

| 场景 | 时间 |
|------|------|
| 首次 `cargo build` | ~18 分钟 |
| 增量 `cargo build` | ~2-5 分钟 |
| 使用 sccache | ~1-2 分钟 |
| `cargo check` | ~10-30 秒 |
| `cargo check`（增量） | ~5-10 秒 |

## 🎓 更多信息

详细的优化说明请查看：[BUILD_OPTIMIZATION.md](BUILD_OPTIMIZATION.md)
