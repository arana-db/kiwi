# Kiwi Development Scripts

This directory contains helper scripts for building and developing Kiwi.

## Quick Start

### Windows

```cmd
# Quick check (fastest)
scripts\dev check

# Auto-watch mode (will prompt to install cargo-watch if needed)
scripts\dev watch

# Build and run (automatically uses sccache if installed)
scripts\dev run
```

### Linux/macOS

```bash
# Make scripts executable (first time only)
chmod +x scripts/*.sh

# Quick check (fastest)
./scripts/dev.sh check

# Auto-watch mode (will prompt to install cargo-watch if needed)
./scripts/dev.sh watch

# Build and run (automatically uses sccache if installed)
./scripts/dev.sh run
```

**Note**: The scripts **automatically detect and use sccache** if installed. For first-time setup, run `quick_setup` to install both sccache and cargo-watch.

## Available Scripts

### Development Helper (`dev`)

Main development tool with multiple commands. **Automatically uses sccache if available.**

**Windows:** `scripts\dev.cmd [command]`  
**Linux/macOS:** `./scripts/dev.sh [command]`

Commands:
- `check` - Quick syntax check (fastest, recommended for development)
- `build` - Build the project (uses sccache automatically if installed)
- `run` - Build and run the server (uses sccache automatically if installed)
- `test` - Run tests
- `clean` - Clean build artifacts
- `watch` - Auto-check on file changes (prompts to install cargo-watch if needed)
- `stats` - Show build statistics and cache info

Options:
- `--release` - Use release profile
- `-v` - Verbose output

Examples:
```bash
# Quick check
./scripts/dev.sh check

# Release build (with sccache if available)
./scripts/dev.sh build --release

# Watch mode with verbose output
./scripts/dev.sh watch -v
```

### Quick Setup (`quick_setup`)

One-command setup to install sccache and cargo-watch.

**Windows:** `scripts\quick_setup.cmd`  
**Linux/macOS:** `./scripts/quick_setup.sh`

This script will:
1. Install sccache if not already installed
2. Configure Cargo to use sccache
3. Install cargo-watch if not already installed
4. Show how to verify the setup

**Run once for maximum build speed!**

### Diagnostics (`diagnose`)

Diagnose build issues and check optimization status.

**Windows:** `scripts\diagnose.cmd`  
**Linux/macOS:** `./scripts/diagnose.sh`

This will check:
- sccache installation and configuration
- Incremental compilation settings
- Build cache status
- Cargo configuration

### Fast Build (`fast_build`)

Optimized build script with caching support.

**Windows:** `scripts\fast_build.ps1 [options]`  
**Linux/macOS:** `./scripts/fast_build.sh [options]`

Options:
- `--release` - Build in release mode
- `--clean` - Clean build artifacts
- `--check` - Run check instead of build

### Setup sccache (`setup_sccache`)

Install and configure sccache only (without cargo-watch).

**Windows:** `scripts\setup_sccache.ps1`  
**Linux/macOS:** `./scripts/setup_sccache.sh`

## Automatic Optimizations

The `dev` scripts automatically apply these optimizations:

### 1. sccache Integration

If sccache is installed, the scripts automatically:
- Set `RUSTC_WRAPPER=sccache` environment variable
- Start the sccache server if not running
- Use cached compilation results

**Result**: 50-90% faster builds after first compilation!

### 2. Incremental Compilation

Scripts automatically set:
- `CARGO_INCREMENTAL=1`
- `CARGO_BUILD_JOBS` to number of CPU cores

**Result**: Only recompile what changed!

### 3. cargo-watch Integration

The `watch` command will:
- Check if cargo-watch is installed
- Prompt to install if not found
- Automatically check code on file save

**Result**: Instant feedback (5-10 seconds) when you save!

## Performance Tips

### Use `check` for Development

During development, use `check` instead of `build`:
```bash
./scripts/dev.sh check  # 5-10x faster than build
```

### Enable Auto-Watch

Use watch mode for automatic checking on file save:
```bash
./scripts/dev.sh watch
```

### Install sccache (Highly Recommended)

For maximum speed, install and configure sccache:
```bash
# One command to install everything
./scripts/quick_setup.sh
```

After setup, compilation times can be reduced by 50-90%!

### Build Only What Changed

Build specific packages instead of the whole workspace:
```bash
cargo build -p server    # Only build server
cargo build -p runtime   # Only build runtime
```

## Troubleshooting

### Scripts Not Executable (Linux/macOS)

```bash
chmod +x scripts/*.sh
```

### PowerShell Execution Policy (Windows)

If you get an execution policy error:
```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

Or run scripts with bypass:
```powershell
powershell -ExecutionPolicy Bypass -File scripts\dev.ps1 check
```

### sccache Not Working

```bash
# Check status
sccache --show-stats

# Restart sccache
sccache --stop-server
sccache --start-server

# Clear cache
sccache --zero-stats
```

### cargo-watch Not Found

The `watch` command will automatically prompt to install cargo-watch. Or install manually:
```bash
cargo install cargo-watch
```

## Performance Comparison

| Operation | Without sccache | With sccache | Using check |
|-----------|----------------|--------------|-------------|
| First build | ~18 min | ~18 min | ~5 min |
| Incremental | ~2-5 min | ~1-2 min | ~10-30 sec |
| Small changes | ~2-5 min | ~30 sec | ~5-10 sec |

## More Information

See the [docs](../docs) directory for detailed documentation:
- [Quick Start Guide](../docs/QUICK_START.md)
- [Build Optimization](../docs/BUILD_OPTIMIZATION.md)
- [Why Recompiling?](../docs/WHY_RECOMPILING.md)
- [Compilation FAQ](../docs/COMPILATION_FAQ.md)
- [中文使用说明](../docs/使用说明.txt)
