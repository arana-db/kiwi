# Recent Changes - Build System Improvements

## Overview

Reorganized project structure and added comprehensive build optimization tools and documentation.

## Changes Made

### 1. Directory Structure

Created two new directories:

- **`scripts/`** - All development and build scripts
- **`docs/`** - All documentation files

### 2. Scripts Added

#### Cross-Platform Development Scripts

**Windows:**
- `scripts/dev.cmd` - Main development helper (batch file)
- `scripts/dev.ps1` - PowerShell version with advanced features
- `scripts/fast_build.ps1` - Fast build script
- `scripts/setup_sccache.ps1` - sccache configuration

**Linux/macOS:**
- `scripts/dev.sh` - Main development helper (bash)
- `scripts/fast_build.sh` - Fast build script
- `scripts/setup_sccache.sh` - sccache configuration

**Documentation:**
- `scripts/README.md` - Comprehensive scripts usage guide

### 3. Documentation Added/Updated

**Main Documentation:**
- `README.md` - Updated with build instructions and quick start
- `README_CN.md` - New Chinese version of README

**Build Guides:**
- `docs/QUICK_START.md` - Quick start guide (updated for cross-platform)
- `docs/BUILD_OPTIMIZATION.md` - Detailed build optimization guide
- `docs/编译加速方案总结.md` - Chinese build optimization guide
- `docs/使用说明.txt` - Chinese usage instructions
- `docs/PROJECT_STRUCTURE.md` - Project structure documentation

### 4. Configuration Improvements

**`.cargo/config.toml`:**
- Enabled incremental compilation
- Configured parallel build jobs
- Optimized build cache settings

**`Cargo.toml`:**
- Reduced debug optimization level for faster dev builds
- Enabled incremental compilation in dev profile

### 5. Bug Fixes

**`src/server/src/main.rs`:**
- Fixed issue where storage server blocked network server startup
- Changed storage server to run in background
- Server now properly listens on port 7379

## Usage

### Quick Start

**Windows:**
```cmd
# Quick check (fastest)
scripts\dev check

# Auto-watch mode
scripts\dev watch

# Build and run
scripts\dev run
```

**Linux/macOS:**
```bash
# Make scripts executable (first time)
chmod +x scripts/*.sh

# Quick check (fastest)
./scripts/dev.sh check

# Auto-watch mode
./scripts/dev.sh watch

# Build and run
./scripts/dev.sh run
```

### Performance Improvements

With these changes:

1. **Development workflow**: Use `cargo check` or `scripts/dev check` for 5-10x faster feedback
2. **Auto-watch mode**: Automatic checking on file save
3. **sccache support**: 50-90% faster rebuilds after initial compilation
4. **Incremental compilation**: Only rebuild what changed

### Expected Build Times

| Operation | Before | After (with optimizations) |
|-----------|--------|---------------------------|
| Full build | ~18 min | ~18 min (first time) |
| Incremental build | ~2-5 min | ~1-2 min (with sccache) |
| cargo check | ~5 min | ~10-30 sec (incremental) |
| Small changes | ~2-5 min | ~5-10 sec (with watch) |

## Migration Guide

### For Existing Developers

1. **Update your workflow:**
   ```bash
   # Old way
   cargo build
   cargo run
   
   # New way (faster)
   scripts/dev check    # or ./scripts/dev.sh check
   scripts/dev run      # or ./scripts/dev.sh run
   ```

2. **Install sccache (optional but recommended):**
   ```bash
   cargo install sccache
   # Windows:
   scripts\setup_sccache.ps1
   # Linux/macOS:
   ./scripts/setup_sccache.sh
   ```

3. **Use auto-watch for development:**
   ```bash
   # Windows:
   scripts\dev watch
   # Linux/macOS:
   ./scripts/dev.sh watch
   ```

### For New Developers

Follow the [Quick Start Guide](docs/QUICK_START.md) or [README](README.md).

## Documentation

All documentation is now organized in the `docs/` directory:

- English: [QUICK_START.md](docs/QUICK_START.md), [BUILD_OPTIMIZATION.md](docs/BUILD_OPTIMIZATION.md)
- Chinese: [编译加速方案总结.md](docs/编译加速方案总结.md), [使用说明.txt](docs/使用说明.txt)

## Breaking Changes

None. All existing workflows continue to work. New scripts and documentation are additions only.

## Future Improvements

- [ ] Add CI/CD integration for scripts
- [ ] Add more development helpers (lint, format, etc.)
- [ ] Add Docker build scripts
- [ ] Add benchmark scripts

## Feedback

If you have suggestions or issues with the new structure, please open an issue or PR.
