# Development Scripts Improvements

## Summary

The development scripts (`dev.sh`, `dev.cmd`, `dev.ps1`) have been enhanced to **automatically detect and use sccache** and **cargo-watch** for optimal build performance.

## Key Improvements

### 1. Automatic sccache Integration

**What it does:**
- Automatically detects if sccache is installed
- Sets `RUSTC_WRAPPER=sccache` environment variable
- Starts sccache server if not running
- Shows helpful tips if sccache is not installed

**Benefits:**
- No manual configuration needed
- 50-90% faster builds after first compilation
- Works across all build commands (`build`, `run`)

**Example:**
```bash
# Before: Manual setup required
export RUSTC_WRAPPER=sccache
cargo build

# After: Automatic
./scripts/dev.sh build  # Automatically uses sccache if installed
```

### 2. Smart cargo-watch Integration

**What it does:**
- Checks if cargo-watch is installed when using `watch` command
- Prompts user to install if not found
- Provides clear installation instructions

**Benefits:**
- No need to remember to install cargo-watch
- Interactive installation prompt
- Clear error messages

**Example:**
```bash
# If cargo-watch not installed:
./scripts/dev.sh watch
# Output:
# cargo-watch not found.
# Install cargo-watch now? (y/n)
```

### 3. Helpful Tips and Warnings

**What it does:**
- Shows optimization tips when sccache is not installed
- Provides quick setup instructions
- Non-intrusive warnings only when relevant

**Example:**
```bash
./scripts/dev.sh build
# If sccache not installed:
# ⚡ Tip: Install sccache for faster builds: cargo install sccache
#    Or run: ./scripts/quick_setup.sh
```

## Updated Scripts

### Linux/macOS (`dev.sh`)

**Changes:**
1. Added automatic sccache detection and setup
2. Added interactive cargo-watch installation prompt
3. Added helpful tips for optimization

**Usage:**
```bash
chmod +x scripts/dev.sh

# All commands automatically use sccache if available
./scripts/dev.sh check
./scripts/dev.sh build
./scripts/dev.sh run
./scripts/dev.sh watch  # Prompts to install cargo-watch if needed
```

### Windows Batch (`dev.cmd`)

**Changes:**
1. Added automatic sccache detection and setup
2. Added interactive cargo-watch installation prompt
3. Added helpful tips for optimization

**Usage:**
```cmd
REM All commands automatically use sccache if available
scripts\dev check
scripts\dev build
scripts\dev run
scripts\dev watch  REM Prompts to install cargo-watch if needed
```

### Windows PowerShell (`dev.ps1`)

**Changes:**
1. Added automatic sccache detection and setup
2. Added interactive cargo-watch installation prompt
3. Added helpful tips for optimization

**Usage:**
```powershell
# All commands automatically use sccache if available
.\scripts\dev.ps1 check
.\scripts\dev.ps1 build
.\scripts\dev.ps1 run
.\scripts\dev.ps1 watch  # Prompts to install cargo-watch if needed
```

## Environment Variables Set

The scripts automatically set these environment variables:

```bash
CARGO_INCREMENTAL=1              # Enable incremental compilation
CARGO_BUILD_JOBS=<cpu_count>     # Parallel compilation
RUSTC_WRAPPER=sccache            # Use sccache (if installed)
```

## Quick Setup

For first-time users, we provide a one-command setup:

```bash
# Linux/macOS
./scripts/quick_setup.sh

# Windows
scripts\quick_setup.cmd
```

This installs and configures:
- sccache (for build caching)
- cargo-watch (for auto-checking)

## Performance Impact

### Before Improvements

```bash
# Manual setup required
export RUSTC_WRAPPER=sccache
cargo install cargo-watch
cargo build  # ~18 min first time, ~2-5 min incremental
```

### After Improvements

```bash
# One-time setup (optional)
./scripts/quick_setup.sh

# Daily usage - automatic optimization
./scripts/dev.sh build  # ~18 min first time, ~1-2 min with sccache
./scripts/dev.sh check  # ~5 min first time, ~10-30 sec incremental
./scripts/dev.sh watch  # ~5-10 sec per change
```

## Comparison Table

| Feature | Before | After |
|---------|--------|-------|
| sccache setup | Manual | Automatic |
| cargo-watch install | Manual | Interactive prompt |
| Environment variables | Manual | Automatic |
| Optimization tips | None | Shown when relevant |
| Build speed (with sccache) | Same | Same (but easier to use) |
| User experience | Complex | Simple |

## Migration Guide

### For Existing Users

No changes needed! The scripts work exactly as before, but now:
- Automatically use sccache if you have it installed
- Prompt to install cargo-watch when needed
- Show helpful tips for optimization

### For New Users

Just run:
```bash
# One-time setup
./scripts/quick_setup.sh  # or quick_setup.cmd on Windows

# Daily usage
./scripts/dev.sh check    # Fast checking
./scripts/dev.sh watch    # Auto-check on save
./scripts/dev.sh run      # Build and run
```

## Diagnostic Tools

New diagnostic script to check optimization status:

```bash
# Linux/macOS
./scripts/diagnose.sh

# Windows
scripts\diagnose.cmd
```

This shows:
- sccache installation status
- cargo-watch installation status
- Environment variable settings
- Build cache status
- Optimization recommendations

## Documentation Updates

Updated documentation:
- [README.md](../README.md) - Main README with new quick start
- [README_CN.md](../README_CN.md) - Chinese version
- [scripts/README.md](../scripts/README.md) - Scripts documentation
- [QUICK_START.md](QUICK_START.md) - Quick start guide
- [WHY_RECOMPILING.md](WHY_RECOMPILING.md) - Compilation troubleshooting
- [COMPILATION_FAQ.md](COMPILATION_FAQ.md) - FAQ

## Benefits Summary

1. **Easier to use**: No manual configuration needed
2. **Faster builds**: Automatic sccache integration
3. **Better UX**: Interactive prompts and helpful tips
4. **Cross-platform**: Works on Windows, Linux, and macOS
5. **Backward compatible**: Existing workflows still work

## Next Steps

1. Run `./scripts/quick_setup.sh` (or `.cmd` on Windows) for one-time setup
2. Use `./scripts/dev.sh check` for daily development
3. Use `./scripts/dev.sh watch` for auto-checking
4. Enjoy faster builds! 🚀
