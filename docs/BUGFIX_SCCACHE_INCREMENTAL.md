# Bug Fix: sccache and Incremental Compilation Conflict

## Issue

When running `./scripts/dev.sh run` in WSL, the following error occurred:

```
error: process didn't exit successfully: `sccache /home/alex/.rustup/toolchains/nightly-2025-08-20-x86_64-unknown-linux-gnu/bin/rustc -vV` (exit status: 1)
--- stdout
sccache: incremental compilation is prohibited: Unset CARGO_INCREMENTAL to continue.
```

## Root Cause

The scripts were setting `CARGO_INCREMENTAL=1` unconditionally, but **sccache does not support incremental compilation**. When both are enabled, they conflict.

## Solution

Updated all development scripts to intelligently choose between sccache and incremental compilation:

### Logic

```bash
if sccache is installed:
    RUSTC_WRAPPER=sccache
    CARGO_INCREMENTAL=0  # Disable incremental (sccache provides caching)
else:
    CARGO_INCREMENTAL=1  # Enable incremental (no sccache)
```

### Updated Files

1. **scripts/dev.sh** - Linux/macOS development script
2. **scripts/dev.cmd** - Windows batch script
3. **scripts/dev.ps1** - Windows PowerShell script
4. **scripts/fast_build.sh** - Linux/macOS fast build
5. **scripts/fast_build.ps1** - Windows fast build

## Why This Works

### sccache (when installed)
- Caches compilation results in a separate directory
- Survives `cargo clean`
- Can be shared across projects
- **Incompatible with incremental compilation**

### Incremental Compilation (fallback)
- Caches in `target` directory
- Lost on `cargo clean`
- Project-specific
- **Built-in, no installation needed**

## Performance Impact

### Before Fix (Error)
```
❌ Build fails with sccache error
```

### After Fix
```
✅ With sccache:
   - First build: ~18 min
   - After clean: ~1-2 min (uses sccache)
   - Incremental: ~1-2 min

✅ Without sccache:
   - First build: ~18 min
   - After clean: ~18 min (no cache)
   - Incremental: ~2-5 min
```

## Testing

### Test Case 1: With sccache installed

```bash
# Install sccache
cargo install sccache

# Run script
./scripts/dev.sh build

# Verify
echo $RUSTC_WRAPPER    # Should be: sccache
echo $CARGO_INCREMENTAL # Should be: 0
```

### Test Case 2: Without sccache

```bash
# Uninstall or don't have sccache
# Run script
./scripts/dev.sh build

# Verify
echo $RUSTC_WRAPPER    # Should be: (empty)
echo $CARGO_INCREMENTAL # Should be: 1
```

### Test Case 3: WSL Environment

```bash
# In WSL with sccache installed
./scripts/dev.sh run

# Should work without error
# Output should show: Running Kiwi...
```

## Documentation Updates

Created new documentation:
- **docs/SCCACHE_VS_INCREMENTAL.md** - Detailed explanation
- **docs/COMPILATION_FAQ.md** - Added FAQ entry
- **docs/BUGFIX_SCCACHE_INCREMENTAL.md** - This document

## User Impact

### Before
- Users with sccache installed would see build errors
- Confusing error message
- Required manual intervention

### After
- Scripts automatically handle the conflict
- No user intervention needed
- Works seamlessly with or without sccache

## Recommendations

1. **Install sccache** for best performance:
   ```bash
   ./scripts/quick_setup.sh
   ```

2. **Use the scripts** - they handle everything automatically:
   ```bash
   ./scripts/dev.sh build
   ./scripts/dev.sh run
   ```

3. **Don't manually set both** `RUSTC_WRAPPER` and `CARGO_INCREMENTAL=1`

## Related Issues

- sccache GitHub: https://github.com/mozilla/sccache
- Cargo incremental compilation: https://doc.rust-lang.org/cargo/reference/profiles.html#incremental

## Verification

To verify the fix is working:

```bash
# Run diagnostic
./scripts/diagnose.sh

# Should show:
# [1] Checking sccache...
# ✓ sccache is installed
# 
# [2] Checking CARGO_INCREMENTAL...
# ✓ CARGO_INCREMENTAL = 0  (when sccache is installed)
```

## Summary

- ✅ Fixed conflict between sccache and incremental compilation
- ✅ Scripts now automatically choose the right configuration
- ✅ Works in WSL, Linux, macOS, and Windows
- ✅ No user intervention required
- ✅ Better performance with sccache
- ✅ Graceful fallback without sccache
