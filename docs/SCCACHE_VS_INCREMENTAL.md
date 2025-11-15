# sccache vs Incremental Compilation

## The Issue

sccache and Cargo's incremental compilation cannot be used together. When both are enabled, you'll see this error:

```
sccache: incremental compilation is prohibited: Unset CARGO_INCREMENTAL to continue.
```

## Why?

- **Incremental compilation** caches compilation results locally in the `target` directory
- **sccache** caches compilation results in a separate cache directory

Using both would be redundant and can cause conflicts.

## Our Solution

The development scripts automatically handle this:

### When sccache is installed:
```bash
export RUSTC_WRAPPER=sccache
export CARGO_INCREMENTAL=0  # Disable incremental compilation
```

### When sccache is NOT installed:
```bash
export CARGO_INCREMENTAL=1  # Enable incremental compilation
```

## Which is Better?

| Feature | sccache | Incremental Compilation |
|---------|---------|------------------------|
| Cache location | Separate cache dir | `target` directory |
| Survives `cargo clean` | ✅ Yes | ❌ No |
| Shared across projects | ✅ Yes | ❌ No |
| Setup required | ✅ Yes (install) | ❌ No (built-in) |
| Speed (first build) | Same | Same |
| Speed (after clean) | ⚡⚡⚡ Very fast | Slow (rebuild) |
| Speed (incremental) | ⚡⚡ Fast | ⚡⚡ Fast |

## Recommendation

1. **Install sccache** for best overall performance:
   ```bash
   ./scripts/quick_setup.sh
   ```

2. **Without sccache**, incremental compilation is automatically enabled

3. **Don't manually set both** - let the scripts handle it

## Performance Comparison

### With sccache (CARGO_INCREMENTAL=0):
```bash
First build:     ~18 minutes
After clean:     ~1-2 minutes  ⚡ (uses cache)
Incremental:     ~1-2 minutes
```

### Without sccache (CARGO_INCREMENTAL=1):
```bash
First build:     ~18 minutes
After clean:     ~18 minutes  (no cache)
Incremental:     ~2-5 minutes
```

## Troubleshooting

### Error: "incremental compilation is prohibited"

**Cause**: Both sccache and incremental compilation are enabled.

**Solution**: The scripts now handle this automatically. If you see this error:

1. Make sure you're using the latest scripts
2. Or manually set:
   ```bash
   export CARGO_INCREMENTAL=0
   cargo build
   ```

### How to check current settings?

```bash
# Check if sccache is being used
echo $RUSTC_WRAPPER

# Check incremental compilation setting
echo $CARGO_INCREMENTAL

# Or use the diagnostic script
./scripts/diagnose.sh
```

## Manual Configuration

If you want to manually control this:

### Use sccache (recommended):
```bash
export RUSTC_WRAPPER=sccache
export CARGO_INCREMENTAL=0
cargo build
```

### Use incremental compilation:
```bash
unset RUSTC_WRAPPER
export CARGO_INCREMENTAL=1
cargo build
```

## Summary

- ✅ **Use sccache** for best performance (install with `quick_setup`)
- ✅ **Scripts automatically handle** the conflict
- ✅ **Don't worry about it** - just use the scripts
- ❌ **Don't manually enable both** - they conflict

The development scripts (`dev.sh`, `dev.cmd`, `dev.ps1`) now automatically:
1. Detect if sccache is installed
2. Set `CARGO_INCREMENTAL=0` when using sccache
3. Set `CARGO_INCREMENTAL=1` when not using sccache

Just use the scripts and enjoy fast builds! 🚀
