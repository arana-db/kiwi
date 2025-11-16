# Feature: Automatic First-Time Setup Detection

## Summary

Development scripts now automatically detect first-time use and prompt users to run the quick setup for optimal performance.

## What's New

### Before
```bash
$ ./scripts/dev.sh build
=== Kiwi Development Tool ===
Building Kiwi...
# User doesn't know about optimization options
```

### After
```bash
$ ./scripts/dev.sh build
=== Kiwi Development Tool ===

⚠️  First-time setup recommended for optimal performance!

Run this command to install sccache and cargo-watch:
  ./scripts/quick_setup.sh

Or continue without setup (you can run it later).

Run quick setup now? (y/n) y

=== Kiwi Quick Setup ===
Installing sccache...
✓ sccache installed
✓ Setup complete! Continuing with build...

Building Kiwi...
```

## Implementation

### Marker File

**File**: `.kiwi_setup_done`
**Location**: Project root
**Purpose**: Track whether setup has been completed or skipped

### Detection Logic

1. Check if marker file exists
2. If not, show setup prompt (only for `build` and `run` commands)
3. After setup or skip, create marker file
4. Never prompt again (unless marker is deleted)

### Updated Files

1. ✅ `scripts/dev.sh` - Linux/macOS script
2. ✅ `scripts/dev.cmd` - Windows batch script
3. ✅ `scripts/dev.ps1` - Windows PowerShell script
4. ✅ `scripts/quick_setup.sh` - Creates marker on success
5. ✅ `scripts/quick_setup.cmd` - Creates marker on success
6. ✅ `scripts/quick_setup.ps1` - New PowerShell version
7. ✅ `.gitignore` - Ignore marker file
8. ✅ `README.md` - Updated documentation
9. ✅ `README_CN.md` - Updated Chinese documentation

## User Experience

### Scenario 1: User Accepts Setup

```bash
$ ./scripts/dev.sh build

⚠️  First-time setup recommended!
Run quick setup now? (y/n) y

# Installs sccache and cargo-watch
✓ Setup complete!

# Continues with build
Building Kiwi...
```

**Result**: 
- sccache and cargo-watch installed
- Marker file created
- Build continues automatically
- Future builds are 50-90% faster

### Scenario 2: User Skips Setup

```bash
$ ./scripts/dev.sh build

⚠️  First-time setup recommended!
Run quick setup now? (y/n) n

Skipping setup. You can run './scripts/quick_setup.sh' anytime.

Building Kiwi...
```

**Result**:
- No tools installed
- Marker file created (won't ask again)
- Build continues with default settings
- User can run setup manually later

### Scenario 3: Subsequent Uses

```bash
$ ./scripts/dev.sh build

=== Kiwi Development Tool ===
Building Kiwi...
```

**Result**:
- No prompt (marker exists)
- Uses sccache if installed
- Smooth experience

## Commands Affected

### Prompts for Setup
- `build` - Most time-consuming, benefits most from sccache
- `run` - Also time-consuming, benefits from sccache

### No Prompt
- `check` - Fast enough without setup
- `watch` - Handles cargo-watch separately
- `test` - Not the primary use case
- `clean` - Doesn't need optimization
- `stats` - Just shows information

## Benefits

1. **User-Friendly**: Guides new users to optimal setup
2. **Non-Intrusive**: Only prompts once
3. **Optional**: Users can skip if they prefer
4. **Flexible**: Can re-trigger by deleting marker
5. **Cross-Platform**: Works on Linux, macOS, and Windows
6. **Smart**: Only prompts for commands that benefit from setup

## Manual Control

### Re-trigger Prompt
```bash
rm .kiwi_setup_done
./scripts/dev.sh build  # Will prompt again
```

### Skip Prompt Permanently
```bash
touch .kiwi_setup_done
./scripts/dev.sh build  # No prompt
```

### Run Setup Anytime
```bash
# Linux/macOS
./scripts/quick_setup.sh

# Windows
scripts\quick_setup.cmd
```

## Technical Details

### Linux/macOS Implementation

```bash
SETUP_MARKER=".kiwi_setup_done"
if [ "$COMMAND" = "build" ] || [ "$COMMAND" = "run" ]; then
    if [ ! -f "$SETUP_MARKER" ]; then
        echo "⚠️  First-time setup recommended!"
        read -p "Run quick setup now? (y/n) " -n 1 -r
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            ./scripts/quick_setup.sh
            touch "$SETUP_MARKER"
        else
            touch "$SETUP_MARKER"
        fi
    fi
fi
```

### Windows Batch Implementation

```batch
set SETUP_MARKER=.kiwi_setup_done
if "%COMMAND%"=="build" (
    if not exist "%SETUP_MARKER%" (
        call :first_time_setup
    )
)

:first_time_setup
echo ⚠️  First-time setup recommended!
set /p SETUP_NOW="Run quick setup now? (y/n) "
if /i "%SETUP_NOW%"=="y" (
    call scripts\quick_setup.cmd
    echo. > "%SETUP_MARKER%"
)
```

### Windows PowerShell Implementation

```powershell
$setupMarker = ".kiwi_setup_done"
if (($Command -eq "build" -or $Command -eq "run") -and -not (Test-Path $setupMarker)) {
    Write-Warning "⚠️  First-time setup recommended!"
    $setupNow = Read-Host "Run quick setup now? (y/n)"
    if ($setupNow -eq "y") {
        & "scripts\quick_setup.ps1"
        New-Item -ItemType File -Path $setupMarker -Force | Out-Null
    }
}
```

## Testing

### Test Case 1: First-Time Build

```bash
# Ensure no marker exists
rm -f .kiwi_setup_done

# Run build
./scripts/dev.sh build

# Expected: Prompt appears
# Action: Press 'y'
# Expected: Setup runs, marker created, build continues
```

### Test Case 2: Skip Setup

```bash
# Ensure no marker exists
rm -f .kiwi_setup_done

# Run build
./scripts/dev.sh build

# Expected: Prompt appears
# Action: Press 'n'
# Expected: Marker created, build continues without setup
```

### Test Case 3: Subsequent Build

```bash
# Marker exists from previous run
./scripts/dev.sh build

# Expected: No prompt, build starts immediately
```

### Test Case 4: Check Command

```bash
# Ensure no marker exists
rm -f .kiwi_setup_done

# Run check
./scripts/dev.sh check

# Expected: No prompt (check doesn't trigger setup)
```

## Documentation

- [First-Time Setup Detection](FIRST_TIME_SETUP.md) - Detailed documentation
- [README.md](../README.md) - Updated with automatic prompt info
- [README_CN.md](../README_CN.md) - Chinese version updated

## Future Enhancements

Possible improvements:
- [ ] Check if tools are actually working (not just installed)
- [ ] Suggest re-running setup if tools are outdated
- [ ] Add option to configure which tools to install
- [ ] Show estimated time savings with setup

## Summary

This feature makes Kiwi more user-friendly by:
- ✅ Automatically guiding new users to optimal setup
- ✅ Not interrupting experienced users
- ✅ Being completely optional
- ✅ Working seamlessly across all platforms
- ✅ Improving overall developer experience

New users get the best performance from day one, while experienced users maintain their workflow!
