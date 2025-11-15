# First-Time Setup Detection

## Feature

The development scripts (`dev.sh`, `dev.cmd`, `dev.ps1`) now automatically detect if this is your first time using them and prompt you to run the quick setup.

## How It Works

### Detection

When you run `build` or `run` commands for the first time, the script checks for a marker file `.kiwi_setup_done`:

- **File exists**: Setup has been completed, continue normally
- **File missing**: First-time use, show setup prompt

### Prompt

On first use, you'll see:

```
⚠️  First-time setup recommended for optimal performance!

Run this command to install sccache and cargo-watch:
  ./scripts/quick_setup.sh

Or continue without setup (you can run it later).

Run quick setup now? (y/n)
```

### User Options

1. **Press 'y'**: Runs `quick_setup` automatically, then continues with your command
2. **Press 'n'**: Skips setup and continues (creates marker to not ask again)

## Marker File

**Location**: `.kiwi_setup_done` (in project root)

**Purpose**: 
- Indicates setup has been completed or user chose to skip
- Prevents repeated prompts
- Ignored by git (added to `.gitignore`)

**When Created**:
- After successful `quick_setup` completion
- When user chooses to skip setup

## Manual Control

### Force Setup Prompt Again

Delete the marker file:
```bash
rm .kiwi_setup_done
```

Next `build` or `run` will prompt again.

### Skip Prompt Permanently

Create the marker file manually:
```bash
touch .kiwi_setup_done
```

### Run Setup Anytime

You can always run setup manually:
```bash
# Linux/macOS
./scripts/quick_setup.sh

# Windows
scripts\quick_setup.cmd
# or
scripts\quick_setup.ps1
```

## Benefits

1. **User-friendly**: New users are guided to optimal setup
2. **Non-intrusive**: Only prompts once
3. **Optional**: Users can skip if they prefer
4. **Flexible**: Can be re-triggered by deleting marker

## Example Flow

### First-Time User

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
✓ sccache configured
✓ cargo-watch installed

=== Setup Complete! ===

✓ Setup complete! Continuing with build...

Building Kiwi...
```

### Subsequent Uses

```bash
$ ./scripts/dev.sh build

=== Kiwi Development Tool ===

Building Kiwi...
# No prompt, continues directly
```

## Commands That Trigger Prompt

- `build` - Building the project
- `run` - Building and running

## Commands That Don't Trigger Prompt

- `check` - Quick syntax check (doesn't need full setup)
- `watch` - Auto-watch mode (prompts for cargo-watch separately if needed)
- `test` - Running tests
- `clean` - Cleaning build artifacts
- `stats` - Showing statistics

## Rationale

We only prompt for `build` and `run` because:
- These are the most time-consuming operations
- They benefit most from sccache
- First-time users typically start with these commands
- Other commands either don't need setup or handle dependencies themselves

## Troubleshooting

### Prompt Doesn't Appear

**Possible causes**:
1. Marker file already exists
2. Using `check` or other non-prompting command

**Solution**:
```bash
# Check if marker exists
ls -la .kiwi_setup_done

# Remove it to see prompt again
rm .kiwi_setup_done
```

### Setup Fails

**If setup fails**:
1. The marker file is NOT created
2. You'll see the prompt again next time
3. You can run setup manually

### Want to Skip Permanently

```bash
# Create marker without running setup
touch .kiwi_setup_done
```

## Implementation Details

### Linux/macOS (dev.sh)

```bash
SETUP_MARKER=".kiwi_setup_done"
if [ "$COMMAND" = "build" ] || [ "$COMMAND" = "run" ]; then
    if [ ! -f "$SETUP_MARKER" ]; then
        # Show prompt and handle response
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

### Windows Batch (dev.cmd)

```batch
set SETUP_MARKER=.kiwi_setup_done
if "%COMMAND%"=="build" (
    if not exist "%SETUP_MARKER%" (
        call :first_time_setup
    )
)
```

### Windows PowerShell (dev.ps1)

```powershell
$setupMarker = ".kiwi_setup_done"
if (($Command -eq "build" -or $Command -eq "run") -and -not (Test-Path $setupMarker)) {
    # Show prompt and handle response
    $setupNow = Read-Host "Run quick setup now? (y/n)"
    if ($setupNow -eq "y") {
        & "scripts\quick_setup.ps1"
        New-Item -ItemType File -Path $setupMarker -Force | Out-Null
    }
}
```

## Summary

- ✅ Automatic detection of first-time use
- ✅ Interactive prompt for setup
- ✅ Non-intrusive (only asks once)
- ✅ Optional (can skip)
- ✅ Flexible (can re-trigger)
- ✅ Cross-platform (Linux, macOS, Windows)
- ✅ User-friendly experience
