#!/usr/bin/env pwsh
# Copyright (c) 2024-present, arana-db Community.  All rights reserved.
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Development helper script for Kiwi

param(
    [Parameter(Position=0)]
    [ValidateSet("check", "build", "run", "test", "clean", "watch", "stats", "gdb")]
    [string]$Command = "check",

    [switch]$Release,
    [switch]$Verbose,
    [switch]$Debug
)

$ErrorActionPreference = "Stop"

# Colors
function Write-Info { param($msg) Write-Host $msg -ForegroundColor Cyan }
function Write-Success { param($msg) Write-Host $msg -ForegroundColor Green }
function Write-Warning { param($msg) Write-Host $msg -ForegroundColor Yellow }
function Write-Error { param($msg) Write-Host $msg -ForegroundColor Red }

# Get Rust toolchain information
function Get-RustToolchain {
    try {
        $rustupOutput = rustup show active-toolchain 2>$null
        if ($rustupOutput -match "nightly") {
            return "nightly"
        } elseif ($rustupOutput -match "stable") {
            return "stable"
        } else {
            # Fallback: check with cargo --version
            $cargoOutput = cargo --version --verbose 2>$null
            if ($cargoOutput -match "nightly") {
                return "nightly"
            } else {
                return "stable"
            }
        }
    } catch {
        # Default to stable if detection fails
        return "stable"
    }
}

# Banner
Write-Info "=== Kiwi Development Tool ==="
Write-Info ""

# Check if this is first-time use (only for build/run commands)
if ($Command -eq "build" -or $Command -eq "run") {
    # Check if sccache is available, if not prompt to install
    $sccacheInstalled = Get-Command sccache -ErrorAction SilentlyContinue
    if (-not $sccacheInstalled) {
        Write-Host ""
        Write-Warning "⚠️  sccache not detected for optimal performance!"
        Write-Host ""
        Write-Host "Run this command to install sccache and cargo-watch:"
        Write-Info "  scripts\quick_setup.ps1"
        Write-Host ""
        Write-Host "Or continue without setup (you can run it later)."
        Write-Host ""
        $setupNow = Read-Host "Run quick setup now? (y/n)"
        if ($setupNow -eq "y" -or $setupNow -eq "Y") {
            & "scripts\quick_setup.ps1"
            if ($LASTEXITCODE -eq 0) {
                Write-Success "✓ Setup complete! Continuing with build..."
                Write-Host ""
            }
        } else {
            Write-Info "Skipping setup. You can run 'scripts\quick_setup.ps1' anytime."
            Write-Host ""
        }
    }
}

# Set environment variables for faster builds
$env:CARGO_BUILD_JOBS = [Environment]::ProcessorCount

# Debug mode configuration
if ($Debug.IsPresent) {
    Write-Info "🐛 Debug mode enabled - using Cargo_debug.toml"

    # Detect Rust toolchain for compatible flags
    $rustToolchain = Get-RustToolchain

    # Set environment variables for debugging
    if ($rustToolchain -eq "nightly") {
        $env:RUSTFLAGS = "-g -Zmacro-backtrace"
        Write-Info "🔧 Using nightly toolchain with -Zmacro-backtrace"
    } else {
        $env:RUSTFLAGS = "-g"
        Write-Info "🔧 Using stable toolchain (macro backtrace unavailable on stable)"
    }

    $env:CARGO_INCREMENTAL = "1"
    # Disable sccache for debug builds to ensure debug symbols
    Remove-Item Env:\RUSTC_WRAPPER -ErrorAction SilentlyContinue

    # Use debug config if available - FIXED: Use array instead of string
    if (Test-Path "Cargo_debug.toml") {
        $cargoConfigFlag = @("--config", "Cargo_debug.toml")
        Write-Info "📋 Using Cargo_debug.toml for maximum debug symbols"
    } else {
        Write-Warning "⚠️ Cargo_debug.toml not found, using default debug settings"
        $cargoConfigFlag = @()
    }
} else {
    # Check and setup sccache if available (normal builds)
    $sccacheInstalled = Get-Command sccache -ErrorAction SilentlyContinue
    if ($sccacheInstalled) {
        $env:RUSTC_WRAPPER = "sccache"
        # sccache doesn't support incremental compilation, so disable it
        $env:CARGO_INCREMENTAL = "0"
        Remove-Item Env:\CARGO_CACHE_RUSTC_INFO -ErrorAction SilentlyContinue
        # Silently start sccache server if not running
        sccache --start-server 2>&1 | Out-Null
    } else {
        # Enable incremental compilation when not using sccache
        $env:CARGO_INCREMENTAL = "1"
        if ($Command -eq "build" -or $Command -eq "run") {
            Write-Warning "⚡ Tip: Install sccache for faster builds: cargo install sccache"
            Write-Warning "   Or run: scripts\quick_setup.ps1"
        }
    }
    $cargoConfigFlag = @()
}

$profileFlag = if ($Release) { "--release" } else { "" }
$verboseFlag = if ($Verbose) { "-v" } else { "" }

switch ($Command) {
    "check" {
        Write-Info "Running cargo check (fast syntax check)..."
        cargo check $profileFlag @cargoConfigFlag $verboseFlag
        if ($LASTEXITCODE -eq 0) {
            Write-Success "✓ Check passed!"
        }
    }

    "build" {
        if ($Debug.IsPresent) {
            Write-Info "Building Kiwi in DEBUG mode..."
        } else {
            Write-Info "Building Kiwi..."
        }

        # Check if librocksdb-sys is cached
        $targetDir = if ($Release) { "target\release" } else { "target\debug" }
        if (Test-Path "$targetDir\deps\*rocksdb*.rlib") {
            Write-Success "✓ Using cached librocksdb-sys"
        } else {
            Write-Warning "⚠ librocksdb-sys will be compiled (this may take a while)..."
        }

        $startTime = Get-Date
        cargo build $profileFlag @cargoConfigFlag $verboseFlag
        $buildTime = (Get-Date) - $startTime

        if ($LASTEXITCODE -eq 0) {
            if ($Debug.IsPresent) {
                Write-Success "✓ Debug build completed in $([math]::Round($buildTime.TotalSeconds, 2)) seconds"
                Write-Host ""
                Write-Info "🐛 Debug binary ready at: target\debug\kiwi"
                Write-Info "🔍 GDB command: gdb -ex 'break main' -ex 'run' target\debug\kiwi"
                Write-Info "🔍 Or use: rust-gdb target\debug\kiwi"
            } else {
                Write-Success "✓ Build completed in $([math]::Round($buildTime.TotalSeconds, 2)) seconds"
            }
        }
    }

    "run" {
        if ($Debug.IsPresent) {
            Write-Info "Running Kiwi in DEBUG mode..."
        } else {
            Write-Info "Running Kiwi..."
        }
        $env:RUST_LOG = "debug"
        cargo run $profileFlag @cargoConfigFlag $verboseFlag
    }

    "test" {
        Write-Info "Running tests..."
        cargo test $profileFlag @cargoConfigFlag $verboseFlag
    }

    "gdb" {
        Write-Info "Starting GDB debug session..."
        # First build with debug symbols if not already built
        if (-not (Test-Path "target\debug\kiwi")) {
            Write-Warning "Debug binary not found, building first..."
            & "$PSScriptRoot\dev.ps1" build -Debug
        }

        if (Test-Path "target\debug\kiwi") {
            Write-Success "✓ Starting GDB with debug symbols"
            $rustGdbInstalled = Get-Command rust-gdb -ErrorAction SilentlyContinue
            if ($rustGdbInstalled) {
                Write-Info "Using rust-gdb for better Rust debugging..."
                rust-gdb -ex 'break main' -ex 'run' target\debug\kiwi
            } else {
                Write-Info "Using standard gdb..."
                gdb -ex 'set debug-file-directory ./target/debug/deps' -ex 'break main' -ex 'run' target\debug\kiwi
            }
        } else {
            Write-Error "Debug binary not found. Run 'dev.ps1 build -Debug' first."
            exit 1
        }
    }

    "clean" {
        Write-Warning "Cleaning build artifacts..."
        cargo clean
        Write-Success "✓ Clean complete"
    }

    "watch" {
        Write-Info "Starting cargo-watch..."
        Write-Info "This will automatically check your code on file changes"
        Write-Info "Press Ctrl+C to stop"
        Write-Info ""

        # Check if cargo-watch is installed
        $watchInstalled = Get-Command cargo-watch -ErrorAction SilentlyContinue
        if (-not $watchInstalled) {
            Write-Warning "cargo-watch not found."
            Write-Info ""
            $install = Read-Host "Install cargo-watch now? (y/n)"
            if ($install -eq "y" -or $install -eq "Y") {
                Write-Info "Installing cargo-watch..."
                cargo install cargo-watch
                Write-Success "✓ cargo-watch installed"
            } else {
                Write-Error "cargo-watch is required for watch mode"
                exit 1
            }
        }

        cargo watch -x check -x "test --lib"
    }

    "stats" {
        Write-Info "Build Statistics:"
        Write-Info ""

        # Check target directory size
        if (Test-Path "target") {
            $targetSize = (Get-ChildItem target -Recurse -File | Measure-Object -Property Length -Sum).Sum
            $targetSizeGB = [math]::Round($targetSize / 1GB, 2)
            Write-Info "Target directory size: $targetSizeGB GB"
        }

        # Check if sccache is available
        $sccacheInstalled = Get-Command sccache -ErrorAction SilentlyContinue
        if ($sccacheInstalled) {
            Write-Info ""
            Write-Info "sccache statistics:"
            sccache --show-stats
        } else {
            Write-Warning ""
            Write-Warning "sccache not installed. Install it for faster builds:"
            Write-Warning "  cargo install sccache"
            Write-Warning "  Then configure in ~/.cargo/config.toml:"
            Write-Warning "  [build]"
            Write-Warning "  rustc-wrapper = `"sccache`""
        }

        # Show last build times
        Write-Info ""
        Write-Info "Recent builds:"
        if (Test-Path "target\debug") {
            $debugTime = (Get-Item "target\debug").LastWriteTime
            Write-Info "  Debug:   $debugTime"
        }
        if (Test-Path "target\release") {
            $releaseTime = (Get-Item "target\release").LastWriteTime
            Write-Info "  Release: $releaseTime"
        }
    }

    default {
        Write-Error "Unknown command: $Command"
        Write-Host ""
        Write-Host "Available commands:"
        Write-Host "  check  - Quick syntax check"
        Write-Host "  build  - Build the project"
        Write-Host "  run    - Build and run"
        Write-Host "  test   - Run tests"
        Write-Host "  gdb    - Build and start GDB"
        Write-Host "  clean  - Clean build artifacts"
        Write-Host "  watch  - Auto-check on file changes"
        Write-Host "  stats  - Show build statistics"
        Write-Host ""
        Write-Host "Parameters:"
        Write-Host "  -Debug     - Enable full debug symbols (slower build, better debugging)"
        Write-Host "  -Release   - Use release profile"
        Write-Host "  -Verbose   - Verbose output"
        Write-Host ""
        Write-Host "Debug examples:"
        Write-Host "  .\dev.ps1 build -Debug     # Build with debug symbols"
        Write-Host "  .\dev.ps1 run -Debug       # Run with debug symbols"
        Write-Host "  .\dev.ps1 gdb              # Build and start GDB"
        exit 1
    }
}

if ($LASTEXITCODE -ne 0) {
    Write-Error "✗ Command failed with exit code $LASTEXITCODE"
    exit $LASTEXITCODE
}