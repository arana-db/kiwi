@echo off
REM Copyright (c) 2024-present, arana-db Community.  All rights reserved.
REM
REM Licensed to the Apache Software Foundation (ASF) under one or more
REM contributor license agreements.  See the NOTICE file distributed with
REM this work for additional information regarding copyright ownership.
REM The ASF licenses this file to You under the Apache License, Version 2.0
REM (the "License"); you may not use this file except in compliance with
REM the License.  You may obtain a copy of the License at
REM
REM   http://www.apache.org/licenses/LICENSE-2.0
REM
REM Unless required by applicable law or agreed to in writing, software
REM distributed under the License is distributed on an "AS IS" BASIS,
REM WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
REM See the License for the specific language governing permissions and
REM limitations under the License.

setlocal enabledelayedexpansion

REM Initialize variables
set COMMAND=
set PROFILE=
set VERBOSE=
set DEBUG_MODE=
set CARGO_CONFIG_FLAG=
set SHIFT_COUNT=0

REM Parse arguments
:parse_args
if "%1"=="" goto end_parse_args
if /i "%1"=="--release" (
    set PROFILE=--release
    shift
    goto parse_args
)
if /i "%1"=="-v" (
    set VERBOSE=-v
    shift
    goto parse_args
)
if /i "%1"=="--verbose" (
    set VERBOSE=-v
    shift
    goto parse_args
)
if /i "%1"=="--debug" (
    set DEBUG_MODE=true
    shift
    goto parse_args
)
if not defined COMMAND (
    set COMMAND=%1
    shift
    goto parse_args
)
shift
goto parse_args

:end_parse_args
if "%COMMAND%"=="" set COMMAND=check

echo === Kiwi Development Tool ===
echo(

REM Check if this is first-time use (only for build/run commands)
if "%COMMAND%"=="build" (
    where sccache >nul 2>&1
    if !errorlevel! neq 0 (
        call :first_time_setup
    )
)
if "%COMMAND%"=="run" (
    where sccache >nul 2>&1
    if !errorlevel! neq 0 (
        call :first_time_setup
    )
)

REM Set environment for faster builds
set CARGO_BUILD_JOBS=%NUMBER_OF_PROCESSORS%

REM Debug mode configuration
if defined DEBUG_MODE (
    echo 🐛 Debug mode enabled - using Cargo_debug.toml
    REM Set environment variables for debugging
    set RUSTFLAGS=-g -Zmacro-backtrace
    set CARGO_INCREMENTAL=1
    REM Disable sccache for debug builds to ensure debug symbols
    set RUSTC_WRAPPER=
    REM Use debug config if available
    if exist "Cargo_debug.toml" (
        set CARGO_CONFIG_FLAG=--config Cargo_debug.toml
        echo 📋 Using Cargo_debug.toml for maximum debug symbols
    ) else (
        echo ⚠️ Cargo_debug.toml not found, using default debug settings
    )
) else (
    REM Check and setup sccache if available (normal builds)
    where sccache >nul 2>&1
    if %errorlevel% equ 0 (
        set RUSTC_WRAPPER=sccache
        set CARGO_INCREMENTAL=0
        set CARGO_CACHE_RUSTC_INFO=
        sccache --start-server >nul 2>&1
    ) else (
        set CARGO_INCREMENTAL=1
        if "%COMMAND%"=="build" (
            echo ⚡ Tip: Install sccache for faster builds: cargo install sccache
            echo    Or run: scripts\quick_setup.cmd
        )
        if "%COMMAND%"=="run" (
            echo ⚡ Tip: Install sccache for faster builds: cargo install sccache
            echo    Or run: scripts\quick_setup.cmd
        )
    )
)

if "%COMMAND%"=="check" (
    echo Running cargo check ^(fast syntax check^)...
    cargo check %PROFILE% %CARGO_CONFIG_FLAG% %VERBOSE%
    if !errorlevel! equ 0 (
        echo ✓ Check passed!
    )
    goto :end
)

if "%COMMAND%"=="build" (
    if defined DEBUG_MODE (
        echo Building Kiwi in DEBUG mode...
    ) else (
        echo Building Kiwi...
    )

    REM Check if librocksdb-sys is cached
    set "TARGET_DIR=target\debug"
    if "%PROFILE%"=="--release" (
        set "TARGET_DIR=target\release"
    )

    if exist "%TARGET_DIR%\deps\*rocksdb*.rlib" (
        echo ✓ Using cached librocksdb-sys
    ) else (
        echo ⚠ librocksdb-sys will be compiled ^(this may take a while^)...
    )

    cargo build %PROFILE% %CARGO_CONFIG_FLAG% %VERBOSE%
    if !errorlevel! equ 0 (
        if defined DEBUG_MODE (
            echo ✓ Debug build completed
            echo(
            echo 🐛 Debug binary ready at: target\debug\kiwi
            echo 🔍 GDB command: gdb -ex "break main" -ex "run" target\debug\kiwi
            echo 🔍 Or use: rust-gdb target\debug\kiwi
        ) else (
            echo ✓ Build completed
        )
    )
    goto :end
)

if "%COMMAND%"=="run" (
    if defined DEBUG_MODE (
        echo Running Kiwi in DEBUG mode...
    ) else (
        echo Running Kiwi...
    )
    set RUST_LOG=debug
    cargo run %PROFILE% %CARGO_CONFIG_FLAG% %VERBOSE%
    goto :end
)

if "%COMMAND%"=="test" (
    echo Running tests...
    cargo test %PROFILE% %CARGO_CONFIG_FLAG% %VERBOSE%
    goto :end
)

if "%COMMAND%"=="gdb" (
    echo Starting GDB debug session...
    REM First build with debug symbols if not already built
    if not exist "target\debug\kiwi" (
        echo Debug binary not found, building first...
        call "%~dp0dev.cmd" build --debug
    )

    if exist "target\debug\kiwi" (
        echo ✓ Starting GDB with debug symbols
        where rust-gdb >nul 2>&1
        if !errorlevel! equ 0 (
            echo Using rust-gdb for better Rust debugging...
            rust-gdb -ex "break main" -ex "run" target\debug\kiwi
        ) else (
            echo Using standard gdb...
            gdb -ex "set debug-file-directory ./target/debug/deps" -ex "break main" -ex "run" target\debug\kiwi
        )
    ) else (
        echo ✗ Debug binary not found. Run 'dev.cmd build --debug' first.
        exit /b 1
    )
    goto :end
)

if "%COMMAND%"=="clean" (
    echo Cleaning build artifacts...
    cargo clean
    echo ✓ Clean complete
    goto :end
)

if "%COMMAND%"=="watch" (
    echo Starting cargo-watch...
    echo This will automatically check your code on file changes
    echo Press Ctrl+C to stop
    echo(

    where cargo-watch >nul 2>&1
    if !errorlevel! neq 0 (
        echo cargo-watch not found.
        echo(
        set /p INSTALL="Install cargo-watch now? (y/n) "
        if /i "!INSTALL!"=="y" (
            echo Installing cargo-watch...
            cargo install cargo-watch
            echo ✓ cargo-watch installed
        ) else (
            echo ✗ cargo-watch is required for watch mode
            exit /b 1
        )
    )

    cargo watch -x check -x "test --lib"
    goto :end
)

if "%COMMAND%"=="stats" (
    echo Build Statistics:
    echo(

    if exist target (
        echo Target directory exists
        dir target /s | find "File(s)"
    )

    echo(
    echo Checking for sccache...
    where sccache >nul 2>&1
    if !errorlevel! equ 0 (
        echo sccache is installed
        sccache --show-stats
    ) else (
        echo sccache not installed. Install it for faster builds:
        echo   cargo install sccache
    )
    goto :end
)

echo Unknown command: %COMMAND%
echo(
echo Available commands:
echo   check  - Quick syntax check
echo   build  - Build the project
echo   run    - Build and run
echo   test   - Run tests
echo   gdb    - Build and start GDB
echo   clean  - Clean build artifacts
echo   watch  - Auto-check on file changes
echo   stats  - Show build statistics
echo(
echo Options:
echo   --debug    - Enable full debug symbols ^(slower build, better debugging^)
echo   --release  - Use release profile
echo   -v         - Verbose output
echo(
echo Debug examples:
echo   dev.cmd build --debug     # Build with debug symbols
echo   dev.cmd run --debug       # Run with debug symbols
echo   dev.cmd gdb               # Build and start GDB

:end
endlocal
exit /b

:first_time_setup
echo(
echo ⚠️  sccache not detected for optimal performance!
echo(
echo Run this command to install sccache and cargo-watch:
echo   scripts\quick_setup.cmd
echo(
echo Or continue without setup ^(you can run it later^).
echo(
set /p SETUP_NOW="Run quick setup now? (y/n) "
if /i "%SETUP_NOW%"=="y" (
    call scripts\quick_setup.cmd
    if !errorlevel! equ 0 (
        echo ✓ Setup complete! Continuing with build...
        echo(
    )
) else (
    echo Skipping setup. You can run 'scripts\quick_setup.cmd' anytime.
    echo(
)
exit /b