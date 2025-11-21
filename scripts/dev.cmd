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

REM Parse arguments - use loop instead of goto
:parse_args_loop
if "%1"=="" goto args_done
if /i "%1"=="--release" (
    set PROFILE=--release
    shift
    goto parse_args_loop
)
if /i "%1"=="-v" (
    set VERBOSE=-v
    shift
    goto parse_args_loop
)
if /i "%1"=="--verbose" (
    set VERBOSE=-v
    shift
    goto parse_args_loop
)
if /i "%1"=="--debug" (
    set DEBUG_MODE=true
    shift
    goto parse_args_loop
)
if not defined COMMAND (
    set COMMAND=%1
    shift
    goto parse_args_loop
)
shift
goto parse_args_loop

:args_done
if "%COMMAND%"=="" set COMMAND=check

echo === Kiwi Development Tool ===
echo(

REM Set environment for faster builds
set CARGO_BUILD_JOBS=%NUMBER_OF_PROCESSORS%

REM Debug mode configuration with toolchain detection
if defined DEBUG_MODE (
    echo 🐛 Debug mode enabled - using Cargo_debug.toml

    REM Detect Rust toolchain for compatible flags
    for /f "tokens=*" %%i in ('rustup show active-toolchain 2^>nul ^| findstr /i "nightly"') do set NIGHTLY_DETECTED=%%i
    if defined NIGHTLY_DETECTED (
        set RUSTFLAGS=-g -Zmacro-backtrace
        echo 🔧 Using nightly toolchain with -Zmacro-backtrace
    ) else (
        REM Fallback: check with cargo --version
        for /f "tokens=*" %%i in ('cargo --version --verbose 2^>nul ^| findstr /i "nightly"') do set NIGHTLY_DETECTED_FALLBACK=%%i
        if defined NIGHTLY_DETECTED_FALLBACK (
            set RUSTFLAGS=-g -Zmacro-backtrace
            echo 🔧 Using nightly toolchain with -Zmacro-backtrace
        ) else (
            set RUSTFLAGS=-g
            echo 🔧 Using stable toolchain (macro backtrace unavailable on stable)
        )
    )

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
    if !errorlevel! equ 0 (
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

REM Check if this is first-time use (only for build/run commands)
call :check_sccache_setup

REM Execute commands using function calls with validation
if "%COMMAND%"=="check" (
    call :execute_check
) else if "%COMMAND%"=="build" (
    call :execute_build
) else if "%COMMAND%"=="run" (
    call :execute_run
) else if "%COMMAND%"=="test" (
    call :execute_test
) else if "%COMMAND%"=="clean" (
    call :execute_clean
) else if "%COMMAND%"=="watch" (
    call :execute_watch
) else if "%COMMAND%"=="stats" (
    call :execute_stats
) else (
    call :show_help
)

REM ==================== Command Execution Functions ====================

:execute_check
echo Running cargo check ^(fast syntax check^)...
cargo check %PROFILE% %CARGO_CONFIG_FLAG% %VERBOSE%
if !errorlevel! equ 0 (
    echo ✓ Check passed!
)
exit /b

:execute_build
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
    ) else (
        echo ✓ Build completed
    )
)
exit /b

:execute_run
if defined DEBUG_MODE (
    echo Running Kiwi in DEBUG mode...
) else (
    echo Running Kiwi...
)
set RUST_LOG=debug
cargo run %PROFILE% %CARGO_CONFIG_FLAG% %VERBOSE%
exit /b

:execute_test
echo Running tests...
cargo test %PROFILE% %CARGO_CONFIG_FLAG% %VERBOSE%
exit /b

:execute_clean
echo Cleaning build artifacts...
cargo clean
echo ✓ Clean complete
exit /b

:execute_watch
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
exit /b

:execute_stats
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
exit /b


REM ==================== Utility Functions ====================

:check_sccache_setup
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
exit /b

:show_help
echo Unknown command: %COMMAND%
echo(
echo Available commands:
echo   check  - Quick syntax check
echo   build  - Build the project
echo   run    - Build and run
echo   test   - Run tests
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

:end
endlocal
exit /b