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

if "%1"=="" (
    set COMMAND=check
) else (
    set COMMAND=%1
)

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

REM Check and setup sccache if available
where sccache >nul 2>&1
if %errorlevel% equ 0 (
    set RUSTC_WRAPPER=sccache
    set CARGO_INCREMENTAL=0
    set CARGO_CACHE_RUSTC_INFO=
    sccache --start-server >nul 2>&1
) else (
    set CARGO_INCREMENTAL=1
    if "%COMMAND%"=="build" (
        echo Tip: Install sccache for faster builds: cargo install sccache
        echo    Or run: scripts\quick_setup.cmd
    )
    if "%COMMAND%"=="run" (
        echo Tip: Install sccache for faster builds: cargo install sccache
        echo    Or run: scripts\quick_setup.cmd
    )
)

if "%COMMAND%"=="check" (
    echo Running cargo check...
    cargo check
    if !errorlevel! equ 0 (
        echo Check passed!
    )
    goto :end
)

if "%COMMAND%"=="build" (
    echo Building Kiwi...
    cargo build
    if !errorlevel! equ 0 (
        echo Build completed!
    )
    goto :end
)

if "%COMMAND%"=="run" (
    echo Running Kiwi...
    set RUST_LOG=debug
    cargo run
    goto :end
)

if "%COMMAND%"=="test" (
    echo Running tests...
    cargo test
    goto :end
)

if "%COMMAND%"=="clean" (
    echo Cleaning build artifacts...
    cargo clean
    echo Clean complete
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
            echo cargo-watch installed
        ) else (
            echo cargo-watch is required for watch mode
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
echo   clean  - Clean build artifacts
echo   watch  - Auto-check on file changes
echo   stats  - Show build statistics

:end
endlocal
exit /b

:first_time_setup
echo(
echo sccache not detected for optimal performance!
echo(
echo Run this command to install sccache and cargo-watch:
echo   scripts\quick_setup.cmd
echo(
echo Or continue without setup (you can run it later).
echo(
set /p SETUP_NOW="Run quick setup now? (y/n) "
if /i "%SETUP_NOW%"=="y" (
    call scripts\quick_setup.cmd
    if !errorlevel! equ 0 (
        echo Setup complete! Continuing with build...
        echo(
    )
) else (
    echo Skipping setup. You can run 'scripts\quick_setup.cmd' anytime.
    echo(
)
exit /b

