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
REM     http://www.apache.org/licenses/LICENSE-2.0
REM
REM Unless required by applicable law or agreed to in writing, software
REM distributed under the License is distributed on an "AS IS" BASIS,
REM WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
REM See the License for the specific language governing permissions and
REM limitations under the License.

REM Quick setup script for Kiwi development

echo === Kiwi Quick Setup ===
echo.

REM Check if sccache is installed
where sccache >nul 2>&1
if %errorlevel% equ 0 (
    echo [32m✓ sccache is already installed[0m
) else (
    echo [33mInstalling sccache (this may take a few minutes)...[0m
    cargo install sccache
    echo [32m✓ sccache installed[0m
)

REM Configure sccache
set CARGO_CONFIG=%USERPROFILE%\.cargo\config.toml
if not exist "%USERPROFILE%\.cargo" mkdir "%USERPROFILE%\.cargo"

findstr /C:"rustc-wrapper" "%CARGO_CONFIG%" >nul 2>&1
if %errorlevel% equ 0 (
    echo [32m✓ sccache is already configured[0m
) else (
    echo. >> "%CARGO_CONFIG%"
    echo [build] >> "%CARGO_CONFIG%"
    echo rustc-wrapper = "sccache" >> "%CARGO_CONFIG%"
    echo [32m✓ sccache configured in %CARGO_CONFIG%[0m
)

REM Check if cargo-watch is installed
where cargo-watch >nul 2>&1
if %errorlevel% equ 0 (
    echo [32m✓ cargo-watch is already installed[0m
) else (
    echo [33mInstalling cargo-watch...[0m
    cargo install cargo-watch
    echo [32m✓ cargo-watch installed[0m
)

echo.
echo [32m=== Setup Complete! ===[0m
echo.

REM Create setup marker file
echo. > .kiwi_setup_done

echo You can now use:
echo   scripts\dev check  - Quick syntax check
echo   scripts\dev watch  - Auto-check on file save
echo   scripts\dev run    - Build and run
echo.
echo Check sccache stats with:
echo   sccache --show-stats
echo.
pause
