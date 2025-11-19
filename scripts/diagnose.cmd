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

REM Diagnose build issues

echo === Kiwi Build Diagnostics ===
echo.

echo [1] Checking sccache...
where sccache >nul 2>&1
if %errorlevel% equ 0 (
    echo [32m✓ sccache is installed[0m
    sccache --show-stats
) else (
    echo [33m✗ sccache is NOT installed[0m
    echo   Install with: cargo install sccache
    echo   Then configure in %%USERPROFILE%%\.cargo\config.toml
)

echo.
echo [2] Checking CARGO_INCREMENTAL...
if defined CARGO_INCREMENTAL (
    echo [32m✓ CARGO_INCREMENTAL = %CARGO_INCREMENTAL%[0m
) else (
    echo [33m✗ CARGO_INCREMENTAL is not set[0m
    echo   Set with: set CARGO_INCREMENTAL=1
)

echo.
echo [3] Checking target directory...
if exist target\debug (
    echo [32m✓ target\debug exists[0m
    dir target\debug\deps\*rocksdb*.rlib 2>nul | find "File(s)"
) else (
    echo [33m✗ target\debug does not exist[0m
)

echo.
echo [4] Checking .cargo\config.toml...
if exist .cargo\config.toml (
    echo [32m✓ Local .cargo\config.toml exists[0m
    findstr /C:"incremental" .cargo\config.toml
) else (
    echo [33m✗ Local .cargo\config.toml not found[0m
)

echo.
echo [5] Checking global Cargo config...
if exist "%USERPROFILE%\.cargo\config.toml" (
    echo [32m✓ Global Cargo config exists[0m
    findstr /C:"rustc-wrapper" "%USERPROFILE%\.cargo\config.toml" >nul 2>&1
    if %errorlevel% equ 0 (
        echo [32m✓ sccache is configured[0m
    ) else (
        echo [33m✗ sccache is not configured[0m
    )
) else (
    echo [33m✗ Global Cargo config not found[0m
)

echo.
echo [6] Checking incremental compilation artifacts...
if exist target\debug (
    echo [32m✓ Build artifacts found in target\debug[0m
    dir target\debug\deps\*.rlib 2>nul | find "File(s)" | find /V " 0 File"
    if %errorlevel% equ 0 (
        echo [32m✓ Incremental artifacts present[0m
    ) else (
        echo [33m✗ No incremental artifacts found - run a build first[0m
    )
) else (
    echo [33m✗ No build artifacts found - run a build first[0m
)

echo.
echo === Recommendations ===
echo.
echo To speed up builds:
echo 1. Install sccache: cargo install sccache
echo 2. Run: scripts\setup_sccache.ps1
echo 3. Use cargo check for development: scripts\dev check
echo 4. Use watch mode: scripts\dev watch
echo.
