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

echo === Kiwi Quick Setup ===
echo(

where sccache >nul 2>&1
if !errorlevel! equ 0 (
    echo sccache is already installed
) else (
    echo Installing sccache...
    cargo install sccache
    if !errorlevel! equ 0 (
        echo sccache installed
    ) else (
        echo Failed to install sccache
        exit /b 1
    )
)

set CARGO_CONFIG=%USERPROFILE%\.cargo\config.toml
if not exist "%USERPROFILE%\.cargo" mkdir "%USERPROFILE%\.cargo"

REM Note: The following code block configures sccache as the Rust compiler wrapper
REM Potential issue: If config.toml already contains [build] and other tables,
REM when [build] exists earlier in the file, directly appending rustc-wrapper may cause it 
REM to be placed in the root table instead of the [build] table
REM This could lead to Cargo ignoring this configuration item
if exist "%CARGO_CONFIG%" (
    findstr /C:"rustc-wrapper" "%CARGO_CONFIG%" >nul 2>&1
    if !errorlevel! equ 0 (
        echo sccache is already configured
    ) else (
        REM Note: There's an issue with this logic. Even if [build] already exists in the file,
        REM we should re-declare [build] to ensure the configuration is correctly assigned
        findstr /C:"[build]" "%CARGO_CONFIG%" >nul 2>&1
        if !errorlevel! equ 0 (
            echo rustc-wrapper = "sccache" >> "%CARGO_CONFIG%"
        ) else (
            echo( >> "%CARGO_CONFIG%"
            echo [build] >> "%CARGO_CONFIG%"
            echo rustc-wrapper = "sccache" >> "%CARGO_CONFIG%"
        )
        echo sccache configured
    )
) else (
    echo [build] > "%CARGO_CONFIG%"
    echo rustc-wrapper = "sccache" >> "%CARGO_CONFIG%"
    echo sccache configured
)

where cargo-watch >nul 2>&1
if !errorlevel! equ 0 (
    echo cargo-watch is already installed
) else (
    echo Installing cargo-watch...
    cargo install cargo-watch
    if !errorlevel! equ 0 (
        echo cargo-watch installed
    ) else (
        echo Failed to install cargo-watch
        exit /b 1
    )
)

echo(
echo === Setup Complete! ===
echo(
echo You can now use:
echo   scripts\dev check  - Quick syntax check
echo   scripts\dev watch  - Auto-check on file save
echo   scripts\dev run    - Build and run
echo(
echo Check sccache stats with: sccache --show-stats
echo(
pause
endlocal
