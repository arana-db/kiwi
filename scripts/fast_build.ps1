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

# Fast build script for Kiwi
# This script helps avoid unnecessary recompilation of heavy dependencies

param(
    [switch]$Release,
    [switch]$Clean,
    [switch]$Check
)

$ErrorActionPreference = "Stop"

Write-Host "=== Kiwi Fast Build Script ===" -ForegroundColor Cyan

# Check if we need to clean
if ($Clean) {
    Write-Host "Cleaning build artifacts..." -ForegroundColor Yellow
    cargo clean
    Write-Host "Clean complete!" -ForegroundColor Green
    exit 0
}

# Set build profile
$profile = if ($Release) { "release" } else { "dev" }
$profileFlag = if ($Release) { "--release" } else { "" }

Write-Host "Building in $profile mode..." -ForegroundColor Yellow

# Check if librocksdb-sys is already compiled
$targetDir = if ($Release) { "target\release" } else { "target\debug" }
$rocksdbLib = "$targetDir\deps\librocksdb_sys-*.rlib"

if (Test-Path $rocksdbLib) {
    Write-Host "✓ librocksdb-sys already compiled, using cache" -ForegroundColor Green
} else {
    Write-Host "⚠ librocksdb-sys not found in cache, will compile..." -ForegroundColor Yellow
}

# Run cargo build with optimizations
if ($Check) {
    Write-Host "Running cargo check..." -ForegroundColor Cyan
    $startTime = Get-Date
    cargo check $profileFlag
} else {
    Write-Host "Running cargo build..." -ForegroundColor Cyan
    
    # Use environment variables to speed up compilation
    $env:CARGO_BUILD_JOBS = [Environment]::ProcessorCount
    
    # Check if sccache is available
    $sccacheInstalled = Get-Command sccache -ErrorAction SilentlyContinue
    if ($sccacheInstalled) {
        $env:RUSTC_WRAPPER = "sccache"
        # sccache doesn't support incremental compilation
        $env:CARGO_INCREMENTAL = "0"
    } else {
        # Enable incremental compilation when not using sccache
        $env:CARGO_INCREMENTAL = "1"
    }
    
    $startTime = Get-Date
    cargo build $profileFlag
}

if ($LASTEXITCODE -eq 0) {
    Write-Host "`n✓ Build successful!" -ForegroundColor Green
    
    # Show build time statistics
    $buildTime = (Get-Date) - $startTime
    Write-Host "Build completed in $($buildTime.TotalSeconds) seconds" -ForegroundColor Cyan
} else {
    Write-Host "`n✗ Build failed!" -ForegroundColor Red
    exit $LASTEXITCODE
}
