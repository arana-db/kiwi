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

# Quick setup script for Kiwi development

$ErrorActionPreference = "Stop"

# Colors
function Write-Info { param($msg) Write-Host $msg -ForegroundColor Cyan }
function Write-Success { param($msg) Write-Host $msg -ForegroundColor Green }
function Write-Warning { param($msg) Write-Host $msg -ForegroundColor Yellow }

Write-Info "=== Kiwi Quick Setup ==="
Write-Host ""

# Check if sccache is installed
$sccacheInstalled = Get-Command sccache -ErrorAction SilentlyContinue
if ($sccacheInstalled) {
    Write-Success "✓ sccache is already installed"
} else {
    Write-Warning "Installing sccache (this may take a few minutes)..."
    cargo install sccache
    Write-Success "✓ sccache installed"
}

# Configure sccache
$cargoConfig = "$env:USERPROFILE\.cargo\config.toml"
if (-not (Test-Path "$env:USERPROFILE\.cargo")) {
    New-Item -ItemType Directory -Path "$env:USERPROFILE\.cargo" | Out-Null
}

if (Test-Path $cargoConfig) {
    $content = Get-Content $cargoConfig -Raw
    if ($content -match 'rustc-wrapper.*sccache') {
        Write-Success "✓ sccache is already configured"
    } else {
        Add-Content -Path $cargoConfig -Value "`n[build]`nrustc-wrapper = `"sccache`"`n"
        Write-Success "✓ sccache configured in $cargoConfig"
    }
} else {
    Set-Content -Path $cargoConfig -Value "[build]`nrustc-wrapper = `"sccache`"`n"
    Write-Success "✓ sccache configured in $cargoConfig"
}

# Check if cargo-watch is installed
$watchInstalled = Get-Command cargo-watch -ErrorAction SilentlyContinue
if ($watchInstalled) {
    Write-Success "✓ cargo-watch is already installed"
} else {
    Write-Warning "Installing cargo-watch..."
    cargo install cargo-watch
    Write-Success "✓ cargo-watch installed"
}

Write-Host ""
Write-Success "=== Setup Complete! ==="
Write-Host ""

# Create setup marker file
New-Item -ItemType File -Path ".kiwi_setup_done" -Force | Out-Null

Write-Host "You can now use:"
Write-Host "  scripts\dev check  - Quick syntax check"
Write-Host "  scripts\dev watch  - Auto-check on file save"
Write-Host "  scripts\dev run    - Build and run"
Write-Host ""
Write-Host "Check sccache stats with:"
Write-Host "  sccache --show-stats"
Write-Host ""
