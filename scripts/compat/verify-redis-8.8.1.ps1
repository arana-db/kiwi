# Copyright (c) 2024-present, arana-db Community.  All rights reserved.
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0.

[CmdletBinding(PositionalBinding = $false)]
param(
    [Parameter(Mandatory = $true)]
    [string]$Source,

    [Parameter(Mandatory = $true)]
    [Alias('primary-metadata')]
    [string]$PrimaryMetadata,

    [Parameter(Mandatory = $true)]
    [string]$Output,

    [Parameter(Mandatory = $true)]
    [Alias('callback-input')]
    [string]$CallbackInput,

    [Parameter(Mandatory = $true, ValueFromRemainingArguments = $true)]
    [Alias('run-after-ready')]
    [string[]]$RunAfterReady
)

$ErrorActionPreference = 'Stop'

function Assert-SupportedWindowsPath {
    param([Parameter(Mandatory = $true)][string]$WindowsPath)

    $isUncAbsolute = $WindowsPath -match '^\\\\[^\\]+\\[^\\]+(?:\\|$)'
    if ($isUncAbsolute) {
        throw "UNC paths are not supported by the WSL verifier wrapper: $WindowsPath"
    }
    $isDriveAbsolute = $WindowsPath -match '^[A-Za-z]:[\\/]'
    if (-not $isDriveAbsolute) {
        throw "Path must be absolute: $WindowsPath"
    }
}

function Convert-ToWslPath {
    param([Parameter(Mandatory = $true)][string]$WindowsPath)

    Assert-SupportedWindowsPath $WindowsPath
    $converted = & wsl.exe --exec /usr/bin/wslpath -a -u -- $WindowsPath
    if ($LASTEXITCODE -ne 0 -or $null -eq $converted) {
        throw "Unable to convert path for WSL: $WindowsPath"
    }
    return $converted.Trim()
}

if ($RunAfterReady.Count -eq 0 -or [string]::IsNullOrWhiteSpace($RunAfterReady[0])) {
    throw '--run-after-ready requires a callback executable and argv.'
}
foreach ($requiredPath in @($Source, $PrimaryMetadata, $Output, $CallbackInput)) {
    Assert-SupportedWindowsPath $requiredPath
}
foreach ($argument in $RunAfterReady) {
    if ($argument -match '^\\\\[^\\]+\\[^\\]+(?:\\|$)') {
        throw "UNC paths are not supported by the WSL verifier wrapper: $argument"
    }
}

$wrapperWindows = Join-Path $PSScriptRoot 'verify-redis-8.8.1.sh'
$wrapperWsl = Convert-ToWslPath $wrapperWindows
$arguments = [System.Collections.Generic.List[string]]::new()
$arguments.Add('--source')
$arguments.Add((Convert-ToWslPath $Source))
$arguments.Add('--primary-metadata')
$arguments.Add((Convert-ToWslPath $PrimaryMetadata))
$arguments.Add('--output')
$arguments.Add((Convert-ToWslPath $Output))
$arguments.Add('--callback-input')
$arguments.Add((Convert-ToWslPath $CallbackInput))
$arguments.Add('--run-after-ready')
foreach ($argument in $RunAfterReady) {
    if ($argument -match '^\\\\[^\\]+\\[^\\]+(?:\\|$)') {
        throw "UNC paths are not supported by the WSL verifier wrapper: $argument"
    }
    if ($argument -match '^[A-Za-z]:[\\/]') {
        $arguments.Add((Convert-ToWslPath $argument))
    }
    else {
        $arguments.Add($argument)
    }
}

& wsl.exe --exec /usr/bin/bash $wrapperWsl @arguments
exit $LASTEXITCODE
