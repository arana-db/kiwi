# Copyright (c) 2024-present, arana-db Community.  All rights reserved.
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

[CmdletBinding()]
param(
    [string]$RepoRoot = (Get-Location).Path,

    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$Title,

    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[A-Za-z0-9][A-Za-z0-9._-]*$')]
    [string]$TaskId,

    [ValidateSet('planning', 'read-only', 'implementation', 'verification', 'awaiting-user', 'blocked')]
    [string]$Mode = 'planning',

    [string[]]$Allowed = @('inspect', 'edit-docs', 'run-read-only-tests'),

    [string[]]$Forbidden = @('commit', 'push', 'merge', 'reset', 'clean'),

    [string[]]$TaskPath = @(),

    [string[]]$MixedPath = @(),

    [string]$Objective = 'Continue the active Kiwi project task from the recorded state.',

    [string]$CurrentPosition = 'Checkpoint captured. Read the linked planning files before continuing.',

    [string[]]$RemainingWork = @('Read .planning/STATE.md and .planning/KANBAN.md, then execute the highest-priority ready item.'),

    [string[]]$Decision = @(),

    [string[]]$Validation = @()
)

$ErrorActionPreference = 'Stop'

function Invoke-GitReadOnly {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Arguments
    )

    $output = @(& git -C $script:ResolvedRepoRoot @Arguments 2>&1)
    if ($LASTEXITCODE -ne 0) {
        throw "git $($Arguments -join ' ') failed: $($output -join [Environment]::NewLine)"
    }
    return $output
}

function Convert-ToListItems {
    param(
        [string[]]$Value,
        [string]$EmptyValue = '- none'
    )

    if (-not $Value -or $Value.Count -eq 0) {
        return @($EmptyValue)
    }

    return @($Value | ForEach-Object { "- $_" })
}

function Normalize-ListArgument {
    param([string[]]$Value)

    $result = [System.Collections.Generic.List[string]]::new()
    foreach ($item in @($Value)) {
        foreach ($part in ($item -split ',')) {
            $normalized = $part.Trim()
            if ($normalized.Length -gt 0 -and -not $result.Contains($normalized)) {
                $result.Add($normalized)
            }
        }
    }
    return @($result)
}

$ResolvedRepoRoot = [System.IO.Path]::GetFullPath($RepoRoot)
$repoProbe = @(& git -C $ResolvedRepoRoot rev-parse --show-toplevel 2>&1)
if ($LASTEXITCODE -ne 0) {
    throw "Not a Git repository: $ResolvedRepoRoot"
}
$gitRoot = [System.IO.Path]::GetFullPath(($repoProbe | Select-Object -First 1).Trim())
if (-not $gitRoot.Equals($ResolvedRepoRoot, [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "RepoRoot must be the Git top-level directory. Expected '$gitRoot', got '$ResolvedRepoRoot'."
}

$allowedItems = Normalize-ListArgument -Value $Allowed
$forbiddenItems = Normalize-ListArgument -Value $Forbidden
$taskPaths = Normalize-ListArgument -Value $TaskPath
$mixedPaths = Normalize-ListArgument -Value $MixedPath
$remainingItems = Normalize-ListArgument -Value $RemainingWork
$decisionItems = Normalize-ListArgument -Value $Decision
$validationItems = Normalize-ListArgument -Value $Validation

$branch = ((Invoke-GitReadOnly -Arguments @('branch', '--show-current')) -join '').Trim()
if ([string]::IsNullOrWhiteSpace($branch)) {
    $branch = '(detached)'
}
$headSha = ((Invoke-GitReadOnly -Arguments @('rev-parse', 'HEAD')) -join '').Trim()
$upstream = @(& git -C $ResolvedRepoRoot rev-parse --abbrev-ref '@{upstream}' 2>$null)
if ($LASTEXITCODE -ne 0 -or $upstream.Count -eq 0) {
    $upstreamText = '(none)'
} else {
    $upstreamText = ($upstream -join '').Trim()
}

$statusLines = @(Invoke-GitReadOnly -Arguments @('status', '--porcelain=v1', '--untracked-files=all'))
$dirtyPaths = [System.Collections.Generic.List[string]]::new()
foreach ($line in $statusLines) {
    if ($line.Length -lt 4) {
        continue
    }
    $path = $line.Substring(3).Trim()
    if ($path.Contains(' -> ')) {
        $path = ($path -split ' -> ')[-1]
    }
    $path = $path.Trim('"') -replace '\\', '/'
    if ($path.StartsWith('.codex/recovery/')) {
        continue
    }
    if (-not $dirtyPaths.Contains($path)) {
        $dirtyPaths.Add($path)
    }
}

$taskOwned = [System.Collections.Generic.List[string]]::new()
foreach ($path in $taskPaths) {
    $normalized = ($path -replace '\\', '/').Trim()
    if ($normalized.StartsWith('./', [System.StringComparison]::Ordinal)) {
        $normalized = $normalized.Substring(2)
    }
    if (-not $taskOwned.Contains($normalized)) {
        $taskOwned.Add($normalized)
    }
}

$mixedOwned = [System.Collections.Generic.List[string]]::new()
foreach ($path in $mixedPaths) {
    $normalized = ($path -replace '\\', '/').Trim()
    if ($normalized.StartsWith('./', [System.StringComparison]::Ordinal)) {
        $normalized = $normalized.Substring(2)
    }
    if (-not $mixedOwned.Contains($normalized)) {
        $mixedOwned.Add($normalized)
    }
}

$preExisting = @($dirtyPaths | Where-Object {
    -not $taskOwned.Contains($_) -and -not $mixedOwned.Contains($_)
})
$timestamp = Get-Date
$timestampSlug = $timestamp.ToString('yyyyMMdd-HHmmss-fff')
$timestampIso = $timestamp.ToString('yyyy-MM-ddTHH:mm:ss.fffzzz')

$recoveryRoot = Join-Path $ResolvedRepoRoot '.codex/recovery'
$checkpointRoot = Join-Path $recoveryRoot 'checkpoints'
$snapshotRoot = Join-Path $recoveryRoot 'snapshots'
$snapshotDir = Join-Path $snapshotRoot $timestampSlug
New-Item -ItemType Directory -Path $checkpointRoot -Force | Out-Null
New-Item -ItemType Directory -Path $snapshotDir -Force | Out-Null

$statusSnapshot = @(Invoke-GitReadOnly -Arguments @('status', '--porcelain=v2', '--branch', '--untracked-files=all'))
$trackedNameStatus = @(Invoke-GitReadOnly -Arguments @('diff', '--name-status'))
$stagedNameStatus = @(Invoke-GitReadOnly -Arguments @('diff', '--cached', '--name-status'))
$diffStat = @(Invoke-GitReadOnly -Arguments @('diff', '--stat'))
$stagedDiffStat = @(Invoke-GitReadOnly -Arguments @('diff', '--cached', '--stat'))

[System.IO.File]::WriteAllText((Join-Path $snapshotDir 'git-head.txt'), "$headSha$([Environment]::NewLine)")
[System.IO.File]::WriteAllText((Join-Path $snapshotDir 'git-branch.txt'), "$branch$([Environment]::NewLine)")
[System.IO.File]::WriteAllText((Join-Path $snapshotDir 'git-upstream.txt'), "$upstreamText$([Environment]::NewLine)")
[System.IO.File]::WriteAllLines((Join-Path $snapshotDir 'git-status.txt'), $statusSnapshot)
[System.IO.File]::WriteAllLines((Join-Path $snapshotDir 'tracked-name-status.txt'), $trackedNameStatus)
[System.IO.File]::WriteAllLines((Join-Path $snapshotDir 'staged-name-status.txt'), $stagedNameStatus)
[System.IO.File]::WriteAllLines((Join-Path $snapshotDir 'diff-stat.txt'), $diffStat)
[System.IO.File]::WriteAllLines((Join-Path $snapshotDir 'staged-diff-stat.txt'), $stagedDiffStat)

$checkpointRelative = ".codex/recovery/checkpoints/$timestampSlug-$TaskId.md"
$snapshotRelative = ".codex/recovery/snapshots/$timestampSlug"
$checkpointPath = Join-Path $ResolvedRepoRoot ($checkpointRelative -replace '/', [System.IO.Path]::DirectorySeparatorChar)

$contentLines = [System.Collections.Generic.List[string]]::new()
@(
    '---',
    'schema: kiwi-workstate/v1',
    'status: active',
    "updated_at: $timestampIso",
    "task_id: $TaskId",
    "title: $Title",
    "repo_root: $ResolvedRepoRoot",
    "branch: $branch",
    "head_sha: $headSha",
    "upstream_or_base: $upstreamText",
    "mode: $Mode",
    "latest_checkpoint: $checkpointRelative",
    "git_snapshot: $snapshotRelative",
    '---',
    '',
    '# Objective',
    '',
    $Objective,
    '',
    '## Authority',
    '',
    '### Allowed'
) | ForEach-Object { $contentLines.Add($_) }
Convert-ToListItems -Value $allowedItems | ForEach-Object { $contentLines.Add($_) }
$contentLines.Add('')
$contentLines.Add('### Forbidden')
Convert-ToListItems -Value $forbiddenItems | ForEach-Object { $contentLines.Add($_) }
$contentLines.Add('')
$contentLines.Add('## Current position')
$contentLines.Add('')
$contentLines.Add($CurrentPosition)
$contentLines.Add('')
$contentLines.Add('## Dirty ownership')
$contentLines.Add('')
$contentLines.Add('### Pre-existing or unknown user changes')
Convert-ToListItems -Value $preExisting | ForEach-Object { $contentLines.Add($_) }
$contentLines.Add('')
$contentLines.Add('### Changes created by this task')
Convert-ToListItems -Value @($taskOwned) | ForEach-Object { $contentLines.Add($_) }
$contentLines.Add('')
$contentLines.Add('### Mixed ownership')
Convert-ToListItems -Value @($mixedOwned) | ForEach-Object { $contentLines.Add($_) }
$contentLines.Add('')
$contentLines.Add('## Decisions')
Convert-ToListItems -Value $decisionItems | ForEach-Object { $contentLines.Add($_) }
$contentLines.Add('')
$contentLines.Add('## Evidence and validation')
Convert-ToListItems -Value $validationItems | ForEach-Object { $contentLines.Add($_) }
$contentLines.Add('')
$contentLines.Add('## Remaining work')
if (-not $remainingItems -or $remainingItems.Count -eq 0) {
    $contentLines.Add('1. none')
} else {
    for ($index = 0; $index -lt $remainingItems.Count; $index++) {
        $contentLines.Add("$($index + 1). $($remainingItems[$index])")
    }
}
$contentLines.Add('')
$contentLines.Add('## Resume instruction')
$contentLines.Add('')
$contentLines.Add('1. Read `AGENTS.md`, `.planning/PROJECT.md`, `.planning/STATE.md`, and `.planning/KANBAN.md`.')
$contentLines.Add('2. Run `git status --porcelain=v2 --branch --untracked-files=all` and compare it with the recorded snapshot.')
$contentLines.Add('3. If branch, HEAD, or dirty ownership differs, report the drift and stop before modifying files.')
$contentLines.Add('4. Continue with the first item in Remaining work only after the recovery anchors match.')

$content = ($contentLines -join [Environment]::NewLine) + [Environment]::NewLine
[System.IO.File]::WriteAllText($checkpointPath, $content, [System.Text.UTF8Encoding]::new($false))

$activePath = Join-Path $recoveryRoot 'ACTIVE.md'
$activeTempPath = Join-Path $recoveryRoot ("ACTIVE.$timestampSlug.tmp")
[System.IO.File]::WriteAllText($activeTempPath, $content, [System.Text.UTF8Encoding]::new($false))
Move-Item -LiteralPath $activeTempPath -Destination $activePath -Force

Write-Output "WORKSTATE SAVED"
Write-Output "Active: $activePath"
Write-Output "Checkpoint: $checkpointPath"
Write-Output "Snapshot: $snapshotDir"
