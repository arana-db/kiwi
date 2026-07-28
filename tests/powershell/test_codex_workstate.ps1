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
    [Parameter(Mandatory = $true)]
    [string]$ScriptPath
)

$ErrorActionPreference = 'Stop'

function Assert-True {
    param(
        [Parameter(Mandatory = $true)]
        [bool]$Condition,
        [Parameter(Mandatory = $true)]
        [string]$Message
    )

    if (-not $Condition) {
        throw "ASSERTION FAILED: $Message"
    }
}

function Assert-Contains {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Text,
        [Parameter(Mandatory = $true)]
        [string]$Expected,
        [Parameter(Mandatory = $true)]
        [string]$Message
    )

    Assert-True -Condition $Text.Contains($Expected) -Message $Message
}

function Assert-NotContains {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Text,
        [Parameter(Mandatory = $true)]
        [string]$Unexpected,
        [Parameter(Mandatory = $true)]
        [string]$Message
    )

    Assert-True -Condition (-not $Text.Contains($Unexpected)) -Message $Message
}

$tempRoot = [System.IO.Path]::GetFullPath([System.IO.Path]::GetTempPath())
$testRoot = Join-Path $tempRoot ("kiwi-codex-workstate-" + [guid]::NewGuid().ToString('N'))

try {
    New-Item -ItemType Directory -Path $testRoot | Out-Null
    git -C $testRoot init --initial-branch=test-branch | Out-Null
    git -C $testRoot config user.email 'kiwi-workstate-test@example.invalid'
    git -C $testRoot config user.name 'Kiwi Workstate Test'

    Set-Content -LiteralPath (Join-Path $testRoot '.gitignore') -Value ".codex/recovery/`n"
    Set-Content -LiteralPath (Join-Path $testRoot 'tracked.txt') -Value "baseline`n"
    git -C $testRoot add .gitignore tracked.txt
    git -C $testRoot commit -m 'test: create baseline' | Out-Null

    Add-Content -LiteralPath (Join-Path $testRoot 'tracked.txt') -Value 'user change'
    Set-Content -LiteralPath (Join-Path $testRoot 'user-untracked.txt') -Value 'user data'
    Set-Content -LiteralPath (Join-Path $testRoot 'task-owned.txt') -Value 'task data'

    $statusBefore = @(git -C $testRoot status --short --untracked-files=all)
    $complexTitle = "TASK-1: fix `"quoted`" path C:\work`nnext`tstep"

    & $ScriptPath `
        -RepoRoot $testRoot `
        -Title $complexTitle `
        -TaskId 'kiwi-foundation' `
        -Mode 'planning' `
        -Allowed 'inspect,edit-docs,run-read-only-tests' `
        -Forbidden 'commit,push,merge,reset,clean' `
        -TaskPath 'TASK-OWNED.TXT','task-owned.txt','DOCS/PROJECT-CHARTER.MD','.PLANNING/state.md' `
        -MixedPath 'TRACKED.TXT','tracked.txt','.GITIGNORE','.gitignore'

    $recoveryRoot = Join-Path $testRoot '.codex/recovery'
    $activePath = Join-Path $recoveryRoot 'ACTIVE.md'
    Assert-True -Condition (Test-Path -LiteralPath $activePath) -Message 'ACTIVE.md must be created'

    $active = Get-Content -Raw -LiteralPath $activePath
    $head = (git -C $testRoot rev-parse HEAD).Trim()
    Assert-Contains -Text $active -Expected 'schema: kiwi-workstate/v1' -Message 'schema must be recorded'
    Assert-Contains -Text $active -Expected 'branch: test-branch' -Message 'branch must be recorded'
    Assert-Contains -Text $active -Expected "head_sha: $head" -Message 'HEAD must be recorded'
    Assert-Contains -Text $active -Expected 'title: "TASK-1: fix \"quoted\" path C:\\work\nnext\tstep"' -Message 'title must be a valid escaped YAML scalar'
    Assert-Contains -Text $active -Expected '- user-untracked.txt' -Message 'pre-existing untracked files must be recorded'
    Assert-Contains -Text $active -Expected '### Mixed ownership' -Message 'mixed ownership section must be recorded'
    Assert-Contains -Text $active -Expected '- TRACKED.TXT' -Message 'mixed tracked changes must preserve the first supplied casing'
    Assert-NotContains -Text $active -Unexpected '- tracked.txt' -Message 'mixed paths that differ only by case must be de-duplicated'
    Assert-Contains -Text $active -Expected '- TASK-OWNED.TXT' -Message 'task-owned dirty paths must match Git paths case-insensitively'
    Assert-NotContains -Text $active -Unexpected '- task-owned.txt' -Message 'task-owned paths that differ only by case must be de-duplicated and excluded from pre-existing changes'
    Assert-Contains -Text $active -Expected '- DOCS/PROJECT-CHARTER.MD' -Message 'task-owned paths must be recorded'
    Assert-Contains -Text $active -Expected '- .PLANNING/state.md' -Message 'leading dots in task-owned paths must be preserved'
    Assert-Contains -Text $active -Expected '- .GITIGNORE' -Message 'leading dots in mixed paths must be preserved'
    Assert-Contains -Text $active -Expected '- commit' -Message 'forbidden authority must be recorded'

    $checkpointFiles = @(Get-ChildItem -LiteralPath (Join-Path $recoveryRoot 'checkpoints') -Filter '*.md')
    Assert-True -Condition ($checkpointFiles.Count -eq 1) -Message 'first save must create one checkpoint'

    $snapshotDirs = @(Get-ChildItem -LiteralPath (Join-Path $recoveryRoot 'snapshots') -Directory)
    Assert-True -Condition ($snapshotDirs.Count -eq 1) -Message 'first save must create one snapshot directory'
    Assert-True -Condition (Test-Path -LiteralPath (Join-Path $snapshotDirs[0].FullName 'git-status.txt')) -Message 'snapshot must contain git-status.txt'
    Assert-True -Condition (Test-Path -LiteralPath (Join-Path $snapshotDirs[0].FullName 'git-head.txt')) -Message 'snapshot must contain git-head.txt'

    Start-Sleep -Milliseconds 1100
    & $ScriptPath `
        -RepoRoot $testRoot `
        -Title $complexTitle `
        -TaskId 'kiwi-foundation' `
        -Mode 'planning' `
        -Allowed 'inspect,edit-docs,run-read-only-tests' `
        -Forbidden 'commit,push,merge,reset,clean' `
        -TaskPath 'TASK-OWNED.TXT','task-owned.txt','DOCS/PROJECT-CHARTER.MD','.PLANNING/state.md' `
        -MixedPath 'TRACKED.TXT','tracked.txt','.GITIGNORE','.gitignore'

    $checkpointFiles = @(Get-ChildItem -LiteralPath (Join-Path $recoveryRoot 'checkpoints') -Filter '*.md')
    Assert-True -Condition ($checkpointFiles.Count -eq 2) -Message 'checkpoints must be append-only'

    $controlTitle = 'nul' + [char]0 + 'bell' + [char]7 + 'backspace' + [char]8 +
        'vertical' + [char]11 + 'form' + [char]12 + 'escape' + [char]27 + 'unit' + [char]31
    Start-Sleep -Milliseconds 1100
    & $ScriptPath `
        -RepoRoot $testRoot `
        -Title $controlTitle `
        -TaskId 'kiwi-foundation' `
        -Mode 'verification' `
        -TaskPath 'TASK-OWNED.TXT' `
        -MixedPath 'TRACKED.TXT'

    $active = Get-Content -LiteralPath $activePath -Raw
    Assert-Contains -Text $active -Expected 'title: "nul\0bell\abackspace\bvertical\vform\fescape\eunit\u001F"' -Message 'all YAML control characters must use valid double-quoted escapes'
    $checkpointFiles = @(Get-ChildItem -LiteralPath (Join-Path $recoveryRoot 'checkpoints') -Filter '*.md')
    Assert-True -Condition ($checkpointFiles.Count -eq 3) -Message 'control-character save must append a third checkpoint'

    $statusAfter = @(git -C $testRoot status --short --untracked-files=all)
    Assert-True -Condition (($statusBefore -join "`n") -eq ($statusAfter -join "`n")) -Message 'saving workstate must not mutate Git-visible files'

    Write-Output 'PASS: codex workstate save is append-only, Git-read-only, and records recovery anchors.'
}
finally {
    if (Test-Path -LiteralPath $testRoot) {
        $resolvedTestRoot = [System.IO.Path]::GetFullPath($testRoot)
        if (-not $resolvedTestRoot.StartsWith($tempRoot, [System.StringComparison]::OrdinalIgnoreCase)) {
            throw "Refusing to remove unexpected path: $resolvedTestRoot"
        }
        Remove-Item -LiteralPath $resolvedTestRoot -Recurse -Force
    }
}
