[CmdletBinding()]
param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$RunnerArguments
)

$ErrorActionPreference = 'Stop'

$wsl = Get-Command wsl.exe -ErrorAction Stop
$scriptPath = Join-Path $PSScriptRoot 'test-vector-storage-compat.sh'
if (-not (Test-Path -LiteralPath $scriptPath -PathType Leaf)) {
    throw "Linux compatibility runner not found: $scriptPath"
}

$linuxScript = & $wsl.Source --exec wslpath -a $scriptPath
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($linuxScript)) {
    throw "Failed to translate runner path into WSL: $scriptPath"
}

& $wsl.Source --exec bash $linuxScript @RunnerArguments
$runnerExitCode = $LASTEXITCODE
exit $runnerExitCode
