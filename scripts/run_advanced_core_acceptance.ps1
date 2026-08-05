param(
    [string]$PythonCommand = "python",
    [string]$LogPath = "reports/advanced-core-acceptance.log",
    [string]$RuntimeLogPath = "reports/advanced-core-runtime.log",
    [string]$EvidencePath = "reports/advanced-core-evidence.json",
    [switch]$KeepEnvironment,
    [switch]$SkipBuild
)

$ErrorActionPreference = "Stop"
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$ResolvedLog = Join-Path $Root $LogPath
$Arguments = @(
    (Join-Path $Root "scripts/run_advanced_acceptance.py"),
    "--profile", "core",
    "--runtime-log-path", (Join-Path $Root $RuntimeLogPath),
    "--evidence-path", (Join-Path $Root $EvidencePath)
)
if ($KeepEnvironment) {
    $Arguments += "--keep-environment"
}
if ($SkipBuild) {
    $Arguments += "--skip-build"
}

New-Item -ItemType Directory -Force (Split-Path $ResolvedLog) | Out-Null
Set-Location $Root
$PreviousErrorActionPreference = $ErrorActionPreference
$ErrorActionPreference = "Continue"
& $PythonCommand @Arguments 2>&1 | Tee-Object -FilePath $ResolvedLog
$ExitCode = $LASTEXITCODE
$ErrorActionPreference = $PreviousErrorActionPreference
if ($ExitCode -ne 0) {
    throw "Advanced Core acceptance failed with exit code $ExitCode"
}
