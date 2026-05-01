param(
    [switch]$Incremental,
    [switch]$Accurate
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = (Resolve-Path (Join-Path $scriptDir "..\..")).Path
Set-Location $projectRoot

if (-not $env:GEO_CONDA_ENV) { $env:GEO_CONDA_ENV = "base" }
if (-not $env:MINIFORGE_ROOT) { $env:MINIFORGE_ROOT = Join-Path $env:USERPROFILE "miniforge3" }
if (-not $env:NOISE_INGEST_MODE) { $env:NOISE_INGEST_MODE = "ogr2ogr" }
if (-not $env:NOISE_OGR2OGR_GDB_WORKERS) { $env:NOISE_OGR2OGR_GDB_WORKERS = "2" }
if (-not $env:NOISE_OGR2OGR_GDB_CHUNK_SIZE) { $env:NOISE_OGR2OGR_GDB_CHUNK_SIZE = "25" }

$timeoutSeconds = 7200
if ($env:NOISE_PRECOMPUTE_WATCHDOG_TIMEOUT_SEC) {
    $timeoutSeconds = [int]$env:NOISE_PRECOMPUTE_WATCHDOG_TIMEOUT_SEC
}

$geoCmd = Join-Path $scriptDir "geo_env.cmd"
$argList = @(
    "/c",
    "`"$geoCmd`"",
    ".\.venv\Scripts\python.exe",
    "main.py",
    "--precompute-dev",
    "--refresh-noise-artifact"
)

if ($Accurate) {
    $argList += "--noise-accurate"
}

if (-not $Incremental) {
    $argList += @("--reimport-noise-source", "--force-noise-artifact", "--force-precompute")
}

if ($Incremental -and $Accurate) {
    Write-Host "[watchdog] starting precompute (incremental, accurate) timeout=${timeoutSeconds}s env=$($env:GEO_CONDA_ENV)"
} elseif ($Incremental) {
    Write-Host "[watchdog] starting precompute (incremental, dev-fast) timeout=${timeoutSeconds}s env=$($env:GEO_CONDA_ENV)"
} elseif ($Accurate) {
    Write-Host "[watchdog] starting precompute (full-reimport, accurate) timeout=${timeoutSeconds}s env=$($env:GEO_CONDA_ENV)"
} else {
    Write-Host "[watchdog] starting precompute (full-reimport, dev-fast) timeout=${timeoutSeconds}s env=$($env:GEO_CONDA_ENV)"
}
$proc = Start-Process -FilePath "cmd.exe" -ArgumentList $argList -PassThru -NoNewWindow

if ($proc.WaitForExit($timeoutSeconds * 1000)) {
    exit $proc.ExitCode
}

Write-Host "[watchdog] timeout reached; killing process tree for PID=$($proc.Id)"
try {
    & taskkill /PID $proc.Id /T /F | Out-Null
} catch {
    Write-Host "[watchdog] taskkill failed: $($_.Exception.Message)"
}

try {
    Get-Process -Name "ogr2ogr" -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
} catch {
}

exit 124
