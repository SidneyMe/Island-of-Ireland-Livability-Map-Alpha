param(
    [ValidateSet("DevReuse", "DevPrepare", "AccuratePrepare")]
    [string]$Mode = "DevReuse"
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = (Resolve-Path (Join-Path $scriptDir "..\..")).Path
Set-Location $projectRoot

if (-not $env:GEO_CONDA_ENV) { $env:GEO_CONDA_ENV = "base" }
if (-not $env:MINIFORGE_ROOT) { $env:MINIFORGE_ROOT = Join-Path $env:USERPROFILE "miniforge3" }
if (-not $env:NOISE_INGEST_MODE) { $env:NOISE_INGEST_MODE = "ogr2ogr" }
$geoCmd = Join-Path $scriptDir "geo_env.cmd"
$baseArgs = @(
    "/c",
    "`"$geoCmd`"",
    ".\.venv\Scripts\python.exe",
    "main.py",
    "--precompute-dev"
)

switch ($Mode) {
    "DevReuse" {
        $modeArgs = @("--force-precompute", "--require-active-noise-artifact")
        $defaultTimeoutSeconds = 1200
    }
    "DevPrepare" {
        $modeArgs = @("--refresh-noise-artifact", "--reimport-noise-source", "--force-noise-artifact", "--force-precompute")
        $defaultTimeoutSeconds = 3600
    }
    "AccuratePrepare" {
        $modeArgs = @("--noise-accurate", "--refresh-noise-artifact", "--reimport-noise-source", "--force-noise-artifact", "--force-precompute")
        $defaultTimeoutSeconds = 7200
    }
    default {
        throw "Unsupported mode: $Mode"
    }
}

if ($env:NOISE_PRECOMPUTE_WATCHDOG_TIMEOUT_SEC) {
    $timeoutSeconds = [int]$env:NOISE_PRECOMPUTE_WATCHDOG_TIMEOUT_SEC
} else {
    $timeoutSeconds = $defaultTimeoutSeconds
}

$argList = @($baseArgs + $modeArgs)
Write-Host "[watchdog] starting noise mode=$Mode timeout=${timeoutSeconds}s env=$($env:GEO_CONDA_ENV)"
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
