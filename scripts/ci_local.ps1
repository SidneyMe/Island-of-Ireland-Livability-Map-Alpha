[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Format-Elapsed {
    param(
        [Parameter(Mandatory)]
        [TimeSpan]$Duration
    )

    if ($Duration.TotalHours -ge 1) {
        return $Duration.ToString('hh\:mm\:ss')
    }

    return $Duration.ToString('mm\:ss')
}

function Invoke-Step {
    param(
        [Parameter(Mandatory)]
        [string]$Name,
        [Parameter(Mandatory)]
        [scriptblock]$Action
    )

    $stepTimer = [System.Diagnostics.Stopwatch]::StartNew()
    Write-Host ""
    Write-Host "=== $Name ==="

    try {
        $global:LASTEXITCODE = 0
        & $Action
        $exitCode = $global:LASTEXITCODE
        if ($exitCode -ne 0) {
            throw "Step '$Name' failed with exit code $exitCode."
        }

        $stepTimer.Stop()
        Write-Host ("--- {0} completed in {1}" -f $Name, (Format-Elapsed -Duration $stepTimer.Elapsed))
    } catch {
        $stepTimer.Stop()
        Write-Host ("--- {0} failed after {1}" -f $Name, (Format-Elapsed -Duration $stepTimer.Elapsed))
        throw
    }
}

function Assert-RequiredCommands {
    $missing = @()
    foreach ($commandName in @('python', 'npm.cmd', 'git')) {
        if (-not (Get-Command $commandName -ErrorAction SilentlyContinue)) {
            $missing += $commandName
        }
    }

    if ($missing.Count -gt 0) {
        throw "Missing required commands: $($missing -join ', ')"
    }
}

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$overallTimer = [System.Diagnostics.Stopwatch]::StartNew()

Push-Location $repoRoot
try {
    Write-Host "Local CI checks for Island-of-Ireland-Livability-Map-Alpha"
    Write-Host "Repo root: $repoRoot"

    Invoke-Step "Verify required commands" {
        Assert-RequiredCommands
    }

    Invoke-Step "Python tests" {
        & python -m pytest -q
    }

    Invoke-Step "Alembic current" {
        & python -m alembic current
    }

    Invoke-Step "Alembic upgrade head" {
        & python -m alembic upgrade head
    }

    Invoke-Step "DB integrity check" {
        & python scripts/db_integrity_check.py
    }

    Invoke-Step "Precompute explain smoke" {
        & python main.py --precompute-dev --explain
    }

    Invoke-Step "Frontend tests" {
        & npm.cmd test --prefix frontend
    }

    Invoke-Step "Frontend build" {
        & npm.cmd run build --prefix frontend
    }

    Invoke-Step "Frontend bundle freshness" {
        & git diff --exit-code -- static/dist
    }

    Invoke-Step "Whitespace check" {
        & git diff --check
    }

    $overallTimer.Stop()
    Write-Host ""
    Write-Host ("All local CI checks passed in {0}." -f (Format-Elapsed -Duration $overallTimer.Elapsed))
} finally {
    Pop-Location
}
