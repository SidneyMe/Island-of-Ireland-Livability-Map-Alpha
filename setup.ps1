<#
.SYNOPSIS
One-command Windows bootstrap: installs every prerequisite, provisions a local
PostgreSQL/PostGIS database, downloads every dataset, builds the helpers, and
runs the full pipeline up to a servable map. Double-click setup.cmd to run it.

.PARAMETER SkipBuild
Install everything but skip the multi-hour import/transit/precompute pipeline.

.PARAMETER RefreshData
Re-download OSM and GTFS from their official sources before building.

.NOTES
Reruns are safe: finished installs, downloads, and pipeline stages are reused.
Prerequisites installed through winget: Miniforge (conda-forge toolchain with
GDAL, PostgreSQL/PostGIS, osm2pgsql, Node.js, Python), Rust + MSVC build tools,
and Docker Desktop (self-hosted basemap).
#>
[CmdletBinding()]
param(
    [switch]$SkipBuild,
    [switch]$RefreshData
)

$ErrorActionPreference = 'Stop'
$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ProjectRoot

$GeoEnvName = if ($env:GEO_CONDA_ENV) { $env:GEO_CONDA_ENV } else { 'livability-gdal' }
$MiniforgeRoot = if ($env:MINIFORGE_ROOT) { $env:MINIFORGE_ROOT } else { Join-Path $env:USERPROFILE 'miniforge3' }
$GeoEnvCmd = Join-Path $ProjectRoot 'scripts\win\geo_env.cmd'
$LocalPgData = Join-Path $env:LOCALAPPDATA 'livability\pgdata'
$EnvFile = Join-Path $ProjectRoot '.env'

function Write-Step([string]$Message) {
    Write-Host ''
    Write-Host "=== $Message ===" -ForegroundColor Cyan
}

function Stop-Setup([string]$Message) {
    Write-Host "setup: $Message" -ForegroundColor Red
    exit 1
}

function Test-Command([string]$Name) {
    return [bool](Get-Command $Name -ErrorAction SilentlyContinue)
}

function Update-SessionPath {
    $machine = [Environment]::GetEnvironmentVariable('Path', 'Machine')
    $user = [Environment]::GetEnvironmentVariable('Path', 'User')
    $env:Path = "$machine;$user"
    $cargoBin = Join-Path $env:USERPROFILE '.cargo\bin'
    if (Test-Path $cargoBin) { $env:Path = "$cargoBin;$env:Path" }
}

function Install-WingetPackage([string]$Id, [string[]]$ExtraArgs = @()) {
    if (-not (Test-Command 'winget')) {
        Stop-Setup "winget is required. Install 'App Installer' from the Microsoft Store, then rerun setup.cmd."
    }
    Write-Host "Installing $Id via winget..."
    $wingetArgs = @('install', '--exact', '--id', $Id, '--accept-source-agreements', '--accept-package-agreements', '--silent') + $ExtraArgs
    & winget @wingetArgs
    # -1978335189 = already installed / no applicable upgrade.
    if ($LASTEXITCODE -ne 0 -and $LASTEXITCODE -ne -1978335189) {
        Stop-Setup "winget install $Id failed (exit $LASTEXITCODE)."
    }
    Update-SessionPath
}

# Run a command inside the activated conda env (GDAL/PROJ/noise env vars set).
function Invoke-GeoEnv {
    & $GeoEnvCmd @args
    if ($LASTEXITCODE -ne 0) {
        Stop-Setup "command failed (exit $LASTEXITCODE): $($args -join ' ')"
    }
}

function Read-DotEnv([string]$Path) {
    $values = @{}
    if (Test-Path $Path) {
        foreach ($line in Get-Content -LiteralPath $Path) {
            if ($line -match '^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)\s*$') {
                $values[$Matches[1]] = $Matches[2].Trim().Trim('"').Trim("'")
            }
        }
    }
    return $values
}

function New-RandomPassword {
    $bytes = New-Object byte[] 24
    [System.Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($bytes)
    return ([Convert]::ToBase64String($bytes) -replace '[^A-Za-z0-9]', '').Substring(0, 24)
}

# Windows PowerShell 5.1 turns redirected native stderr into terminating errors
# under ErrorActionPreference=Stop, so probe Docker with a relaxed preference.
function Test-DockerEngine {
    if (-not (Test-Command 'docker')) { return $false }
    $previous = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        & docker info *> $null
        return ($LASTEXITCODE -eq 0)
    } catch {
        return $false
    } finally {
        $ErrorActionPreference = $previous
    }
}

function Test-TcpPort([string]$HostName, [int]$Port) {
    $client = New-Object System.Net.Sockets.TcpClient
    try {
        $async = $client.BeginConnect($HostName, $Port, $null, $null)
        return ($async.AsyncWaitHandle.WaitOne(1500) -and $client.Connected)
    } catch {
        return $false
    } finally {
        $client.Close()
    }
}

# ---------------------------------------------------------------------------
Write-Step '0. Installing prerequisites (Miniforge, Rust, Docker Desktop)'
Update-SessionPath

$CondaExe = Join-Path $MiniforgeRoot 'Scripts\conda.exe'
if (-not (Test-Path $CondaExe)) {
    Install-WingetPackage 'CondaForge.Miniforge3' @('--scope', 'user', '--override', "/InstallationType=JustMe /RegisterPython=0 /AddToPath=0 /S /D=$MiniforgeRoot")
    if (-not (Test-Path $CondaExe)) { Stop-Setup "Miniforge was not found at $MiniforgeRoot after install; set MINIFORGE_ROOT." }
}

if (-not (Test-Command 'cargo')) {
    # The MSVC Rust toolchain links with the Visual C++ build tools.
    Install-WingetPackage 'Microsoft.VisualStudio.2022.BuildTools' @('--override', '--quiet --wait --norestart --add Microsoft.VisualStudio.Workload.VCTools --includeRecommended')
    Install-WingetPackage 'Rustlang.Rustup'
    if (Test-Command 'rustup') { & rustup default stable | Out-Host }
    if (-not (Test-Command 'cargo')) { Stop-Setup 'cargo is still unavailable after installing rustup; open a new terminal and rerun setup.cmd.' }
}

if (-not (Test-Command 'docker')) {
    Install-WingetPackage 'Docker.DockerDesktop'
}

# ---------------------------------------------------------------------------
Write-Step "1. Creating/updating conda env '$GeoEnvName' from environment.yml"
$envList = & $CondaExe env list
$envExists = $false
foreach ($line in $envList) {
    $name = ($line -split '\s+')[0]
    if ($name -eq $GeoEnvName) { $envExists = $true }
}
if ($envExists) {
    & $CondaExe env update --name $GeoEnvName --file environment.yml
} else {
    & $CondaExe env create --name $GeoEnvName --file environment.yml
}
if ($LASTEXITCODE -ne 0) { Stop-Setup 'conda env create/update failed.' }
$env:MINIFORGE_ROOT = $MiniforgeRoot
$env:GEO_CONDA_ENV = $GeoEnvName
Invoke-GeoEnv python -c "from osgeo import gdal; print('GDAL', gdal.__version__)"

# ---------------------------------------------------------------------------
Write-Step '2. Provisioning PostgreSQL/PostGIS'
$dotenv = Read-DotEnv $EnvFile
$dbHost = if ($dotenv['POSTGRES_HOST']) { $dotenv['POSTGRES_HOST'] } else { 'localhost' }
$dbPort = if ($dotenv['POSTGRES_PORT']) { [int]$dotenv['POSTGRES_PORT'] } else { 5432 }
if ($dotenv['DATABASE_URL'] -and $dotenv['DATABASE_URL'] -match '@([^:/?]+)(?::(\d+))?/') {
    $dbHost = $Matches[1]
    if ($Matches[2]) { $dbPort = [int]$Matches[2] } else { $dbPort = 5432 }
}
$hasCredentials = [bool]($dotenv['DATABASE_URL'] -or $dotenv['POSTGRES_PASSWORD'])
$hasLocalCluster = Test-Path (Join-Path $LocalPgData 'PG_VERSION')

if ($hasCredentials) {
    if ($hasLocalCluster -and -not (Test-TcpPort $dbHost $dbPort)) {
        # Our private cluster from a previous run (it does not auto-start after a reboot).
        Invoke-GeoEnv pg_ctl -D $LocalPgData -l (Join-Path $LocalPgData 'server.log') -o "-p $dbPort" -w start
    }
    if (-not (Test-TcpPort $dbHost $dbPort)) {
        Stop-Setup ".env points at $dbHost`:$dbPort but nothing is listening there. Start that PostgreSQL server, or remove its settings from .env to let setup create a private one."
    }
    Write-Host "Reusing the database configured in .env ($dbHost`:$dbPort)."
} else {
    $dbName = 'livability'
    $dbUser = 'livability'
    $dbPassword = New-RandomPassword
    $dbPort = 5432
    while (Test-TcpPort 'localhost' $dbPort) { $dbPort++ }
    Write-Host "Creating a private PostgreSQL cluster at $LocalPgData on port $dbPort."
    New-Item -ItemType Directory -Force -Path (Split-Path $LocalPgData) | Out-Null
    $pwFile = [System.IO.Path]::GetTempFileName()
    try {
        [System.IO.File]::WriteAllText($pwFile, $dbPassword)
        Invoke-GeoEnv initdb -D $LocalPgData -U $dbUser --pwfile=$pwFile -A scram-sha-256 -E UTF8
    } finally {
        Remove-Item -LiteralPath $pwFile -Force -ErrorAction SilentlyContinue
    }
    Invoke-GeoEnv pg_ctl -D $LocalPgData -l (Join-Path $LocalPgData 'server.log') -o "-p $dbPort" -w start
    $env:PGPASSWORD = $dbPassword
    Invoke-GeoEnv createdb -h localhost -p $dbPort -U $dbUser $dbName
    Invoke-GeoEnv psql -h localhost -p $dbPort -U $dbUser -d $dbName -v ON_ERROR_STOP=1 -c "CREATE EXTENSION IF NOT EXISTS postgis; CREATE EXTENSION IF NOT EXISTS pgcrypto;"
    Remove-Item Env:\PGPASSWORD
    # Never overwrite an existing .env: append the database block instead.
    Add-Content -LiteralPath $EnvFile -Encoding ascii -Value @(
        '# PostgreSQL settings written by setup.ps1. Keep this file private.',
        'POSTGRES_HOST=localhost',
        "POSTGRES_PORT=$dbPort",
        "POSTGRES_DB=$dbName",
        "POSTGRES_USER=$dbUser",
        "POSTGRES_PASSWORD=$dbPassword"
    )
    Write-Host 'Saved the private cluster settings to .env. After a reboot, rerun setup.cmd (or pg_ctl start) to start it.'
}

# ---------------------------------------------------------------------------
Write-Step '3. Downloading and validating local datasets'
if ($RefreshData) {
    Invoke-GeoEnv python scripts\fetch_datasets.py --refresh
} else {
    Invoke-GeoEnv python scripts\fetch_datasets.py
}

Write-Step '4. Building Rust walkgraph'
& cargo build --release --manifest-path walkgraph\Cargo.toml
if ($LASTEXITCODE -ne 0) { Stop-Setup 'cargo build failed.' }
$dotenv = Read-DotEnv $EnvFile
if (-not $dotenv['WALKGRAPH_BIN']) {
    Add-Content -LiteralPath $EnvFile -Value ("WALKGRAPH_BIN=" + (Join-Path $ProjectRoot 'walkgraph\target\release\walkgraph.exe')) -Encoding ascii
}

Write-Step '5. Running database migrations'
Invoke-GeoEnv python -m alembic upgrade head

Write-Step '6. Building frontend bundle'
# npm is a .cmd shim: `call` makes geo_env.cmd regain control after it exits.
Invoke-GeoEnv call npm ci --prefix frontend
Invoke-GeoEnv call npm run build --prefix frontend

if ($SkipBuild) {
    Write-Step 'Setup complete (pipeline skipped: -SkipBuild)'
    Write-Host 'Build the map later by running setup.cmd again (finished steps are reused).'
    exit 0
}

# ---------------------------------------------------------------------------
Write-Step '7. Starting Docker Desktop for the self-hosted basemap'
if (-not (Test-DockerEngine)) {
    $dockerDesktop = Join-Path $env:ProgramFiles 'Docker\Docker\Docker Desktop.exe'
    if (Test-Path $dockerDesktop) { Start-Process $dockerDesktop }
    $deadline = (Get-Date).AddMinutes(5)
    $dockerReady = $false
    while (-not $dockerReady -and (Get-Date) -lt $deadline) {
        Start-Sleep -Seconds 5
        $dockerReady = Test-DockerEngine
    }
    if (-not $dockerReady) {
        Stop-Setup "Docker Desktop's engine did not start. Finish Docker Desktop's first-run setup (it may need WSL 2 and a reboot), then rerun setup.cmd."
    }
}

Write-Step '8. Importing OSM and building the self-hosted basemap'
Invoke-GeoEnv python main.py import

Write-Step '9. Building GTFS transit reality'
Invoke-GeoEnv python main.py transit

Write-Step '10. Precomputing the full map (accurate noise overlay)'
Invoke-GeoEnv python main.py precompute --profile full

Write-Step '11. Sanity-checking the published build'
Invoke-GeoEnv python scripts\sanity_check.py --profile full

Write-Step 'Setup complete'
Write-Host 'Start the map with:  scripts\win\geo_env.cmd python main.py serve --deployment'
Write-Host 'Then open http://127.0.0.1:8000'
