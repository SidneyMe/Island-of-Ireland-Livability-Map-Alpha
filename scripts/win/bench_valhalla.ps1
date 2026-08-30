param(
    [int]$Workers = 8,
    [int]$Requests = 1000,
    [int]$Targets = 5,
    [int]$Warmup = 20,
    [string]$Url = "http://127.0.0.1:8002/sources_to_targets",
    [string]$LocateUrl = "http://127.0.0.1:8002/locate"
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = (Resolve-Path (Join-Path $scriptDir "..\\..")).Path
$pythonExe = Join-Path $repoRoot ".venv\\Scripts\\python.exe"

if (-not (Test-Path $pythonExe)) {
    $pythonExe = "python"
}

& $pythonExe (Join-Path $repoRoot "scripts\\bench_valhalla.py") `
    --workers $Workers `
    --requests $Requests `
    --targets $Targets `
    --warmup $Warmup `
    --url $Url `
    --locate-url $LocateUrl
