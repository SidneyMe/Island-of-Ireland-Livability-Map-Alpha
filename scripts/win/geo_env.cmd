@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%..\..") do set "PROJECT_ROOT=%%~fI"

if not defined MINIFORGE_ROOT (
  set "MINIFORGE_ROOT=%USERPROFILE%\miniforge3"
)

if not exist "%MINIFORGE_ROOT%\Scripts\activate.bat" (
  echo [geo_env] ERROR: Missing conda activate script: "%MINIFORGE_ROOT%\Scripts\activate.bat"
  echo [geo_env] Set MINIFORGE_ROOT to your Miniforge install path.
  exit /b 1
)

call "%MINIFORGE_ROOT%\Scripts\activate.bat" livability-gdal
if errorlevel 1 (
  echo [geo_env] ERROR: Failed to activate conda env "livability-gdal"
  exit /b 1
)

if not defined CONDA_PREFIX (
  echo [geo_env] ERROR: CONDA_PREFIX is not set after conda activation.
  exit /b 1
)

set "PATH=%CONDA_PREFIX%\Library\bin;%CONDA_PREFIX%\Scripts;%CONDA_PREFIX%;%PATH%"
set "GDAL_DRIVER_PATH=%CONDA_PREFIX%\Library\lib\gdalplugins"
set "GDAL_DATA=%CONDA_PREFIX%\Library\share\gdal"
set "PROJ_DATA=%CONDA_PREFIX%\Library\share\proj"
set "PROJ_LIB=%CONDA_PREFIX%\Library\share\proj"

set "NOISE_INGEST_MODE=ogr2ogr"
set "NOISE_ROAD_GDB_CANONICAL_CACHE=1"
set "NOISE_REBUILD_ROAD_GDB_CACHE=0"
set "NOISE_MIN_FREE_DISK_GB=30"

if not defined NOISE_OGR2OGR_GDB_WORKERS set "NOISE_OGR2OGR_GDB_WORKERS=1"
if not defined NOISE_OGR2OGR_GDB_CHUNK_SIZE set "NOISE_OGR2OGR_GDB_CHUNK_SIZE=25"
if not defined NOISE_OGR2OGR_TIMEOUT_SECONDS set "NOISE_OGR2OGR_TIMEOUT_SECONDS=300"

cd /d "%PROJECT_ROOT%"

if "%~1"=="" (
  echo [geo_env] ERROR: No command supplied.
  echo [geo_env] Usage: scripts\win\geo_env.cmd ^<command^> [args...]
  exit /b 2
)

cmd /c %*
set "CMD_EXIT=%ERRORLEVEL%"
endlocal & exit /b %CMD_EXIT%
