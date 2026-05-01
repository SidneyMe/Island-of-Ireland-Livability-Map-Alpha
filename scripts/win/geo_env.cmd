@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%..\..") do set "PROJECT_ROOT=%%~fI"

if not defined MINIFORGE_ROOT (
  set "MINIFORGE_ROOT=%USERPROFILE%\miniforge3"
)
if not defined GEO_CONDA_ENV (
  set "GEO_CONDA_ENV=livability-gdal"
)

if not exist "%MINIFORGE_ROOT%\Scripts\activate.bat" (
  echo [geo_env] ERROR: Missing conda activate script: "%MINIFORGE_ROOT%\Scripts\activate.bat"
  echo [geo_env] Set MINIFORGE_ROOT to your Miniforge install path.
  exit /b 1
)

call "%MINIFORGE_ROOT%\Scripts\activate.bat" "%GEO_CONDA_ENV%"
if errorlevel 1 (
  echo [geo_env] ERROR: Failed to activate conda env "%GEO_CONDA_ENV%".
  echo [geo_env] Conda env not found: %GEO_CONDA_ENV%
  echo [geo_env] Run: scripts\win\bootstrap_geo_env.cmd
  echo [geo_env] Or use existing base env: set GEO_CONDA_ENV=base
  exit /b 1
)

if not defined CONDA_PREFIX (
  echo [geo_env] ERROR: CONDA_PREFIX is not set after conda activation.
  exit /b 1
)

set "PATH=%CONDA_PREFIX%\Library\bin;%CONDA_PREFIX%\Scripts;%CONDA_PREFIX%;%PATH%"
set "CONDAGDALDRIVER=%CONDA_PREFIX%\Library\lib\gdalplugins"
set "CONDAGDALDATA=%CONDA_PREFIX%\Library\share\gdal"
set "CONDAPROJDATA=%CONDA_PREFIX%\Library\share\proj"

echo(%GDAL_DATA%| findstr /I /C:"\Program Files\PostgreSQL\" >nul
if not errorlevel 1 (
  echo [geo_env] WARNING: Detected PostgreSQL GDAL_DATA pollution, overriding with conda path.
)
echo(%PROJ_LIB%| findstr /I /C:"\Program Files\PostgreSQL\" >nul
if not errorlevel 1 (
  echo [geo_env] WARNING: Detected PostgreSQL PROJ_LIB pollution, overriding with conda path.
)
echo(%PROJ_DATA%| findstr /I /C:"\Program Files\PostgreSQL\" >nul
if not errorlevel 1 (
  echo [geo_env] WARNING: Detected PostgreSQL PROJ_DATA pollution, overriding with conda path.
)

set "GDAL_DRIVER_PATH=%CONDAGDALDRIVER%"
set "GDAL_DATA=%CONDAGDALDATA%"
set "PROJ_DATA=%CONDAPROJDATA%"
set "PROJ_LIB=%CONDAPROJDATA%"

set "NOISE_INGEST_MODE=ogr2ogr"
set "NOISE_ROAD_GDB_CANONICAL_CACHE=1"
set "NOISE_REBUILD_ROAD_GDB_CACHE=0"
set "NOISE_MIN_FREE_DISK_GB=30"

if not defined NOISE_OGR2OGR_GDB_WORKERS set "NOISE_OGR2OGR_GDB_WORKERS=2"
if not defined NOISE_OGR2OGR_GDB_CHUNK_SIZE set "NOISE_OGR2OGR_GDB_CHUNK_SIZE=25"
if not defined NOISE_OGR2OGR_HARD_TIMEOUT_SECONDS set "NOISE_OGR2OGR_HARD_TIMEOUT_SECONDS=900"
if not defined NOISE_OGR2OGR_NO_PROGRESS_TIMEOUT_SECONDS set "NOISE_OGR2OGR_NO_PROGRESS_TIMEOUT_SECONDS=180"
if not defined NOISE_OGR2OGR_TIMEOUT_SECONDS set "NOISE_OGR2OGR_TIMEOUT_SECONDS=300"
if not defined NOISE_OGR2OGR_IDLE_TIMEOUT_SEC set "NOISE_OGR2OGR_IDLE_TIMEOUT_SEC=600"
if not defined NOISE_OGR2OGR_TOTAL_TIMEOUT_SEC set "NOISE_OGR2OGR_TOTAL_TIMEOUT_SEC=1800"
if not defined NOISE_OGR2OGR_ROAD_CHUNK_TIMEOUT_SEC set "NOISE_OGR2OGR_ROAD_CHUNK_TIMEOUT_SEC=1200"

cd /d "%PROJECT_ROOT%"

if /I "%GEO_ENV_SHOW_SUMMARY%"=="1" (
  echo [geo_env] GEO_CONDA_ENV=%GEO_CONDA_ENV%
  echo [geo_env] CONDA_PREFIX=%CONDA_PREFIX%
  echo [geo_env] GDAL_DRIVER_PATH=%GDAL_DRIVER_PATH%
  echo [geo_env] GDAL_DATA=%GDAL_DATA%
  echo [geo_env] PROJ_DATA=%PROJ_DATA%
  echo [geo_env] PROJ_LIB=%PROJ_LIB%
)

if "%~1"=="" (
  echo [geo_env] ERROR: No command supplied.
  echo [geo_env] Usage: scripts\win\geo_env.cmd ^<command^> [args...]
  exit /b 2
)

%*
set "CMD_EXIT=%ERRORLEVEL%"
endlocal & exit /b %CMD_EXIT%
