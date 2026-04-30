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
  echo [bootstrap_geo_env] ERROR: Missing conda activate script: "%MINIFORGE_ROOT%\Scripts\activate.bat"
  echo [bootstrap_geo_env] Set MINIFORGE_ROOT to your Miniforge install path.
  exit /b 1
)

call "%MINIFORGE_ROOT%\Scripts\activate.bat" base
if errorlevel 1 (
  echo [bootstrap_geo_env] ERROR: Failed to activate conda base environment.
  exit /b 1
)

where mamba >nul 2>nul
if errorlevel 1 (
  echo [bootstrap_geo_env] ERROR: mamba is not available in base environment.
  echo [bootstrap_geo_env] Install mamba in base:
  echo [bootstrap_geo_env]   conda install -n base -c conda-forge mamba
  exit /b 1
)

set "ENV_EXISTS="
for /f "tokens=1,2,*" %%A in ('conda env list ^| findstr /R /C:"^[A-Za-z0-9._-][A-Za-z0-9._-]*"') do (
  if /I "%%A"=="%GEO_CONDA_ENV%" set "ENV_EXISTS=1"
  if /I "%%B"=="%GEO_CONDA_ENV%" set "ENV_EXISTS=1"
)

cd /d "%PROJECT_ROOT%"

if defined ENV_EXISTS (
  echo [bootstrap_geo_env] Conda env already exists: %GEO_CONDA_ENV%
) else (
  echo [bootstrap_geo_env] Creating conda env "%GEO_CONDA_ENV%" from environment.yml
  mamba env create -f environment.yml -n "%GEO_CONDA_ENV%"
  if errorlevel 1 (
    echo [bootstrap_geo_env] ERROR: Failed to create conda env from environment.yml
    exit /b 1
  )
)

call "%SCRIPT_DIR%check_geo_env.cmd"
set "CHECK_EXIT=%ERRORLEVEL%"
endlocal & exit /b %CHECK_EXIT%
