@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
set "GEO_WRAPPER=%SCRIPT_DIR%geo_env.cmd"
if not defined GEO_CONDA_ENV set "GEO_CONDA_ENV=livability-gdal"

if not exist "%GEO_WRAPPER%" (
  echo [check_geo_env] ERROR: Missing wrapper "%GEO_WRAPPER%"
  exit /b 1
)

echo [check_geo_env] GEO_CONDA_ENV=%GEO_CONDA_ENV%
echo [check_geo_env] Conda/GDAL environment summary:
call "%GEO_WRAPPER%" python -c "import os,sys,shutil; print('CONDA_PREFIX=' + os.getenv('CONDA_PREFIX','')); print('python_executable=' + sys.executable); print('python_version=' + sys.version.split()[0]); print('ogr2ogr_path=' + str(shutil.which('ogr2ogr'))); print('GDAL_DRIVER_PATH=' + os.getenv('GDAL_DRIVER_PATH','')); print('GDAL_DATA=' + os.getenv('GDAL_DATA','')); print('PROJ_DATA=' + os.getenv('PROJ_DATA','')); print('PROJ_LIB=' + os.getenv('PROJ_LIB',''))"
if errorlevel 1 (
  echo [check_geo_env] ERROR: Failed to initialize geo environment.
  echo [check_geo_env] If env is missing, run: scripts\win\bootstrap_geo_env.cmd
  exit /b %errorlevel%
)

echo [check_geo_env] ogr2ogr location:
call "%GEO_WRAPPER%" where ogr2ogr
if errorlevel 1 exit /b %errorlevel%

echo [check_geo_env] ogr2ogr --version:
call "%GEO_WRAPPER%" ogr2ogr --version
if errorlevel 1 exit /b %errorlevel%

echo [check_geo_env] GDAL PostgreSQL driver check:
call "%GEO_WRAPPER%" cmd /d /s /c "ogrinfo --formats | findstr /I PostgreSQL"
if errorlevel 1 (
  echo [check_geo_env] ERROR: PostgreSQL GDAL driver is missing.
  echo [check_geo_env] Fix:
  echo [check_geo_env]   mamba install -n %GEO_CONDA_ENV% -c conda-forge libgdal-pg
  exit /b 1
)

call "%GEO_WRAPPER%" cmd /d /s /c "ogrinfo --formats | findstr /I PostgreSQL/PostGIS >nul"
if errorlevel 1 (
  echo [check_geo_env] ERROR: PostgreSQL/PostGIS GDAL driver signature not found.
  echo [check_geo_env] Expected line contains: PostgreSQL/PostGIS
  echo [check_geo_env] Fix:
  echo [check_geo_env]   mamba install -n %GEO_CONDA_ENV% -c conda-forge libgdal-pg
  exit /b 1
)

echo [check_geo_env] PASS: PostgreSQL/PostGIS GDAL driver is available.
exit /b 0
