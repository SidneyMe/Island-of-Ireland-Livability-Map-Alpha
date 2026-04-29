@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
set "GEO_WRAPPER=%SCRIPT_DIR%geo_env.cmd"

if not exist "%GEO_WRAPPER%" (
  echo [check_geo_env] ERROR: Missing wrapper "%GEO_WRAPPER%"
  exit /b 1
)

echo [check_geo_env] Python/Conda/GDAL environment:
call "%GEO_WRAPPER%" python -c "import os,sys,shutil; print('python_exe=' + sys.executable); print('conda_prefix=' + os.getenv('CONDA_PREFIX','')); print('ogr2ogr_path=' + str(shutil.which('ogr2ogr'))); print('GDAL_DRIVER_PATH=' + os.getenv('GDAL_DRIVER_PATH','')); print('GDAL_DATA=' + os.getenv('GDAL_DATA','')); print('PROJ_DATA=' + os.getenv('PROJ_DATA',''))"
if errorlevel 1 exit /b %errorlevel%

echo [check_geo_env] ogr2ogr --version:
call "%GEO_WRAPPER%" ogr2ogr --version
if errorlevel 1 exit /b %errorlevel%

echo [check_geo_env] ogrinfo driver check:
call "%GEO_WRAPPER%" cmd /c "ogrinfo --formats | findstr /I \"PostgreSQL PGDUMP FileGDB OpenFileGDB\""
exit /b %errorlevel%
