@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
set "GEO_WRAPPER=%SCRIPT_DIR%geo_env.cmd"

call "%SCRIPT_DIR%check_geo_env.cmd"
if errorlevel 1 exit /b %errorlevel%

call "%GEO_WRAPPER%" python -c "import shutil; p=shutil.which('ogr2ogr'); assert p, 'ogr2ogr missing'; print(p)"
if errorlevel 1 exit /b %errorlevel%

call "%GEO_WRAPPER%" python -c "import os; print(os.environ.get('GDAL_DRIVER_PATH')); print(os.environ.get('PROJ_LIB') or os.environ.get('PROJ_DATA'))"
if errorlevel 1 exit /b %errorlevel%

call "%GEO_WRAPPER%" cmd /c "ogrinfo --formats | findstr /I \"PostgreSQL\""
if errorlevel 1 exit /b %errorlevel%

call "%GEO_WRAPPER%" python -c "import sqlalchemy, psycopg; print('sqlalchemy=' + sqlalchemy.__version__); print('psycopg=' + psycopg.__version__)"
exit /b %errorlevel%
