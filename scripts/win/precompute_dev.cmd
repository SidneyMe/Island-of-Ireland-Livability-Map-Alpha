@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
set "GEO_ENV_SHOW_SUMMARY=1"

call "%SCRIPT_DIR%geo_env.cmd" python main.py --precompute-dev %*
exit /b %errorlevel%
