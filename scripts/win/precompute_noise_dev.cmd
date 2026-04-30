@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
set "GEO_ENV_SHOW_SUMMARY=1"
call "%SCRIPT_DIR%geo_env.cmd" .\.venv\Scripts\python.exe main.py --precompute-dev --refresh-noise-artifact --reimport-noise-source --force-noise-artifact --force-precompute
exit /b %errorlevel%
