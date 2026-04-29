@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
call "%SCRIPT_DIR%geo_env.cmd" python main.py --precompute-dev --refresh-noise-artifact --reimport-noise-source --force-noise-artifact --force-precompute
exit /b %errorlevel%
