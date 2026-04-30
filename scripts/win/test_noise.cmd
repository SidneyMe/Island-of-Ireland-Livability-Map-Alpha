@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
set "GEO_ENV_SHOW_SUMMARY=1"
call "%SCRIPT_DIR%geo_env.cmd" .\.venv\Scripts\python.exe -m unittest tests.test_noise_ogr_ingest tests.test_noise_artifacts tests.test_noise_loader
exit /b %errorlevel%
