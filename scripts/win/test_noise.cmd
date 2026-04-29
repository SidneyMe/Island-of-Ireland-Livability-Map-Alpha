@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
call "%SCRIPT_DIR%geo_env.cmd" python -m unittest tests.test_noise_ogr_ingest tests.test_noise_artifacts tests.test_noise_loader
exit /b %errorlevel%
