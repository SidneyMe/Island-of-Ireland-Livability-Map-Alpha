@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
set "GEO_ENV_SHOW_SUMMARY=1"
if not defined NOISE_PRECOMPUTE_WATCHDOG_TIMEOUT_SEC set "NOISE_PRECOMPUTE_WATCHDOG_TIMEOUT_SEC=1200"

powershell -NoProfile -ExecutionPolicy Bypass -File "%SCRIPT_DIR%run_noise_precompute_watchdog.ps1" -Mode DevReuse
exit /b %errorlevel%
