@echo off
rem Double-click entry point for the Windows bootstrap (see setup.ps1).
rem   setup.cmd                 install everything and build the published map
rem   setup.cmd -SkipBuild      install everything, skip the multi-hour pipeline
rem   setup.cmd -RefreshData    re-download OSM and GTFS from official sources
setlocal
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0setup.ps1" %*
set "SETUP_EXIT=%ERRORLEVEL%"
if not "%SETUP_EXIT%"=="0" echo. & echo Setup stopped with exit code %SETUP_EXIT%. Fix the message above and run setup.cmd again.
rem Keep the window open when launched by double-click.
echo(%cmdcmdline% | findstr /I /C:"/c" >nul && pause
endlocal & exit /b %SETUP_EXIT%
