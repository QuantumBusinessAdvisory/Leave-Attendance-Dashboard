@echo off
setlocal
title QBA Leave & Attendance Dashboard

:: 1. Anchor to the launcher's folder
cd /d "%~dp0"

echo ===================================================
echo   DASHBOARD LIVE SYNC ACTIVE
echo ===================================================

:: 2. Port and Process cleanup
echo [1/2] Resetting environment...
taskkill /F /IM python.exe /T >nul 2>&1
taskkill /F /IM uvicorn.exe /T >nul 2>&1

:: 3. Run Shiny via Python Launcher (Handles browser open & validation)
cd ShinyApps
python launch.py

if %ERRORLEVEL% NEQ 0 (
    echo.
    echo [CRITICAL ERROR] The dashboard stopped with error code %ERRORLEVEL%.
)

echo.
echo Application ended.
pause
