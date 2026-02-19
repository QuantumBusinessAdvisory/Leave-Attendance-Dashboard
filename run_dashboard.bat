@echo off
title QBA Dashboard Launcher
echo Cleaning up existing processes...
taskkill /F /IM python.exe /T >nul 2>&1
taskkill /F /IM uvicorn.exe /T >nul 2>&1

echo Starting Dashboard...
cd Frontend\ShinyApps
python launch.py
pause
