@echo off
cd /d "%~dp0"
echo Killing existing processes on port 8000 and Python...
for /f "tokens=5" %%a in ('netstat -aon ^| findstr :8000 ^| findstr LISTENING') do taskkill /f /pid %%a 2>nul
taskkill /F /IM python.exe /T 2>nul
python launch.py
pause
