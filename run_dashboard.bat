@echo off
setlocal
cd /d "%~dp0"
echo Starting QBA Dashboard...
:: This will run the dashboard and open the browser automatically
python app.py
pause
