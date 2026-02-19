@echo off
setlocal
cd /d "%~dp0"
echo [ %DATE% %TIME% ] Data refresh started...
python main.py
if %ERRORLEVEL% EQU 0 (
    echo [ %DATE% %TIME% ] Data refresh successful.
) else (
    echo [ %DATE% %TIME% ] Data refresh failed!
)
pause
