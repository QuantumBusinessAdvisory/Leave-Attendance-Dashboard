@echo off
setlocal
set "SCRIPT_DIR=%~dp0"
set "LOG_FILE=%SCRIPT_DIR%refresh.log"

echo ======================================== >> "%LOG_FILE%"
echo Execution Date: %DATE% %TIME% >> "%LOG_FILE%"
echo Starting Dashboard Data Refresh... >> "%LOG_FILE%"

cd /d "%SCRIPT_DIR%"
python main.py >> "%LOG_FILE%" 2>&1

if %ERRORLEVEL% EQU 0 (
    echo [SUCCESS] Refresh completed successfully. >> "%LOG_FILE%"
) else (
    echo [FAILURE] Refresh failed with error code %ERRORLEVEL%. >> "%LOG_FILE%"
)

echo ======================================== >> "%LOG_FILE%"
echo. >> "%LOG_FILE%"

endlocal
