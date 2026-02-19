#!/bin/bash
# Description: Linux shell script to refresh dashboard data.

# Navigate to Backend directory
cd "$(dirname "$0")"

LOG_FILE="refresh.log"
echo "[$(date)] Data refresh started..." >> "$LOG_FILE"

# Check if python3 is available, fallback to python
if command -v python3 &>/dev/null; then
    PYTHON_CMD=python3
else
    PYTHON_CMD=python
fi

# Run the main script
$PYTHON_CMD main.py >> "$LOG_FILE" 2>&1

if [ $? -eq 0 ]; then
    echo "[$(date)] Data refresh successful." >> "$LOG_FILE"
else
    echo "[$(date)] Data refresh failed. Check $LOG_FILE for details." >> "$LOG_FILE"
fi
