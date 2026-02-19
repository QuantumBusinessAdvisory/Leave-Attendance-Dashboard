#!/bin/bash
# Description: Linux shell script to run the Shiny application.

# Navigate to the script's directory
cd "$(dirname "$0")"

echo "Cleaning up port 8000 and existing python processes..."

# Kill process listening on port 8000 (Linux equivalent of netstat/taskkill)
if command -v fuser &>/dev/null; then
    fuser -k 8000/tcp 2>/dev/null
elif command -v lsof &>/dev/null; then
    lsof -ti:8000 | xargs kill -9 2>/dev/null
fi

# Kill other python processes if needed (be careful not to kill itself)
# pkill -f "python3 launch.py" 2>/dev/null

echo "Starting launch.py..."

# Check if python3 is available, fallback to python
if command -v python3 &>/dev/null; then
    PYTHON_CMD=python3
else
    PYTHON_CMD=python
fi

$PYTHON_CMD launch.py

# Keep terminal open if run interactively
if [ -t 0 ]; then
    read -p "Press enter to continue..."
fi
