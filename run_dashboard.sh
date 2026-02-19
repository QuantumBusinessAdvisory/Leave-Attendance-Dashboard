#!/bin/bash
# Description: Linux launcher for the QBA Dashboard.

# Navigate to project root
cd "$(dirname "$0")"

echo "Cleaning up existing processes..."

# Kill processes on port 8000
if command -v fuser &>/dev/null; then
    fuser -k 8000/tcp 2>/dev/null
elif command -v lsof &>/dev/null; then
    lsof -ti:8000 | xargs kill -9 2>/dev/null
fi

echo "Starting Dashboard..."
cd Frontend/ShinyApps
if command -v python3 &>/dev/null; then
    python3 launch.py
else
    python launch.py
fi
