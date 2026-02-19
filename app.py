# Root level proxy for Azure App Service / Hosting
import os
import sys

# Add the dashboard directory to python path so imports work correctly inside app.py
sys.path.append(os.path.join(os.path.dirname(__file__), "Frontend", "ShinyApps"))

# Import the app object from the actual dashboard file
from Frontend.ShinyApps.app import app

# Azure/Gunicorn looks for 'app' in the entry point file
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
