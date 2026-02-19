# Root level proxy for Azure App Service / Hosting
import os
import sys

# Define the path to the actual dashboard folder
DASHBOARD_DIR = os.path.join(os.path.dirname(__file__), "Frontend", "ShinyApps")

# Add the dashboard directory to the python path
if DASHBOARD_DIR not in sys.path:
    sys.path.append(DASHBOARD_DIR)

# Import the dashboard app using its module name directly
# This avoids relative/package import issues on some hosting environments
import app as dashboard_module
app = dashboard_module.app

# Local runner (for testing)
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
