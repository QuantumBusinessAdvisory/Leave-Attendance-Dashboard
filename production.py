# Production entry point for Azure Hosting
import os
import sys
import importlib.util

# 1. Setup paths to avoid module name collisions
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DASHBOARD_DIR = os.path.join(BASE_DIR, "Frontend", "ShinyApps")

# Add subfolder to path (at the end to prefer explicit loading)
if DASHBOARD_DIR not in sys.path:
    sys.path.append(DASHBOARD_DIR)

# 2. Robustly load the actual app.py from the subfolder
# This avoids the error "App.__call__() missing 1 required positional argument: 'send'"
# by ensuring we are importing the correct Shiny App instance.
app_path = os.path.join(DASHBOARD_DIR, "app.py")
spec = importlib.util.spec_from_file_location("dashboard_app", app_path)
module = importlib.util.module_from_spec(spec)
sys.modules["dashboard_app"] = module
spec.loader.exec_module(module)

# The 'app' object required by Gunicorn/Uvicorn
app = module.app

# Local testing
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
