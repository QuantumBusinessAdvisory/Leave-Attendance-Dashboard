# Root entry point for QBA Dashboard (Local & Azure)
import os
import sys
import importlib.util

# 1. Setup paths to avoid module name collisions
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DASHBOARD_DIR = os.path.join(BASE_DIR, "Frontend", "ShinyApps")

# 2. Robustly load the actual app.py from the subfolder
# We use importlib to avoid "import app" colliding with the root app.py filename
app_path = os.path.join(DASHBOARD_DIR, "app.py")
spec = importlib.util.spec_from_file_location("dashboard_app", app_path)
module = importlib.util.module_from_spec(spec)
sys.modules["dashboard_app"] = module
spec.loader.exec_module(module)

# The 'app' object required by Gunicorn/Uvicorn
app = module.app

# Local runner for "Double-Click" experience
if __name__ == "__main__":
    import uvicorn
    import webbrowser
    import threading
    import time
    
    URL = "http://127.0.0.1:8000"
    
    def open_browser():
        # Small delay to ensure server is starting
        time.sleep(2)
        print(f"Opening browser at {URL}...")
        try:
            webbrowser.open(URL)
        except Exception as e:
            print(f"[INFO] Manual navigation required: {URL}")

    # Only open browser on Windows local
    if os.name == 'nt':
        threading.Thread(target=open_browser, daemon=True).start()
        
    print(f"Starting QBA Dashboard at {URL}")
    uvicorn.run(app, host="127.0.0.1", port=8000)
