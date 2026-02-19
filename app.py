# Root entry point for QBA Dashboard (Local & Azure)
# Runs Backend ETL (data refresh) first, then serves the Frontend dashboard.
# Works with both Gunicorn (Azure/Linux) and Uvicorn (Windows local).
import os
import sys
import importlib.util

# ====================================================
#   PATHS
# ====================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BACKEND_DIR = os.path.join(BASE_DIR, "Backend")
DASHBOARD_DIR = os.path.join(BASE_DIR, "Frontend", "ShinyApps")

# ====================================================
#   PHASE 1: BACKEND ETL (Data Refresh)
# ====================================================
# Add Backend to sys.path so its modules resolve correctly
sys.path.insert(0, BACKEND_DIR)

try:
    from src.extract import extract_data
    from src.transform import transform_data
    import Config as config

    print("=" * 60)
    print("  PHASE 1: Data Extraction (Fetching from HRMS APIs)")
    print("=" * 60)
    for name, url in config.API_ENDPOINTS.items():
        extract_data(name, url, config.API_HEADERS)

    print("\n" + "=" * 60)
    print("  PHASE 2: Data Transformation")
    print("=" * 60)
    transform_data()
    print("[SUCCESS] ETL Pipeline completed.\n")

except Exception as e:
    # If ETL fails, log the error but don't crash — 
    # the dashboard can still serve if data from a prior run exists.
    print(f"\n[WARNING] ETL Pipeline failed: {e}")
    print("[WARNING] Dashboard will attempt to load with existing data.\n")

# ====================================================
#   PHASE 3: FRONTEND DASHBOARD
# ====================================================
print("=" * 60)
print("  PHASE 3: Loading Dashboard")
print("=" * 60)

# Robustly load the actual app.py from the subfolder
# We use importlib to avoid "import app" colliding with the root app.py filename
app_path = os.path.join(DASHBOARD_DIR, "app.py")
spec = importlib.util.spec_from_file_location("dashboard_app", app_path)
module = importlib.util.module_from_spec(spec)
sys.modules["dashboard_app"] = module
spec.loader.exec_module(module)

# The 'app' object required by Gunicorn/Uvicorn
app = module.app

# ====================================================
#   LOCAL RUNNER (Windows: python app.py)
# ====================================================
if __name__ == "__main__":
    import uvicorn
    import webbrowser
    import threading
    import time

    HOST = "127.0.0.1"
    PORT = 8000
    URL = f"http://{HOST}:{PORT}"

    def open_browser():
        time.sleep(2)
        print(f"Opening browser at {URL}...")
        try:
            webbrowser.open(URL)
        except Exception:
            print(f"[INFO] Manual navigation required: {URL}")

    # Only auto-open browser on Windows
    if os.name == 'nt':
        threading.Thread(target=open_browser, daemon=True).start()

    print(f"\nStarting QBA Dashboard at {URL}")
    print(f"Press Ctrl+C to stop.\n")
    uvicorn.run(app, host=HOST, port=PORT)
