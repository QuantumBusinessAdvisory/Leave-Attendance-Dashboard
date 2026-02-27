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
#   PHASE 1 & 2: BACKEND ETL (Data Refresh)
# ====================================================
# Add Backend to sys.path so its modules resolve correctly
sys.path.insert(0, BACKEND_DIR)

def run_etl_if_needed():
    try:
        from src.extract import extract_data
        from src.transform import transform_data
        import Config as config
        from concurrent.futures import ThreadPoolExecutor
        import time

        # 1. Check if ETL is needed (Caching Logic)
        PROCESSED_DATA_DIR = os.path.join(BACKEND_DIR, "data", "processed")
        threshold_seconds = config.ETL_CACHE_THRESHOLD_HOURS * 3600
        
        # We check the 'attendance' file as a proxy for data freshness
        check_file = os.path.join(PROCESSED_DATA_DIR, "attendance.parquet")
        if not os.path.exists(check_file):
            check_file = os.path.join(PROCESSED_DATA_DIR, "attendance.csv")

        if os.path.exists(check_file):
            file_age = time.time() - os.path.getmtime(check_file)
            if file_age < threshold_seconds:
                print("=" * 60)
                print(f" [INFO] Data is fresh (age: {int(file_age/60)} mins). Skipping ETL.")
                print(f" [INFO] Threshold: {config.ETL_CACHE_THRESHOLD_HOURS} hours.")
                print("=" * 60)
                return

        print("=" * 60)
        print(f"  PHASE 1: Parallel Data Extraction (Fetching from HRMS)")
        print("=" * 60)
        start_time = time.time()
        
        # Use ThreadPoolExecutor for parallel API calls
        with ThreadPoolExecutor(max_workers=len(config.API_ENDPOINTS)) as executor:
            futures = [
                executor.submit(extract_data, name, url, config.API_HEADERS)
                for name, url in config.API_ENDPOINTS.items()
            ]
            for future in futures:
                future.result() # Wait for all to finish

        print(f"\n[SUCCESS] Extraction completed in {time.time() - start_time:.2f} seconds.")

        print("\n" + "=" * 60)
        print("  PHASE 2: Data Transformation")
        print("=" * 60)
        start_trans = time.time()
        transform_data()
        print(f"[SUCCESS] Transformation completed in {time.time() - start_trans:.2f} seconds.")
        print(f"[TOTAL ETL TIME] {time.time() - start_time:.2f} seconds.\n")

    except Exception as e:
        print(f"\n[WARNING] ETL Pipeline failed: {e}")
        print("[WARNING] Dashboard will attempt to load with existing data.\n")

# Run ETL before starting the dashboard
run_etl_if_needed()

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
