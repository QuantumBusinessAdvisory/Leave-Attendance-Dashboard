import requests
import json
import os
from datetime import datetime

# Path relative to Backend directory
BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_RAW_DIR = os.path.join(BACKEND_DIR, "data", "raw")

def extract_data(endpoint_name, url, headers):
    print(f"[{datetime.now()}] Fetching {endpoint_name}...")
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        os.makedirs(DATA_RAW_DIR, exist_ok=True)
        filename = os.path.join(DATA_RAW_DIR, f"{endpoint_name}_{timestamp}.json")
        with open(filename, 'w') as f:
            json.dump(data, f, indent=4)
        print(f"SUCCESS: Saved raw data to {filename}")
        return filename
    except Exception as e:
        print(f"ERROR fetching {endpoint_name}: {e}")
        return None
