import requests
import json
import os
from datetime import datetime


def extract_data(endpoint_name, url, headers):
    """
    Fetches data from the API and saves the raw JSON to data/raw/
    """
    print(f"[{datetime.now()}] Fetching {endpoint_name}...")
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        data = response.json()
        
        # Create filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"data/raw/{endpoint_name}_{timestamp}.json"
        
        # Ensure directory exists (just in case)
        os.makedirs("data/raw", exist_ok=True)
        
        with open(filename, 'w') as f:
            json.dump(data, f, indent=4)
            
        print(f"SUCCESS: Saved raw data to {filename}")
        return filename
        
    except requests.exceptions.RequestException as e:
        print(f"ERROR fetching {endpoint_name}: {e}")
        return None

if __name__ == "__main__":
    # Test run for all configured endpoints
    for name, url in config.API_ENDPOINTS.items():
        extract_data(name, url)
