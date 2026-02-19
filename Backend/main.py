from src.extract import extract_data
from src.transform import transform_data
import Config as config
import os

def main():
    print("starting ETL Pipeline...")
    for name, url in config.API_ENDPOINTS.items():
        extract_data(name, url, config.API_HEADERS)
    print("\n--- Phase 2: Transformation ---")
    transform_data()

if __name__ == "__main__":
    main()
