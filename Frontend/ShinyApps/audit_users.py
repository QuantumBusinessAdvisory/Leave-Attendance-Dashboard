import pandas as pd
import os

APP_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.normpath(os.path.join(APP_DIR, "..", "..", "Backend", "data", "processed"))

# Load and Filter Active Users
ud = pd.read_parquet(os.path.join(DATA_DIR, 'users_details.parquet'))
active = ud[ud['employee_status'] == 'Active'].copy()
print(f"Total Active: {len(active)}")
print(f"Missing Name: {active['employee_name'].isna().sum()}")
print(f"Empty Name: {(active['employee_name'] == '').sum()}")
print(f"Missing User ID: {active['user_id'].isna().sum()}")

# Check for duplicates
print(f"Unique User IDs: {active['user_id'].nunique()}")
print(f"Unique Employee Names: {active['employee_name'].nunique()}")

# If there are duplicates in user_id, that might be an issue.
if active['user_id'].nunique() < len(active):
    print("\nDuplicate User IDs found:")
    print(active[active.duplicated('user_id', keep=False)][['user_id', 'employee_name', 'email']])
