import pandas as pd
import os

data_dir = r"..\..\Backend\data\processed"
ud_path = os.path.join(data_dir, "users_details.parquet")
la_path = os.path.join(data_dir, "leave_applications.parquet")

if os.path.exists(ud_path) and os.path.exists(la_path):
    ud = pd.read_parquet(ud_path)
    la = pd.read_parquet(la_path)
    
    # Names to check
    names = ["Harshal Bhagwat", "Md Noorullah", "Mohammed Belkheiri", "Soumyarup Debnath", "Prarthana Majumdar"]
    
    print("--- User Status Check ---")
    for name in names:
        user = ud[ud['employee_name'].str.contains(name, case=False, na=False)]
        if not user.empty:
            print(f"{name}: {user[['employee_name', 'employee_status']].values.tolist()}")
        else:
            print(f"{name}: NOT FOUND in users_details")
            
    print("\n--- Employee Status Unique Values ---")
    if 'employee_status' in ud.columns:
        print(ud['employee_status'].value_counts())
    else:
        print("COL 'employee_status' NOT FOUND")

else:
    print("Files not found")
