import pandas as pd
import os

APP_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.normpath(os.path.join(APP_DIR, "..", "..", "Backend", "data", "processed"))

# Load and Filter Active Users
ud = pd.read_parquet(os.path.join(DATA_DIR, 'users_details.parquet'))
active_users = ud[ud['employee_status'] == 'Active']['user_id'].unique()

# Load Attendance
att = pd.read_parquet(os.path.join(DATA_DIR, 'attendance.parquet'))
att = att[att['user_id'].isin(active_users)].copy()
att['dt'] = pd.to_datetime(att['attendance_date'])
att['Month_Year'] = att['dt'].dt.strftime('%b %Y')

# Q4 2025
q4 = att[(att['dt'].dt.year == 2025) & (att['dt'].dt.month.isin([10, 11, 12]))].copy()

# Calculate WFH Days per Employee per Month using UNIQUE days
def calc_wfh_unique(group):
    wfh_days = group[group['presence_type'] == 'Work From Home']['dt'].nunique()
    return pd.Series({'WFH_Days': wfh_days})

wfh_counts = q4.groupby(['Month_Year', 'employee_name']).apply(calc_wfh_unique).reset_index()
wfh_counts['WFH_Bucket'] = wfh_counts['WFH_Days'].apply(lambda x: "WFH > 9" if x > 9 else "WFH <= 9")

res = wfh_counts.groupby(['Month_Year', 'WFH_Bucket']).size().unstack(fill_value=0)

print("Q4 2025 WFH Compliance (Unique Days per Employee per Month):")
print(res)
