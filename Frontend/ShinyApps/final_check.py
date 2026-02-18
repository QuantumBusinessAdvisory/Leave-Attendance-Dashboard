import pandas as pd
import os

APP_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.normpath(os.path.join(APP_DIR, "..", "..", "Backend", "data", "processed"))

# Load and Filter Active Users
ud = pd.read_parquet(os.path.join(DATA_DIR, 'users_details.parquet'))
active_users = ud[ud['employee_status'] == 'Active']['user_id'].unique()

# Load Attendance
att = pd.read_parquet(os.path.join(DATA_DIR, 'attendance.parquet'))
att = att[att['user_id'].isin(active_users)]
att['dt'] = pd.to_datetime(att['attendance_date'])

# Q4 2025
q4 = att[(att['dt'].dt.year == 2025) & (att['dt'].dt.month.isin([10, 11, 12]))].copy()

# 1. Office Hours Distribution (WFO records only)
q4_wfo = q4[q4['presence_type'] == 'Work From Office'].copy()
def get_hrs_bucket(h):
    if pd.isna(h): return None
    if h < 3: return "< 3 hours"
    if h < 6: return "3-6 hours"
    return "6+ hours"
q4_wfo['Bucket'] = q4_wfo['working_hours'].apply(get_hrs_bucket)
res_hrs = q4_wfo.groupby('Bucket')['employee_name'].nunique()

print("Q4 2025 Office Hours Distribution (Distinct Count of Employees per Bucket):")
print(res_hrs)

# 2. WFH Compliance (Per Employee Per Month)
q4['Month_Year'] = q4['dt'].dt.strftime('%b %Y')
wfh_counts = q4.groupby(['Month_Year', 'employee_name']).apply(
    lambda g: (g['presence_type'] == 'Work From Home').sum()
).reset_index(name='WFH_Days')
wfh_counts['WFH_Bucket'] = wfh_counts['WFH_Days'].apply(lambda x: "WFH > 9" if x > 9 else "WFH <= 9")
res_wfh = wfh_counts.groupby(['Month_Year', 'WFH_Bucket']).size().unstack(fill_value=0)

print("\nQ4 2025 WFH Compliance (Employees per Bucket per Month):")
print(res_wfh)
