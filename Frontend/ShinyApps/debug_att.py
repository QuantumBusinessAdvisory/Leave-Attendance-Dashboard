import pandas as pd
import os

APP_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.normpath(os.path.join(APP_DIR, "..", "..", "Backend", "data", "processed"))

# Load and Filter Active Users
ud = pd.read_parquet(os.path.join(DATA_DIR, 'users_details.parquet'))
active_users = ud[ud['employee_status'] == 'Active']['user_id'].unique()
print(f"Active Users: {len(active_users)}")

# Load Attendance
att = pd.read_parquet(os.path.join(DATA_DIR, 'attendance.parquet'))
att = att[att['user_id'].isin(active_users)]
att['dt'] = pd.to_datetime(att['attendance_date'])
att['Presence Type'] = att['presence_type']
att['Working Hours'] = att['working_hours']

# Filter 2025 Qtr 4 (Oct, Nov, Dec)
q4_2025 = att[
    (att['dt'].dt.year == 2025) & 
    (att['dt'].dt.month.isin([10, 11, 12]))
]
print(f"Q4 2025 Records (Active): {len(q4_2025)}")

# Apply Bucket
def get_bucket(h):
    if pd.isna(h): return None
    if h < 3: return "< 3 hours"
    if h < 6: return "3-6 hours"
    if h >= 6: return "6+ hours"
    return None

q4_wfo = q4_2025[q4_2025['Presence Type'] == 'Work From Office'].copy()
q4_wfo['Bucket'] = q4_wfo['Working Hours'].apply(get_bucket)

# Results
res = q4_wfo.groupby('Bucket')['employee_name'].nunique()
print("\nResults (Distinct Count of Employees):")
print(res)

# Check if maybe they meant Average per employee FIRST
emp_avg = q4_wfo.groupby('employee_name')['Working Hours'].mean().reset_index()
emp_avg['Bucket'] = emp_avg['Working Hours'].apply(get_bucket)
res_avg = emp_avg.groupby('Bucket')['employee_name'].nunique()
print("\nResults (Avg per Employee first):")
print(res_avg)
