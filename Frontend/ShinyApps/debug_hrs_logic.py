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

# Q4 2025 (Oct, Nov, Dec)
q4 = att[(att['dt'].dt.year == 2025) & (att['dt'].dt.month.isin([10, 11, 12]))].copy()
q4_wfo = q4[q4['presence_type'] == 'Work From Office'].copy()

def get_bucket(h):
    if pd.isna(h): return None
    if h < 3: return "< 3 hours"
    if h < 6: return "3-6 hours"
    return "6+ hours"

print(f"Total Active Employees with WFO in Q4 2025: {q4_wfo['employee_name'].nunique()}")

# Approach A: Row-level bucketing (current)
q4_wfo['Bucket_Row'] = q4_wfo['working_hours'].apply(get_bucket)
res_row = q4_wfo.groupby('Bucket_Row')['employee_name'].nunique()
print("\nResults (Row-level Bucketing - Current):")
print(res_row)

# Approach B: Avg per Employee per period FIRST
emp_avg = q4_wfo.groupby('employee_name')['working_hours'].mean().reset_index()
emp_avg['Bucket_Emp'] = emp_avg['working_hours'].apply(get_bucket)
res_emp = emp_avg.groupby('Bucket_Emp')['employee_name'].nunique()
print("\nResults (Avg per Employee first - Proposed):")
print(res_emp)
