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
df = att[(att['dt'].dt.year == 2025) & (att['dt'].dt.month.isin([10, 11, 12]))].copy()
df = df[df['presence_type'] == 'Work From Office']

def get_bucket(h):
    if pd.isna(h): return None
    if h < 3: return "< 3 hours"
    if h < 6: return "3-6 hours"
    return "6+ hours"

df['Office Hrs Bucket'] = df['working_hours'].apply(get_bucket)
df = df.dropna(subset=['Office Hrs Bucket'])

order = ['< 3 hours', '3-6 hours', '6+ hours']
res = df.groupby('Office Hrs Bucket').agg(
    Total_Emp_WFO=('employee_name', 'nunique')
).reindex(order)

print("Current Logic Results for Q4 2025:")
print(res)
