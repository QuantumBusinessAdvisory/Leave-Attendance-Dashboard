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

def get_bucket(h):
    if pd.isna(h): return None
    if h < 3: return "< 3 hours"
    if h < 6: return "3-6 hours"
    return "6+ hours"

att['Bucket'] = att['working_hours'].apply(get_bucket)
att_wfo = att[att['presence_type'] == 'Work From Office'].copy()

# Test different months or period WITHOUT working day filter
for label, m_range in [("Dec", [12]), ("Q4", [10, 11, 12])]:
    df = att_wfo[att_wfo['dt'].dt.month.isin(m_range)]
    res = df.groupby('Bucket')['employee_name'].nunique()
    print(f"\n--- {label} Results (No Working Day Filter) ---")
    print(res)
