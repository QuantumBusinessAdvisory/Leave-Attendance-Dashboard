import pandas as pd
import os

APP_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.normpath(os.path.join(APP_DIR, "..", "..", "Backend", "data", "processed"))

# Load and Filter Active Users
ud = pd.read_parquet(os.path.join(DATA_DIR, 'users_details.parquet'))
active_users = ud[ud['employee_status'] == 'Active']['user_id'].unique()

# Load Attendance & Date Table
att = pd.read_parquet(os.path.join(DATA_DIR, 'attendance.parquet'))
att = att[att['user_id'].isin(active_users)].copy()
att['dt'] = pd.to_datetime(att['attendance_date'])

dt = pd.read_parquet(os.path.join(DATA_DIR, 'date_table.parquet'))
dt['dt'] = pd.to_datetime(dt['Date'])
working_days = dt[dt['IsWorkingDay'] == 1]['dt'].dt.normalize()

# Filter only Working Days
att['dt_norm'] = att['dt'].dt.normalize()
att = att[att['dt_norm'].isin(working_days)]

def get_bucket(h):
    if pd.isna(h): return None
    if h < 3: return "< 3 hours"
    if h < 6: return "3-6 hours"
    return "6+ hours"

att['Bucket'] = att['working_hours'].apply(get_bucket)
att_wfo = att[att['presence_type'] == 'Work From Office'].copy()

# Test different months or period
for label, m_range in [("Oct", [10]), ("Nov", [11]), ("Dec", [12]), ("Q4", [10, 11, 12])]:
    df = att_wfo[att_wfo['dt'].dt.month.isin(m_range)]
    res = df.groupby('Bucket')['employee_name'].nunique()
    print(f"\n--- {label} Results (Distinct Emp per Bucket) ---")
    print(res)
    
    # Also check total count
    print(f"Total Records: {len(df)}")
    print(f"Total Distinct Emps: {df['employee_name'].nunique()}")
