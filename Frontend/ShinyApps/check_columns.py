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

# Group by Month and Employee to see what WFH Days and WFH Bucket we have in the parquet
# Since these are row-level columns, they should be consistent for an employee in a month
months = ['Oct 2025', 'Nov 2025', 'Dec 2025']
df = att[att['Month_Year'].isin(months)].copy()

# Print sample values to see how they are populated
print("Sample WFH Data from Parquet (Row Level):")
print(df[['Month_Year', 'employee_name', 'presence_type', 'WFH Days', 'WFH Bucket']].head(10))

# Try counting based on the existing WFH Bucket column
# But first, replace the ≤ character to avoid encoding issues or comparison issues
def safe_bucket(val):
    if val is None: return "None"
    s = str(val)
    if "> 9" in s: return "WFH > 9"
    return "WFH <= 9"

df['Safe_Bucket'] = df['WFH Bucket'].apply(safe_bucket)

# Group by Month and Employee, taking the FIRST bucket found (should be unique per group)
emp_monthly_buckets = df.groupby(['Month_Year', 'employee_name']).agg({'Safe_Bucket': 'first'}).reset_index()

res = emp_monthly_buckets.groupby(['Month_Year', 'Safe_Bucket']).size().unstack(fill_value=0)
print("\nEmployee Counts Using Existing 'WFH Bucket' Column:")
print(res)
