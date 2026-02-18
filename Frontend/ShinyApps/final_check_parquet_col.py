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
months = ['Oct 2025', 'Nov 2025', 'Dec 2025']
df = att[att['Month_Year'].isin(months)].copy()

# Use the existing 'WFH Days' column from the parquet
# Since it's row-level and duplicated for all records of that emp-month, take one per group
emp_monthly_stats = df.groupby(['Month_Year', 'employee_name']).agg({'WFH Days': 'first'}).reset_index()

# Apply the > 9 logic
emp_monthly_stats['WFH_Bucket_Calc'] = emp_monthly_stats['WFH Days'].apply(lambda x: "WFH > 9" if x > 9 else "WFH <= 9")

res = emp_monthly_stats.groupby(['Month_Year', 'WFH_Bucket_Calc']).size().unstack(fill_value=0)

print("Q4 2025 WFH Compliance (Using Parquet's Pre-calculated WFH Days):")
print(res)
