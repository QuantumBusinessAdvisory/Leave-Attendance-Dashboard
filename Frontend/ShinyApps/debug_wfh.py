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
att['Month_Year'] = att['dt'].dt.strftime('%b %Y')

# Let's see if 'presence_type' contains 'Work From Home'
print(f"Presence Types: {att['presence_type'].unique()}")

# Calculate WFH Days per Employee per Month
# Usually, WFH Days is the count of records where presence_type is 'Work From Home'
wfh_records = att[att['presence_type'] == 'Work From Home']
wfh_counts = wfh_records.groupby(['Month_Year', 'employee_name']).size().reset_index(name='Actual_WFH_Days')

# Bucket logic: > 9 and <= 9
wfh_counts['WFH_Bucket'] = wfh_counts['Actual_WFH_Days'].apply(lambda x: "WFH > 9" if x > 9 else "WFH <= 9")

# Wait, if an employee has 0 WFH days, they should also be in "WFH <= 9"
# So we need the full list of employees who had ANY attendance in that month
total_monthly_emps = att.groupby(['Month_Year', 'employee_name']).size().reset_index()[['Month_Year', 'employee_name']]
final_df = total_monthly_emps.merge(wfh_counts, on=['Month_Year', 'employee_name'], how='left')
final_df['Actual_WFH_Days'] = final_df['Actual_WFH_Days'].fillna(0)
final_df['WFH_Bucket'] = final_df['Actual_WFH_Days'].apply(lambda x: "WFH > 9" if x > 9 else "WFH <= 9")

# Results for Oct, Nov, Dec 2025
months = ['Oct 2025', 'Nov 2025', 'Dec 2025']
res = final_df[final_df['Month_Year'].isin(months)].groupby(['Month_Year', 'WFH_Bucket']).size().unstack(fill_value=0)

print("\nSimulated Results (Counting Employees per Bucket per Month):")
print(res)
