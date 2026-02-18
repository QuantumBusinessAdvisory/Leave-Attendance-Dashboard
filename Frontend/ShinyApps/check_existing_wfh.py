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

# Group by Month and WFH Bucket (using the existing column)
months = ['Oct 2025', 'Nov 2025', 'Dec 2025']
df = att[att['Month_Year'].isin(months)].copy()

# The user wants to count EMPLOYEES per month.
# Since 'WFH Bucket' is a property of the attendance record (row),
# but likely reflects the monthly status, we should take the bucket for each employee per month.
res = df.groupby(['Month_Year', 'employee_name', 'WFH Bucket']).size().reset_index()
# Now count distinct employees per bucket per month
final_res = res.groupby(['Month_Year', 'WFH Bucket']).size().unstack(fill_value=0)

print("\nResults Using Existing 'WFH Bucket' Column (Counting Employees):")
print(final_res)
