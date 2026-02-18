import pandas as pd
import os

APP_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.normpath(os.path.join(APP_DIR, "..", "..", "Backend", "data", "processed"))

# Load and Filter Active Users
ud = pd.read_parquet(os.path.join(DATA_DIR, 'users_details.parquet'))
active_users_df = ud[ud['employee_status'] == 'Active']
active_users_ids = active_users_df['user_id'].unique()

# Load Attendance
att = pd.read_parquet(os.path.join(DATA_DIR, 'attendance.parquet'))
att = att[att['user_id'].isin(active_users_ids)].copy()
att['dt'] = pd.to_datetime(att['attendance_date'])
att['Month_Year'] = att['dt'].dt.strftime('%b %Y')

# Filter Oct 2025
oct_2025 = att[att['Month_Year'] == 'Oct 2025'].copy()
oct_emps = oct_2025.groupby('employee_name').first().reset_index()

print(f"Total Unique Employees in Oct 2025 Attendance (Active): {len(oct_emps)}")

# Check for "Abnormal" employees
print("\nEmployees with suspect names or types:")
print(oct_emps[oct_emps['employee_name'].str.contains('admin|test|training|dummy', case=False)][['employee_name', 'department_name']])

# Check for employees who have ONLY one attendance record for the whole month?
emp_counts = oct_2025.groupby('employee_name').size()
low_attendance = emp_counts[emp_counts < 3]
print(f"\nEmployees with < 3 attendance records in Oct: {len(low_attendance)}")
print(low_attendance)
