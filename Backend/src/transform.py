import pandas as pd
import json
import os
import glob
from datetime import datetime
import re
import numpy as np
import ast

def get_latest_file(endpoint_name):
    """Finds the latest timestamped file for a given endpoint name."""
    pattern = f"data/raw/{endpoint_name}_*.json"
    list_of_files = glob.glob(pattern)
    
    if not list_of_files:
        return None
    return max(list_of_files, key=os.path.getctime)

def process_generic(raw_data):
    """
    Standard cleaning: Extract 'message' -> 'data' list and flatten it.
    """
    try:
        data_list = raw_data['message']['data']
        df = pd.json_normalize(data_list)
        return df
    except Exception as e:
        print(f"Generic processing failed: {e}")
        return pd.DataFrame()

def process_leave_balance(raw_data):
    """
    Specific cleaning for leave_balance (nested list).
    """
    try:
        employees_list = raw_data['message']['data']
        df = pd.json_normalize(
            employees_list, 
            record_path=['leave_balances'], 
            meta=['employee_name', 'company', 'department_name'],
            errors='ignore'
        )
        
        # Power Query Renames
        df.rename(columns={
            'employee_name': 'Employee Name',
            'company': 'Company',
            'department_name': 'Department Name',
            'leave_type': 'Leave Type',
            'leave_period_from': 'Period From',
            'leave_period_to': 'Period To',
            'total_leaves': 'Total Leaves',
            'availed': 'Leave Availed',
            'balance': 'Leave Balance'
        }, inplace=True)
        
        # Type conversions
        df['Period From'] = pd.to_datetime(df['Period From'], errors='coerce')
        df['Period To'] = pd.to_datetime(df['Period To'], errors='coerce')
        for col in ['Total Leaves', 'Leave Availed', 'Leave Balance']:
             df[col] = pd.to_numeric(df[col], errors='coerce')
             
        return df
    except Exception as e:
        print(f"Leave balance processing failed: {e}")
        return pd.DataFrame()

def apply_calculations(df, endpoint):
    """
    Applies Power BI equivalent calculated columns (DAX -> Pandas).
    """
    try:
        if endpoint == 'attendance':
            # Ensure types
            df['attendance_date'] = pd.to_datetime(df['attendance_date'], errors='coerce')
            df['working_hours'] = pd.to_numeric(df['working_hours'], errors='coerce').fillna(0)
            
            # 1. YearMonth = FORMAT(tblattendance[Attendance Date], "YYYY-MM")
            df['YearMonth'] = df['attendance_date'].dt.strftime('%Y-%m')
            
            # 2. WFH Days (Complex Calculation)
            # CALCULATE(DISTINCTCOUNT(Date), ALLEXCEPT(Employee, YearMonth), Mode="WFH")
            # Logic: Filter WFH -> GroupBy Employee/YM -> Count Unique Date -> Merge back
            
            # Create a localized subset for calculation
            wfh_subset = df[df['mode_of_attendance'] == 'WFH'].copy()
            if not wfh_subset.empty:
                wfh_counts = wfh_subset.groupby(['employee', 'YearMonth'])['attendance_date'].nunique().reset_index()
                wfh_counts.rename(columns={'attendance_date': 'WFH Days'}, inplace=True)
                
                # Merge back to main DF
                df = pd.merge(df, wfh_counts, on=['employee', 'YearMonth'], how='left')
                df['WFH Days'] = df['WFH Days'].fillna(0)
            else:
                df['WFH Days'] = 0

            # 3. WFH Bucket
            # IF(tblattendance[WFH Days] > 9, "WFH > 9", "WFH ≤ 9")
            df['WFH Bucket'] = df['WFH Days'].apply(lambda x: "WFH > 9" if x > 9 else "WFH ≤ 9")
            
            # 4. Office Hrs Bucket
            # IF(Presence Type <> "Work From Office" || ISBLANK(Working Hours), BLANK(), SWITCH(...))
            # Note: "Work From Office" might come from 'mode_of_attendance' or 'presence_type'. 
            # User said: tblattendance[Presence Type] <> "Work From Office"
            
            def calc_office_bucket(row):
                # Check for "Work From Office" in presence_type OR mode_of_attendance (being safe)
                p_type = str(row.get('presence_type', '')).strip()
                # The user specifically mentioned "Work From Office".
                # In CSV we saw "Work From Home" and "On Duty" and "On Leave". 
                # We assume the user creates "Work From Office" logic or expects matches.
                # Let's strictly follow the provided formula logic:
                if p_type != "Work From Office": 
                    return None # BLANK()
                
                wh = row['working_hours']
                if pd.isna(wh):
                    return None
                
                if wh < 3:
                    return "< 3 hours"
                elif 3 <= wh < 6:
                    return "3–6 hours"
                elif wh >= 6:
                    return "6+ hours"
                return None

            df['Office Hrs Bucket'] = df.apply(calc_office_bucket, axis=1)

        elif endpoint == 'leave_applications':
            # Ensure types
            # 'Leave Application Date' col 7, 'from_date' col 5, 'to_date' col 6
            # We need to rely on column names. Let's normalize if needed or assume standard
            # Based on CSV header observed: "Leave Application Date", "from_date"
            
            # Date conversions
            df['Leave Application Date'] = pd.to_datetime(df['Leave Application Date'], errors='coerce')
            df['from_date'] = pd.to_datetime(df['from_date'], errors='coerce')
            df['total_leave_days'] = pd.to_numeric(df['total_leave_days'], errors='coerce').fillna(0)
            
            # 1. Leave Application Category
            # IF(App Date < From Date, "Applied Before Availing", "Applied Post Availing")
            def calc_app_category(row):
                app_date = row['Leave Application Date']
                from_date = row['from_date']
                if pd.isna(app_date) or pd.isna(from_date):
                    return None
                if app_date < from_date:
                    return "Applied Before Availing"
                else:
                    return "Applied Post Availing"
            
            df['Leave Application Category'] = df.apply(calc_app_category, axis=1)
            
            # 2. Total Leave Days (Adjusted)
            # IF(Leave Days = 0 && (Half Day From = "Yes" || Half Day To = "Yes"), 0.5, Leave Days)
            def calc_total_days(row):
                days = row['total_leave_days']
                h_from = str(row.get('Half day on From Date', '')).lower()
                h_to = str(row.get('Half day on To Date', '')).lower()
                
                if days == 0 and (h_from == 'yes' or h_to == 'yes'):
                    return 0.5
                return days
                
            df['Total Leave Days'] = df.apply(calc_total_days, axis=1)

    except Exception as e:
        print(f"Calculation error for {endpoint}: {e}")
        # Return df anyway to avoid pipeline crash
    
    return df

def transform_data():
    """
    Loops through all known endpoints and cleans them + adds calculations.
    """
    print(f"\n[{datetime.now()}] Starting Transformation for ALL files (with Calculations)...")
    
    all_files = glob.glob('data/raw/*.json')
    endpoints_found = set()
    for f in all_files:
        basename = os.path.basename(f)
        match = re.search(r'^(.*)_\d{8}_\d{6}\.json$', basename)
        if match:
             endpoints_found.add(match.group(1))
    
    print(f"Found datasets to process: {list(endpoints_found)}")
    os.makedirs("data/processed", exist_ok=True)
    
    for endpoint in endpoints_found:
        latest_file = get_latest_file(endpoint)
        if not latest_file:
            continue
            
        print(f"Processing {endpoint}...")
        
        with open(latest_file, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)
            
        # 1. Base Cleaning
        if endpoint == 'leave_balance':
            df = process_leave_balance(raw_data)
        else:
            df = process_generic(raw_data)
            
        # 2. Apply Custom Calculations (Power BI Replacements)
        if not df.empty:
            df = apply_calculations(df, endpoint)
            
            output_path = f"data/processed/{endpoint}.csv"
            df.to_csv(output_path, index=False)
            print(f"SUCCESS: Saved {output_path} ({len(df)} rows)")
        else:
            print(f"WARNING: Result empty for {endpoint}")
            
    # 3. Create DateTable (Post-Processing)
    try:
        create_date_table()
    except Exception as e:
        print(f"DateTable Creation Failed: {e}")

def create_date_table():
    """
    Generates a master DateTable based on Leave Application range.
    Includes Holiday and Working Day logic.
    """
    print("\nGenerating DateTable...")
    
    # 1. Load Source Data
    try:
        df_leave = pd.read_csv("data/processed/leave_applications.csv")
        df_holidays = pd.read_csv("data/processed/holidays.csv")
    except FileNotFoundError:
        print("Skipping DateTable: Source files (leave_applications or holidays) not found.")
        return

    # 2. Determine Date Range
    # Formula: CALENDAR(MIN(From Date), MAX(To Date))
    # Note: Using 'from_date' and 'to_date' columns as per PBI logic request
    
    # Ensure date types
    df_leave['from_date'] = pd.to_datetime(df_leave['from_date'], errors='coerce')
    df_leave['to_date'] = pd.to_datetime(df_leave['to_date'], errors='coerce')
    
    min_date = df_leave['from_date'].min()
    max_date = df_leave['to_date'].max()
    
    if pd.isna(min_date) or pd.isna(max_date):
        print("Skipping DateTable: Valid date range could not be determined.")
        return
        
    print(f"Date Range: {min_date.date()} to {max_date.date()}")
    
    date_range = pd.date_range(start=min_date, end=max_date, freq='D')
    df_date = pd.DataFrame({'Date': date_range})
    
    # 3. Process Holidays
    # Filter: holiday_list_id <> "QBAPL 2025-2026 Optional Holidays"
    valid_holidays = df_holidays[df_holidays['holiday_list_id'] != "QBAPL 2025-2026 Optional Holidays"]
    
    holiday_dates = set()
    
    for _, row in valid_holidays.iterrows():
        # Parsing the 'holidays' column (Stringified JSON/List of Dicts)
        try:
            # Handle potential string format issues
            raw_val = str(row['holidays'])
            if not raw_val or raw_val == 'nan':
                continue
                
            # Use ast.literal_eval because the string likely uses Python syntax (None, True, single quotes)
            # creating a safe eval
            holiday_list = ast.literal_eval(raw_val)
            
            if isinstance(holiday_list, list):
                for h in holiday_list:
                    h_date_str = h.get('holiday_date')
                    if h_date_str:
                         holiday_dates.add(h_date_str) # Keep as string YYYY-MM-DD for matching
        except Exception as e:
             print(f"Error parsing holiday row: {e}")
             
    # 4. Add Columns
    
    # Day = FORMAT(Date, "dddd") -> Full name e.g., Monday
    df_date['Day'] = df_date['Date'].dt.day_name()
    
    # Day No = WEEKDAY(Date, 2) -> Mon=1, Sun=7
    df_date['Day No'] = df_date['Date'].dt.weekday + 1
    
    # IsHoliday = IF(Date in holiday_dates, 1, 0)
    # Convert Date to string YYYY-MM-DD for lookup
    df_date['DateStr'] = df_date['Date'].dt.strftime('%Y-%m-%d')
    df_date['IsHoliday'] = df_date['DateStr'].apply(lambda x: 1 if x in holiday_dates else 0)
    
    # IsWeekend = IF(Day No >= 6, 1, 0)
    df_date['IsWeekend'] = df_date['Day No'].apply(lambda x: 1 if x >= 6 else 0)
    
    # IsWorkingDay = IF(IsWeekend = 0 && IsHoliday = 0, 1, 0)
    df_date['IsWorkingDay'] = ((df_date['IsWeekend'] == 0) & (df_date['IsHoliday'] == 0)).astype(int)
    
    # Cleanup
    df_date.drop(columns=['DateStr'], inplace=True)
    
    output_path = "data/processed/date_table.csv"
    df_date.to_csv(output_path, index=False)
    print(f"SUCCESS: Saved {output_path} ({len(df_date)} rows)")

if __name__ == "__main__":
    transform_data()
