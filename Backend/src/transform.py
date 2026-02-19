import pandas as pd
import json
import os
import glob
from datetime import datetime
import re
import numpy as np
import ast

# Path relative to Backend directory
BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_RAW_DIR = os.path.join(BACKEND_DIR, "data", "raw")
DATA_PROCESSED_DIR = os.path.join(BACKEND_DIR, "data", "processed")

def get_latest_file(endpoint_name):
    pattern = os.path.join(DATA_RAW_DIR, f"{endpoint_name}_*.json")
    list_of_files = glob.glob(pattern)
    if not list_of_files: return None
    return max(list_of_files, key=os.path.getctime)

def process_generic(raw_data):
    try:
        data_list = raw_data['message']['data']
        return pd.json_normalize(data_list)
    except: return pd.DataFrame()

def process_leave_balance(raw_data):
    try:
        employees_list = raw_data['message']['data']
        df = pd.json_normalize(employees_list, record_path=['leave_balances'], meta=['employee_name', 'company', 'department_name'], errors='ignore')
        df.rename(columns={'employee_name': 'Employee Name', 'company': 'Company', 'department_name': 'Department Name', 'leave_type': 'Leave Type', 'leave_period_from': 'Period From', 'leave_period_to': 'Period To', 'total_leaves': 'Total Leaves', 'availed': 'Leave Availed', 'balance': 'Leave Balance'}, inplace=True)
        df['Period From'] = pd.to_datetime(df['Period From'], errors='coerce')
        df['Period To'] = pd.to_datetime(df['Period To'], errors='coerce')
        for col in ['Total Leaves', 'Leave Availed', 'Leave Balance']: df[col] = pd.to_numeric(df[col], errors='coerce')
        return df
    except: return pd.DataFrame()

def apply_calculations(df, endpoint):
    try:
        if endpoint == 'attendance':
            df['attendance_date'] = pd.to_datetime(df['attendance_date'], errors='coerce')
            df['working_hours'] = pd.to_numeric(df['working_hours'], errors='coerce').fillna(0)
            df['YearMonth'] = df['attendance_date'].dt.strftime('%Y-%m')
            wfh_subset = df[df['mode_of_attendance'] == 'WFH'].copy()
            if not wfh_subset.empty:
                wfh_counts = wfh_subset.groupby(['employee', 'YearMonth'])['attendance_date'].nunique().reset_index()
                wfh_counts.rename(columns={'attendance_date': 'WFH Days'}, inplace=True)
                df = pd.merge(df, wfh_counts, on=['employee', 'YearMonth'], how='left')
                df['WFH Days'] = df['WFH Days'].fillna(0)
            else: df['WFH Days'] = 0
            df['WFH Bucket'] = df['WFH Days'].apply(lambda x: "WFH > 9" if x > 9 else "WFH ≤ 9")
            def calc_office_bucket(row):
                if str(row.get('presence_type', '')).strip() != "Work From Office": return None
                wh = row['working_hours']
                if pd.isna(wh): return None
                if wh < 3: return "< 3 hours"
                elif 3 <= wh < 6: return "3–6 hours"
                elif wh >= 6: return "6+ hours"
                return None
            df['Office Hrs Bucket'] = df.apply(calc_office_bucket, axis=1)
        elif endpoint == 'leave_applications':
            df['Leave Application Date'] = pd.to_datetime(df['Leave Application Date'], errors='coerce')
            df['from_date'] = pd.to_datetime(df['from_date'], errors='coerce')
            df['total_leave_days'] = pd.to_numeric(df['total_leave_days'], errors='coerce').fillna(0)
            df['Leave Application Category'] = df.apply(lambda r: "Applied Before Availing" if r['Leave Application Date'] < r['from_date'] else "Applied Post Availing", axis=1)
            def calc_total_days(row):
                days = row['total_leave_days']
                h_from, h_to = str(row.get('Half day on From Date', '')).lower(), str(row.get('Half day on To Date', '')).lower()
                return 0.5 if days == 0 and (h_from == 'yes' or h_to == 'yes') else days
            df['Total Leave Days'] = df.apply(calc_total_days, axis=1)
    except Exception as e: print(f"Calculation error: {e}")
    return df

def transform_data():
    all_files = glob.glob(os.path.join(DATA_RAW_DIR, '*.json'))
    endpoints_found = {re.search(r'^(.*)_\d{8}_\d{6}\.json$', os.path.basename(f)).group(1) for f in all_files if re.search(r'^(.*)_\d{8}_\d{6}\.json$', os.path.basename(f))}
    os.makedirs(DATA_PROCESSED_DIR, exist_ok=True)
    for endpoint in endpoints_found:
        latest_file = get_latest_file(endpoint)
        if not latest_file: continue
        with open(latest_file, 'r', encoding='utf-8') as f: raw_data = json.load(f)
        df = process_leave_balance(raw_data) if endpoint == 'leave_balance' else process_generic(raw_data)
        if not df.empty:
            df = apply_calculations(df, endpoint)
            df.to_csv(os.path.join(DATA_PROCESSED_DIR, f"{endpoint}.csv"), index=False)
    try: create_date_table()
    except: pass

def create_date_table():
    try:
        df_leave = pd.read_csv(os.path.join(DATA_PROCESSED_DIR, "leave_applications.csv"))
        df_holidays = pd.read_csv(os.path.join(DATA_PROCESSED_DIR, "holidays.csv"))
    except: return
    df_leave['from_date'] = pd.to_datetime(df_leave['from_date'], errors='coerce')
    df_leave['to_date'] = pd.to_datetime(df_leave['to_date'], errors='coerce')
    min_date, max_date = df_leave['from_date'].min(), df_leave['to_date'].max()
    if pd.isna(min_date) or pd.isna(max_date): return
    date_range = pd.date_range(start=min_date, end=max_date, freq='D')
    df_date = pd.DataFrame({'Date': date_range})
    valid_holidays = df_holidays[df_holidays['holiday_list_id'] != "QBAPL 2025-2026 Optional Holidays"]
    holiday_dates = set()
    for _, row in valid_holidays.iterrows():
        try:
            raw_val = str(row['holidays'])
            if not raw_val or raw_val == 'nan': continue
            holiday_list = ast.literal_eval(raw_val)
            for h in holiday_list:
                if h.get('holiday_date'): holiday_dates.add(h.get('holiday_date'))
        except: pass
    df_date['Day'] = df_date['Date'].dt.day_name()
    df_date['Day No'] = df_date['Date'].dt.weekday + 1
    df_date['DateStr'] = df_date['Date'].dt.strftime('%Y-%m-%d')
    df_date['IsHoliday'] = df_date['DateStr'].apply(lambda x: 1 if x in holiday_dates else 0)
    df_date['IsWeekend'] = df_date['Day No'].apply(lambda x: 1 if x >= 6 else 0)
    df_date['IsWorkingDay'] = ((df_date['IsWeekend'] == 0) & (df_date['IsHoliday'] == 0)).astype(int)
    df_date.drop(columns=['DateStr'], inplace=True)
    df_date.to_csv(os.path.join(DATA_PROCESSED_DIR, "date_table.csv"), index=False)

if __name__ == "__main__":
    transform_data()
