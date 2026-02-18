import pandas as pd
import os
APP_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.normpath(os.path.join(APP_DIR, "..", "..", "Backend", "data", "processed"))
dt = pd.read_parquet(os.path.join(DATA_DIR, 'date_table.parquet'))
dt['dt'] = pd.to_datetime(dt['Date'])
dt['Year'] = dt['dt'].dt.year.astype(str)
dt['Qtr'] = 'Qtr ' + dt['dt'].dt.quarter.astype(str)
dt['Month'] = dt['dt'].dt.month_name()
tree = {}
for _, r in dt[['Year', 'Qtr', 'Month']].drop_duplicates().iterrows():
    y, q, m = r['Year'], r['Qtr'], r['Month']
    if y not in tree: tree[y] = {}
    if q not in tree[y]: tree[y][q] = []
    if m not in tree[y][q]: tree[y][q].append(m)
print(f"Years: {list(tree.keys())}")
if '2025' in tree:
    print(f"2025 Quarters: {list(tree['2025'].keys())}")
    for q in tree['2025']:
        print(f"2025 {q} Months: {tree['2025'][q]}")
