import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import glob
from datetime import datetime
from streamlit_tree_select import tree_select

# Page Config
st.set_page_config(page_title="QBA Leave & Attendance", layout="wide", initial_sidebar_state="expanded")

# Custom CSS
st.markdown("""
<style>
    .main-header {
        background-color: #2e4b85;
        padding: 5px; 
        border-radius: 5px;
        color: white;
        display: flex;
        justify-content: center;
        align-items: center;
        text-align: center;
        margin-bottom: 20px;
        font-family: 'Segoe UI', sans-serif;
    }
    .main-header h2 {
        font-size: 1.5rem;
        margin: 0;
    }
    .metric-card {
        background-color: white;
        padding: 10px;
        border-radius: 5px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        text-align: center;
    }
    .block-container {
        padding-top: 1rem;
    }
    /* Tree Select Customization? */
</style>
""", unsafe_allow_html=True)

# ----------------- DATA LOADING ----------------- #
current_dir = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(current_dir, "..", "..", "Backend", "data", "processed")

@st.cache_data
def load_data():
    data = {}
    if not os.path.exists(DATA_DIR):
         return data
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    for f in files:
        name = os.path.basename(f).replace(".csv", "")
        try:
             data[name] = pd.read_csv(f, low_memory=False)
        except Exception as e:
             pass
    return data

data = load_data()

# Validation - Core files still required for existing dashboard logic
required_files = ['attendance', 'users_details', 'leave_applications', 'date_table']
if not all(f in data for f in required_files):
    missing = [f for f in required_files if f not in data]
    st.error(f"Missing core datasets: {missing}")
    st.stop()

# Unpack All Tables
# Core 4
df_att = data['attendance']
df_users = data['users_details']
df_leave = data['leave_applications']
df_date = data['date_table']

# Additional Tables
df_holidays = data.get('holidays', pd.DataFrame())
df_leave_balance = data.get('leave_balance', pd.DataFrame())
df_managers = data.get('managers', pd.DataFrame())
df_proj_alloc = data.get('project_allocations', pd.DataFrame())
df_proj_details = data.get('projects_details', pd.DataFrame())
df_timesheet = data.get('timesheet', pd.DataFrame())

# Metadata for informational display if needed
loaded_tables = list(data.keys())

# ----------------- PRE-PROCESSING ----------------- #

# 1. DateTable Setup
df_date['Date'] = pd.to_datetime(df_date['Date'], errors='coerce').dt.normalize()
df_date['Year'] = df_date['Date'].dt.year
df_date['Quarter'] = df_date['Date'].dt.quarter.apply(lambda x: f"Q{x}")
df_date['MonthName'] = df_date['Date'].dt.strftime('%B')
df_date['MonthNum'] = df_date['Date'].dt.month
# Day for hierarchy
df_date['DayDay'] = df_date['Date'].dt.day
df_date['YearMonth'] = df_date['Date'].dt.strftime('%Y-%m')
df_date['MonthYearLabel'] = df_date['Date'].dt.strftime('%b %Y')

# 2. Master Lists
df_users['employee_name'] = df_users['employee_name'].astype(str).str.strip()
df_users['department_name'] = df_users['department_name'].astype(str).str.strip()
df_att['presence_type'] = df_att['presence_type'].astype(str).str.strip()

all_employees = sorted(df_users['employee_name'].dropna().unique().tolist())
all_depts = sorted(df_users['department_name'].dropna().unique().tolist())
all_mgrs = sorted(df_users['reporting_manager_name'].dropna().unique().tolist())
all_etypes = sorted(df_users['employment_type'].dropna().unique().tolist())
all_ltypes = sorted(df_leave['leave_type'].dropna().unique().tolist())
all_att_modes = sorted(df_att['mode_of_attendance'].dropna().unique().tolist())

# 3. Leave Dates conversion
df_leave['from_date'] = pd.to_datetime(df_leave['from_date'], errors='coerce').dt.normalize()
df_leave['to_date'] = pd.to_datetime(df_leave['to_date'], errors='coerce').dt.normalize()

# 4. Merge Data & Cleaning
df_att['employee'] = df_att['employee'].astype(str).str.strip()
df_users['employee_id'] = df_users['employee_id'].astype(str).str.strip()

# Source of Truth: ensure master lists are clean and standardized
for col in ['employee_name', 'department_name', 'reporting_manager_name', 'employment_type', 'employee_status']:
    if col in df_users.columns:
        df_users[col] = df_users[col].astype(str).str.strip().str.title() # Force title case for consistency

# Main Attendance Merge - Drop redundants from df_att first to avoid _x/_y
cols_to_drop = [c for c in ['department_name', 'employee_name', 'reporting_manager_name'] if c in df_att.columns]
df_att_clean = df_att.drop(columns=cols_to_drop)

df_main = pd.merge(
    df_att_clean,
    df_users[['employee_id', 'employee_name', 'reporting_manager_name', 'department_name', 'employment_type', 'employee_status']],
    left_on='employee',
    right_on='employee_id',
    how='left'
)
df_main['attendance_date'] = pd.to_datetime(df_main['attendance_date']).dt.normalize()
df_main['presence_type'] = df_main['presence_type'].astype(str).str.strip().str.title()

# Fix for blank presence
df_main['presence_type'] = df_main['presence_type'].replace(['', 'Nan', 'None'], 'On Duty')

# Merge with Date Table for Working Day info
df_main = pd.merge(
    df_main,
    df_date[['Date', 'IsWorkingDay']],
    left_on='attendance_date',
    right_on='Date',
    how='left'
)
df_main['IsWorkingDay'] = pd.to_numeric(df_main['IsWorkingDay'], errors='coerce').fillna(0).astype(int)

# Global Date Labels
df_main['YearMonth'] = df_main['attendance_date'].dt.strftime('%Y-%m')
df_main['MonthYearLabel'] = df_main['attendance_date'].dt.strftime('%b %Y')
df_main['MonthFull'] = df_main['attendance_date'].dt.strftime('%B')
df_main['DayDay'] = df_main['attendance_date'].dt.day

# Leave Merge - ensure consistency with Users
df_leave['Employee id'] = df_leave['Employee id'].astype(str).str.strip()
# Drop redundants from df_leave to force master values
l_cols_drop = [c for c in ['department_name', 'Employee Name', 'reporting_manager_name'] if c in df_leave.columns]
df_leave_clean = df_leave.drop(columns=l_cols_drop)

df_leave_merged = pd.merge(
    df_leave_clean,
    df_users[['employee_id', 'employee_name', 'reporting_manager_name', 'department_name', 'employee_status', 'employment_type']],
    left_on='Employee id',
    right_on='employee_id',
    how='left'
).rename(columns={'employee_name': 'Employee Name'}) # Keep compat
df_leave_merged['Leave Application Date'] = pd.to_datetime(df_leave_merged['Leave Application Date'], errors='coerce').dt.normalize()
df_leave_merged['YearMonth'] = df_leave_merged['Leave Application Date'].dt.strftime('%Y-%m')
df_leave_merged['MonthYearLabel'] = df_leave_merged['Leave Application Date'].dt.strftime('%b %Y')


# ----------------- HIERARCHY HELPER ----------------- #
@st.cache_data
def create_date_tree(df):
    """
    Constructs the nodes list for streamlit-tree-select.
    Hierarchy: Year -> Quarter -> Month -> Date
    """
    nodes = []
    
    # Iterate Years
    years = sorted(df['Year'].unique(), reverse=True)
    for y in years:
        y_node = {
            "label": str(y),
            "value": f"y_{y}",
            "children": []
        }
        
        # Iter Quarters
        df_y = df[df['Year'] == y]
        qtrs = sorted(df_y['Quarter'].unique())
        for q in qtrs:
            q_node = {
                "label": q,
                "value": f"q_{y}_{q}",
                "children": []
            }
            
            # Iter Months
            df_q = df_y[df_y['Quarter'] == q]
            # Sort months by Num
            months = df_q[['MonthName', 'MonthNum']].drop_duplicates().sort_values('MonthNum')
            
            for _, m_row in months.iterrows():
                m_name = m_row['MonthName']
                m_node = {
                    "label": m_name,
                    "value": f"m_{y}_{q}_{m_name}",
                    "children": []
                }
                
                # Iter Dates
                # Too many nodes if we do every date? 
                # User asked for "dates in that month".
                # Let's limit or just show it. 365 nodes/year is manageable for Tree Select.
                df_m = df_q[df_q['MonthName'] == m_name]
                dates = sorted(df_m['Date'].dt.day.unique())
                
                for d in dates:
                    # Value must be unique and parseable
                    full_date_str = f"{y}-{m_row['MonthNum']:02d}-{d:02d}"
                    m_node["children"].append({
                        "label": str(d),
                        "value": f"d_{full_date_str}"
                    })
                    
                q_node["children"].append(m_node)
            
            y_node["children"].append(q_node)
        
        nodes.append(y_node)
        
    return nodes

def get_selected_dates(checked_values, full_date_df):
    """
    Translates checked tree nodes into a set of actual python dates.
    Optimized: if year selected, add all dates for year, etc.
    """
    if not checked_values:
        return set() # Or all? Handled outside.

    selected_dates = set()
    
    # We can rely on the fact that if a parent is checked, usually all children are checked in the output list 
    # OR we handle just the leaf nodes if the component works that way.
    # streamlit-tree-select usually returns ALL checked nodes (parents and children).
    
    # Strategy: detailed parsing.
    # Note: If we just look for 'd_' nodes, we get exact dates. 
    # If a user checks a Year, does the component return all 'd_' children of it?
    # YES, typically "leaf" selection mode or full selection.
    # Let's assume we get the list of ALL checked keys.
    
    # We will prioritize granular dates 'd_'.
    # If the list contains 'd_YYYY-MM-DD', we use that date.
    
    for val in checked_values:
        if val.startswith("d_"):
            d_str = val.replace("d_", "")
            try:
                selected_dates.add(pd.Timestamp(d_str).date())
            except:
                pass
                
    return selected_dates

# Build Tree
period_nodes = create_date_tree(df_date)

# ----------------- HEADER ----------------- #
st.markdown('<div class="main-header"><h2>QBA Leave & Attendance</h2></div>', unsafe_allow_html=True)

# ----------------- TOP FILTERS ----------------- #

with st.container():
    c_tree, c_rest = st.columns([1, 3])
    
    with c_tree:
        st.caption("Period Selection")
        # Tree Select Widget
        # Default: Select latest month? Or nothing (All)?
        # Let's default to nothing -> Logic handles as "All"
        return_val = tree_select(period_nodes, check_model='leaf', checked=[]) 
        # check_model='leaf' ensures we only get the leaf nodes (dates) in the 'checked' list.
        # This simplifies our parsing logic immensely!
        
        selected_leaves = return_val['checked']
        
    with c_rest:
        # Other Slicers
        r1_c1, r1_c2, r1_c3, r1_c4 = st.columns(4)
        sel_ltype = r1_c1.selectbox("Leave Type", ["All"] + all_ltypes)
        sel_dept = r1_c2.selectbox("Department", ["All"] + all_depts)
        sel_emp = r1_c3.selectbox("Employee Name", ["All"] + all_employees)
        sel_mgr = r1_c4.selectbox("Reporting Manager", ["All"] + all_mgrs)
        
        r2_c1, r2_c2 = st.columns(2)
        sel_etype = r2_c1.selectbox("Employment Type", ["All"] + all_etypes)
        sel_att_mode = r2_c2.selectbox("Attendance Mode", ["All"] + all_att_modes)

# ----------------- FILTERING ENGINE ----------------- #
# 1. Date Logic
if selected_leaves:
    # Converting leaves (d_YYYY-MM-DD) to date objects
    valid_dates = set()
    for val in selected_leaves:
        if val.startswith("d_"):
            d_str = val.replace("d_", "")
            try:
                valid_dates.add(pd.Timestamp(d_str).date())
            except:
                pass
    
    # Filter Masks
    # Attendance
    # df_main['attendance_date'] is datetime. Compare .date()
    mask_att_date = df_main['attendance_date'].dt.date.isin(valid_dates)
    
    # Leave
    # df_leave_merged['Leave Application Date']
    mask_leave_date = df_leave_merged['Leave Application Date'].dt.date.isin(valid_dates)
else:
    # All
    mask_att_date = pd.Series(True, index=df_main.index)
    mask_leave_date = pd.Series(True, index=df_leave_merged.index)

# 2. Other Filters
# ... (Standard standard Logic) ...

# Consolidated Masks
mask_att = mask_att_date
mask_leave = mask_leave_date

if sel_ltype != "All":
    mask_leave &= (df_leave_merged['leave_type'] == sel_ltype)

if sel_dept != "All":
    mask_att &= (df_main['department_name'] == sel_dept)
    mask_leave &= (df_leave_merged['department_name'] == sel_dept)

if sel_emp != "All":
    mask_att &= (df_main['employee_name'] == sel_emp)
    mask_leave &= (df_leave_merged['Employee Name'] == sel_emp)

if sel_mgr != "All":
    mask_att &= (df_main['reporting_manager_name'] == sel_mgr)
    mask_leave &= (df_leave_merged['reporting_manager_name'] == sel_mgr)

if sel_etype != "All":
    mask_att &= (df_main['employment_type'] == sel_etype)
    # Leave merge ignored for simplicity as noted before, or handled if critical

if sel_att_mode != "All":
    mask_att &= (df_main['mode_of_attendance'] == sel_att_mode)

# Apply
dff_att = df_main[mask_att]
dff_leave = df_leave_merged[mask_leave]

# ----------------- PAGE NAVIGATION ----------------- #
st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["Leave Dashboard", "Analysis", "Attendance"])

def check_data():
    if dff_att.empty and dff_leave.empty:
        st.warning("No data available for the selected filters.")
        return False
    return True
        
# ----------------- PAGE 1: LEAVE DASHBOARD ----------------- #
if page == "Leave Dashboard":
    if check_data():
        st.markdown("###")

        # --- DATA PREPARATION (GLOBAL FOR PAGE) ---
        try:
            # 1. Filter Leave Data (Approved/Open)
            # Use 'status' (lowercase)
            dff_leave_filtered = dff_leave[dff_leave['status'].isin(["Approved", "Open"])]
            
            # 2. Date Axis (Selected Range)
            if selected_leaves:
                current_dates = [pd.Timestamp(d_str.replace("d_", "")).date() for d_str in selected_leaves if d_str.startswith("d_")]
                # Assuming valid_dates available from earlier global scope
                mask_date_axis = df_date['Date'].dt.date.isin(valid_dates)
                df_date_filtered = df_date[mask_date_axis]
            else:
                df_date_filtered = df_date
            
            # 3. Unique Month-Years for Axis
            axis_data = df_date_filtered[['YearMonth', 'MonthYearLabel']].drop_duplicates().sort_values('YearMonth')
        except Exception as e:
            st.error(f"Error in Data Preparation: {e}")
            st.stop()
        # ------------------------------------------

        # Row 1: Trend
        with st.container(border=True):
            st.subheader("Leave Application Trend")
            
            if not axis_data.empty:
                # Aggregate
                cats = ["Applied Before Availing", "Applied Post Availing"]
                all_months = axis_data['YearMonth'].unique()
                
                # Mapping
                month_map = axis_data.set_index('YearMonth')['MonthYearLabel'].to_dict()
                all_labels = [month_map[m] for m in all_months]

                counts = dff_leave_filtered.groupby(['YearMonth', 'Leave Application Category']).size()
                idx = pd.MultiIndex.from_product([all_months, cats], names=['YearMonth', 'Leave Application Category'])
                
                trend_data = counts.reindex(idx, fill_value=0).reset_index(name='Count')
                trend_data['MonthYearLabel'] = trend_data['YearMonth'].map(month_map)

                fig_trend = px.bar(
                    trend_data, x='MonthYearLabel', y='Count', color='Leave Application Category', barmode='group', # Reverted to group
                    color_discrete_map={"Applied Before Availing": "#00b0f0", "Applied Post Availing": "#1f4e78"},
                    text_auto=True,
                    category_orders={"MonthYearLabel": all_labels} 
                )
                fig_trend.update_layout(
                    xaxis_title="Month", 
                    yaxis_title="Count", 
                    template="plotly_white", 
                    height=350,
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=1.02,
                        xanchor="left",
                        x=0,
                        title="",
                        font=dict(family="Arial", size=11)
                    )
                )
                fig_trend.update_traces(
                    textposition='inside',
                    insidetextanchor='end',
                    textfont=dict(family="Arial", size=12, color='white')
                )
                fig_trend.update_xaxes(
                    type='category', 
                    tickfont=dict(family="Arial")
                )
                fig_trend.update_yaxes(tickfont=dict(family="Arial"))
                st.plotly_chart(fig_trend, use_container_width=True)
            else:
                st.info("No Date Range Selected")

        # Row 2: KPIs / Split
        c_left, c_right = st.columns(2)
        
        with c_left:
            with st.container(border=True):
                st.subheader("Monthly Leave Utilization Trend")
                
                if not axis_data.empty:
                    try:
                        # 1. Active EMP
                        mask_users = (df_users['employee_status'] == 'Active')
                        if sel_dept != "All": mask_users &= (df_users['department_name'] == sel_dept)
                        if sel_mgr != "All": mask_users &= (df_users['reporting_manager_name'] == sel_mgr)
                        if sel_emp != "All": mask_users &= (df_users['employee_name'] == sel_emp)
                        if sel_etype != "All": mask_users &= (df_users['employment_type'] == sel_etype)
                        
                        active_emp_count = df_users[mask_users]['employee_id'].nunique()
                        
                        # 2. Working Days
                        dates_in_range = df_date_filtered
                        working_days_df = dates_in_range[dates_in_range['IsWorkingDay'] == 1].groupby('YearMonth').size().reset_index(name='WorkingDays')
                        
                        # 3. Denominator
                        denom_df = pd.merge(axis_data, working_days_df, on='YearMonth', how='left').fillna(0)
                        denom_df['TotalAvailableHours'] = active_emp_count * 8 * denom_df['WorkingDays']
                        
                        # 4. Numerator
                        if 'total_leave_days' in dff_leave_filtered.columns:
                             leave_hours_df = dff_leave_filtered.groupby('YearMonth')['total_leave_days'].sum().reset_index(name='TotalLeaveDays')
                             leave_hours_df['TotalLeaveHours'] = leave_hours_df['TotalLeaveDays'] * 8
                        else:
                             leave_hours_df = dff_leave_filtered.groupby('YearMonth').size().reset_index(name='Count')
                             leave_hours_df['TotalLeaveHours'] = leave_hours_df['Count'] * 8

                        # 5. Merge
                        final_util = pd.merge(denom_df, leave_hours_df[['YearMonth', 'TotalLeaveHours']], on='YearMonth', how='left').fillna(0)
                        
                        final_util['Leave Impact %'] = final_util.apply(
                            lambda x: (x['TotalLeaveHours'] / x['TotalAvailableHours'] * 100) if x['TotalAvailableHours'] > 0 else 0, axis=1
                        )
                        
                        # Labels
                        final_util['MonthYearLabel'] = final_util['YearMonth'].map(
                            axis_data.set_index('YearMonth')['MonthYearLabel'].to_dict()
                        )

                        fig_util = px.line(
                            final_util, 
                            x='MonthYearLabel', 
                            y='Leave Impact %', 
                            markers=True, 
                            text='Leave Impact %',
                            hover_data=['TotalLeaveHours', 'TotalAvailableHours']
                        )
                        fig_util.update_traces(
                            texttemplate='%{y:.2f}%', 
                            line_color='#1f4e78',
                            textposition='top center',
                            textfont=dict(family="Arial", size=12),
                            hovertemplate="<b>%{x}</b><br>Total Leave Hours: %{customdata[0]:.2f}<br>Total Available Hours: %{customdata[1]:.2f}<br>Leave Impact %: %{y:.2f}%<extra></extra>"
                        )
                        fig_util.update_layout(
                            template="plotly_white", 
                            height=350, 
                            yaxis_title="Leave Impact %"
                        )
                        fig_util.update_xaxes(type='category', tickfont=dict(family="Arial"))
                        fig_util.update_yaxes(tickfont=dict(family="Arial"))
                        st.plotly_chart(fig_util, use_container_width=True)
                    except Exception as e:
                        st.error(f"Error in Utilization Chart: {e}")
                else:
                    st.info("No Date Data")

        with c_right:
            with st.container(border=True):
                st.subheader("Top 10 Employees with Frequent Unplanned Leave Instances")
                
                # 1. Filter for Unplanned leaves (Applied Post Availing) and Approved/Open status
                unplanned = dff_leave_filtered[dff_leave_filtered['Leave Application Category'] == 'Applied Post Availing']
                
                if not unplanned.empty:
                    # 2. Aggregate Metrics: Leave Instances (Count) and Leave Days (Sum)
                    top_emp = unplanned.groupby('Employee Name').agg(
                        Leave_Instances=('Employee Name', 'size'),
                        Leave_Days=('Total Leave Days', 'sum')
                    ).reset_index()
                    
                    # 3. Sort by Leave Instances (Desc) then Employee Name (Asc) to match image
                    top_emp = top_emp.sort_values(['Leave_Instances', 'Employee Name'], ascending=[False, True]).head(10)
                    
                    # 4. Rename for Legend and Melt
                    # Important: Put 'Leave Instances' FIRST in the melt to stack it on the left
                    top_emp = top_emp.rename(columns={
                        'Leave_Instances': 'Leave Instances',
                        'Leave_Days': 'Leave Days'
                    })
                    
                    top_emp_plot = top_emp.melt(
                        id_vars='Employee Name', 
                        value_vars=['Leave Instances', 'Leave Days'], 
                        var_name='Metric', 
                        value_name='Value'
                    )
                    
                    fig_top = px.bar(
                        top_emp_plot, 
                        y='Employee Name', 
                        x='Value', 
                        color='Metric', 
                        barmode='stack', 
                        orientation='h', 
                        text='Value',
                        color_discrete_map={"Leave Instances": "#00b0f0", "Leave Days": "#1f4e78"}
                    )
                    
                    fig_top.update_layout(
                        template="plotly_white", 
                        height=400, 
                        xaxis_title="Leave Instances and Leave Days", 
                        yaxis_title="", 
                        yaxis={'categoryorder': 'array', 'categoryarray': top_emp['Employee Name'].iloc[::-1].tolist()},
                        legend=dict(
                            orientation="h",
                            yanchor="bottom",
                            y=1.02,
                            xanchor="left",
                            x=0,
                            title="",
                            font=dict(family="Arial", size=11)
                        ),
                        margin=dict(l=0, r=0, t=60, b=0)
                    )
                    fig_top.update_traces(
                        texttemplate='%{text:.1f}',
                        textposition='inside',
                        insidetextanchor='end',
                        textfont=dict(size=14, color='white', family="Arial")
                    )
                    st.plotly_chart(fig_top, use_container_width=True)
                else:
                    st.info("No Unplanned Leaves Found")

# ----------------- PAGE 2: ANALYSIS ----------------- #
elif page == "Analysis":
    st.header("Leave & Availability Analysis")
    
    # --- Availability Forecast Logic ---
    with st.container(border=True):
        st.subheader("Employees Availability Forecast")
        
        # Determine the date range to check (Priority: Tree Select > df_date range)
        if 'valid_dates' in locals() and valid_dates:
            target_dates = sorted(list(valid_dates))
        else:
            # Fallback to the LATEST month that has entries in either data or date table
            # Forecast should work even if attendance (df_main) is missing for the period.
            if not df_date.empty:
                # If df_main has data, anchor there. Otherwise, anchor to latest in Date Table.
                if not dff_att.empty:
                    anchor_date = dff_att['attendance_date'].max().date()
                else:
                    anchor_date = df_date['Date'].max().date()
                
                # Ensure anchor is within DateTable bounds
                max_tbl = df_date['Date'].max().date()
                anchor_date = min(anchor_date, max_tbl)
                
                latest_m = anchor_date.strftime('%Y-%m')
                target_dates = sorted(df_date[
                    (df_date['YearMonth'] == latest_m) & 
                    (df_date['IsWorkingDay'] == 1)
                ]['Date'].dt.date.unique().tolist())
            else:
                target_dates = []
            
        if target_dates:
            # 1. Base Pool: Active employees only (respecting slicers)
            # Ensure case insensitive and clean 'Active'
            dff_users_active = df_users[df_users['employee_status'].str.title() == 'Active'].copy()
            if sel_dept != "All": dff_users_active = dff_users_active[dff_users_active['department_name'] == sel_dept]
            if sel_mgr != "All": dff_users_active = dff_users_active[dff_users_active['reporting_manager_name'] == sel_mgr]
            if sel_etype != "All": dff_users_active = dff_users_active[dff_users_active['employment_type'] == sel_etype]
            if sel_emp != "All": dff_users_active = dff_users_active[dff_users_active['employee_name'] == sel_emp]
            
            # Active EMP 
            active_ids = set(dff_users_active['employee_id'].unique())
            total_active_pool = len(active_ids)
            
            # 2. Daily Calculation
            daily_data = []
            
            # We need the full merged leave df to check 'employee_status' per leave row
            # AND respect the other slicers for the specific employee pool
            # df_leave_merged already has from_date/to_date and employee_status
            
            # Filter leave records once for performance (status and active employee pool)
            # Ensure status is compared against title-cased list if needed
            base_leave = df_leave_merged[
                (df_leave_merged['employee_status'].str.title() == 'Active') & 
                (df_leave_merged['status'].str.title().isin(['Approved', 'Open']))
            ].copy()
            
            # Convert leave dates to date objects for faster comparison in loop
            base_leave['f_date'] = base_leave['from_date'].dt.date
            base_leave['t_date'] = base_leave['to_date'].dt.date
            
            # Slicer filters on leave
            if sel_dept != "All": base_leave = base_leave[base_leave['department_name'] == sel_dept]
            if sel_mgr != "All": base_leave = base_leave[base_leave['reporting_manager_name'] == sel_mgr]
            if sel_emp != "All": base_leave = base_leave[base_leave['Employee Name'] == sel_emp]
            
            # 2. Daily Calculation
            daily_data = []
            
            for d in target_dates:
                # Count distinct active employees who have a leave covering date 'd'
                on_leave = base_leave[
                    (base_leave['f_date'] <= d) & 
                    (base_leave['t_date'] >= d) &
                    (base_leave['Employee id'].isin(active_ids))
                ]['Employee id'].nunique()
                
                avail = max(0, total_active_pool - on_leave)
                
                daily_data.append({
                    'Day': str(d.day),
                    'MonthYear': d.strftime('%b %Y'),
                    'Available Employees': avail,
                    'Employees on Leave': on_leave
                })
            
            df_forecast = pd.DataFrame(daily_data)
            
            if not df_forecast.empty:
                # Add a combined label for X-axis to avoid multicategory issues
                df_forecast['DateLabel'] = df_forecast['MonthYear'] + " " + df_forecast['Day']
                
                df_plot = df_forecast.melt(
                    id_vars=['DateLabel', 'MonthYear', 'Day'], 
                    value_vars=['Available Employees', 'Employees on Leave'],
                    var_name='Status', value_name='Count'
                )

                fig_forecast = px.bar( 
                    df_plot, 
                    x='DateLabel',
                    y='Count', 
                    color='Status', 
                    barmode='stack',
                    text='Count',
                    orientation='v', 
                    color_discrete_map={
                        'Available Employees': '#00adef', 
                        'Employees on Leave': '#1f4e78'
                    }
                )
                
                # Force X-axis to maintain data order
                fig_forecast.update_xaxes(
                    categoryorder='array', 
                    categoryarray=df_forecast['DateLabel'].tolist(),
                    tickangle=-45 # Tilt labels for readability
                )

                fig_forecast.update_layout(
                    template="plotly_white",
                    height=500,
                    xaxis_title="Date", 
                    yaxis_title="Count",
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=1.02,
                        xanchor="left",
                        x=0,
                        title="",
                        font=dict(family="Arial", size=11)
                    ),
                    margin=dict(l=50, r=50, t=100, b=100),
                    bargap=0.3
                )
                
                # Show labels INSIDE bars near the top, as per the reference image
                fig_forecast.update_traces(
                    textposition='inside', 
                    insidetextanchor='end',
                    textfont=dict(color='white', size=13, family="Arial")
                )
                # Ensure labels are visible on lighter bars if needed (though #00adef is usually okay with white)
                
                fig_forecast.update_xaxes(
                    tickangle=0,
                    categoryorder='trace',
                    showgrid=False,
                    rangeslider=dict(visible=True, thickness=0.05), # Added scrollbar
                    type='multicategory'
                )
                
                # Light y-axis grid
                fig_forecast.update_yaxes(showgrid=True, gridcolor='whitesmoke')
                
                # Show summary metrics above chart for visibility
                m1, m2 = st.columns(2)
                m1.metric("Active Employee Pool", total_active_pool)
                m2.metric("Date Range", f"{target_dates[0]} to {target_dates[-1]}")

                st.plotly_chart(fig_forecast, use_container_width=True)
            else:
                st.info("No data for selection.")
        else:
            st.info("Please select a date range.")

    # --- Department wise Total Leaves by Leavetype Matrix ---
    with st.container(border=True):
        st.subheader("Department wise Total Leaves by Leavetype")
        
        # Filter leave data for active employees and current selection
        # mask_leave already accounts for date, leave type, department, and mgr filters
        dff_leave_active = df_leave_merged[mask_leave].copy()
        dff_leave_active = dff_leave_active[dff_leave_active['employee_status'] == 'Active']
        
        if not dff_leave_active.empty:
            # Create Pivot Table
            matrix_df = dff_leave_active.pivot_table(
                index='leave_type',
                columns='department_name',
                values='total_leave_days',
                aggfunc='sum',
                fill_value=0
            )
            
            # Add Total Row
            total_row = matrix_df.sum(axis=0).to_frame().T
            total_row.index = ['Total']
            matrix_df = pd.concat([matrix_df, total_row])
            
            # Reset index to make 'Leave type' a column for display
            matrix_df = matrix_df.reset_index().rename(columns={'index': 'Leave type'})
            
            # Format numeric columns to 2 decimal places
            # We identify numeric columns (all except the first 'Leave type' column)
            numeric_cols = matrix_df.columns[1:]
            
            # Apply formatting and display
            # We use st.dataframe with a style to match the image (numbers formatted as 1.00)
            st.dataframe(
                matrix_df.style.format({col: "{:,.2f}" for col in numeric_cols})
                .set_properties(**{'text-align': 'center', 'font-family': 'Arial'}, subset=numeric_cols)
                .set_properties(**{'font-family': 'Arial'}, subset=['Leave type'])
                .set_table_styles([
                    {'selector': 'th', 'props': [('background-color', '#f0f2f6'), ('color', '#2e4b85'), ('font-weight', 'normal'), ('font-family', 'Arial')]}
                ]),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("No leave data available for the current selection.")

# ----------------- PAGE 3: ATTENDANCE ----------------- #
elif page == "Attendance":
    if not dff_att.empty:
        # 1. Layout: 2 Columns (Left large for Daily, Right for the two buckets)
        col_main, col_buckets = st.columns([2, 1.2])

        # --- A. DAILY ATTENDANCE (Stacked Bar) ---
        with col_main:
            with st.container(border=True):
                # --- A. DAILY ATTENDANCE (Stacked Column) ---
                # 1. Determine the relevant working days in the selected range
                # Use df_date to verify working days
                if selected_leaves:
                    # Filter for working days within the selected set
                    working_dates_all = df_date[df_date['IsWorkingDay'] == 1]['Date'].dt.date.tolist()
                    working_days_period = sorted([d for d in valid_dates if d in working_dates_all])
                else:
                    if not dff_att.empty:
                        min_d = dff_att['attendance_date'].min().date()
                        max_d = dff_att['attendance_date'].max().date()
                        all_working_dates = df_date[
                            (df_date['Date'].dt.date >= min_d) & 
                            (df_date['Date'].dt.date <= max_d) & 
                            (df_date['IsWorkingDay'] == 1)
                        ]['Date'].dt.date.sort_values().tolist()
                        # Default to the most recent month of available attendance data that exists in DateTable
                        max_d_att = df_main['attendance_date'].max().date()
                        max_d_tbl = df_date['Date'].max().date()
                        anchor_d = min(max_d_att, max_d_tbl)
                        latest_att_month = anchor_d.strftime('%Y-%m')
                        
                        working_days_period = df_date[
                            (df_date['YearMonth'] == latest_att_month) & 
                            (df_date['IsWorkingDay'] == 1)
                        ]['Date'].dt.date.sort_values().tolist()
                    else:
                        working_days_period = []

                # 2. Aggregation - Handling blank presence types (e.g. On Leave)
                dff_att_proc = dff_att.copy()
                dff_att_proc['presence_type'] = dff_att_proc['presence_type'].fillna('On Leave').replace(['', 'Nan', 'None'], 'On Leave')
                
                df_counts = dff_att_proc.groupby(['attendance_date', 'presence_type'])['employee'].nunique().reset_index()
                df_counts.columns = ['Date', 'Presence Type', 'Count']
                df_counts['Date'] = pd.to_datetime(df_counts['Date']).dt.date

                # 3. Create a skeleton to ensure ALL working days and ALL presence types are present
                if working_days_period:
                    # Categories from reference + 'On Leave'
                    fixed_p_types = ["Work From Office", "Work From Home", "Work From Anywhere", "On Duty", "Missed Entry", "On Leave"]
                    all_combos = pd.MultiIndex.from_product(
                        [working_days_period, fixed_p_types], 
                        names=['Date', 'Presence Type']
                    ).to_frame(index=False)
                    
                    df_daily_att = pd.merge(all_combos, df_counts, on=['Date', 'Presence Type'], how='left').fillna(0)
                else:
                    df_daily_att = df_counts

                # 4. Format for Charting
                df_daily_att['DateObj'] = pd.to_datetime(df_daily_att['Date'])
                df_daily_att['Month'] = df_daily_att['DateObj'].dt.strftime('%b %Y') # Format like 'Dec 2025'
                df_daily_att['DayNum'] = df_daily_att['DateObj'].dt.day.astype(str)
                df_daily_att['Count'] = pd.to_numeric(df_daily_att['Count']).astype(int)
                
                # Labels: Hide 0s
                df_daily_att['label'] = df_daily_att['Count'].apply(lambda x: str(x) if x > 0 else "")
                
                # Sort for sequence
                df_daily_att = df_daily_att.sort_values('Date')
                
                # Axis Orders
                ord_months = df_daily_att['Month'].unique().tolist()
                ord_days = [str(i) for i in range(1, 32)] # All possible days to ensure order

                # Vertical Stacked Column Chart
                fig_daily = px.bar(
                    df_daily_att,
                    x=['Month', 'DayNum'],
                    y='Count',
                    color='Presence Type',
                    title="Daily Attendance",
                    barmode='stack',
                    text='label',
                    orientation='v', 
                    category_orders={
                        "Month": ord_months,
                        "DayNum": ord_days,
                        "Presence Type": ["Work From Office", "Work From Home", "Work From Anywhere", "On Duty", "Missed Entry", "On Leave"]
                    },
                    color_discrete_map={
                        'Work From Office': '#d9318a', 
                        'Work From Home': '#700c82',   
                        'On Duty': '#1c3e96',          
                        'Missed Entry': '#00adef',    
                        'Work From Anywhere': '#f28e2b',
                        'On Leave': '#e5e5e5'
                    }
                )
                
                fig_daily.update_layout(
                    template="plotly_white",
                    height=550,
                    xaxis_title="Day",
                    yaxis_title="Employee Count",
                    legend=dict(
                        orientation="h", 
                        yanchor="bottom", 
                        y=1.02, 
                        xanchor="left", 
                        x=0, 
                        title="", 
                        font=dict(family="Arial", size=11)
                    ),
                    margin=dict(l=0, r=0, t=100, b=80), 
                    bargap=0.1,
                    title_font=dict(family="Arial", size=18, color='#2e4b85'),
                    title_x=0.0
                )
                
                fig_daily.update_traces(
                    textposition='inside', 
                    insidetextanchor='end',
                    textfont=dict(color='white', size=13, family="Arial"),
                    texttemplate='%{text}'
                )
                # Specialized black labels for On Leave
                fig_daily.for_each_trace(lambda t: t.update(textfont=dict(color="black")) if t.name == "On Leave" else ())
                
                fig_daily.update_xaxes(
                    tickangle=0, 
                    type='multicategory', 
                    showgrid=False,
                    rangeslider=dict(visible=True, thickness=0.05) # Add Scrollbar
                )
                
                st.plotly_chart(fig_daily, use_container_width=True)

        # --- RIGHT COLUMN ---
        with col_buckets:
            # --- B. WFH COMPLIANCE (Bucketized) - Moved to first ---
            with st.container(border=True):
                # Prepare WFH Bucket context
                df_wfh_comp = dff_att.groupby(['MonthYearLabel', 'WFH Bucket'])['employee'].nunique().reset_index()
                df_wfh_comp.columns = ['Month Year', 'WFH Bucket', 'Count']
                
                fig_wfh = px.bar(
                    df_wfh_comp,
                    x='Month Year',
                    y='Count',
                    color='WFH Bucket',
                    barmode='group',
                    title="Work From Home Compliance",
                    text='Count',
                    color_discrete_map={
                        'WFH > 9': '#00adef',
                        'WFH ≤ 9': '#1c3e96'
                    }
                )
                fig_wfh.update_layout(
                    template="plotly_white", 
                    height=300, 
                    xaxis_title="", 
                    yaxis_title="Employee Count", 
                    legend=dict(
                        orientation="h", 
                        yanchor="bottom", 
                        y=1.02, 
                        xanchor="left", 
                        x=0, 
                        title="",
                        font=dict(family="Arial", size=11)
                    ),
                    margin=dict(l=0, r=0, t=80, b=20),
                    title_font=dict(size=18, family="Arial", color='#2e4b85'),
                    title_x=0.0
                )
                fig_wfh.update_traces(
                    textposition='inside',
                    insidetextanchor='end',
                    textfont=dict(size=14, color='white', family="Arial")
                )
                fig_wfh.update_xaxes(
                    type='category',
                    tickfont=dict(family="Arial")
                )
                st.plotly_chart(fig_wfh, use_container_width=True)

            # --- C. EMPLOYEE DISTRIBUTION BY AVG OFFICE HOURS ---
            with st.container(border=True):
                # Logic: Average working hours per employee for WFO records
                df_wfo = dff_att[dff_att['presence_type'] == 'Work From Office'].copy()
                if not df_wfo.empty:
                    df_emp_avg = df_wfo.groupby('employee')['working_hours'].mean().reset_index()
                    
                    def bucket_hrs(h):
                        if h < 3: return "< 3 hours"
                        elif 3 <= h < 6: return "3–6 hours"
                        else: return "6+ hours"
                    
                    df_emp_avg['Bucket'] = df_emp_avg['working_hours'].apply(bucket_hrs)
                    df_bucket_counts = df_emp_avg['Bucket'].value_counts().reset_index()
                    df_bucket_counts.columns = ['Bucket', 'Total Emp Count']
                    
                    # Sort buckets
                    cat_order = ["< 3 hours", "3–6 hours", "6+ hours"]
                    df_bucket_counts['Bucket'] = pd.Categorical(df_bucket_counts['Bucket'], categories=cat_order, ordered=True)
                    df_bucket_counts = df_bucket_counts.sort_values('Bucket')
                    
                    fig_dist = px.bar(
                        df_bucket_counts,
                        x='Bucket',
                        y='Total Emp Count',
                        title="Employee Distribution by Average Office Work Hours",
                        text='Total Emp Count',
                        color_discrete_sequence=['#00adef']
                    )
                    fig_dist.update_layout(
                        template="plotly_white", 
                        height=260, 
                        xaxis_title="Office Hrs Bucket", 
                        yaxis_title="Employee Count", 
                        margin=dict(l=0, r=0, t=50, b=20),
                        title_font=dict(family="Arial", size=18, color='#2e4b85'),
                        title_x=0.0,
                        uniformtext_minsize=12, 
                        uniformtext_mode='hide'
                    )
                    fig_dist.update_traces(
                        textposition='inside',
                        insidetextanchor='end',
                        textfont=dict(size=14, color='white', family="Arial")
                    )
                    st.plotly_chart(fig_dist, use_container_width=True)
                else:
                    st.info("No 'Work From Office' data to display distribution.")

    else:
        st.warning("No Attendance Data available for the selected filters.")

# Footer
st.markdown("---")
