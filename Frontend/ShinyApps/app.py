from shiny import App, ui, render, reactive
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os, ast
from shinywidgets import output_widget, render_plotly

# ====================================================
#   CONFIG & PATHS
# ====================================
APP_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.normpath(os.path.join(APP_DIR, "..", "..", "Backend", "data", "processed"))

# ====================================================
#   DATA LAYER
# ====================================================
class DashboardData:
    def __init__(self):
        self.DF = {}
        self.Lists = {}
        self.Tree = {}
        self.load()

    def load(self):
        print("--- Loading Data (v3.1) ---")
        tables = [
            'attendance', 'users_details', 'leave_applications', 'date_table', 
            'leave_balance', 'project_allocations', 'projects_details'
        ]
        for f in tables:
            pq = os.path.join(DATA_DIR, f"{f}.parquet")
            cv = os.path.join(DATA_DIR, f"{f}.csv")
            if os.path.exists(pq):
                self.DF[f] = pd.read_parquet(pq)
            elif os.path.exists(cv):
                self.DF[f] = pd.read_csv(cv, low_memory=False)
            else:
                self.DF[f] = pd.DataFrame()
            print(f"Loaded {f}: {len(self.DF[f])} rows")

        # Process Users (Source of Truth - Filter for 203 Active Employees)
        if not self.DF['users_details'].empty:
            ud = self.DF['users_details']
            if 'employee_status' in ud.columns:
                ud = ud[ud['employee_status'] == 'Active'].copy()
                print(f"Filtered to {len(ud)} Active Employees")
            
            cols = ['user_id', 'department_name', 'employee_name', 'reporting_manager_name', 'employment_type', 'email']
            avail_cols = [c for c in cols if c in ud.columns]
            u = ud[avail_cols].copy()
            for col in avail_cols:
                if col not in ['user_id', 'email']: u[f"{col}_t"] = u[col].astype(str).str.title().str.strip()
            
            # Map Projects & Managers from project_details
            if not self.DF['projects_details'].empty:
                pd_dt = self.DF['projects_details'][['name', 'project_name', 'owner']].copy()
                # Map Owner (Email) to Full Name using UD (Full List for manager mapping)
                ud_map = self.DF['users_details'][['email', 'full_name']].dropna().drop_duplicates('email')
                pd_dt = pd_dt.merge(ud_map, left_on='owner', right_on='email', how='left')
                pd_dt = pd_dt.rename(columns={'name': 'proj_id', 'full_name': 'project_manager'})
                self.DF['projects_details_mapped'] = pd_dt

            # Join Title Cased columns back to UD
            self.DF['users_details'] = ud.merge(u, on=['user_id', 'email'], how='left', suffixes=('', '_dup'))
            
            # Joins to Charts Data (Using INNER join to restrict to 203 active employees)
            for k in ['leave_applications', 'attendance', 'leave_balance']:
                if k in self.DF and not self.DF[k].empty:
                    df_target = self.DF[k]
                    id_col = 'User Id' if k == 'leave_applications' else ('user_id' if 'user_id' in df_target.columns else ('EmployeeId' if 'EmployeeId' in df_target.columns else None))
                    
                    if id_col and id_col in df_target.columns:
                        df_target = df_target.drop(columns=[c for c in u.columns if c in df_target.columns and c != id_col and c != 'email'], errors='ignore')
                        self.DF[k] = df_target.merge(u, left_on=id_col, right_on="user_id", how="inner")
                    elif 'Employee Name' in df_target.columns:
                        # Fallback for leave_balance using exact name join
                        self.DF[k] = df_target.merge(u, left_on='Employee Name', right_on='employee_name', how='inner')

        # Dates & Keys
        for k in ['attendance', 'leave_applications']:
            if not self.DF[k].empty:
                d_col = 'attendance_date' if k == 'attendance' else 'Leave Application Date'
                if d_col in self.DF[k].columns:
                    self.DF[k]['dt'] = pd.to_datetime(self.DF[k][d_col], errors='coerce')
                    # Use 'from_date' for leave applications YM_KEY if available for better period alignment
                    dt_col = 'from_date' if 'from_date' in self.DF[k].columns else 'dt'
                    self.DF[k]['YM_KEY'] = pd.to_datetime(self.DF[k][dt_col]).dt.year.astype(str) + "_" + pd.to_datetime(self.DF[k][dt_col]).dt.month_name()

        # Hierarchy for Slicer (Same logic)
        if not self.DF['date_table'].empty:
            d = self.DF['date_table'].copy()
            d['dt'] = pd.to_datetime(d['Date'], errors='coerce').dropna()
            d['YM_KEY'] = d['dt'].dt.year.astype(str) + "_" + d['dt'].dt.month_name()
            d['Year'] = d['dt'].dt.year.astype(str)
            d['Qtr'] = "Qtr " + d['dt'].dt.quarter.astype(str)
            d['Month'] = d['dt'].dt.month_name()
            d = d.sort_values('dt')
            self.DF['date_table'] = d
            for _, r in d[['Year', 'Qtr', 'Month']].drop_duplicates().iterrows():
                y, q, m = r['Year'], r['Qtr'], r['Month']
                if y not in self.Tree: self.Tree[y] = {}
                if q not in self.Tree[y]: self.Tree[y][q] = []
                if m not in self.Tree[y][q]: self.Tree[y][q].append(m)

        # Process Project Allocations (Flatten nested JSON strings)
        if 'project_allocations' in self.DF and not self.DF['project_allocations'].empty:
            alloc = self.DF['project_allocations']
            flattened = []
            for _, r in alloc.iterrows():
                try:
                    p_list = r['project_allocations']
                    if isinstance(p_list, str): p_list = ast.literal_eval(p_list)
                    if isinstance(p_list, list):
                        for p in p_list:
                            flattened.append({
                                'user_id': r['user_id'],
                                'proj_id': p.get('project'),
                                'proj_name': p.get('project_name')
                            })
                except: pass
            self.DF['alloc_mapped'] = pd.DataFrame(flattened).drop_duplicates()

        # Slicer Lists
        ud = self.DF.get('users_details', pd.DataFrame())
        pdm = self.DF.get('projects_details_mapped', pd.DataFrame())
        
        # Helper to get unique sorted list safely
        def get_list(df, col):
            if df.empty or col not in df.columns: return []
            return sorted(df[col].dropna().unique().tolist())

        self.Lists = {
            'D': get_list(ud, 'department_name_t'),
            'E': get_list(ud, 'employee_name_t'),
            'M': get_list(ud, 'reporting_manager_name_t'),
            'ET': get_list(ud, 'employment_type_t'),
            'PN': get_list(pdm, 'project_name') if not pdm.empty else [],
            'PM': get_list(pdm, 'project_manager') if not pdm.empty else [],
            'WS': get_list(self.DF.get('attendance', pd.DataFrame()), 'workflow_state'),
            'LT': [str(x).title() for x in get_list(self.DF.get('leave_applications', pd.DataFrame()), 'leave_type')],
            'AT': [str(x).title() for x in get_list(self.DF.get('attendance', pd.DataFrame()), 'mode_of_attendance')]
        }

DB = DashboardData()

# ====================================================
#   UI HELPERS
# ====================================================
def slicer_box(label, id, choices, selected="All", cls=""):
    return ui.div({"class": f"slicer-box {cls}"}, ui.span(label, class_="slicer-label"), ui.input_select(id, "", ["All"] + choices, selected=selected))

def period_ui():
    """Returns the placeholder for the dynamic period content."""
    return ui.output_ui("ui_period_popover_content")

# ====================================================
#   APP UI
# ====================================================
app_ui = ui.page_fluid(
    ui.tags.head(
        ui.tags.link(rel="stylesheet", href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap")
    ),
    ui.tags.style("""
        :root {
            --primary: #1f3d7a;
            --accent: #F6A9CA;
            --bg: #f8fafc;
            --card-bg: #ffffff;
            --text-main: #334155;
            --text-light: #64748b;
            --shadow: 0 4px 6px -1px rgb(0 0 0 / 0.1), 0 2px 4px -2px rgb(0 0 0 / 0.1);
            --shadow-md: 0 10px 15px -3px rgb(0 0 0 / 0.1), 0 4px 6px -4px rgb(0 0 0 / 0.1);
        }

        body { 
            background: var(--bg); 
            font-family: 'Inter', sans-serif; 
            color: var(--text-main);
            margin: 0;
            padding: 0;
        }

        /* Modern Header */
        .header { 
            background: linear-gradient(135deg, #1f3d7a 0%, #2e5cb8 100%); 
            color: white; 
            padding: 12px 30px; 
            display: flex; 
            align-items: center; 
            justify-content: center; 
            position: relative; 
            box-shadow: var(--shadow);
            margin-bottom: 20px;
        }
        .title { 
            font-size: 1.4rem; 
            font-weight: 700; 
            letter-spacing: -0.01em; 
        }

        /* Modern Slicer Row */
        .slicer-row { 
            display: flex; 
            gap: 12px; 
            padding: 12px 20px; 
            background: white; 
            border-bottom: 1px solid #e2e8f0; 
            overflow-x: auto; 
            white-space: nowrap;
            margin-bottom: 20px;
            box-shadow: var(--shadow);
            align-items: center;
        }
        
        .slicer-box { 
            display: flex;
            flex-direction: column;
            justify-content: center;
            flex: 1 1 0px;
            min-width: 140px; 
            height: 68px;
            background: white; 
            border-radius: 12px; 
            padding: 14px 12px 10px 12px;
            border: 1px solid var(--accent);
            text-align: center;
            transition: all 0.25s cubic-bezier(0.4, 0, 0.2, 1);
            box-shadow: 0 4px 6px -1px rgb(0 0 0 / 0.05);
        }
        .slicer-box:hover {
            transform: translateY(-2px);
            box-shadow: 0 10px 15px -3px rgb(0 0 0 / 0.1);
            filter: brightness(0.98);
            z-index: 5;
        }
        .slicer-label { 
            display: block; 
            font-size: 0.65rem; 
            font-weight: 700; 
            color: #64748b; 
            margin-bottom: 2px; 
            text-transform: uppercase; 
            letter-spacing: 0.06em;
            line-height: 1.2;
        }
        .slicer-box select, .tree-btn { 
            width: 100%; 
            border: none !important; 
            font-size: 0.9rem; 
            background: transparent !important; 
            color: var(--primary); 
            font-weight: 700;
            outline: none; 
            padding: 0 !important;
            cursor: pointer;
            text-align: center;
            text-align-last: center;
            height: 30px;
            line-height: 30px;
        }
        .slicer-box select option {
            text-align: center;
        }

        /* Modern Tabs */
        .nav-tabs { 
            border: none; 
            gap: 8px; 
            padding: 0 20px; 
            position: sticky; 
            top: 0; 
            background: var(--bg); 
            z-index: 100;
            margin-bottom: 15px;
        }
        .nav-link { 
            border: none !important; 
            border-radius: 10px !important; 
            color: var(--text-light) !important; 
            font-weight: 600; 
            padding: 8px 20px; 
            background: white !important;
            box-shadow: var(--shadow);
            transition: all 0.2s;
        }
        .nav-link.active { 
            background: var(--accent) !important; 
            color: white !important;
        }

        /* Modern Cards */
        .card { 
            background: white; 
            border-radius: 14px; 
            padding: 20px; 
            border: 1px solid #f1f5f9; 
            margin-bottom: 20px; 
            box-shadow: var(--shadow); 
            transition: box-shadow 0.3s;
        }
        .card:hover { box-shadow: var(--shadow-md); }
        .card-title { 
            color: var(--primary); 
            font-weight: 700; 
            font-size: 1rem; 
            margin-bottom: 12px; 
            border-left: 4px solid var(--accent); 
            padding-left: 10px; 
        }

        .tree-container { min-width: 220px; padding: 15px; }
        table { font-size: 0.85rem !important; width: 100%; border-collapse: collapse; }
        th { background: #f8fafc; color: var(--primary); font-weight: 700; padding: 12px; border-bottom: 2px solid #e2e8f0; text-align: center; }
        td { padding: 12px; border-bottom: 1px solid #f1f5f9; white-space: nowrap; text-align: center; color: var(--text-main); }
        .total-row { font-weight: 700; background: #f8fafc; }
        .table-scroll { overflow: auto; max-height: 480px; border: 1px solid #e2e8f0; border-radius: 12px; }
        table thead th { position: sticky; top: 0; background: #f8fafc; z-index: 10; }
        table thead th:first-child { position: sticky; left: 0; z-index: 20; text-align: center; }
        table tbody tr td:first-child { position: sticky; left: 0; background: #fff; z-index: 5; font-weight: 600; text-align: center; }
        
        /* Modern Buttons */
        .btn-primary { 
            background: var(--primary) !important; 
            border: none !important; 
            border-radius: 8px !important; 
            font-weight: 600 !important; 
            padding: 10px 24px !important; 
            transition: all 0.2s !important;
            box-shadow: var(--shadow);
            color: white !important;
        }
        .btn-primary:hover { 
            background: #2e5cb8 !important; 
            transform: translateY(-1px);
            box-shadow: var(--shadow-md);
        }
        
        /* Custom Context Menu Styling */
        .custom-menu {
            display: none;
            z-index: 2000;
            position: absolute;
            background-color: #fff;
            border: 1px solid #e2e8f0;
            box-shadow: var(--shadow-md);
            padding: 6px 0;
            border-radius: 8px;
            min-width: 160px;
        }
        .custom-menu li {
            padding: 10px 16px;
            cursor: pointer;
            list-style-type: none;
            font-size: 0.85rem;
            color: #1f3d7a;
            font-weight: 600;
            transition: all 0.2s;
        }
        .custom-menu li:hover {
            background-color: #F6A9CA;
            color: white;
        }
    """),

    # Drill-Through Bridge Script (Right Click Context Menu)
    ui.tags.script("""
        // Suppress harmless internal library noise (bootstrap-datepicker / anywidget)
        (function() {
            const wrap = (orig, pattern) => function(...args) {
                if (args[0] && typeof args[0] === 'string' && pattern.test(args[0])) return;
                orig.apply(console, args);
            };
            console.warn = wrap(console.warn, /DEPRECATED|bootstrap-datepicker/);
            console.log = wrap(console.log, /anywidget/);
        })();

        $(document).on('shiny:connected', function(event) {
            if (!$('#drill-menu').length) {
                $('<ul id="drill-menu" class="custom-menu"><li id="do-drill">Drill through</li></ul>').appendTo('body');
            }

            setInterval(function() {
                var attCharts = ['plt_wfh_comp', 'plt_hrs_dist', 'plt_daily_att'];
                var analysisCharts = ['plt_avail'];
                var summaryCharts = ['plt_trend', 'plt_util', 'plt_top'];
                
                attCharts.concat(analysisCharts).concat(summaryCharts).forEach(function(id) {
                    var container = document.getElementById(id);
                    if (!container) return;
                    var plot = container.querySelector('.js-plotly-plot');
                    
                    if (plot && typeof plot.on === 'function' && !plot.getAttribute('data-drill-bound')) {
                        plot.setAttribute('data-drill-bound', 'true');
                        
                        // Left Click - Direct Drill
                        plot.on('plotly_click', function(data) {
                            if(data.points && data.points.length > 0) {
                                var pt = data.points[0];
                                if (pt.customdata) {
                                    var type = (id === 'plt_wfh_comp') ? 'WFH' : (id === 'plt_hrs_dist' ? 'HRS' : (id === 'plt_daily_att' ? 'DAILY' : id));
                                    var isSummary = summaryCharts.includes(id);
                                    var isAnalysis = analysisCharts.includes(id);
                                    
                                    var payload = { type: type };
                                    
                                    // 1. Map Bucket/Category
                                    if (type === 'HRS') {
                                        payload.bucket = pt.customdata[0]; // Office Hrs Bucket
                                    } else if (type === 'plt_top') {
                                        payload.bucket = pt.y; // Employee Name (displayed on Y)
                                        payload.month = pt.customdata[0]; // Logic uses month for name
                                    } else if (type === 'DAILY' || type === 'plt_avail') {
                                        payload.bucket = pt.customdata[1]; // Category/Presence Type
                                    } else {
                                        payload.bucket = pt.customdata[1] || pt.customdata[0] || pt.y;
                                    }
                                    
                                    // 2. Map Date/Month
                                    if (type === 'DAILY' || type === 'plt_avail') {
                                        payload.date = pt.customdata[0];
                                    } else if (type === 'WFH' || type === 'plt_trend' || type === 'plt_util') {
                                        payload.month = pt.customdata[0];
                                    }
                                    
                                    var inputName = isSummary ? 'summary_drill_event' : (isAnalysis ? 'leave_drill_event' : 'drill_event');
                                    Shiny.setInputValue(inputName, payload, {priority: 'event'});
                                }
                            }
                        });

                        // Right Click - Context Menu
                        plot.addEventListener('contextmenu', function(e) {
                            e.preventDefault();
                            var hoverData = plot.hoverData;
                            if (hoverData && hoverData.length > 0) {
                                var pt = hoverData[0];
                                if (pt.customdata) {
                                    var type = (id === 'plt_wfh_comp') ? 'WFH' : (id === 'plt_hrs_dist' ? 'HRS' : (id === 'plt_daily_att' ? 'DAILY' : id));
                                    var isSummary = summaryCharts.includes(id);
                                    var isAnalysis = analysisCharts.includes(id);
                                    
                                    var payload = { type: type };
                                    
                                    // Match Left-Click Mapping
                                    if (type === 'HRS') payload.bucket = pt.customdata[0];
                                    else if (type === 'plt_top') { payload.bucket = pt.y; payload.month = pt.customdata[0]; }
                                    else if (type === 'DAILY' || type === 'plt_avail') payload.bucket = pt.customdata[1];
                                    else payload.bucket = pt.customdata[1] || pt.customdata[0];
                                    
                                    if (type === 'DAILY' || type === 'plt_avail') payload.date = pt.customdata[0];
                                    else if (type === 'WFH' || type === 'plt_trend' || type === 'plt_util') payload.month = pt.customdata[0];
                                    
                                    var mode = isSummary ? 'SUMMARY' : (isAnalysis ? 'LEAVE' : 'ATT');
                                    $('#drill-menu').data('eventData', {payload: payload, mode: mode});
                                    $('#drill-menu').css({
                                        top: e.pageY + "px",
                                        left: e.pageX + "px",
                                        display: "block"
                                    });
                                }
                            }
                        });
                    }
                });
            }, 1000);

            $(document).on('mousedown', function(e) {
                if (!$(e.target).closest("#drill-menu").length) {
                    $("#drill-menu").hide();
                }
            });

            $(document).on('click', '#do-drill', function() {
                var data = $('#drill-menu').data('eventData');
                if (data && data.payload) {
                    var inputName = 'drill_event';
                    if (data.mode === 'SUMMARY') inputName = 'summary_drill_event';
                    else if (data.mode === 'LEAVE') inputName = 'leave_drill_event';
                    
                    Shiny.setInputValue(inputName, data.payload, {priority: 'event'});
                }
                $("#drill-menu").hide();
            });

            // Handle selection reset
            Shiny.addCustomMessageHandler('deselectplots', function(msg) {
                var allCharts = ['plt_wfh_comp', 'plt_hrs_dist', 'plt_daily_att', 'plt_avail', 'plt_trend', 'plt_util', 'plt_top'];
                allCharts.forEach(function(id) {
                    var container = document.getElementById(id);
                    if (container) {
                        var plot = container.querySelector('.js-plotly-plot');
                        if (plot && typeof Plotly !== 'undefined') {
                            Plotly.restyle(plot, {selectedpoints: [null]});
                        }
                    }
                });
            });
        });
    """),
    
    ui.div({"class": "header"}, 
        ui.div("QBA Leave & Attendance Dashboard", class_="title")
    ),
    
    # Global Synced Slicer Row (Static for 100% Persistence)
    ui.div({"class": "slicer-row"},
        # 1. Period (Universal)
        ui.div({"class": "slicer-box"}, ui.span("Period", class_="slicer-label"), 
            ui.popover(
                ui.tags.button(ui.output_text("txt_period"), class_="tree-btn"), 
                ui.div({"class": "tree-container"}, period_ui()), 
                placement="bottom"
            )
        ),
        # 2. Universal Slicers
        slicer_box("Department", "s_dept", DB.Lists['D']),
        slicer_box("Employee Name", "s_emp", DB.Lists['E']),
        slicer_box("Employment Type", "s_et", DB.Lists['ET']),
        
        # 3. Conditional Slicers (JS Toggle prevents DOM destruction)
        ui.panel_conditional("input.tabs != 'Attendance'", slicer_box("Leave Type", "s_lt", DB.Lists['LT'])),
        ui.panel_conditional("input.tabs != 'Analysis'", slicer_box("Reporting Manager", "s_mgr", DB.Lists['M'])),
        ui.panel_conditional("input.tabs != 'Analysis'", slicer_box("Attendance Type", "s_at", DB.Lists['AT'])),
        ui.panel_conditional("input.tabs == 'Analysis'", slicer_box("Project Name", "s_proj", DB.Lists['PN'])),
        ui.panel_conditional("input.tabs == 'Analysis'", slicer_box("Project Manager", "s_pm", DB.Lists['PM'])),
        ui.panel_conditional("input.tabs == 'Attendance'", slicer_box("Workflow State", "s_ws", DB.Lists['WS']))
    ),

    ui.navset_tab(
        # --- TAB 1: SUMMARY ---
        ui.nav_panel("Summary",
            ui.div({"style": "padding:15px"},
                ui.div({"class": "card"}, ui.div("Leave Application Trend", class_="card-title"), output_widget("plt_trend")),
                ui.layout_columns(
                    ui.div({"class": "card"}, ui.div("Monthly Leave Utilization Trend", class_="card-title"), output_widget("plt_util")),
                    ui.div({"class": "card"}, ui.div("Top 10 Employees with Frequent Unplanned Leave Instances", class_="card-title"), output_widget("plt_top")),
                    col_widths=[4, 8]
                )
            )
        ),
        ui.nav_panel("Analysis",
            ui.div({"style": "padding:15px"},
                ui.div({"class": "card"}, ui.div("Employees Availability Forecast", class_="card-title"), output_widget("plt_avail")),
                ui.div({"class": "card"}, 
                    ui.div("Department wise Total Leaves by Leavetype", class_="card-title"), 
                    ui.div({"class": "table-scroll"}, ui.output_table("tbl_matrix"))
                )
            )
        ),
        ui.nav_panel("Attendance",
            ui.div({"style": "padding:15px"},
                ui.div({"class": "card"}, ui.div("Daily Attendance Overview", class_="card-title"), output_widget("plt_daily_att")),
                ui.layout_columns(
                    ui.div({"class": "card"}, ui.div("Employee Distribution by Average Office Work Hours", class_="card-title"), output_widget("plt_hrs_dist")),
                    ui.div({"class": "card"}, ui.div("Work From Home Compliance", class_="card-title"), output_widget("plt_wfh_comp")),
                    col_widths=[6, 6]
                )
            )
        ),
        ui.nav_panel("Summary Drill Details",
            ui.div({"style": "padding:15px"},
                ui.div({"class": "card"}, 
                    ui.div(ui.output_text("txt_summary_drill_title"), class_="card-title"),
                    ui.div({"class": "table-scroll"}, ui.output_table("tbl_summary_drill")),
                    ui.div({"style": "margin-top: 15px"}, 
                        ui.input_action_button("btn_summary_back", "Back to Summary", class_="btn-primary")
                    )
                )
            )
        ),
        ui.nav_panel("Attendance Drill Details",
            ui.div({"style": "padding:15px"},
                ui.div({"class": "card"}, 
                    ui.div(ui.output_text("txt_drill_title"), class_="card-title"),
                    ui.div({"class": "table-scroll"}, ui.output_table("tbl_drill")),
                    ui.div({"style": "margin-top: 15px"}, 
                        ui.input_action_button("btn_back", "Back to Dashboard", class_="btn-primary")
                    )
                )
            )
        ),
        ui.nav_panel("Leave Drill Details",
            ui.div({"style": "padding:15px"},
                ui.div({"class": "card"}, 
                    ui.div(ui.output_text("txt_leave_drill_title"), class_="card-title"),
                    ui.div({"class": "table-scroll"}, ui.output_table("tbl_leave_drill")),
                    ui.div({"style": "margin-top: 15px"}, 
                        ui.input_action_button("btn_leave_back", "Back to Analysis", class_="btn-primary")
                    )
                )
            )
        ),
        id="tabs"
    )
)

# ====================================================
#   SERVER
# ====================================================
def server(input, output, session):
    
    # Initialize Persistent Filter State
    S_STATE = {
        'year': reactive.Value("2025"),
        'qtr': reactive.Value("Qtr 4"),
        'month': reactive.Value([]),
        'dept': reactive.Value("All"),
        'emp': reactive.Value("All"),
        'et': reactive.Value("All"),
        'lt': reactive.Value("All"),
        'mgr': reactive.Value("All"),
        'proj': reactive.Value("All"),
        'pm': reactive.Value("All"),
        'ws': reactive.Value("All"),
        'at': reactive.Value("All")
    }
    
    # Drill-Through State
    DRILL_DATA = reactive.Value({'month': None, 'bucket': None})
    LEAVE_DRILL_DATA = reactive.Value({'date': None, 'bucket': None, 'type': None})
    SUMMARY_DRILL_DATA = reactive.Value({'month': None, 'bucket': None, 'type': None})

    # Navigation Listener
    @reactive.effect
    @reactive.event(input.btn_back)
    def _go_back():
        ui.update_navset("tabs", selected="Attendance")
        session.send_custom_message("deselectplots", {})

    @reactive.effect
    @reactive.event(input.btn_leave_back)
    def _go_back_leave():
        ui.update_navset("tabs", selected="Analysis")
        session.send_custom_message("deselectplots", {})

    @reactive.effect
    @reactive.event(input.btn_summary_back)
    def _go_back_summary():
        ui.update_navset("tabs", selected="Summary")
        session.send_custom_message("deselectplots", {})

    @reactive.effect
    @reactive.event(input.tabs)
    def _tab_changed():
        # Clear selections whenever returning to a main dashboard page
        if input.tabs() in ["Summary", "Analysis", "Attendance"]:
            session.send_custom_message("deselectplots", {})

    # Observers to Capture State Changes and Sync (Surgical to prevent resets during navigation)
    def is_dash():
        return input.tabs() in ["Summary", "Analysis", "Attendance"]

    @reactive.effect
    @reactive.event(input.s_year)
    def _sync_y():
        if not is_dash(): return
        v = input.s_year()
        if v is not None:
            with reactive.isolate():
                if v != S_STATE['year'](): S_STATE['year'].set(v)

    @reactive.effect
    @reactive.event(input.s_qtr)
    def _sync_q():
        if not is_dash(): return
        v = input.s_qtr()
        if v is not None:
            with reactive.isolate():
                if v != S_STATE['qtr'](): S_STATE['qtr'].set(v)

    @reactive.effect
    @reactive.event(input.s_month)
    def _sync_m():
        if not is_dash(): return
        v = input.s_month()
        if v is None: return
        v = list(v)
        with reactive.isolate():
            if v != S_STATE['month'](): S_STATE['month'].set(v)

    # General Slicer Syncers
    def create_syncer(inp_id, state_key):
        @reactive.effect
        @reactive.event(getattr(input, inp_id))
        def _sync():
            if not is_dash(): return
            v = getattr(input, inp_id)()
            if v is not None:
                with reactive.isolate():
                    if v != S_STATE[state_key]():
                        S_STATE[state_key].set(v)
        return _sync

    _sync_dept = create_syncer('s_dept', 'dept')
    _sync_emp = create_syncer('s_emp', 'emp')
    _sync_et = create_syncer('s_et', 'et')
    _sync_lt = create_syncer('s_lt', 'lt')
    _sync_mgr = create_syncer('s_mgr', 'mgr')
    _sync_proj = create_syncer('s_proj', 'proj')
    _sync_pm = create_syncer('s_pm', 'pm')
    _sync_ws = create_syncer('s_ws', 'ws')
    _sync_at = create_syncer('s_at', 'at')

    def filter_df(raw_df):
        if raw_df is None or raw_df.empty: return pd.DataFrame()
        df = raw_df.copy()
        try:
            # 1. Period Filter
            y, q, m = S_STATE['year'](), S_STATE['qtr'](), S_STATE['month']()
            # print(f"DEBUG Filter: Y={y}, Q={q}, M={m}")
            
            if y != "All" and y:
                if q != "All" and q in DB.Tree.get(y, {}):
                    available_months = DB.Tree[y][q]
                    sel_m = m if (m and len(m) > 0) else available_months
                    keys = [f"{y}_{mon}" for mon in sel_m]
                else:
                    keys = [f"{y}_{mon}" for qtr in DB.Tree.get(y, {}) for mon in DB.Tree[y][qtr]]
                
                # print(f"DEBUG: Filtering for keys {keys}")
                
                # Multi-column period filtering strategy
                matched = False
                for col in ['YM_KEY', 'dt', 'from_date', 'to_date']:
                    if col in df.columns:
                        if col == 'YM_KEY':
                            df = df[df['YM_KEY'].isin(keys)]
                        else:
                            # Generate key on the fly for date columns
                            tmp_keys = pd.to_datetime(df[col]).dt.year.astype(str) + "_" + pd.to_datetime(df[col]).dt.month_name()
                            df = df[tmp_keys.isin(keys)]
                        matched = True
                        break
                
                if not matched:
                    print(f"WARNING: No date column found for period filter in dataframe with columns: {df.columns.tolist()}")
            
            # 2. General Slicers
            if S_STATE['dept']() != "All": df = df[df['department_name_t'] == S_STATE['dept']()]
            if S_STATE['emp']() != "All": df = df[df['employee_name_t'] == S_STATE['emp']()]
            if S_STATE['et']() != "All": df = df[df['employment_type_t'] == S_STATE['et']()]
            if S_STATE['mgr']() != "All": df = df[df['reporting_manager_name_t'] == S_STATE['mgr']()]
            if S_STATE['ws']() != "All" and 'workflow_state' in df.columns: df = df[df['workflow_state'] == S_STATE['ws']()]
            
            # 3. Leave/Att Specific
            if 'leave_type' in df.columns and S_STATE['lt']() != "All":
                df = df[df['leave_type'].astype(str).str.title() == S_STATE['lt']()]
            
            at_val = S_STATE['at']()
            if at_val != "All":
                if 'mode_of_attendance' in df.columns:
                    df = df[df['mode_of_attendance'].astype(str).str.title() == at_val]
                else:
                    # Indirect filtering: filter by users who have entries for this attendance type
                    att_raw = DB.DF.get('attendance', pd.DataFrame())
                    if not att_raw.empty:
                        match_ids = att_raw[att_raw['mode_of_attendance'].astype(str).str.title() == at_val]['user_id'].unique()
                        u_col = 'user_id' if 'user_id' in df.columns else ('User Id' if 'User Id' in df.columns else None)
                        if u_col: df = df[df[u_col].isin(match_ids)]
            
            # 4. Project Filters (Using Allocation mapping)
            proj = S_STATE['proj']()
            pm = S_STATE['pm']()
            
            if proj != "All" or pm != "All":
                pdm = DB.DF.get('projects_details_mapped', pd.DataFrame())
                am = DB.DF.get('alloc_mapped', pd.DataFrame())
                if not pdm.empty and not am.empty:
                    p_mask = pd.Series(True, index=pdm.index)
                    if proj != "All": p_mask &= (pdm['project_name'].astype(str).str.title() == proj)
                    if pm != "All": p_mask &= (pdm['project_manager'].astype(str).str.title() == pm)
                    
                    target_projs = pdm[p_mask]['proj_id'].tolist()
                    target_users = am[am['proj_id'].isin(target_projs)]['user_id'].unique().tolist()
                    
                    user_col = 'user_id' if 'user_id' in df.columns else ('User Id' if 'User Id' in df.columns else None)
                    if user_col: df = df[df[user_col].isin(target_users)]

        except Exception as e: print(f"Filter Error: {e}")
        return df


    @reactive.calc
    def f_leave(): return filter_df(DB.DF.get('leave_applications', pd.DataFrame()))
    @reactive.calc
    def f_att(): return filter_df(DB.DF.get('attendance', pd.DataFrame()))
    @reactive.calc
    def f_lb(): return filter_df(DB.DF.get('leave_balance', pd.DataFrame()))

    @output
    @render.ui
    def ui_period_popover_content():
        # Removed input.tabs() to prevent re-render (and closure) of popover during navigation.
        # This keeps the period selection perfectly persistent across pages.
        
        # Render hierarchy based on current Year/Qtr
        years = sorted(list(DB.Tree.keys()), reverse=True)
        y = S_STATE['year']()
        
        controls = [ui.input_select("s_year", "Select Year", ["All"] + years, selected=y)]
        
        if y != "All" and y in DB.Tree:
            qtrs = list(DB.Tree[y].keys())
            q = S_STATE['qtr']()
            if q != "All" and q not in qtrs: q = "Qtr 4" if "Qtr 4" in qtrs else "All"
            controls.append(ui.input_select("s_qtr", "Select Quarter", ["All"] + qtrs, selected=q))
            
            if q != "All" and q in DB.Tree[y]:
                months = DB.Tree[y][q]
                with reactive.isolate():
                    m = S_STATE['month']()
                # Ensure selected months exist in current quarter
                final_m = [mon for mon in m if mon in months]
                if not final_m: final_m = months
                controls.append(ui.input_select("s_month", "Select Month(s)", months, multiple=True, selected=final_m))
        
        return ui.TagList(*controls)

    @render.text
    def txt_period():
        y, q, m = S_STATE['year'](), S_STATE['qtr'](), S_STATE['month']()
        if y == "All": return "All Time"
        if q == "All": return y
        return f"{y} {q}" + (f" ({len(m)})" if m else "")

    def stylize(fig):
        fig.update_layout(
            template="plotly_white",
            margin=dict(l=20, r=20, t=10, b=20),
            font=dict(family="Inter, sans-serif", color="#334155", size=11),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="center",
                x=0.5,
                title_text='',
                font=dict(size=10)
            ),
            hoverlabel=dict(
                bgcolor="white",
                font_size=12,
                font_family="Inter, sans-serif"
            )
        )
        fig.update_xaxes(showline=True, linecolor="#e2e8f0", gridcolor="#f1f5f9")
        fig.update_yaxes(showline=True, linecolor="#e2e8f0", gridcolor="#f1f5f9")
        return fig

    # --- TAB 1 (Summary) ---
    @render_plotly
    def plt_trend():
        df = f_leave().copy()
        if df.empty or 'dt' not in df.columns: return px.bar()
        df = df[df['status'].isin(['Approved', 'Open'])]
        if df.empty: return px.bar()
        
        # 0. Prep columns
        df['Month_Year'] = df['dt'].dt.strftime('%b %Y')
        # Sort by actual date and filter by period strictly
        y, q, m = S_STATE['year'](), S_STATE['qtr'](), S_STATE['month']()
        
        if y != "All":
            if q != "All":
                target_months = m if (m and len(m) > 0) else DB.Tree[y][q]
            else:
                target_months = [mon for qtr in DB.Tree[y] for mon in DB.Tree[y][qtr]]
            
            trend_keys = [f"{mon[:3]} {y}" for mon in target_months]
            df = df[df['Month_Year'].isin(trend_keys)]
        
        if df.empty: return px.bar()
        
        # Determine sorted month order from filtered data
        month_order = df.sort_values('dt')['Month_Year'].unique().tolist()

        c = df.groupby(['Month_Year', 'Leave Application Category'], sort=False).size().reset_index(name='Count')
        fig = px.bar(c, x='Month_Year', y='Count', color='Leave Application Category', barmode='group', text_auto=True, 
                     color_discrete_map={"Applied Before Availing": "#00adef", "Applied Post Availing": "#1f3d7a"},
                     custom_data=['Month_Year', 'Leave Application Category'])
        fig.update_layout(xaxis={'categoryorder':'array', 'categoryarray': month_order},
                          xaxis_title="Month", yaxis_title="Application Count",
                          clickmode='event')
        fig.update_traces(
            textfont=dict(size=10, weight="bold"),
            hovertemplate="<b>%{x}</b><br>Category: %{customdata[1]}<br>Count: %{y}<br><i>Click to Drill Through</i><extra></extra>"
        )
        return stylize(fig)

    @render_plotly
    def plt_util():
        df = f_leave().copy()
        if df.empty or 'dt' not in df.columns: return px.line()
        df = df[df['status'].isin(['Approved', 'Open'])]
        if df.empty: return px.line()

        # 0. Prep columns
        df['Month_Year'] = df['dt'].dt.strftime('%b %Y')
        
        # 1. Filter by period strictly
        y, q, m = S_STATE['year'](), S_STATE['qtr'](), S_STATE['month']()
        if y != "All":
            if q != "All":
                target_months = m if (m and len(m) > 0) else DB.Tree[y][q]
            else:
                target_months = [mon for qtr in DB.Tree[y] for mon in DB.Tree[y][qtr]]
            
            trend_keys = [f"{mon[:3]} {y}" for mon in target_months]
            df = df[df['Month_Year'].isin(trend_keys)]

        if df.empty: return px.line()
        
        # 2. Total Leave Hours calculation
        h_col = 'Total Leave hrs' if 'Total Leave hrs' in df.columns else ('total_leave_hrs' if 'total_leave_hrs' in df.columns else None)
        if h_col:
            df['Hours'] = df[h_col].fillna(0).astype(float)
        else:
            val_col = 'Total Leave Days' if 'Total Leave Days' in df.columns else 'total_leave_days'
            df['Hours'] = df[val_col].fillna(0).astype(float) * 8
            
        res_leave = df.groupby('Month_Year', sort=False)['Hours'].sum().reset_index(name='Total Leave Hours')

        # 3. Monthly Capacity calculation (Active EMP * 8 * Working Days)
        dt_df = filter_df(DB.DF.get('date_table', pd.DataFrame()))
        if dt_df.empty: return px.line()
        
        # Map date_table to Month_Year
        dt_df['Month_Year'] = pd.to_datetime(dt_df['dt']).dt.strftime('%b %Y')
        res_wd = dt_df[dt_df['IsWorkingDay'] == 1].groupby('Month_Year').size().reset_index(name='Working Days')
        
        # Active Employees (Filtered by current slicers via filter_df)
        active_emp_count = len(filter_df(DB.DF['users_details']))
        
        # 4. Merge and Calculate Impact
        res = res_leave.merge(res_wd, on='Month_Year', how='inner')
        res['Active EMP'] = active_emp_count
        res['Total Available Org Hours'] = res['Active EMP'] * 8 * res['Working Days']
        res['Leave Impact %'] = (res['Total Leave Hours'] / res['Total Available Org Hours'].replace(0, 1)) * 100
        
        # Determine sorted month order from filtered data for X-axis
        month_order = df.sort_values('dt')['Month_Year'].unique().tolist()
        
        fig = px.line(res, x='Month_Year', y='Leave Impact %', text=res['Leave Impact %'].map('{:.2f}%'.format), markers=True,
                      custom_data=['Month_Year', 'Total Leave Hours', 'Total Available Org Hours', 'Working Days'])
        
        tooltip = (
            "<b>%{customdata[0]}</b><br>" +
            "Total Leave Hours: %{customdata[1]:.2f}<br>" +
            "Working Days: %{customdata[3]}<br>" +
            "Total Available Org Hours: %{customdata[2]:,.0f}<br>" +
            "Leave Impact %: %{y:.2f}%<br>" +
            "<i>Click to Drill Through</i><extra></extra>"
        )
        
        fig.update_traces(line=dict(color="#00adef", width=3), textposition='top center', 
                          textfont=dict(size=10, weight="bold"),
                          hovertemplate=tooltip)
        fig.update_yaxes(ticksuffix="%", griddash="dot")
        fig.update_layout(xaxis={'categoryorder':'array', 'categoryarray': month_order},
                          xaxis_title="Month", yaxis_title="Leave Utilization Impact %",
                          clickmode='event')
        return stylize(fig)

    @render_plotly
    def plt_top():
        df = f_leave().copy()
        if df.empty: return px.bar()
        
        # 1. Filter: Valid Status (Strictly following DAX Logic)
        # DAX Logic: Status IN {"Approved", "Open"}
        un = df[df['status'].isin(['Approved', 'Open'])]
        if un.empty: return px.bar(title="No Approved/Open Leaves Found")
            
        val_col = 'Total Leave Days' if 'Total Leave Days' in un.columns else 'total_leave_days'
        
        # 2. Aggregate Metrics (Matching User DAX)
        # Instances = COUNTROWS, Days = SUM(Total Leave Days)
        top = un.groupby('employee_name_t').agg(**{
            "Leave Instances": ('user_id', 'count'), 
            "Leave Days": (val_col, 'sum')
        }).reset_index()
        
        # 3. Select Top 10 Employees with highest Leave Instances
        top = top.sort_values(['Leave Instances', 'Leave Days'], ascending=[False, False]).head(10)
        
        # 4. Melt for Stacked Bar Chart
        m = top.melt(id_vars='employee_name_t', value_vars=['Leave Instances', 'Leave Days'], 
                     var_name='Metric', value_name='Value')
        
        # Text labels (int for instances, .1f for days)
        m['txt'] = m.apply(lambda r: f"{int(r['Value'])}" if r['Metric'] == 'Leave Instances' else f"{r['Value']:.1f}", axis=1)
        
        fig = px.bar(m, y='employee_name_t', x='Value', color='Metric', 
                     barmode='stack', orientation='h', 
                     text='txt',
                     color_discrete_map={"Leave Instances": "#5FB6FF", "Leave Days": "#1f3d7a"},
                     custom_data=['employee_name_t', 'Metric'])
        
        fig.update_layout(
            xaxis_title="Instances / Total Days",
            yaxis_title="",
            yaxis={'categoryorder':'array', 'categoryarray': top['employee_name_t'].tolist()[::-1]},
            legend_title="",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
            clickmode='event'
        )
        fig.update_traces(
            textfont=dict(size=10, weight="bold"),
            hovertemplate="<b>%{y}</b><br>%{customdata[1]}: %{x}<br><i>Click to Drill Through</i><extra></extra>"
        )
        return stylize(fig)

    # --- TAB 2 (Analysis) ---
    @render_plotly
    def plt_avail():
        # 1. Get Base Date Range from date_table and FILTER FOR WORKING DAYS ONLY
        dates_df = filter_df(DB.DF.get('date_table', pd.DataFrame()))
        if dates_df.empty: return px.bar(title="No Dates in Selected Period")
        dates_df = dates_df[dates_df['IsWorkingDay'] == 1]
        if dates_df.empty: return px.bar(title="No Working Days in Selected Period")
        
        # 2. Get Leave Data
        leaves_df = f_leave().copy()
        leaves_df = leaves_df[leaves_df['status'].isin(['Approved', 'Open'])]
        
        # 3. Get Filtered Total Employee Count
        u_filt = filter_df(DB.DF['users_details'])
        total_count = len(u_filt['user_id'].unique()) if not u_filt.empty else 0
        
        # 4. Map Leaves to Dates
        expanded = []
        if not leaves_df.empty:
            for _, r in leaves_df.iterrows():
                try:
                    for d in pd.date_range(r['from_date'], r['to_date']):
                        expanded.append({'Date': d})
                except: pass
        
        on_leave_counts = pd.DataFrame(expanded).groupby('Date').size().reset_index(name='Employees on Leave') if expanded else pd.DataFrame(columns=['Date', 'Employees on Leave'])
        on_leave_counts['Date'] = pd.to_datetime(on_leave_counts['Date']).dt.normalize()
        
        # 5. Join to Base Date Range
        base = dates_df[['dt']].copy().rename(columns={'dt': 'Date'})
        base['Date'] = base['Date'].dt.normalize()
        
        res = base.merge(on_leave_counts, on='Date', how='left').fillna(0)
        res['Available Employees'] = (total_count - res['Employees on Leave']).clip(lower=0)
        
        # 6. Plot Side-by-Side Bars
        # Use simple day numbers if single month, or short date if multiple
        # Always use day + short month (e.g., "05 Feb") for clear context
        res['DayLabel'] = res['Date'].dt.strftime('%d %b')
            
        m = res.melt(id_vars=['Date', 'DayLabel'], value_vars=['Available Employees', 'Employees on Leave'], 
                     var_name='Category', value_name='Count')
        
        fig = px.bar(m, x='DayLabel', y='Count', color='Category', barmode='group', text_auto=True,
                     color_discrete_map={"Available Employees": "#00adef", "Employees on Leave": "#1f3d7a"},
                     custom_data=['Date', 'Category'])
        
        fig.update_layout(xaxis_title="Day of Month", yaxis_title="Employee Count", 
                          xaxis={'type': 'category'}, clickmode='event',
                          yaxis=dict(range=[0, total_count * 1.15])) # Padding for outside text
        
        fig.update_traces(
            selector=dict(name="Available Employees"),
            textposition='inside', textfont=dict(size=10, weight="bold"), textangle=0,
            hovertemplate="<b>%{x}</b><br>Category: %{customdata[1]}<br>Count: %{y}<br><i>Click to Drill Through</i><extra></extra>"
        )
        fig.update_traces(
            selector=dict(name="Employees on Leave"),
            textposition='outside', textfont=dict(size=10, weight="bold"), textangle=0, cliponaxis=False,
            hovertemplate="<b>%{x}</b><br>Category: %{customdata[1]}<br>Count: %{y}<br><i>Click to Drill Through</i><extra></extra>"
        )
        return stylize(fig)

    @render.table
    def tbl_matrix():
        df = f_leave()
        if df.empty or 'department_name_t' not in df.columns: return pd.DataFrame()
        
        val_col = 'Total Leave Days' if 'Total Leave Days' in df.columns else 'total_leave_days'
        
        # Pivot by Department, Sum of days
        p = df.pivot_table(index='leave_type', columns='department_name_t', values=val_col, aggfunc='sum', fill_value=0)
        
        # Clear index names to prevent rogue headers in some renderers
        p.index.name = None
        p.columns.name = None
        
        # Add Totals
        p.loc['Total'] = p.sum()
        p['Total'] = p.sum(axis=1)
        
        # Format decimals
        p = p.map(lambda x: f"{x:.2f}" if x != 0 else "")
        
        res = p.reset_index().rename(columns={'index': 'Leave type'})
        return res


    # --- TAB 3 (Attendance) ---
    @render_plotly
    def plt_daily_att():
        df = f_att().copy()
        if df.empty or 'dt' not in df.columns: return px.bar()
        
        # Merge with date_table to filter only working days
        dt_ref = DB.DF['date_table'][['dt', 'IsWorkingDay']].copy()
        dt_ref['dt'] = pd.to_datetime(dt_ref['dt']).dt.normalize()
        df['dt_norm'] = df['dt'].dt.normalize()
        
        df = df.merge(dt_ref, left_on='dt_norm', right_on='dt', how='inner', suffixes=('', '_ref'))
        df = df[df['IsWorkingDay'] == 1]
        
        if df.empty: return px.bar(title="No Attendance on Working Days")
        
        # Sort and create day labels
        df = df.sort_values('dt')
        # Always use day + short month (e.g., "05 Feb") for clear context
        df['DayLabel'] = df['dt'].dt.strftime('%d %b')
            
        c = df.groupby(['dt_norm', 'DayLabel', 'presence_type'], sort=False).size().reset_index(name='Count')
        
        # Consistent color map
        colors = {
            "Work From Office": "#00d28d", 
            "Work From Home": "#ff5a5f", 
            "On Duty": "#5c7cfa", 
            "Work From Anywhere": "#ffa94d", 
            "Missed Entry": "#be4bdb"
        }
        
        fig = px.bar(c, x='DayLabel', y='Count', color='presence_type', barmode='stack', text_auto=True,
                     color_discrete_map=colors,
                     custom_data=['dt_norm', 'presence_type'])
        
        fig.update_layout(xaxis_title="Day of Month", yaxis_title="Count", xaxis={'type': 'category'}, clickmode='event')
        fig.update_traces(
            textposition='inside', textfont=dict(size=9, weight="bold"), textangle=0, cliponaxis=False,
            hovertemplate="<b>%{x}</b><br>Type: %{customdata[1]}<br>Count: %{y}<br><i>Click to Drill Through</i><extra></extra>"
        )
        return stylize(fig)

    @render_plotly
    def plt_hrs_dist():
        df = f_att().copy()
        if df.empty: return px.bar()
        
        # 1. Filter: Presence Type = "Work From Office" (Active already filtered in f_att)
        df = df[df['presence_type'] == 'Work From Office']
        if df.empty or 'working_hours' not in df.columns: return px.bar(title="No WFO Data")
        
        # 2. Row-level Bucketing (DAX logic)
        def get_bucket(h):
            if pd.isna(h): return None
            if h < 3: return "< 3 hours"
            if h < 6: return "3-6 hours"
            return "6+ hours"
        
        df['Office Hrs Bucket'] = df['working_hours'].apply(get_bucket)
        df = df.dropna(subset=['Office Hrs Bucket'])
        
        # 3. Aggregate: X = Bucket, Y = Distinct Count of Employees, Tooltip = Avg Hours
        order = ['< 3 hours', '3-6 hours', '6+ hours']
        res = df.groupby('Office Hrs Bucket').agg(
            Total_Emp_WFO=('employee_name_t', 'nunique'),
            Avg_Office_Hours=('working_hours', 'mean')
        ).reindex(order).reset_index()
        
        fig = px.bar(res, x='Office Hrs Bucket', y='Total_Emp_WFO', 
                     text_auto=True,
                     custom_data=['Office Hrs Bucket', 'Avg_Office_Hours'],
                     color_discrete_sequence=["#00adef"])
        
        fig.update_traces(
            textfont=dict(size=10, weight="bold"),
            hovertemplate="<b>%{x}</b><br>Total Emp WFO: %{y}<br>Avg Office Hours: %{customdata[1]:.2f}h<br><i>Click to Drill Through</i><extra></extra>"
        )
        
        fig.update_layout(xaxis_title="Office Hrs Bucket", yaxis_title="Total Emp WFO", clickmode='event')
        return stylize(fig)

    @render_plotly
    def plt_wfh_comp():
        df = f_att().copy()
        if df.empty or 'dt' not in df.columns: return px.bar()
        
        # 1. Month-Year Key
        df['Month_Year'] = df['dt'].dt.strftime('%b %Y')
        
        # 2. Daily WFH Calculation per Employee per Month - VECTORIZED for speed
        df['dt_norm'] = df['dt'].dt.normalize()
        wfh_counts = df[df['presence_type'] == 'Work From Home'].groupby(['Month_Year', 'user_id'])['dt_norm'].nunique().reset_index(name='WFH_Days')
        
        # Get all active users per month to include those with 0 WFH days (essential for parity)
        all_users_month = df.groupby(['Month_Year', 'user_id']).size().reset_index()[['Month_Year', 'user_id']]
        emp_stats = all_users_month.merge(wfh_counts, on=['Month_Year', 'user_id'], how='left').fillna(0)
        
        # 3. Apply Bucket (DAX logic: > 9)
        emp_stats['WFH Bucket'] = emp_stats['WFH_Days'].apply(lambda x: "WFH > 9" if x > 9 else "WFH <= 9")
        
        # Chronological Sort
        df_sorted = df.sort_values('dt')
        month_order = df_sorted['Month_Year'].unique().tolist()
        
        c = emp_stats.groupby(['Month_Year', 'WFH Bucket']).size().reset_index(name='Distinct_Employees')
        
        fig = px.bar(c, x='Month_Year', y='Distinct_Employees', color='WFH Bucket', barmode='group', text_auto=True,
                     color_discrete_map={"WFH > 9": "#ff5a5f", "WFH <= 9": "#1c7ed6"},
                     custom_data=['Month_Year', 'WFH Bucket'])
        
        fig.update_layout(xaxis={'categoryorder':'array', 'categoryarray': month_order},
                          xaxis_title="Month", yaxis_title="Total Count",
                          clickmode='event')
        
        fig.update_traces(
            textfont=dict(size=10, weight="bold"),
            hovertemplate="<b>%{x}</b><br>Bucket: %{customdata[1]}<br>Count: %{y}<br><i>Click to Drill Through</i><extra></extra>"
        )
        return stylize(fig)

    # --- DRILL THROUGH SECTION ---
    
    # Click Event Listener (Via JS Bridge)
    @reactive.effect
    @reactive.event(input.drill_event)
    def _drill_wfh_js():
        ev = input.drill_event()
        # print(f"DEBUG: Drill event received: {ev}")
        if ev and 'bucket' in ev:
            DRILL_DATA.set(ev) # type, bucket, month
            ui.update_navset("tabs", selected="Attendance Drill Details")

    @reactive.effect
    @reactive.event(input.plt_wfh_comp_click)
    def _drill_wfh_fallback():
        click_data = input.plt_wfh_comp_click()
        if click_data and 'points' in click_data:
            pt = click_data['points'][0]
            if 'customdata' in pt:
                DRILL_DATA.set({'type': 'WFH', 'month': pt['customdata'][0], 'bucket': pt['customdata'][1]})
                ui.update_navset("tabs", selected="Attendance Drill Details")

    @reactive.effect
    @reactive.event(input.plt_daily_att_click)
    def _drill_daily_fallback():
        click_data = input.plt_daily_att_click()
        if click_data and 'points' in click_data:
            pt = click_data['points'][0]
            if 'customdata' in pt:
                DRILL_DATA.set({'type': 'DAILY', 'date': pt['customdata'][0], 'bucket': pt['customdata'][1]})
                ui.update_navset("tabs", selected="Attendance Drill Details")

    @render.text
    def txt_drill_title():
        d = DRILL_DATA()
        if not d['bucket']: return "Detailed Drill-Down"
        
        mode_map = {'HRS': 'Work Hours', 'DAILY': 'Daily Attendance', 'WFH': 'WFH'}
        mode = mode_map.get(d.get('type'), "Drill Details")
        
        period = ""
        if d.get('type') == 'DAILY' and d.get('date'):
            period = f" for {pd.to_datetime(d['date']).strftime('%d %b %Y')}"
        elif d.get('month'):
            period = f" for {d['month']}"
            
        return f"Drill Details: {mode} ({d['bucket']}){period}"

    @render.table
    def tbl_drill():
        d = DRILL_DATA()
        if not d['bucket']: return pd.DataFrame()
        
        # 1. Get filtered base data
        df_base = f_att().copy()
        if df_base.empty or 'dt' not in df_base.columns: return pd.DataFrame()
        
        # 2. Setup Metadata from UD
        ud = DB.DF['users_details'][['user_id', 'employee_id', 'employee_name', 'department_name', 'designation']].copy()
        ud = ud.rename(columns={'employee_id': 'EmployeeID'})
        for col in ['employee_name', 'department_name', 'designation']:
            if col in ud.columns: ud[col] = ud[col].astype(str).str.title().str.strip()

        # 3. Mode Processing
        if d.get('type') == 'DAILY':
            # DAILY Mode (Filter by exact date and presence type)
            # Use string-based comparison for robustness against timestamp nuances
            target_date_str = pd.to_datetime(d['date']).strftime('%Y-%m-%d')
            df_m = df_base[pd.to_datetime(df_base['dt']).dt.strftime('%Y-%m-%d') == target_date_str].copy()
            
            # Robust presence_type matching
            res = df_m[df_m['presence_type'].astype(str).str.strip() == str(d['bucket']).strip()].copy()
            metric_col = "Presence Type"
            res[metric_col] = res['presence_type']
            
        elif d.get('type') == 'HRS':
            # OFFICE HOURS Mode
            df_m = df_base[df_base['presence_type'] == 'Work From Office'].copy()
            if df_m.empty: return pd.DataFrame()
            res = df_m.groupby('user_id').agg(Metric_Value=('working_hours', 'mean')).reset_index()
            def get_bucket(h):
                if pd.isna(h): return None
                if h < 3: return "< 3 hours"
                if h < 6: return "3-6 hours"
                return "6+ hours"
            res['Bucket'] = res['Metric_Value'].apply(get_bucket)
            res = res[res['Bucket'] == d['bucket']]
            metric_col = "Avg Office Hours"
            res[metric_col] = res['Metric_Value']
        else:
            # WFH Mode
            if not d.get('month'): return pd.DataFrame()
            df_base['Month_Year'] = df_base['dt'].dt.strftime('%b %Y')
            df_m = df_base[df_base['Month_Year'] == d['month']].copy()
            if df_m.empty: return pd.DataFrame()
            df_m['dt_norm'] = df_m['dt'].dt.normalize()
            wfh_counts = df_m[df_m['presence_type'] == 'Work From Home'].groupby('user_id')['dt_norm'].nunique().reset_index(name='Metric_Value')
            all_u = pd.DataFrame({'user_id': df_m['user_id'].unique()})
            res = all_u.merge(wfh_counts, on='user_id', how='left').fillna(0)
            res['Bucket'] = res['Metric_Value'].apply(lambda x: "WFH > 9" if x > 9 else "WFH <= 9")
            res = res[res['Bucket'] == d['bucket']]
            metric_col = "WFH Days"
            res[metric_col] = res['Metric_Value']

        # 4. Merge Metadata & Format
        # Drop existing identification columns to avoid collisions during merge
        meta_keys = ['employee_id', 'employee_name', 'department_name', 'designation']
        res = res.drop(columns=[c for c in meta_keys if c in res.columns], errors='ignore')
        
        # Merge with fresh metadata
        res = res.merge(ud, on='user_id', how='left')
        
        # Map nice column names and handle missing values
        res['Employee ID'] = res['EmployeeID'].astype(str).replace(['nan', 'None'], 'N/A')
        res['Employee Name'] = res['employee_name'].fillna("Unknown")
        res['User ID'] = res['user_id']
        res['Department'] = res['department_name'].fillna("N/A")
        res['Designation'] = res['designation'].fillna("N/A")
        
        final_cols = ['Employee ID', 'Employee Name', 'User ID', 'Department', 'Designation', metric_col]
        # For Daily mode, we show ALL records to match chart row count. For aggregations, we drop unique.
        if d.get('type') == 'DAILY':
            return res[final_cols].sort_values(['Employee Name'])
        return res[final_cols].drop_duplicates('User ID').sort_values([metric_col, 'Employee Name'], ascending=[False, True])

    @reactive.effect
    @reactive.event(input.leave_drill_event)
    def _drill_leave_js():
        ev = input.leave_drill_event()
        if ev and 'bucket' in ev:
            LEAVE_DRILL_DATA.set(ev)
            ui.update_navset("tabs", selected="Leave Drill Details")

    @render.text
    def txt_leave_drill_title():
        d = LEAVE_DRILL_DATA()
        if not d['bucket']: return "Leave Application Details"
        
        period = ""
        if d.get('type') == 'plt_top':
            period = f" for {d.get('bucket')}"
        elif d.get('date'):
            period = f" for {pd.to_datetime(d['date']).strftime('%d %b %Y')}"
        elif d.get('month'):
            period = f" for {d['month']}"
            
        return f"Leave Details ({d['bucket']}){period}"

    @render.table
    def tbl_leave_drill():
        d = LEAVE_DRILL_DATA()
        if not d['bucket']: return pd.DataFrame()
        
        # 1. Base Data
        df = f_leave().copy()
        if df.empty: return pd.DataFrame()
        
        # 2. Filtering
        if d.get('type') == 'plt_avail':
            # Cast both to naive normalized timestamps for robust comparison
            target_date = pd.to_datetime(d['date']).replace(tzinfo=None).normalize()
            if d['bucket'] == 'Employees on Leave':
                df = df[df['status'].isin(['Approved', 'Open'])]
                # Ensure series are naive before comparison
                s_from = pd.to_datetime(df['from_date']).dt.tz_localize(None).dt.normalize()
                s_to = pd.to_datetime(df['to_date']).dt.tz_localize(None).dt.normalize()
                mask = (s_from <= target_date) & (s_to >= target_date)
                res = df[mask].copy()
            else:
                return pd.DataFrame()
        
        elif d.get('type') == 'plt_trend':
            df['Month_Year'] = df['dt'].dt.strftime('%b %Y')
            res = df[(df['Month_Year'] == d['month']) & (df['Leave Application Category'] == d['bucket'])].copy()
            res = res[res['status'].isin(['Approved', 'Open'])]

        elif d.get('type') == 'plt_util':
            df['Month_Year'] = df['dt'].dt.strftime('%b %Y')
            res = df[df['Month_Year'] == d['month']].copy()
            res = res[res['status'].isin(['Approved', 'Open'])]

        elif d.get('type') == 'plt_top':
            # bucket here is the employee name (customdata[0]) if we changed payload logic, 
            # but wait, JS set bucket = customdata[1] || customdata[0]
            # For plt_top, customdata[0] is name, customdata[1] is metric.
            emp_name = d.get('month') # Wait, check JS bridge payload mapping
            # In JS: bucket: pt.customdata[1] || pt.customdata[0], month: pt.customdata[0] for trend.
            # For plt_top: pt.customdata is [name, metric].
            # payload.bucket = metric, payload.month = name.
            res = df[df['employee_name_t'] == d.get('month')].copy()
            res = res[res['status'].isin(['Approved', 'Open'])]
            
        else:
            res = df.copy()

        if res.empty: return pd.DataFrame()

        # 3. Metadata Join (Ensure Employee ID and Name are accurate)
        ud = DB.DF['users_details'][['user_id', 'employee_id', 'employee_name']].copy()
        ud = ud.rename(columns={'employee_id': 'EmployeeID', 'employee_name': 'EmployeeName'})
        
        # Drop collision columns if they exist in res
        res = res.drop(columns=[c for c in ['employee_id', 'employee_name'] if c in res.columns], errors='ignore')
        res = res.merge(ud, on='user_id', how='left')
        
        # 4. Final Projection (User Requested Columns)
        # 1) employee id, 2) employee name, 3) user id, 4) status, 
        # 5) leave date, 6) total leave days, 7) category
        
        res['Employee ID'] = res['EmployeeID'].fillna("N/A")
        res['Employee Name'] = res['EmployeeName'].fillna("Unknown")
        res['User ID'] = res['user_id']
        res['Status'] = res['status']
        res['Leave Date'] = res.apply(lambda r: f"{pd.to_datetime(r['from_date']).strftime('%d %b')} - {pd.to_datetime(r['to_date']).strftime('%d %b %Y')}", axis=1)
        
        val_col = 'Total Leave Days' if 'Total Leave Days' in res.columns else 'total_leave_days'
        res['Total Leave Days'] = res[val_col]
        res['Category'] = res['Leave Application Category'] if 'Leave Application Category' in res.columns else "N/A"
        
        cols = ['Employee ID', 'Employee Name', 'User ID', 'Status', 'Leave Date', 'Total Leave Days', 'Category']
        return res[cols].sort_values('Employee Name')

    @reactive.effect
    @reactive.event(input.summary_drill_event)
    def _drill_summary_js():
        ev = input.summary_drill_event()
        if ev and 'bucket' in ev:
            SUMMARY_DRILL_DATA.set(ev)
            ui.update_navset("tabs", selected="Summary Drill Details")

    @render.text
    def txt_summary_drill_title():
        d = SUMMARY_DRILL_DATA()
        if not d['bucket']: return "Summary Application Details"
        
        period = ""
        if d.get('type') == 'plt_top':
            period = f" for {d.get('month')}"
        elif d.get('month'):
            period = f" for {d['month']}"
            
        return f"Summary Details ({d['bucket']}){period}"

    @render.table
    def tbl_summary_drill():
        d = SUMMARY_DRILL_DATA()
        if not d['bucket']: return pd.DataFrame()
        
        # 1. Base Data
        df = f_leave().copy()
        if df.empty: return pd.DataFrame()
        
        # 2. Filtering
        if d.get('type') == 'plt_trend':
            df['Month_Year'] = df['dt'].dt.strftime('%b %Y')
            res = df[(df['Month_Year'] == d['month']) & (df['Leave Application Category'] == d['bucket'])].copy()
            res = res[res['status'].isin(['Approved', 'Open'])]

        elif d.get('type') == 'plt_util':
            df['Month_Year'] = df['dt'].dt.strftime('%b %Y')
            res = df[df['Month_Year'] == d['month']].copy()
            res = res[res['status'].isin(['Approved', 'Open'])]

        elif d.get('type') == 'plt_top':
            # Robust name matching: trim and case-insensitive
            target_name = str(d.get('month', '')).strip().lower()
            res = df[df['employee_name_t'].astype(str).str.strip().str.lower() == target_name].copy()
            res = res[res['status'].isin(['Approved', 'Open'])]
            
        else:
            res = df.copy()

        if res.empty: return pd.DataFrame()

        # 3. Apply Logical Columns (User Requested)
        # Logical Column 7: Total Leave Days
        # IF(Days=0 && (HalfDayFrom="Yes" || HalfDayTo="Yes"), 0.5, Days)
        def calc_days(r):
            base_days = r.get('total_leave_days', 0)
            h1 = str(r.get('Half day on From Date', 'No')).strip().lower() == 'yes'
            h2 = str(r.get('Half day on To Date', 'No')).strip().lower() == 'yes'
            if base_days == 0 and (h1 or h2):
                return 0.5
            return base_days
        
        res['Calc_Leave_Days'] = res.apply(calc_days, axis=1)
        
        # Logical Column 9: Total Leave Hours (Sum for Approved/Open)
        # (Already filtered for status above, so we just sum the column if it exists)
        h_col = 'Total Leave hrs' if 'Total Leave hrs' in res.columns else 'total_leave_hrs'
        if h_col not in res.columns: res[h_col] = 0
        res['Calc_Leave_Hours'] = res[h_col].fillna(0).astype(float)

        # 4. Metadata Join
        ud = DB.DF['users_details'][['user_id', 'employee_id', 'employee_name']].copy()
        ud = ud.rename(columns={'employee_id': 'EmployeeID', 'employee_name': 'EmployeeName'})
        res = res.drop(columns=[c for c in ['employee_id', 'employee_name'] if c in res.columns], errors='ignore')
        res = res.merge(ud, on='user_id', how='left')
        
        # 5. Final Projection (9 Columns)
        res['Employee ID'] = res['EmployeeID'].fillna("N/A")
        res['Employee Name'] = res['EmployeeName'].fillna("Unknown")
        res['User ID'] = res['user_id']
        res['Status'] = res['status']
        res['From Date'] = pd.to_datetime(res['from_date']).dt.strftime('%d %b %Y')
        res['To Date'] = pd.to_datetime(res['to_date']).dt.strftime('%d %b %Y')
        res['Total Leave Days'] = res['Calc_Leave_Days']
        res['Category'] = res['Leave Application Category'].fillna("N/A")
        res['Total Leave Hours'] = res['Calc_Leave_Hours']
        
        cols = [
            'Employee ID', 'Employee Name', 'User ID', 'Status', 
            'From Date', 'To Date', 'Total Leave Days', 'Category', 'Total Leave Hours'
        ]
        return res[cols].sort_values(['Employee Name', 'From Date'])

app = App(app_ui, server)
