#!/usr/bin/env python3
"""
HUCM Zoom Attendance Dashboard — Auto-Update Script
====================================================
Drop any new Zoom CSV or XLSX meeting export into this folder,
then run this script to regenerate the dashboard.

Usage:
    python3 update_dashboard.py

Output:
    zoom_attendance_dashboard.html  (in the same folder)
"""

import pandas as pd
import glob
import json
import os
from datetime import datetime

FOLDER = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(FOLDER, "zoom_attendance_dashboard.html")
REG_FILE = os.path.join(FOLDER, "ZOOM Master registration_2026_01_21.xlsx")

# ─── STEP 1: LOAD ALL MEETING DATA ─────────────────────────────────────────
print("📂 Scanning folder for meeting data files...")
dfs = []
for pattern in ['meetinglistdetails_*.csv', 'meetinglistdetails_*.xlsx']:
    for f in sorted(glob.glob(os.path.join(FOLDER, pattern))):
        print(f"   → Loading {os.path.basename(f)}")
        try:
            df = pd.read_csv(f) if f.endswith('.csv') else pd.read_excel(f)
            dfs.append(df)
        except Exception as e:
            print(f"   ⚠️  Skipped (error: {e})")

if not dfs:
    print("❌ No meeting data files found. Add CSV files and retry.")
    exit(1)

df_all = pd.concat(dfs, ignore_index=True)
df_clean = df_all.dropna(subset=['Topic']).copy()
df_clean['Start time'] = pd.to_datetime(df_clean['Start time'], errors='coerce')
df_clean = df_clean.dropna(subset=['Start time'])
df_clean['date_key'] = df_clean['Start time'].dt.date
df_clean['month_label'] = df_clean['Start time'].dt.strftime('%b %Y')
df_clean['month_sort'] = df_clean['Start time'].dt.strftime('%Y-%m')
print(f"   ✅ {len(df_clean)} attendance records across {len(dfs)} files")

# ─── STEP 2: CLASSIFY MEETING SERIES ───────────────────────────────────────
def get_series(t):
    t = str(t)
    if '"All of Us"' in t or 'All of Us' in t: return 'All of Us'
    if 'APT' in t and 'Criteria' not in t:      return 'APT Process'
    if 'APT Criteria' in t or 'Faculty Meeting' in t: return 'Faculty Meeting'
    if 'Dean' in t and 'Dynamic' in t:          return "Dean's Lecture"
    if 'WAG' in t or 'Writing Accountability' in t: return 'WAG'
    if 'Write-A-Thon' in t or 'SWAT' in t:     return 'SWAT'
    if 'Loan' in t or 'Repayment' in t:         return 'Seminar'
    if 'FACULTY TOOLS' in t or 'Faculty Tools' in t: return 'Faculty Workshop'
    if 'Pipeline' in t or 'Leadership' in t:    return 'Leadership'
    if 'OFD' in t or 'JEDI' in t:              return 'OFD'
    if 'Investigators' in t or ('HUCM' in t and 'I&A' not in t): return 'HUCM I&A'
    return 'Other'

df_clean['series'] = df_clean['Topic'].apply(get_series)

# ─── STEP 3: AGGREGATE TO SESSION LEVEL ────────────────────────────────────
meet_agg = df_clean.drop_duplicates(subset=['ID','date_key']).copy().sort_values('Start time')

gc = (df_clean.drop_duplicates(subset=['ID','date_key','Name (original name)'])
      .groupby(['ID','date_key','Guest']).size().unstack(fill_value=0).reset_index())
gc.columns = ['ID','date_key'] + [f'g_{c}' for c in gc.columns[2:]]

avg_dur = (df_clean.groupby(['ID','date_key'])['Duration (minutes).1']
           .mean().reset_index())
avg_dur.columns = ['ID','date_key','avgDur']

meetings_out = []
for _, row in meet_agg.iterrows():
    mid = str(row['ID']); dk = row['date_key']
    gr  = gc[(gc['ID']==mid) & (gc['date_key']==dk)]
    adr = avg_dur[(avg_dur['ID']==mid) & (avg_dur['date_key']==dk)]
    internal = int(gr['g_No'].values[0])  if not gr.empty and 'g_No'  in gr.columns else 0
    external = int(gr['g_Yes'].values[0]) if not gr.empty and 'g_Yes' in gr.columns else 0
    avg  = round(float(adr['avgDur'].values[0]), 1) if not adr.empty else 0
    t    = str(row['Topic'])
    pax  = int(row['Participants'])             if pd.notna(row['Participants']) else 0
    tot  = int(row['Total participant minutes']) if pd.notna(row['Total participant minutes']) else 0
    dur  = int(row['Duration (minutes)'])        if pd.notna(row['Duration (minutes)']) else 0
    meetings_out.append({
        'topic': t, 'short': t[:36] + ('…' if len(t) > 36 else ''),
        'series': row['series'], 'month': row['month_label'],
        'month_sort': row['month_sort'], 'date': str(dk),
        'participants': pax, 'duration': dur,
        'totalMin': tot, 'internal': internal,
        'external': external, 'avgDur': avg
    })

# ─── STEP 4: MONTHLY & SERIES ROLLUPS ──────────────────────────────────────
real_meets = [m for m in meetings_out if m['participants'] > 1]

monthly_map = {}
for m in real_meets:
    k = m['month_sort']
    if k not in monthly_map:
        monthly_map[k] = {'label': m['month'], 'sort': k,
                          'meetings': 0, 'participants': 0, 'totalMin': 0}
    monthly_map[k]['meetings']     += 1
    monthly_map[k]['participants'] += m['participants']
    monthly_map[k]['totalMin']     += m['totalMin']
monthly_out = sorted(monthly_map.values(), key=lambda x: x['sort'])

series_map = {}
for m in real_meets:
    s = m['series']
    if s not in series_map:
        series_map[s] = {'series': s, 'sessions': 0, 'participants': 0, 'totalMin': 0}
    series_map[s]['sessions']     += 1
    series_map[s]['participants'] += m['participants']
    series_map[s]['totalMin']     += m['totalMin']
series_out = sorted(series_map.values(), key=lambda x: -x['participants'])

all_months = sorted(set(m['month'] for m in meetings_out),
                    key=lambda x: next(m['month_sort'] for m in meetings_out if m['month']==x))
all_series = sorted(set(m['series'] for m in meetings_out))

# ─── STEP 5: REGISTRATION DATA ─────────────────────────────────────────────
reg_position   = []
reg_degree     = []
reg_timeline   = []
reg_total      = 0
reg_event_name = "The Dean's Dynamic Duo Lecture Series"
reg_event_date = "Jan 21, 2026"

if os.path.exists(REG_FILE):
    print(f"   → Loading registration data from {os.path.basename(REG_FILE)}")
    df_reg = pd.read_excel(REG_FILE, skiprows=5, header=0)
    df_reg['Registration Time'] = pd.to_datetime(df_reg['Registration Time'], errors='coerce')

    def norm_pos(p):
        if pd.isna(p): return 'Other'
        p = str(p).strip().title()
        return p if p in ('Faculty','Staff','Student','Guest') else 'Other'

    def norm_deg(d):
        if pd.isna(d): return 'Other'
        d = str(d).strip().upper()
        if 'MD' in d and 'PHD' in d: return 'MD/PhD'
        if d in ('MD','M.D.','MDD','MD,','MD '): return 'MD'
        if d in ('PHD','PH.D.','PHD PHYSIOLOGY','PH.D','PHD '): return 'PhD'
        if d in ('DO','D.O.'): return 'DO'
        if d in ('MS','M.S.','M.S'): return 'MS'
        if d in ('BS','B.S.','BIOLOGY, B.S.'): return 'BS'
        if d in ('MBBS','M.B.B.S'): return 'MBBS'
        return 'Other'

    df_reg['pos_norm'] = df_reg['Position Status'].apply(norm_pos)
    df_reg['deg_norm'] = df_reg['Degree'].apply(norm_deg)
    pos_map = {'Faculty':'#003580','Staff':'#E5001A','Student':'#F4A31A','Guest':'#18a558','Other':'#888'}
    reg_position = [{'label':k,'value':int(v),'color':pos_map.get(k,'#888')}
                    for k,v in df_reg['pos_norm'].value_counts().items()]
    deg_order = ['MD','PhD','Other','BS','MS','MBBS','DO','MD/PhD']
    deg_counts = df_reg['deg_norm'].value_counts().to_dict()
    reg_degree = [{'label':k,'value':deg_counts.get(k,0)} for k in deg_order if k in deg_counts]

    df_reg['reg_date_fmt'] = df_reg['Registration Time'].dt.strftime('%b %d')
    tl = df_reg.dropna(subset=['Registration Time']).groupby('reg_date_fmt').size()
    reg_timeline = [{'date':k,'count':int(v)} for k,v in sorted(tl.items(),
                    key=lambda x: pd.to_datetime(x[0]+' 2025', format='%b %d %Y', errors='coerce'))]
    reg_total = len(df_reg)

print(f"\n✅ Data ready: {len(real_meets)} sessions, "
      f"{sum(m['participants'] for m in real_meets):,} participants, "
      f"{len(monthly_out)} months")

# ─── STEP 6: INJECT DATA INTO HTML TEMPLATE ────────────────────────────────
GENERATED_AT = datetime.now().strftime('%B %d, %Y at %I:%M %p')
DATE_RANGE   = f"{all_months[0]} – {all_months[-1]}" if all_months else "N/A"

HTML = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>HUCM Zoom Attendance Dashboard</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.5.1" integrity="sha384-jb8JQMbMoBUzgWatfe6COACi2ljcDdZQ2OxczGA3bGNeWe+6DChMTBJemed7ZnvJ" crossorigin="anonymous"></script>
  <style>
    :root {{
      --bg:#f0f2f5; --card:#fff; --header:#003366; --accent:#E5001A;
      --text:#1a1a2e; --muted:#6c757d; --border:#e0e4ea;
      --pos:#18a558; --neg:#dc3545; --gold:#F4A31A;
      --gap:16px; --r:10px;
    }}
    *{{margin:0;padding:0;box-sizing:border-box;}}
    body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:var(--bg);color:var(--text);font-size:14px;line-height:1.5;}}
    .wrap{{max-width:1440px;margin:0 auto;padding:var(--gap);}}
    /* Header */
    .hdr{{background:var(--header);color:#fff;padding:18px 26px;border-radius:var(--r);margin-bottom:var(--gap);display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:12px;}}
    .hdr h1{{font-size:19px;font-weight:700;}}
    .hdr p{{font-size:11px;color:rgba(255,255,255,.6);margin-top:3px;}}
    .filters{{display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end;}}
    .fg{{display:flex;flex-direction:column;gap:3px;}}
    .fg label{{font-size:11px;color:rgba(255,255,255,.6);text-transform:uppercase;letter-spacing:.4px;}}
    .fg select{{padding:6px 10px;border:1px solid rgba(255,255,255,.25);border-radius:6px;background:rgba(255,255,255,.12);color:#fff;font-size:12px;cursor:pointer;min-width:140px;}}
    .fg select option{{background:#003366;}}
    .refresh-btn{{padding:7px 14px;background:rgba(255,255,255,.15);border:1px solid rgba(255,255,255,.3);border-radius:6px;color:#fff;font-size:12px;cursor:pointer;white-space:nowrap;}}
    .refresh-btn:hover{{background:rgba(255,255,255,.25);}}
    /* KPIs */
    .kpis{{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:var(--gap);margin-bottom:var(--gap);}}
    .kpi{{background:var(--card);border-radius:var(--r);padding:18px 20px;box-shadow:0 1px 4px rgba(0,0,0,.08);border-top:3px solid var(--header);}}
    .kpi.r{{border-top-color:var(--accent);}} .kpi.g{{border-top-color:var(--pos);}} .kpi.o{{border-top-color:var(--gold);}}
    .kpi-lbl{{font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;margin-bottom:5px;}}
    .kpi-val{{font-size:28px;font-weight:800;line-height:1;}}
    .kpi-sub{{font-size:11px;color:var(--muted);margin-top:4px;}}
    /* Cards */
    .grid2{{display:grid;grid-template-columns:1fr 1fr;gap:var(--gap);margin-bottom:var(--gap);}}
    .grid3{{display:grid;grid-template-columns:1fr 1fr 1fr;gap:var(--gap);margin-bottom:var(--gap);}}
    .grid21{{display:grid;grid-template-columns:2fr 1fr;gap:var(--gap);margin-bottom:var(--gap);}}
    .card{{background:var(--card);border-radius:var(--r);padding:18px 20px;box-shadow:0 1px 4px rgba(0,0,0,.08);}}
    .card h3{{font-size:12px;font-weight:700;text-transform:uppercase;letter-spacing:.4px;margin-bottom:14px;color:var(--text);}}
    .card canvas{{max-height:260px;}}
    /* Section label */
    .sec{{font-size:12px;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:1px;border-left:3px solid var(--accent);padding-left:9px;margin:18px 0 10px;}}
    /* Table */
    .tbl-wrap{{background:var(--card);border-radius:var(--r);padding:18px 20px;box-shadow:0 1px 4px rgba(0,0,0,.08);overflow-x:auto;margin-bottom:var(--gap);}}
    .tbl-wrap h3{{font-size:12px;font-weight:700;text-transform:uppercase;letter-spacing:.4px;margin-bottom:12px;}}
    table{{width:100%;border-collapse:collapse;font-size:12px;min-width:750px;}}
    thead th{{padding:8px 11px;border-bottom:2px solid var(--border);color:var(--muted);font-size:11px;font-weight:700;text-transform:uppercase;cursor:pointer;user-select:none;white-space:nowrap;text-align:left;}}
    thead th:hover{{color:var(--text);background:#f8f9fa;}}
    tbody td{{padding:8px 11px;border-bottom:1px solid #f2f4f6;}}
    tbody tr:hover{{background:#fafbfc;}}
    tbody tr:last-child td{{border-bottom:none;}}
    .badge{{display:inline-block;padding:2px 7px;border-radius:20px;font-size:10px;font-weight:700;}}
    .b1{{background:#e8f0fe;color:#2956b2;}} .b2{{background:#fce8e8;color:#b71c1c;}}
    .b3{{background:#e8f8ee;color:#1a7a43;}} .b4{{background:#fff4e0;color:#b07a00;}}
    .b5{{background:#f3e8ff;color:#6a1b9a;}} .b6{{background:#e0f7fa;color:#006064;}}
    .b7{{background:#fce4ec;color:#880e4f;}} .b8{{background:#e8eaf6;color:#283593;}}
    /* Reg banner */
    .reg-banner{{background:linear-gradient(135deg,#003366 0%,#00509E 100%);color:#fff;border-radius:var(--r);padding:15px 22px;margin-bottom:var(--gap);display:flex;align-items:center;gap:16px;flex-wrap:wrap;}}
    .reg-banner .rt{{font-size:15px;font-weight:700;}} .reg-banner .rm{{font-size:11px;color:rgba(255,255,255,.65);margin-top:3px;}}
    .reg-pill{{background:rgba(255,255,255,.15);border-radius:30px;padding:6px 18px;font-size:22px;font-weight:800;white-space:nowrap;}}
    /* Footer */
    .foot{{text-align:center;color:var(--muted);font-size:11px;padding:10px 0 4px;}}
    .upd-badge{{display:inline-block;background:#e8f0fe;color:#2956b2;border-radius:20px;padding:3px 10px;font-size:11px;font-weight:600;margin-bottom:12px;}}
    /* Pagination */
    .pager{{display:flex;justify-content:space-between;align-items:center;margin-top:10px;font-size:12px;color:var(--muted);}}
    .pager button{{padding:4px 10px;border:1px solid var(--border);border-radius:5px;background:var(--card);cursor:pointer;font-size:12px;}}
    .pager button:hover{{background:#f0f2f5;}}
    .pager button:disabled{{opacity:.4;cursor:not-allowed;}}
    @media(max-width:900px){{.grid2,.grid3,.grid21{{grid-template-columns:1fr;}} .kpis{{grid-template-columns:repeat(2,1fr);}}}}
    @media print{{.filters,.refresh-btn{{display:none;}} body{{background:#fff;}}}}
  </style>
</head>
<body>
<div class="wrap">
  <header class="hdr">
    <div>
      <h1>Howard University — HUCM Zoom Attendance Dashboard</h1>
      <p>College of Medicine &nbsp;·&nbsp; Host: Veronica Bruce &nbsp;·&nbsp; {DATE_RANGE} &nbsp;·&nbsp; Updated: {GENERATED_AT}</p>
    </div>
    <div class="filters">
      <div class="fg"><label>Month</label>
        <select id="f-month" onchange="dash.filter()">
          <option value="all">All Months</option>
          {''.join(f'<option value="{m}">{m}</option>' for m in all_months)}
        </select>
      </div>
      <div class="fg"><label>Series</label>
        <select id="f-series" onchange="dash.filter()">
          <option value="all">All Series</option>
          {''.join(f'<option value="{s}">{s}</option>' for s in all_series)}
        </select>
      </div>
      <div class="fg"><label>Min Participants</label>
        <select id="f-minpax" onchange="dash.filter()">
          <option value="0">Show All</option>
          <option value="2" selected>2+ (exclude tests)</option>
          <option value="5">5+</option>
          <option value="10">10+</option>
        </select>
      </div>
    </div>
  </header>

  <!-- KPIs -->
  <section class="kpis" id="kpis"></section>

  <div class="sec">Attendance Trends</div>
  <div class="grid21">
    <div class="card"><h3>Monthly Participants Trend</h3><canvas id="c-trend"></canvas></div>
    <div class="card"><h3>Sessions by Series</h3><canvas id="c-series-donut" style="max-height:240px;"></canvas></div>
  </div>

  <div class="grid2">
    <div class="card"><h3>Participants by Series</h3><canvas id="c-series-bar"></canvas></div>
    <div class="card"><h3>Internal vs External Attendees (by Series)</h3><canvas id="c-int-ext"></canvas></div>
  </div>

  <div class="sec">Session Detail</div>
  <div class="tbl-wrap">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;">
      <h3 style="margin:0;">All Sessions</h3>
      <span class="upd-badge" id="session-count">— sessions</span>
    </div>
    <div id="tbl"></div>
    <div class="pager">
      <span id="pager-info"></span>
      <div style="display:flex;gap:6px;">
        <button id="btn-prev" onclick="dash.prevPage()">← Prev</button>
        <button id="btn-next" onclick="dash.nextPage()">Next →</button>
      </div>
    </div>
  </div>

  <div class="sec">Registration — The Dean's Dynamic Duo Lecture Series</div>
  <div class="reg-banner">
    <div>
      <div class="rt">{reg_event_name}</div>
      <div class="rm">Scheduled: {reg_event_date} &nbsp;·&nbsp; Duration: 60 min &nbsp;·&nbsp; All approved</div>
    </div>
    <div class="reg-pill">{reg_total} Registrants</div>
  </div>
  <div class="grid3">
    <div class="card"><h3>By Position</h3><canvas id="c-pos" style="max-height:220px;"></canvas></div>
    <div class="card"><h3>By Degree</h3><canvas id="c-deg" style="max-height:220px;"></canvas></div>
    <div class="card"><h3>Registration Timeline</h3><canvas id="c-regtl" style="max-height:220px;"></canvas></div>
  </div>

  <footer class="foot">Data auto-generated from Zoom exports in this folder &nbsp;·&nbsp; HUCM, Howard University &nbsp;·&nbsp; Last updated: {GENERATED_AT}</footer>
</div>
<script>
// ── EMBEDDED DATA (auto-generated) ────────────────────────────────────────
const ALL_MEETINGS = {json.dumps(meetings_out)};
const MONTHLY_DATA = {json.dumps(monthly_out)};
const SERIES_DATA  = {json.dumps(series_out)};
const REG_POSITION = {json.dumps(reg_position)};
const REG_DEGREE   = {json.dumps(reg_degree)};
const REG_TIMELINE = {json.dumps(reg_timeline)};

// ── PALETTE ───────────────────────────────────────────────────────────────
const PAL = ["#003580","#E5001A","#F4A31A","#18a558","#7B61FF","#00B4D8","#FF6B6B","#845EC2","#D4AC0D","#2E86AB","#A23B72","#F18F01","#4CAF50","#FF5722"];
const SERIES_PAL = {{}};
[...new Set(ALL_MEETINGS.map(m=>m.series))].forEach((s,i)=>SERIES_PAL[s]=PAL[i%PAL.length]);
const BADGE = ["b1","b2","b3","b4","b5","b6","b7","b8"];
const seriesList = [...new Set(ALL_MEETINGS.map(m=>m.series))].sort();
seriesList.forEach((s,i)=>SERIES_PAL[s+'_badge']=BADGE[i%BADGE.length]);

function fmt(v,t='n'){{
  if(v==null)return'-';
  if(t==='n')return v>=1e6?(v/1e6).toFixed(1)+'M':v>=1e3?(v/1e3).toFixed(1)+'K':v.toLocaleString();
  if(t==='h')return(v/60).toFixed(1)+' hrs';
  if(t==='m')return v.toFixed(1)+' min';
  return v;
}}

// ── DASHBOARD ─────────────────────────────────────────────────────────────
class Dashboard {{
  constructor() {{
    this.data = ALL_MEETINGS;
    this.fd = ALL_MEETINGS;
    this.charts = {{}};
    this.page = 0;
    this.pageSize = 20;
    this.sortCol = 'date'; this.sortDir = 'asc';
    this.init();
  }}
  init() {{
    this.filter();
    this.buildStaticCharts();
  }}
  filter() {{
    const month  = document.getElementById('f-month').value;
    const series = document.getElementById('f-series').value;
    const minpax = parseInt(document.getElementById('f-minpax').value)||0;
    this.fd = this.data.filter(m=>{{
      if(month!=='all'&&m.month!==month) return false;
      if(series!=='all'&&m.series!==series) return false;
      if(m.participants<minpax) return false;
      return true;
    }});
    this.page = 0;
    this.renderKPIs();
    this.updateCharts();
    this.renderTable();
  }}
  renderKPIs() {{
    const d=this.fd;
    const sessions   = d.length;
    const totalPax   = d.reduce((s,m)=>s+m.participants,0);
    const totalMin   = d.reduce((s,m)=>s+m.totalMin,0);
    const avgPax     = sessions ? (totalPax/sessions).toFixed(1) : 0;
    const avgDur     = sessions ? (d.reduce((s,m)=>s+m.avgDur,0)/sessions).toFixed(1) : 0;
    const topSeries  = d.length ? d.reduce((acc,m)=>{{acc[m.series]=(acc[m.series]||0)+m.participants;return acc;}},{{}}) : {{}};
    const topS       = Object.entries(topSeries).sort((a,b)=>b[1]-a[1])[0];
    document.getElementById('kpis').innerHTML = `
      <div class="kpi"><div class="kpi-lbl">Total Sessions</div><div class="kpi-val">${{sessions}}</div><div class="kpi-sub">Zoom meetings</div></div>
      <div class="kpi r"><div class="kpi-lbl">Total Participants</div><div class="kpi-val">${{fmt(totalPax)}}</div><div class="kpi-sub">across all sessions</div></div>
      <div class="kpi g"><div class="kpi-lbl">Engagement Hours</div><div class="kpi-val">${{fmt(totalMin,'h')}}</div><div class="kpi-sub">${{totalMin.toLocaleString()}} participant-min</div></div>
      <div class="kpi o"><div class="kpi-lbl">Avg Attendance</div><div class="kpi-val">${{avgPax}}</div><div class="kpi-sub">per session</div></div>
      <div class="kpi"><div class="kpi-lbl">Avg Session Duration</div><div class="kpi-val">${{avgDur}}<span style="font-size:15px;font-weight:500"> min</span></div><div class="kpi-sub">per participant</div></div>
      ${{topS ? `<div class="kpi"><div class="kpi-lbl">Top Series</div><div class="kpi-val" style="font-size:17px;margin-top:4px">${{topS[0]}}</div><div class="kpi-sub">${{topS[1].toLocaleString()}} participants</div></div>` : ''}}
    `;
  }}
  updateCharts() {{
    // Monthly trend
    const monthMap={{}};
    this.fd.forEach(m=>{{
      if(!monthMap[m.month_sort]) monthMap[m.month_sort]={{label:m.month,sort:m.month_sort,participants:0,meetings:0}};
      monthMap[m.month_sort].participants+=m.participants;
      monthMap[m.month_sort].meetings+=1;
    }});
    const months = Object.values(monthMap).sort((a,b)=>a.sort.localeCompare(b.sort));
    this._upsertLine('c-trend', months.map(m=>m.label),
      [{{label:'Participants',data:months.map(m=>m.participants),color:'#003580'}},
       {{label:'Sessions',data:months.map(m=>m.meetings),color:'#E5001A',yAxisID:'y2'}}]);

    // Series bar
    const seriesMap={{}};
    this.fd.forEach(m=>{{
      if(!seriesMap[m.series]) seriesMap[m.series]={{series:m.series,participants:0}};
      seriesMap[m.series].participants+=m.participants;
    }});
    const sArr = Object.values(seriesMap).sort((a,b)=>b.participants-a.participants);
    this._upsertBar('c-series-bar', sArr.map(s=>s.series), sArr.map(s=>s.participants),
      sArr.map(s=>SERIES_PAL[s.series]||'#003580'));

    // Internal vs External stacked
    const siMap={{}};
    this.fd.forEach(m=>{{
      if(!siMap[m.series]) siMap[m.series]={{series:m.series,internal:0,external:0}};
      siMap[m.series].internal+=m.internal;
      siMap[m.series].external+=m.external;
    }});
    const siArr = Object.values(siMap).sort((a,b)=>(b.internal+b.external)-(a.internal+a.external));
    this._upsertStacked('c-int-ext', siArr.map(s=>s.series),
      siArr.map(s=>s.internal), siArr.map(s=>s.external));

    // Series donut
    this._upsertDonut('c-series-donut', sArr.map(s=>s.series), sArr.map(s=>s.participants));
  }}
  _upsertLine(id, labels, datasets) {{
    const ctx = document.getElementById(id).getContext('2d');
    if(this.charts[id]) {{
      this.charts[id].data.labels = labels;
      datasets.forEach((ds,i)=>{{ this.charts[id].data.datasets[i].data=ds.data; }});
      this.charts[id].update('none'); return;
    }}
    this.charts[id] = new Chart(ctx, {{
      type:'line',
      data:{{ labels, datasets: datasets.map(ds=>({{
        label:ds.label, data:ds.data,
        borderColor:ds.color, backgroundColor:ds.color+'20',
        borderWidth:2.5, tension:0.3, fill:false,
        pointRadius:4, pointHoverRadius:7,
        yAxisID: ds.yAxisID||'y'
      }}))  }},
      options:{{ responsive:true, maintainAspectRatio:false,
        interaction:{{mode:'index',intersect:false}},
        plugins:{{ legend:{{position:'top',labels:{{usePointStyle:true,padding:16,font:{{size:11}}}}}} }},
        scales:{{
          x:{{grid:{{display:false}},ticks:{{font:{{size:11}}}}}},
          y:{{beginAtZero:true,position:'left',ticks:{{font:{{size:11}}}},grid:{{color:'#f0f0f0'}}}},
          y2:{{beginAtZero:true,position:'right',ticks:{{font:{{size:11}}}},grid:{{display:false}}}}
        }}
      }}
    }});
  }}
  _upsertBar(id, labels, data, colors) {{
    const ctx = document.getElementById(id).getContext('2d');
    if(this.charts[id]) {{
      this.charts[id].data.labels=labels;
      this.charts[id].data.datasets[0].data=data;
      this.charts[id].data.datasets[0].backgroundColor=colors.map(c=>c+'CC');
      this.charts[id].update('none'); return;
    }}
    this.charts[id] = new Chart(ctx, {{
      type:'bar',
      data:{{labels, datasets:[{{data, backgroundColor:colors.map(c=>c+'CC'), borderWidth:0, borderRadius:4}}]}},
      options:{{
        indexAxis:'y', responsive:true, maintainAspectRatio:false,
        plugins:{{legend:{{display:false}},tooltip:{{callbacks:{{label:c=>c.parsed.x.toLocaleString()+' participants'}}}}}},
        scales:{{x:{{beginAtZero:true,ticks:{{font:{{size:11}}}}}},y:{{ticks:{{font:{{size:11}}}}}}}}
      }}
    }});
  }}
  _upsertStacked(id, labels, intData, extData) {{
    const ctx = document.getElementById(id).getContext('2d');
    if(this.charts[id]) {{
      this.charts[id].data.labels=labels;
      this.charts[id].data.datasets[0].data=intData;
      this.charts[id].data.datasets[1].data=extData;
      this.charts[id].update('none'); return;
    }}
    this.charts[id] = new Chart(ctx, {{
      type:'bar',
      data:{{labels, datasets:[
        {{label:'Internal',data:intData,backgroundColor:'#003580CC',borderWidth:0}},
        {{label:'External',data:extData,backgroundColor:'#E5001ACC',borderWidth:0}}
      ]}},
      options:{{
        indexAxis:'y', responsive:true, maintainAspectRatio:false,
        plugins:{{legend:{{position:'top',labels:{{usePointStyle:true,font:{{size:11}}}}}}}},
        scales:{{
          x:{{stacked:true,ticks:{{font:{{size:11}}}}}},
          y:{{stacked:true,ticks:{{font:{{size:11}}}}}}
        }}
      }}
    }});
  }}
  _upsertDonut(id, labels, data) {{
    const ctx = document.getElementById(id).getContext('2d');
    const colors = labels.map(l=>SERIES_PAL[l]||'#888');
    if(this.charts[id]) {{
      this.charts[id].data.labels=labels;
      this.charts[id].data.datasets[0].data=data;
      this.charts[id].data.datasets[0].backgroundColor=colors.map(c=>c+'CC');
      this.charts[id].update('none'); return;
    }}
    this.charts[id] = new Chart(ctx, {{
      type:'doughnut',
      data:{{labels,datasets:[{{data,backgroundColor:colors.map(c=>c+'CC'),borderColor:'#fff',borderWidth:2}}]}},
      options:{{
        responsive:true, maintainAspectRatio:false, cutout:'55%',
        plugins:{{
          legend:{{position:'right',labels:{{usePointStyle:true,font:{{size:10}},padding:8}}}},
          tooltip:{{callbacks:{{label:c=>{{
            const tot=c.dataset.data.reduce((a,b)=>a+b,0);
            return `${{c.label}}: ${{c.parsed}} (${{((c.parsed/tot)*100).toFixed(0)}}%)`;
          }}}}}}
        }}
      }}
    }});
  }}
  renderTable() {{
    const sorted = [...this.fd].sort((a,b)=>{{
      let av=a[this.sortCol], bv=b[this.sortCol];
      if(typeof av==='string') av=av.toLowerCase(), bv=bv.toLowerCase();
      const c=av<bv?-1:av>bv?1:0;
      return this.sortDir==='asc'?c:-c;
    }});
    const start=this.page*this.pageSize, end=Math.min(start+this.pageSize, sorted.length);
    const page = sorted.slice(start,end);
    document.getElementById('session-count').textContent=`${{sorted.length}} sessions`;
    const cols=[
      {{k:'date',l:'Date'}},{{k:'topic',l:'Meeting'}},{{k:'series',l:'Series'}},
      {{k:'participants',l:'Participants'}},{{k:'duration',l:'Duration'}},
      {{k:'totalMin',l:'Engagement Min'}},{{k:'internal',l:'Internal'}},
      {{k:'external',l:'External'}},{{k:'avgDur',l:'Avg Duration'}}
    ];
    let h='<table><thead><tr>';
    cols.forEach(c=>{{
      const arrow=this.sortCol===c.k?(this.sortDir==='asc'?' ▲':' ▼'):'';
      h+=`<th onclick="dash.sort('${{c.k}}')">${{c.l}}${{arrow}}</th>`;
    }});
    h+='</tr></thead><tbody>';
    if(!page.length) h+=`<tr><td colspan="9" style="text-align:center;padding:30px;color:#999">No sessions match filters.</td></tr>`;
    page.forEach(m=>{{
      const bc = SERIES_PAL[m.series+'_badge']||'b1';
      const extPct = (m.internal+m.external)>0 ? Math.round((m.external/(m.internal+m.external))*100) : 0;
      h+=`<tr>
        <td>${{m.date}}</td>
        <td style="max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="${{m.topic}}">${{m.short}}</td>
        <td><span class="badge ${{bc}}">${{m.series}}</span></td>
        <td><strong>${{m.participants}}</strong></td>
        <td>${{m.duration}} min</td>
        <td>${{m.totalMin.toLocaleString()}}</td>
        <td>${{m.internal}}</td>
        <td>${{m.external}} <span style="color:#999;font-size:10px">(${{extPct}}%)</span></td>
        <td>${{m.avgDur.toFixed(1)}} min</td>
      </tr>`;
    }});
    h+='</tbody></table>';
    document.getElementById('tbl').innerHTML=h;
    document.getElementById('pager-info').textContent=`Showing ${{start+1}}–${{end}} of ${{sorted.length}}`;
    document.getElementById('btn-prev').disabled=this.page===0;
    document.getElementById('btn-next').disabled=end>=sorted.length;
  }}
  sort(col) {{
    if(this.sortCol===col) this.sortDir=this.sortDir==='asc'?'desc':'asc';
    else {{ this.sortCol=col; this.sortDir='asc'; }}
    this.page=0; this.renderTable();
  }}
  prevPage() {{ if(this.page>0){{ this.page--; this.renderTable(); }} }}
  nextPage() {{
    const max=Math.ceil(this.fd.length/this.pageSize)-1;
    if(this.page<max){{ this.page++; this.renderTable(); }}
  }}
  buildStaticCharts() {{
    // Position doughnut
    if(REG_POSITION.length) {{
      new Chart(document.getElementById('c-pos').getContext('2d'),{{
        type:'doughnut',
        data:{{labels:REG_POSITION.map(d=>d.label),
               datasets:[{{data:REG_POSITION.map(d=>d.value),
                           backgroundColor:REG_POSITION.map(d=>d.color+'DD'),
                           borderColor:'#fff',borderWidth:2}}]}},
        options:{{responsive:true,maintainAspectRatio:false,cutout:'58%',
          plugins:{{legend:{{position:'right',labels:{{usePointStyle:true,font:{{size:11}}}}}},
            tooltip:{{callbacks:{{label:c=>{{
              const t=c.dataset.data.reduce((a,b)=>a+b,0);
              return `${{c.label}}: ${{c.parsed}} (${{((c.parsed/t)*100).toFixed(0)}}%)`;
            }}}}}}}}
        }}
      }});
    }}
    // Degree bar
    if(REG_DEGREE.length) {{
      new Chart(document.getElementById('c-deg').getContext('2d'),{{
        type:'bar',
        data:{{labels:REG_DEGREE.map(d=>d.label),
               datasets:[{{data:REG_DEGREE.map(d=>d.value),
                           backgroundColor:PAL.map(c=>c+'CC'),borderWidth:0,borderRadius:4}}]}},
        options:{{indexAxis:'y',responsive:true,maintainAspectRatio:false,
          plugins:{{legend:{{display:false}}}},
          scales:{{x:{{beginAtZero:true,ticks:{{font:{{size:11}}}}}},y:{{ticks:{{font:{{size:11}}}}}}}}
        }}
      }});
    }}
    // Registration timeline
    if(REG_TIMELINE.length) {{
      new Chart(document.getElementById('c-regtl').getContext('2d'),{{
        type:'bar',
        data:{{labels:REG_TIMELINE.map(d=>d.date),
               datasets:[{{data:REG_TIMELINE.map(d=>d.count),
                           backgroundColor:'#003580CC',borderWidth:0,borderRadius:4}}]}},
        options:{{responsive:true,maintainAspectRatio:false,
          plugins:{{legend:{{display:false}}}},
          scales:{{x:{{ticks:{{font:{{size:10}}}}}},y:{{beginAtZero:true,ticks:{{font:{{size:11}}}}}}}}
        }}
      }});
    }}
  }}
}}
const dash = new Dashboard();
</script>
</body>
</html>"""

with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
    f.write(HTML)

print(f"\n✅ Dashboard saved to: {OUTPUT_FILE}")
print(f"   Open zoom_attendance_dashboard.html in any browser to view.")
print(f"\n💡 To update: drop new CSV files in this folder and run this script again.")
