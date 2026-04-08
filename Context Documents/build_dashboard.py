import json, pickle, re

# ── Load payload ──────────────────────────────────────────────────────────────
D = json.load(open('/tmp/final_payload.json'))

# ── Helpers for inline HTML generation ───────────────────────────────────────
def series_tags(series_str):
    colors = {
        'CME': '#4f86c6',
        'Grand Rounds': '#e07b39',
        'Research': '#5aab61',
        'Lecture': '#a855c8',
        'Conference': '#c84a4a',
        'Workshop': '#d4a017',
        'Seminar': '#2aacbb',
    }
    tags = []
    for s in series_str.split(' | '):
        s = s.strip()
        if not s: continue
        col = '#888'
        for k, v in colors.items():
            if k.lower() in s.lower():
                col = v
                break
        tags.append(f'<span class="stag" style="background:{col}22;color:{col};border:1px solid {col}44">{s}</span>')
    return ''.join(tags)

def dir_rows():
    rows = []
    for p in D['table']:
        deg = f'<span class="deg-inline">{p["degree"]}</span>' if p.get('degree') else ''
        dept = p.get('dept','') or ''
        pos  = p.get('pos','') or ''
        rank = p.get('rank','') or ''
        email = p.get('email','') or ''
        sess = p.get('session_count', 0)
        ser  = series_tags(p.get('series',''))
        first = p.get('first_seen','')[:7]
        last  = p.get('last_seen','')[:7]
        rows.append(f'''<tr data-dept="{dept}" data-pos="{pos}" data-rank="{rank}" data-sessions="{sess}">
  <td><span class="name-cell">{p["name"]}</span>{deg}</td>
  <td>{dept}</td>
  <td>{pos}</td>
  <td>{rank}</td>
  <td>{email}</td>
  <td class="num">{sess}</td>
  <td>{ser}</td>
  <td>{first}</td>
  <td>{last}</td>
</tr>''')
    return '\n'.join(rows)

def dept_filter_opts():
    opts = ['<option value="">All Departments</option>']
    for d in sorted(D['dept_list']):
        if d: opts.append(f'<option value="{d}">{d}</option>')
    return '\n'.join(opts)

def rank_filter_opts():
    opts = ['<option value="">All Ranks</option>']
    for r in sorted(D['rank_list']):
        if r: opts.append(f'<option value="{r}">{r}</option>')
    return '\n'.join(opts)

SERIES_JSON    = json.dumps(D['series']['labels'])
SERIES_DATA    = json.dumps(D['series']['values'])
SESSIONS_JSON  = json.dumps([s['label'][:45] for s in D['sessions']])
SESSIONS_DATA  = json.dumps([s['n'] for s in D['sessions']])
# Normalized avg per session
SERIES_AVG_JSON    = json.dumps(D['series_avg']['labels'])
SERIES_AVG_DATA    = json.dumps(D['series_avg']['values'])
SERIES_AVG_SESS    = json.dumps(D['series_avg']['sessions'])
SERIES_AVG_TOTALS  = json.dumps(D['series_avg']['totals'])
# Sessions grouped by series
SESSIONS_BY_SERIES = json.dumps(D['sessions_by_series'])
# Unique reach per series
SERIES_REACH_JSON  = json.dumps(list(D['series_reach'].keys()))
SERIES_REACH_DATA  = json.dumps(list(D['series_reach'].values()))
# Faculty loyalty buckets
LOYALTY_JSON  = json.dumps(D['loyalty']['labels'])
LOYALTY_DATA  = json.dumps(D['loyalty']['values'])
# Attendance timeline
TIMELINE_JSON = json.dumps([s['label'] for s in D['timeline']])
TIMELINE_DATA = json.dumps([s['n'] for s in D['timeline']])
TIMELINE_DATES= json.dumps([s['date'] for s in D['timeline']])
DEPTS_JSON     = json.dumps(D['depts']['labels'])
DEPTS_DATA     = json.dumps(D['depts']['values'])
RANKS_JSON     = json.dumps(D['ranks']['labels'])
RANKS_DATA     = json.dumps(D['ranks']['values'])

# positions / ethnicity / gender — already have labels/values structure
def la_arrays(d):
    return json.dumps(d['labels']), json.dumps(d['values'])

POS_LABELS, POS_DATA   = la_arrays(D['positions'])
ETH_LABELS, ETH_DATA   = la_arrays(D['ethnicity'])
GEN_LABELS, GEN_DATA   = la_arrays(D['gender'])

KPI_RECORDS  = D['kpis']['total_records']
KPI_PEOPLE   = D['kpis']['unique_people']
KPI_MEETINGS = D['kpis']['unique_meetings']
KPI_DEPT     = D['kpis']['dept_pct']

DIR_ROWS       = dir_rows()
DEPT_OPTS      = dept_filter_opts()
RANK_OPTS      = rank_filter_opts()

# ── HTML template — NO f-string, use __PLACEHOLDER__ replacement ──────────────
HTML = open('/tmp/dashboard_template.html').read()

replacements = {
    '__SERIES_JSON__':   SERIES_JSON,
    '__SERIES_DATA__':   SERIES_DATA,
    '__SESSIONS_JSON__': SESSIONS_JSON,
    '__SESSIONS_DATA__': SESSIONS_DATA,
    '__DEPTS_JSON__':    DEPTS_JSON,
    '__DEPTS_DATA__':    DEPTS_DATA,
    '__RANKS_JSON__':    RANKS_JSON,
    '__RANKS_DATA__':    RANKS_DATA,
    '__POS_LABELS__':    POS_LABELS,
    '__POS_DATA__':      POS_DATA,
    '__ETH_LABELS__':    ETH_LABELS,
    '__ETH_DATA__':      ETH_DATA,
    '__GEN_LABELS__':    GEN_LABELS,
    '__GEN_DATA__':      GEN_DATA,
    '__KPI_RECORDS__':   str(KPI_RECORDS),
    '__KPI_PEOPLE__':    str(KPI_PEOPLE),
    '__KPI_MEETINGS__':  str(KPI_MEETINGS),
    '__KPI_DEPT__':      str(KPI_DEPT),
    '__DIR_ROWS__':          DIR_ROWS,
    '__DEPT_OPTS__':         DEPT_OPTS,
    '__RANK_OPTS__':         RANK_OPTS,
    '__SERIES_AVG_JSON__':   SERIES_AVG_JSON,
    '__SERIES_AVG_DATA__':   SERIES_AVG_DATA,
    '__SERIES_AVG_SESS__':   SERIES_AVG_SESS,
    '__SERIES_AVG_TOTALS__': SERIES_AVG_TOTALS,
    '__SESSIONS_BY_SERIES__':SESSIONS_BY_SERIES,
    '__SCATTER_PEOPLE__':    json.dumps(D['scatter_people']),
    '__DEPT_BUBBLES__':      json.dumps(D['dept_bubbles']),
    '__SERIES_REACH_JSON__': SERIES_REACH_JSON,
    '__SERIES_REACH_DATA__': SERIES_REACH_DATA,
    '__LOYALTY_JSON__':      LOYALTY_JSON,
    '__LOYALTY_DATA__':      LOYALTY_DATA,
    '__TIMELINE_JSON__':     TIMELINE_JSON,
    '__TIMELINE_DATA__':     TIMELINE_DATA,
    '__TIMELINE_DATES__':    TIMELINE_DATES,
}

for k, v in replacements.items():
    HTML = HTML.replace(k, v)

out = '/sessions/modest-eager-hamilton/mnt/Impact_Dashboard/hucm_unified_dashboard.html'
with open(out, 'w') as f:
    f.write(HTML)
print(f"Written {len(HTML):,} chars to {out}")
