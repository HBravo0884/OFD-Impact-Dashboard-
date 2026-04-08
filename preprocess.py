#!/usr/bin/env python3
"""
HUCM OFD Dashboard — Preprocessing Pipeline
============================================
Reads new Zoom meeting-detail CSV/XLSX files, merges them into the
master dataset (all_final.pkl), regenerates final_payload.json, and
rebuilds hucm_ofd_dashboard.html.

Usage:
    python3 preprocess.py

Just drop new Zoom export files into this folder and run.
"""

import pandas as pd
import json
import glob
import os
import sys
import re
import pickle
from datetime import datetime
from collections import defaultdict
import difflib

FOLDER    = os.path.dirname(os.path.abspath(__file__))
PKL_FILE  = os.path.join(FOLDER, "all_final.pkl")
PAYLOAD   = os.path.join(FOLDER, "final_payload.json")
TEMPLATE  = os.path.join(FOLDER, "dashboard_template.html")
OUTPUT    = os.path.join(FOLDER, "hucm_ofd_dashboard.html")
OVERRIDES = os.path.join(FOLDER, "directory_overrides.csv")
SESSION_OVERRIDES = os.path.join(FOLDER, "ignored_sessions.csv")

print("=" * 58)
print("  HUCM OFD Dashboard — Full Preprocessing Pipeline")
print("=" * 58)

# ── HELPERS ─────────────────────────────────────────────────
def canon(s):
    """Canonical name: lowercase, strip extra spaces, fix dots/apostrophes."""
    if pd.isna(s):
        return ""
    s = str(s).strip()
    # Strip parenthetical descriptors entirely (e.g. "Dr. Kerr (Doctor)" -> "Dr. Kerr ")
    s = re.sub(r'\(.*?\)', '', s)
    # Destroy prefixes
    if s.lower().startswith('dr. '): s = s[4:]
    if s.lower().startswith('dr '): s = s[3:]
    
    # Structural Character Destruction (hyphens, apostrophes)
    s = s.replace("'", "").replace("’", "").replace("-", " ")
    s = s.replace(".", " ").replace("_", " ")
    
    # Reversal Assumption: "Smith, John" -> "John Smith"
    if s.count(',') == 1:
        parts = s.split(',')
        if len(parts) == 2 and 'md' not in parts[1].lower() and 'phd' not in parts[1].lower():
            s = f"{parts[1].strip()} {parts[0].strip()}"
            
    # Generalized Trailing Degree Scrubber
    s = re.sub(r'\s+(md|m\.d\.|phd|ph\.d\.|dds|d\.d\.s\.|do|d\.o\.|ms|m\.s\.|ma|m\.a\.|mph|b\.s\.|bs|ba|b\.a\.|crnp|rn|np)$', '', s, flags=re.IGNORECASE)
            
    return re.sub(r'\s+', ' ', s.strip().lower())

# ── STEP 1: LOAD EXISTING MASTER DATASET ───────────────────
print("\n📦  Loading master dataset (all_final.pkl) ...")
if os.path.exists(PKL_FILE):
    df_master = pd.read_pickle(PKL_FILE)
    
    # Retroactively fix any legacy names that were saved before the new canon rules
    if 'name' in df_master.columns:
        df_master['name_n_canon'] = df_master['name'].apply(lambda x: canon(str(x)))
        df_master['name_canon']   = df_master['name_n_canon'].str.title()
        
    print(f"    ✅  {len(df_master):,} existing records, "
          f"{df_master['name_canon'].nunique()} unique people")
else:
    print("    ⚠️  No existing pkl found — starting fresh.")
    df_master = pd.DataFrame()

# ── STEP 2: FIND NEW CSV / XLSX FILES ──────────────────────
print("\n📂  Scanning for new Zoom export files ...")

# SharePoint/OneDrive override (Update this path to your exact Mac sync folder)
SHAREPOINT_DIR = "/Users/entreprneuros/Examples Impact DashBoards Demo"

search_paths = [
    os.path.join(FOLDER, "meetinglistdetails_*.csv"),
    os.path.join(FOLDER, "meetinglistdetails_*.xlsx"),
    os.path.join(FOLDER, "Data", "meetinglistdetails_*.csv"),
    os.path.join(FOLDER, "Data", "meetinglistdetails_*.xlsx")
]

if SHAREPOINT_DIR and os.path.exists(SHAREPOINT_DIR):
    search_paths.append(os.path.join(SHAREPOINT_DIR, "meetinglistdetails_*.csv"))
    search_paths.append(os.path.join(SHAREPOINT_DIR, "meetinglistdetails_*.xlsx"))

new_files = []
for p in search_paths:
    new_files.extend(glob.glob(p))
new_files = sorted(list(set(new_files)))

if not new_files:
    print("    ℹ️  No new meetinglistdetails_* files found.")
    print("       Drop new Zoom CSV exports here and re-run.\n")
else:
    print(f"    Found {len(new_files)} file(s):")
    for f in new_files:
        print(f"      → {os.path.basename(f)}")


def assign_series(topic):
    t = str(topic)
    if 'All of Us' in t or '"All of Us"' in t:
        return '"All of Us" Training'
    if 'APT' in t and 'Criteria' not in t:
        return 'APT Process'
    if 'Dean' in t and 'Dynamic' in t:
        return "Dean's Dynamic Duo"
    if 'Investigators' in t:
        return 'Investigators & Admin'
    if 'Write-A-Thon' in t or 'SWAT' in t or 'WAG' in t or 'Writing Accountability' in t:
        return 'SWAT Write-A-Thon'
    if 'Faculty Meeting' in t or 'APT Criteria' in t:
        return 'Faculty Meeting'
    if 'FACULTY TOOLS' in t or 'Faculty Tools' in t:
        return 'Faculty Workshop'
    if 'Loan' in t or 'Repayment' in t:
        return 'Seminar'
    if 'Pipeline' in t or 'Leadership' in t:
        return 'Leadership'
    if 'OFD' in t or 'JEDI' in t:
        return 'OFD'
    return 'Other'

def norm_dept(raw):
    if pd.isna(raw) or str(raw).strip() in ('', '-', 'College of Medicine'):
        return ''
    mapping = {
        'gastroenterology': 'Internal Medicine',
        'internal medicine': 'Internal Medicine',
        'im ': 'Internal Medicine',
        'medicine': 'Internal Medicine',
        'physiology': 'Physiology & Biophysics',
        'biophysics': 'Physiology & Biophysics',
        'rad oncology': 'Radiology',
        'radiology': 'Radiology',
        'pediatrics': 'Pediatrics & Child Health',
        'anatomy': 'Anatomy',
        'microbiology': 'Microbiology',
        'pharmacology': 'Pharmacology',
        'pathology': 'Pathology',
        'surgery': 'Surgery',
        'dermatology': 'Dermatology',
        'biochemistry': 'Biochemistry & Molecular Biology',
        'community': 'Community & Family Medicine',
        'family medicine': 'Community & Family Medicine',
        'ofd': 'Office of Faculty Development',
        'faculty development': 'Office of Faculty Development',
        'nursing': 'Nursing',
        'health': 'External / Non-HUCM',
        'ministry': 'External / Non-HUCM',
    }
    raw_l = str(raw).strip().lower()
    for k, v in mapping.items():
        if k in raw_l:
            return v
    if 'howard' not in raw_l:
        return 'External / Non-HUCM'
    return str(raw).strip()

def norm_degree(raw):
    if pd.isna(raw):
        return ''
    d = str(raw).strip().upper()
    if 'MD' in d and 'PHD' in d:
        return 'MD, PhD'
    for pat, val in [('MD','MD'),('PHD','PhD'),('DO','DO'),('MBBS','MBBS'),
                     ('M.S','MS'),('B.S','BS'),('BA','BA'),('MSW','MSW'),
                     ('MPH','MPH'),('PHARMD','PharmD')]:
        if pat in d:
            return val
    return str(raw).strip()[:20]

# ── STEP 3: PARSE NEW CSV FILES ─────────────────────────────
new_records = []

for fpath in new_files:
    fname = os.path.basename(fpath)
    try:
        raw = pd.read_csv(fpath) if fpath.endswith('.csv') else pd.read_excel(fpath)
    except Exception as e:
        print(f"    ⚠️  Skipping {fname}: {e}")
        continue

    raw = raw.dropna(subset=['Topic'])
    raw['Start time'] = pd.to_datetime(raw['Start time'], errors='coerce')
    raw = raw.dropna(subset=['Start time'])
    raw['date_key'] = raw['Start time'].dt.date

    # One record per person per session-date
    seen = set()
    for _, row in raw.iterrows():
        name = str(row.get('Name (original name)', '')).strip()
        if not name or name.lower() in ('nan', ''):
            continue
        key = (str(row.get('ID', '')), str(row['date_key']), canon(name))
        if key in seen:
            continue
        seen.add(key)

        topic    = str(row.get('Topic', ''))
        email    = str(row.get('Email', '')).strip().lower()
        email    = '' if email in ('nan', 'none') else email
        dur      = row.get('Duration (minutes).1', None)
        dept_raw = str(row.get('Department', '')).strip()
        pos_raw  = 'Guest' if str(row.get('Guest', 'No')).strip() == 'Yes' else ''
        guest    = str(row.get('Guest', 'No')).strip()

        new_records.append({
            'name':         name,
            'email':        email,
            'name_n':       canon(name),
            'duration':     float(dur) if pd.notna(dur) else None,
            'dept_raw':     dept_raw,
            'pos_raw':      pos_raw,
            'degree_raw':   '',
            'guest':        guest,
            'topic':        topic,
            'date':         pd.Timestamp(row['date_key']),
            'data_source':  'Account export',
            'series':       assign_series(topic),
            'name_canon':   canon(name).title(),
            'name_n_canon': canon(name),
        })

print(f"\n🔄  Parsed {len(new_records):,} attendance records from new files")

# ── STEP 4: ENRICH WITH EXISTING ROSTER DATA ───────────────
if new_records and not df_master.empty:
    print("🔗  Matching new attendees against existing roster ...")
    # Build a lookup: name_n_canon → enrichment fields
    enrichment_cols = ['dept','pos','degree','rank','ethnicity','gender','admin_title']
    roster = (df_master[df_master['source_lookup'].isin(['CME export','Roster'])]
              .groupby('name_n_canon')
              .first()
              [enrichment_cols]
              .to_dict('index'))

    # Also build email lookup
    email_lookup = {}
    for _, r in df_master[df_master['email'].notna()].iterrows():
        if r['email']:
            email_lookup[str(r['email']).lower()] = r['name_n_canon']

    matched = 0
    for rec in new_records:
        canon_name = rec['name_n_canon']
        # Try name match first, then email match
        roster_entry = roster.get(canon_name)
        if not roster_entry and rec.get('email'):
            lookup_canon = email_lookup.get(rec['email'])
            if lookup_canon:
                roster_entry = roster.get(lookup_canon)

        if roster_entry:
            for col in enrichment_cols:
                rec[col] = roster_entry.get(col, '')
            matched += 1
        else:
            # Use what we have from the CSV
            rec['dept']        = norm_dept(rec['dept_raw'])
            rec['pos']         = 'Guest' if rec['guest'] == 'Yes' else rec['pos_raw'] or 'Unknown'
            rec['degree']      = norm_degree(rec['degree_raw'])
            rec['rank']        = ''
            rec['ethnicity']   = ''
            rec['gender']      = ''
            rec['admin_title'] = ''
        rec['source_lookup'] = 'CME export' if roster_entry else 'Account export'

    print(f"    ✅  Matched {matched}/{len(new_records)} attendees to existing roster")
else:
    for rec in new_records:
        rec.update({'dept': norm_dept(rec.get('dept_raw','')),
                    'pos': 'Guest' if rec.get('guest') == 'Yes' else '',
                    'degree': '', 'rank': '', 'ethnicity': '',
                    'gender': '', 'admin_title': '', 'source_lookup': 'Account export'})

# ── STEP 5: MERGE INTO MASTER DATASET ──────────────────────
# Load ignored sessions blocklist
ignored_sessions = set()
if os.path.exists(SESSION_OVERRIDES):
    try:
        ign_df = pd.read_csv(SESSION_OVERRIDES)
        for t in ign_df.get('topic', []):
            if str(t).strip():
                ignored_sessions.add(str(t).strip().lower())
    except:
        pass

if new_records:
    df_new = pd.DataFrame(new_records)

    if not df_master.empty:
        # Identify records already in the master (by name_n_canon + topic + date)
        existing_keys = set(
            zip(df_master['name_n_canon'],
                df_master['topic'],
                df_master['date'].dt.date if hasattr(df_master['date'].dtype, 'tz') else df_master['date'].dt.date)
        )
        df_new['_key'] = list(zip(df_new['name_n_canon'], df_new['topic'], df_new['date'].dt.date))
        
        # Drop ignored sessions
        df_new_only = df_new[~df_new['topic'].str.lower().str.strip().isin(ignored_sessions)]
        
        # Drop records that already exist
        df_new_only = df_new_only[~df_new_only['_key'].isin(existing_keys)].drop(columns=['_key'])
        
        added_count = len(df_new_only)
        
        if added_count > 0:
            # 1. Provide an Audit Preview
            unique_new_sessions = df_new_only.groupby('topic').agg(
                attendance=('name_n_canon', 'nunique'),
                date=('date', lambda x: x.iloc[0].date() if len(x)>0 else '')
            ).reset_index()
            
            audit_file = os.path.join(FOLDER, "audit_preview.csv")
            unique_new_sessions.to_csv(audit_file, index=False)
            
            print("\n==========================================================")
            print(f"🛑 SESSION AUDIT REQUIRED")
            print(f"Found {len(unique_new_sessions)} new potential sessions.")
            for _, sess in unique_new_sessions.iterrows():
                print(f"  → Assumed Valid: '{sess['topic']}' ({sess['attendance']} attendees on {sess['date']})")
                print(f"    - Justification: Not found in ignored_sessions.csv blocklist.")
            print(f"\nIf any of these are tests or junk, type 'N' to cancel,")
            print("then add their exact topic name to ignored_sessions.csv.")
            print(f"You can review the full breakdown in: {os.path.basename(audit_file)}")
            print("==========================================================")
            
            # Interactive prompt
            ans = input("\n[Press 'Y' to COMMIT these records, or 'N' to REJECT]: ").strip().upper()
            if ans != 'Y':
                print("❌ Update Cancelled by User. The master dataset was untouched.")
                sys.exit(0)
                
            print(f"\n✅ Committing {added_count:,} new records "
                  f"({len(df_new) - added_count:,} already existed or ignored)")
            df_master = pd.concat([df_master, df_new_only], ignore_index=True)
            df_master.to_pickle(PKL_FILE)
            df_master.to_csv(os.path.join(FOLDER, "raw_master_dataset.csv"), index=False)
            print(f"💾  Saved → all_final.pkl and raw_master_dataset.csv ({len(df_master):,} total records)")
        else:
            print(f"\n⏭️   Analyzed {len(df_new)} records — all were ignored or already existed.")
            df_master.to_csv(os.path.join(FOLDER, "raw_master_dataset.csv"), index=False)
    else:
        df_master = df_new
        # If it's a completely fresh build, just commit it
        print(f"\n➕  Starting fresh with {len(df_master):,} records")
        df_master.to_pickle(PKL_FILE)
        df_master.to_csv(os.path.join(FOLDER, "raw_master_dataset.csv"), index=False)
        print(f"💾  Saved → all_final.pkl and raw_master_dataset.csv ({len(df_master):,} total records)")
else:
    print("\n⏭️   No new records to add — using existing pkl.")
    if not df_master.empty:
        df_master.to_csv(os.path.join(FOLDER, "raw_master_dataset.csv"), index=False)

# ── STEP 6: GENERATE final_payload.json ─────────────────────
print("\n📊  Building final_payload.json ...")
if df_master.empty:
    print("⏭️   No data available in pickle or payload. Exiting early.")
    sys.exit(0)

df = df_master.copy()
df['date'] = pd.to_datetime(df['date'])

# ---- CATEGORY EXCLUSION FILTER ----
blocked_series = ['Faculty Meeting', 'OFD', 'Seminar']
pre_series = len(df)
df = df[~df['series'].isin(blocked_series)]
if pre_series > len(df):
    print(f"    🗑️  Category Filter stripped {pre_series - len(df)} records from disabled categories.")


# ---- MICRO-SESSION 10 MINUTE DROP ----
original_total = len(df)
df['duration'] = pd.to_numeric(df['duration'], errors='coerce')
df = df[df['duration'].isna() | (df['duration'] >= 10)]
if original_total > len(df):
    print(f"    ⏱️  Micro-Session Filter burned {original_total - len(df)} ghost entries (<10 minutes).")

# ---- GOD-MODE MANUAL OVERRIDES ----
if os.path.exists(OVERRIDES):
    ov = pd.read_csv(OVERRIDES, dtype=str).fillna('')
    if 'action_delete' in ov.columns:
        deletes = ov[ov['action_delete'].str.strip().str.lower() == 'x']['name_n_canon'].str.strip().tolist()
        if deletes:
            pre_len = len(df)
            df = df[~df['name_n_canon'].isin(deletes)]
            print(f"    ☠️  GOD MODE: Permanently purged {pre_len - len(df)} session logs matching action_delete='X'.")
            
    if 'merge_target' in ov.columns:
        merges = ov[ov['merge_target'].str.strip() != '']
        if not merges.empty:
            merge_dict = dict(zip(merges['name_n_canon'].str.strip(), merges['merge_target'].str.strip().apply(canon)))
            print(f"    🔗  GOD MODE: Forcibly redirecting {len(merge_dict)} broken identities to their merge targets.")
            df['name_n_canon'] = df['name_n_canon'].apply(lambda x: merge_dict.get(x, x))
            df['name_canon'] = df['name_n_canon'].str.title()


# ---- FUZZY MATCHING INTERLOCK ----
print("    🧠  Running fuzzy match algorithm & conflict detector...")
unique_names = df['name_n_canon'].dropna().unique()
merged_mapping = {}

conflict_dict = {}
for n in unique_names:
    sub = df[df['name_n_canon'] == n]
    emails = sub['email'].dropna().str.lower().str.strip().unique()
    depts  = sub['dept'].replace('', pd.NA).dropna().str.lower().str.strip().unique()
    conflict_dict[n] = {'email': list(emails), 'dept': list(depts)}

for n in unique_names:
    if n in merged_mapping: continue
    
    # Generate close matches based on >= 90% Levenshtein similarity
    matches = difflib.get_close_matches(n, unique_names, n=15, cutoff=0.90)
    if len(matches) > 1:
        # Sort so the shortest string is master (e.g. "Lela Brooks" > "Lela Brooks (Howard)")
        matches = sorted(matches, key=len)
        master = matches[0]
        
        for cand in matches:
            if cand == master or cand in merged_mapping: continue
            
            # Extract historical tags
            m_emails, m_depts = conflict_dict[master]['email'], conflict_dict[master]['dept']
            c_emails, c_depts = conflict_dict[cand]['email'], conflict_dict[cand]['dept']
            
            clash = False
            # Clash verification: If both entities have recorded emails, but share NONE, they are disparate people!
            if m_emails and c_emails and not set(m_emails).intersection(c_emails): clash = True
            if m_depts and c_depts and not set(m_depts).intersection(c_depts): clash = True
            
            if not clash:
                merged_mapping[cand] = master
                conflict_dict[master]['email'].extend(c_emails)
                conflict_dict[master]['dept'].extend(c_depts)

if merged_mapping:
    print(f"    🔗  Fuzzy Match Engine algorithm collapsed {len(merged_mapping)} identity fractures!")
    df['name_n_canon'] = df['name_n_canon'].apply(lambda x: merged_mapping.get(x, x))
    df['name_canon'] = df['name_n_canon'].str.title()

# ---- EMAIL AUTHENTICATION INTERLOCK ----
print("    📧  Executing strict Email Trust Authentication...")
df_email = df.dropna(subset=['email']).copy()
df_email['email'] = df_email['email'].str.strip().str.lower()
df_email = df_email[df_email['email'] != '']

email_merged_mapping = {}
garbage_terms = ['iphone', 'ipad', 'mudd', 'room', 'guest', 'unknown', 'device', 'tablet']

for email, group in df_email.groupby('email'):
    names = group['name_n_canon'].dropna().unique()
    if len(names) > 1:
        # Determine the definitive Master Name: The longest string that lacks garbage terms
        def score(n):
            n_val = str(n).lower()
            penalty = 0
            for g in garbage_terms:
                if g in n_val: penalty -= 1000
            return penalty + len(str(n))
            
        best_name = max(names, key=score)
        
        # Override the fractured aliases
        for n in names:
            if n != best_name:
                email_merged_mapping[n] = best_name

if email_merged_mapping:
    print(f"    🔐  Email Authentication forcibly mapped {len(email_merged_mapping)} identity fractures!")
    df['name_n_canon'] = df['name_n_canon'].apply(lambda x: email_merged_mapping.get(x, x))
    df['name_canon'] = df['name_n_canon'].str.title()

# ---- ORPHANED DEVICE BURNER ----
def is_garbage(n):
    n = str(n).lower()
    return any(g in n for g in garbage_terms)

garbage_mask = df['name_n_canon'].apply(is_garbage) & (df['email'].isna() | (df['email'].str.strip() == ''))
dropped_garbage = garbage_mask.sum()
if dropped_garbage > 0:
    print(f"    🔥  Burned {dropped_garbage} unrecoverable garbage device/room sessions!")
    df = df[~garbage_mask]

# ---- REMOVE GHOST SESSIONS (< 5 participants) ----
original_len = len(df)
session_counts = df.groupby(['topic', 'date']).size().reset_index(name='count')
valid_sessions = session_counts[session_counts['count'] >= 5]
df = df.merge(valid_sessions[['topic', 'date']], on=['topic', 'date'], how='inner')
print(f"    🗑️  Filtered ghost sessions (< 5 participants). Clean records: {len(df)} (Dropped: {original_len - len(df)})")

overrides = {}
if os.path.exists(OVERRIDES):
    ov = pd.read_csv(OVERRIDES, dtype=str).fillna('')
    override_cols = ['dept', 'division', 'rank', 'degree', 'pos',
                     'admin_title', 'gender', 'ethnicity', 'email', 'name', 'suffix']
    for _, row in ov.iterrows():
        key = str(row.get('name_n_canon', '')).strip()
        if key:
            overrides[key] = {c: row[c] for c in override_cols if c in row and row[c] != ''}
    print(f"    📋  Loaded {len(overrides)} override entries from directory_overrides.csv")

# Ingest MASTER ROSTER
master_data = {}
master_files = glob.glob(os.path.join(FOLDER, 'Context Data', '*MASTER.ROSTER*.xlsx'))
for mf in master_files:
    try:
        m_df = pd.read_excel(mf).dropna(subset=['Last Name', 'First Name'])
        for _, r in m_df.iterrows():
            fname, lname = str(r.get('First Name', '')).strip(), str(r.get('Last Name', '')).strip()
            name_c = canon(f"{fname} {lname}")
            if name_c not in master_data: master_data[name_c] = {}
            if pd.notna(r.get('Department')) and str(r['Department']).strip():
                master_data[name_c]['dept'] = str(r['Department']).strip()
            if pd.notna(r.get('Division')) and str(r['Division']).lower() not in ['nan','none','']:
                master_data[name_c]['division'] = str(r['Division']).strip()
            if pd.notna(r.get('Rank')) and str(r['Rank']).strip():
                master_data[name_c]['rank'] = str(r['Rank']).strip()
    except: pass

# Ingest ADMINISTRATIVE UNITS
admin_mapping = {}
admin_files = glob.glob(os.path.join(FOLDER, 'Context Data', '*Administrative Units*.xlsx'))
for af in admin_files:
    try:
        a_df = pd.read_excel(af, header=None)
        for _, row in a_df.iterrows():
            dept_name = str(row[0]).strip()
            if dept_name and dept_name.lower() not in ['nan', 'department/center', 'none']:
                for i in range(1, len(row)):
                    val = str(row[i]).replace('Ms.', '').replace('Mr.', '').replace(', M.D.', '').replace(', Ph.D.', '').replace(', MD', '').strip()
                    if val and val.lower() not in ['nan', 'none', '']:
                        import re
                        clean_name = re.sub(r'[\d\-]', '', val).strip()
                        if clean_name and len(clean_name) > 3:
                            admin_mapping[canon(clean_name)] = dept_name
    except: pass

# Load Org Charts for Additive Sync (Hierarchy & Locations)
org_charts = glob.glob(os.path.join(FOLDER, 'Context Data', '*Wayne Frederick*.xlsx')) + \
             glob.glob(os.path.join(FOLDER, 'Context Data', '*Academic Org*.xlsx'))
org_data = {}
for oc_path in org_charts:
    try:
        odf = pd.read_excel(oc_path)
        for _, row in odf.dropna(subset=['Name']).iterrows():
            raw_name = str(row['Name']).strip()
            is_cl = '[C]' in raw_name
            name_c = canon(raw_name.replace('[C]', ''))
            
            title = str(row.get('Line Detail 1', '')).replace('[C]','').strip()
            title = '' if title.lower() in ['nan', 'none', ''] else title
            loc = str(row.get('Line Detail 2', '')).replace('[C]','').strip()
            loc = '' if loc.lower() in ['nan', 'none', ''] else loc
            
            reports = str(row.get('Reports To', '')).strip()
            mgr_key = ''
            if reports and reports.lower() not in ['nan', 'none', '']:
                mgr_str = reports.split('_', 1)[-1].replace('_', ' ').strip()
                mgr_key = canon(mgr_str)
            
            if name_c not in org_data:
                org_data[name_c] = {}
            if title: org_data[name_c]['admin_title'] = title
            if loc: org_data[name_c]['location'] = loc
            if mgr_key: org_data[name_c]['reports_to'] = mgr_key
            if is_cl: org_data[name_c]['clinical'] = True
    except Exception as e:
        pass
if org_data:
    print(f"    🔗  Loaded {len(org_data)} hierarchy references from Org Charts")

# Stage 1: Collapse multiple-joins into single unique events per date/topic
session_events = df.groupby(['name_n_canon', 'topic', 'date']).agg(
    name_canon  = ('name_canon',  'first'),
    email       = ('email',       lambda x: next((v for v in x if str(v).strip() and str(v) != 'nan'), '')),
    degree      = ('degree',      'first'),
    dept        = ('dept',        'first'),
    pos         = ('pos',         'first'),
    rank        = ('rank',        'first'),
    admin_title = ('admin_title', 'first'),
    gender      = ('gender',      'first'),
    ethnicity   = ('ethnicity',   'first'),
    series      = ('series',      'first'),
    duration    = ('duration',    'sum'),
    raw_names   = ('name',        lambda x: list(set(x))),
).reset_index()

# Stage 2: Aggregate events to unique people targets securely
people = session_events.groupby('name_n_canon').agg(
    name        = ('name_canon',  'first'),
    email       = ('email',       lambda x: next((v for v in x if str(v).strip() and str(v) != 'nan'), '')),
    degree      = ('degree',      'first'),
    dept        = ('dept',        'first'),
    pos         = ('pos',         'first'),
    rank        = ('rank',        'first'),
    admin_title = ('admin_title', 'first'),
    gender      = ('gender',      'first'),
    ethnicity   = ('ethnicity',   'first'),
    session_count = ('topic',     'count'),
    series_list = ('series',      lambda x: ' | '.join(sorted(set([s for s in x if str(s) != 'nan'])))),
    cumulative_minutes = ('duration', lambda x: int(x.sum()) if x.notna().any() else 0),
    first_seen  = ('date',        'min'),
    last_seen   = ('date',        'max'),
    known_aliases = ('raw_names', lambda x: ' | '.join(sorted(set([str(n).strip() for sublist in x for n in sublist if str(n).strip()])))),
).reset_index()

# Apply overrides from directory_overrides.csv
people['division'] = ''
people['location'] = ''
people['suffix']   = ''
for col in ['name','email','dept','division','location','rank','degree','pos','admin_title','gender','ethnicity','suffix']:
    if col not in people.columns:
        people[col] = ''
for idx, row in people.iterrows():
    name_c = row['name_n_canon']
    
    # 1. MASTER ROSTER SYNC
    md = master_data.get(name_c)
    if md:
        if md.get('dept') and not people.at[idx, 'dept']:
            people.at[idx, 'dept'] = md['dept']
        if md.get('division') and not people.at[idx, 'division']:
            people.at[idx, 'division'] = md['division']
        if md.get('rank') and not people.at[idx, 'rank']:
            people.at[idx, 'rank'] = md['rank']

    # 2. ADDITIVE ORG CHART SYNC
    oc = org_data.get(name_c)
    if oc:
        if oc.get('admin_title') and not people.at[idx, 'admin_title']:
            people.at[idx, 'admin_title'] = oc['admin_title']
        if oc.get('location') and not people.at[idx, 'location']:
            people.at[idx, 'location'] = oc['location']
        if oc.get('clinical'):
            people.at[idx, 'pos'] = 'Clinical Faculty'

    # 3. OVERRIDES FORCE SYNC
    ov = overrides.get(name_c, {})
    for col, val in ov.items():
        if col in people.columns and val:
            people.at[idx, col] = val

    # 4. RECURSIVE ADMINISTRATIVE UNIT INFERENCE
    current_dept = str(people.at[idx, 'dept']).strip().lower()
    if current_dept in ['nan', 'none', '', 'unknown']:
        if name_c in admin_mapping:
            people.at[idx, 'dept'] = admin_mapping[name_c]
        elif oc and oc.get('reports_to'):
            mgr_key = oc['reports_to']
            if mgr_key in admin_mapping:
                people.at[idx, 'dept'] = admin_mapping[mgr_key]
                
    # 5. NORMALIZE DEGREE
    people.at[idx, 'degree'] = norm_degree(people.at[idx, 'degree'])
    
    # 4. REFINED POSITION CLEANUP & STANDARDIZATION
    rank_str = str(people.at[idx, 'rank']).lower()
    current_pos_raw = str(people.at[idx, 'pos']).strip()
    dept_clean = str(people.at[idx, 'dept']).strip().lower()
    
    clinical_depts = [
        'internal medicine', 'pathology', 'dermatology', 'radiology', 
        'pediatrics & child health', 'surgery', 'community & family medicine', 
        'psychiatry & behavioral sciences', 'orthopedic surgery', 'neurology', 
        'radiation oncology', 'obstetrics & gynecology', 'anesthesiology', 'emergency medicine'
    ]
    is_faculty_rank = any(q in rank_str for q in ['professor', 'instructor'])

    # If position is generic, refine it based on department to Clinical vs Basic Science
    if current_pos_raw in ['Faculty', 'Other Faculty', 'Unknown', ''] or (not current_pos_raw and is_faculty_rank):
        if dept_clean in clinical_depts or current_pos_raw.lower() == 'clinical faculty':
            people.at[idx, 'pos'] = 'Clinical Faculty'
        elif is_faculty_rank or 'faculty' in current_pos_raw.lower():
            people.at[idx, 'pos'] = 'Basic Science Faculty'

    # Manual User Overrides for specialized cases
    if name_c == 'anjanette antonio':
        people.at[idx, 'pos'] = 'Investigators & Admin'

    # 5. INTELLIGENT CREDENTIAL INFERENCE 
    final_pos = str(people.at[idx, 'pos']).lower()
    current_degree = str(people.at[idx, 'degree']).strip()
    
    if not current_degree and ('faculty' in final_pos or is_faculty_rank):
        if final_pos == 'clinical faculty' or dept_clean in clinical_depts:
            people.at[idx, 'degree'] = 'MD'
        else:
            people.at[idx, 'degree'] = 'PhD'

people['session_count'] = people['session_count'].astype(str)
people['first_seen']    = people['first_seen'].dt.strftime('%Y-%m-%d')
people['last_seen']     = people['last_seen'].dt.strftime('%Y-%m-%d')

# ---- GENERATE MISSING METADATA TO-DO LIST ----
print("    📝  Generating missing_metadata_profiles.csv to-do list...")
missing_mask = (people['dept'].isna()) | (people['dept'].str.strip() == '') | (people['dept'].str.lower() == 'unknown')
missing_df = people[missing_mask].copy()
if not missing_df.empty:
    missing_df['sessions'] = missing_df['session_count']
    missing_df['action_delete'] = ''
    missing_df['merge_target'] = ''
    
    # Ensure all exact directory_overrides.csv columns exist
    ordered_cols = ['name_n_canon', 'name', 'email', 'dept', 'division', 'rank', 'degree', 'pos', 'admin_title', 'gender', 'ethnicity', 'sessions', 'first_seen', 'last_seen', 'action_delete', 'merge_target']
    
    for c in ordered_cols:
        if c not in missing_df.columns:
            missing_df[c] = ''
            
    # Purge any extra internal columns and export exactly matching the schema
    missing_df = missing_df[ordered_cols]
    missing_df.to_csv(os.path.join(FOLDER, "missing_metadata_profiles.csv"), index=False)

# ---- STRICT QUARANTINE PROTOCOL ----
quarantined_names = people[missing_mask]['name_n_canon'].tolist()
if quarantined_names:
    print(f"    🚫  STRICT QUARANTINE: Amputating {len(quarantined_names)} unvetted profiles from UI payload.")
    people = people[~missing_mask]
    df = df[~df['name_n_canon'].isin(quarantined_names)]

# ---- EXPORT ACCEPTED DIRECTORY ----
# Export a clean, user-readable CSV of only the profiles that successfully survived quarantine
accepted_cols = ['name', 'email', 'dept', 'division', 'rank', 'degree', 'pos', 'admin_title', 'session_count', 'cumulative_minutes', 'first_seen', 'last_seen']
export_df = people[[c for c in accepted_cols if c in people.columns]].copy()
export_df.to_csv(os.path.join(FOLDER, "accepted_directory_profiles.csv"), index=False)
print(f"    ✅  Saved → accepted_directory_profiles.csv ({len(export_df)} verified profiles)")

# ---- ALIAS ADJUDICATION DATABASE ----
alias_cols = ['name', 'name_n_canon', 'known_aliases', 'email', 'dept']
alias_df = people[[c for c in alias_cols if c in people.columns]].copy()
alias_df.to_csv(os.path.join(FOLDER, "accepted_members_aliases.csv"), index=False)
print(f"    ✅  Saved → accepted_members_aliases.csv (Security Audit Trail)")
# KPIs
sessions_per_person = session_events.groupby('name_n_canon')['topic'].count()
unique_sessions     = session_events.drop_duplicates(subset=['topic', 'date'])
dept_covered        = people[(people['dept'].notna()) &
                              (people['dept'] != '') &
                              (people['dept'] != 'External / Non-HUCM')]
dept_pct = round(len(dept_covered) / max(len(people), 1) * 100)

kpis = {
    'total_records':   len(session_events),
    'unique_people':   len(people),
    'unique_meetings': session_events.drop_duplicates(subset=['topic', 'date']).shape[0],
    'dept_pct':        dept_pct,
}

# Series: total records + unique reach
series_total   = session_events.groupby('series').size().sort_values(ascending=False)
series_reach_d = session_events.groupby('series')['name_n_canon'].nunique().sort_values(ascending=False)

# Sessions per series and avg
sess_by_series = session_events.drop_duplicates(subset=['series', 'topic', 'date']).groupby('series').size()
series_avg_v   = (series_total / sess_by_series).round(1)

# Per-session attendance for timeline
tl_df = (session_events.groupby(['date','topic','series'])
          .size().reset_index(name='n')
          .sort_values('date'))
timeline = [{'label': f"{r['date'].strftime('%Y-%m')} · {r['topic'][:35]}",
             'date':  r['date'].strftime('%Y-%m-%d'),
             'n':     int(r['n']),
             'series': str(r['series'])}
            for _, r in tl_df.iterrows()]

# Sessions list (for session bar chart)
sessions_list = sorted(
    [{'label': f"{r['date'].strftime('%b %d')}·{r['topic'][:35]}",
      'series': r['series'], 'n': int(r['n']),
      'date_str': r['date'].strftime('%Y-%m-%d'),
      'topic': r['topic']}
     for _, r in tl_df.iterrows()],
    key=lambda x: x['date_str']
)

# Sessions by series (for grouped chart)
sessions_by_series = {}
for _, row in tl_df.iterrows():
    s = row['series']
    if s not in sessions_by_series:
        sessions_by_series[s] = []
    sessions_by_series[s].append({'label': row['topic'][:30],
                                   'date': row['date'].strftime('%Y-%m-%d'),
                                   'n': int(row['n'])})

# Dept / rank / pos / degree / ethnicity / gender
def top_n(series_obj, n=12):
    s = series_obj.dropna()
    s = s[s != '']
    s = s.value_counts().head(n)
    return {'labels': s.index.tolist(), 'values': s.tolist()}

depts    = top_n(people['dept'], 12)

# Custom Rank Buckets
rank_map = {'Professor': 0, 'Associate Professor': 0, 'Assistant Professor': 0, 'Staff': 0}
for _, p in people.iterrows():
    r = str(p.get('rank', '')).strip().lower()
    pos = str(p.get('pos', '')).strip().lower()
    if 'assistant professor' in r:
        rank_map['Assistant Professor'] += 1
    elif 'associate professor' in r:
        rank_map['Associate Professor'] += 1
    elif 'professor' in r:
        rank_map['Professor'] += 1
    elif 'staff' in pos or 'staff' in r:
        rank_map['Staff'] += 1
ranks = {'labels': list(rank_map.keys()), 'values': list(rank_map.values())}

pos_cnt  = people.groupby('pos').size().sort_values(ascending=False)
pos_out  = {'labels': pos_cnt.index.tolist(), 'values': pos_cnt.tolist()}
eth_cnt  = people['ethnicity'].replace('', None).dropna().value_counts()
gen_cnt  = people['gender'].replace('', None).dropna().value_counts()

# Loyalty buckets (unique people by total sessions attended)
sp = session_events.groupby('name_n_canon').size()
loyalty_labels = ['1 session', '2–3 sessions', '4–6 sessions', '7+ sessions']
loyalty_values = [
    int((sp == 1).sum()),
    int(((sp >= 2) & (sp <= 3)).sum()),
    int(((sp >= 4) & (sp <= 6)).sum()),
    int((sp >= 7).sum()),
]

# Scatter: people × sessions attended
sc_df = session_events.groupby('name_n_canon').agg(
    x=('date', 'count'),
    y=('series', 'nunique'),
    pos=('pos', 'first'),
    dept=('dept', 'first'),
    name=('name_canon', 'first')
).reset_index()
scatter = [{'x': int(r['x']), 'y': int(r['y']), 'pos': r['pos'], 'dept': r['dept'], 'name': r['name']}
           for _, r in sc_df.iterrows() if r['x'] > 0][:300]

# Dept bubbles
dept_b = (session_events[session_events['dept'].notna() & (session_events['dept'] != '')]
          .groupby('dept')
          .agg(people=('name_n_canon','nunique'), records=('name_canon','count'))
          .reset_index()
          .sort_values('people', ascending=False)
          .head(15))
dept_bubbles = [{'dept': r['dept'], 'people': int(r['people']),
                 'records': int(r['records']),
                 'avg_sessions': round(float(r['records']) / max(1, int(r['people'])), 1),
                 'total': int(r['records'])}
                for _, r in dept_b.iterrows()]

# Directory table
table_rows = []
for _, p in people.sort_values('name').iterrows():
    table_rows.append({
        'name':          p['name'],
        'suffix':        p.get('suffix', ''),
        'degree':        p.get('degree', ''),
        'email':         p.get('email', ''),
        'dept':          p.get('dept', ''),
        'division':      p.get('division', ''),
        'pos':           p.get('pos', ''),
        'rank':          p.get('rank', ''),
        'admin_title':   p.get('admin_title', ''),
        'series':        p.get('series_list', ''),
        'session_count': p['session_count'],
        'cumulative_minutes': p.get('cumulative_minutes', 0),
        'first_seen':    p['first_seen'],
        'last_seen':     p['last_seen'],
    })

# Unique sorted lists for filter dropdowns
dept_list     = sorted(people['dept'].dropna().unique().tolist())
rank_list     = sorted(people['rank'].dropna().unique().tolist())
division_list = sorted([d for d in people['division'].dropna().unique().tolist() if d])
person_list   = sorted(people['name'].dropna().unique().tolist())

# Drilldown Structures: Dept -> Series counts & Person -> Series counts
dept_series_b = session_events.groupby(['dept', 'series']).size().reset_index(name='n')
dept_series_dict = {}
for _, r in dept_series_b.iterrows():
    d = str(r['dept']).strip()
    if not d: continue
    s = str(r['series'])
    if d not in dept_series_dict: dept_series_dict[d] = {}
    dept_series_dict[d][s] = int(r['n'])

person_series_b = session_events.groupby(['name_canon', 'series']).size().reset_index(name='n')
person_series_dict = {}
for _, r in person_series_b.iterrows():
    p = str(r['name_canon'])
    s = str(r['series'])
    if p not in person_series_dict: person_series_dict[p] = {}
    person_series_dict[p][s] = int(r['n'])

payload = {
    'kpis':     kpis,
    'series':   {'labels': series_total.index.tolist(),
                 'values': [int(v) for v in series_total.values]},
    'series_reach': {k: int(v) for k, v in series_reach_d.items()},
    'series_avg': {
        'labels':   series_total.index.tolist(),
        'values':   [float(series_avg_v.get(s, 0)) for s in series_total.index],
        'sessions': [int(sess_by_series.get(s, 0)) for s in series_total.index],
        'totals':   [int(series_total.get(s, 0)) for s in series_total.index],
    },
    'sessions':           sessions_list,
    'sessions_by_series': sessions_by_series,
    'timeline':           timeline,
    'depts':              depts,
    'ranks':              ranks,
    'positions':          pos_out,
    'ethnicity':          {'labels': eth_cnt.index.tolist(), 'values': [int(v) for v in eth_cnt.values]},
    'gender':             {'labels': gen_cnt.index.tolist(), 'values': [int(v) for v in gen_cnt.values]},
    'loyalty':            {'labels': loyalty_labels, 'values': loyalty_values},
    'scatter_people':     scatter,
    'dept_bubbles':       dept_bubbles,
    'table':              table_rows,
    'dept_list':          dept_list,
    'rank_list':          rank_list,
    'division_list':      division_list,
    'person_list':        person_list,
    'dept_series':        dept_series_dict,
    'person_series':      person_series_dict,
}

with open(PAYLOAD, 'w') as f:
    json.dump(payload, f)
print(f"    ✅  Saved → final_payload.json  "
      f"({len(table_rows)} people, {kpis['unique_meetings']} sessions)")

# ── STEP 7: BUILD DASHBOARD HTML ─────────────────────────────
if not os.path.exists(TEMPLATE):
    print(f"\n⚠️  dashboard_template.html not found — skipping HTML build.")
else:
    print("\n🎨  Rebuilding dashboard HTML ...")
    D = payload

    def series_tags(series_str):
        colors = {'CME':'#4f86c6','Grand Rounds':'#e07b39','Research':'#5aab61',
                  'Lecture':'#a855c8','Conference':'#c84a4a','Workshop':'#d4a017',
                  'Seminar':'#2aacbb'}
        tags = []
        for s in str(series_str).split(' | '):
            s = s.strip()
            if not s: continue
            col = '#888'
            for k, v in colors.items():
                if k.lower() in s.lower(): col = v; break
            tags.append(f'<span class="stag" style="background:{col}22;'
                        f'color:{col};border:1px solid {col}44">{s}</span>')
        return ''.join(tags)

    def dir_rows():
        rows = []
        for p in D['table']:
            deg = f'<span class="deg-inline">{p["degree"]}</span>' if p.get('degree') else ''
            sfx = p.get('suffix', '')
            sfx_cell = f'<span class="suffix-tag">{sfx}</span>' if sfx else ''
            div = p.get('division', '')
            div_cell = f'<span class="division-tag">{div}</span>' if div else ''
            rows.append(
                f'<tr data-dept="{p.get("dept","")}" data-division="{div}" '
                f'data-pos="{p.get("pos","")}" '
                f'data-rank="{p.get("rank","")}" data-sessions="{p.get("session_count",0)}">\n'
                f'  <td><span class="name-cell">{p["name"]}</span>{deg}</td>\n'
                f'  <td>{p.get("admin_title","")}</td>\n'
                f'  <td>{p.get("dept","")}</td>\n'
                f'  <td>{div_cell}</td>\n'
                f'  <td>{p.get("rank","")}</td>\n'
                f'  <td>{p.get("pos","")}</td>\n'
                f'  <td>{p.get("email","")}</td>\n'
                f'  <td class="num">{p.get("cumulative_minutes","")}</td>\n'
                f'  <td class="num">{p.get("session_count",0)}</td>\n'
                f'  <td>{series_tags(p.get("series",""))}</td>\n'
                f'  <td>{p.get("first_seen","")[:7]}</td>\n'
                f'  <td>{p.get("last_seen","")[:7]}</td>\n'
                f'</tr>'
            )
        return '\n'.join(rows)

    def top_table_rows():
        sorted_p = sorted(D['table'], key=lambda x: (int(x.get('session_count', 0)), int(x.get('cumulative_minutes', 0))), reverse=True)
        top10 = sorted_p[:10]
        rows = []
        for i, p in enumerate(top10):
            rows.append(
                f'<tr>\n'
                f'  <td style="font-weight:700; color:var(--c1d)">#{i+1}</td>\n'
                f'  <td class="name-cell">{p["name"]}</td>\n'
                f'  <td>{p.get("dept","")}</td>\n'
                f'  <td class="num" style="color:var(--c2d); font-size:1.1rem">{p.get("session_count",0)}</td>\n'
                f'  <td class="num">{p.get("cumulative_minutes",0)}</td>\n'
                f'</tr>'
            )
        return '\n'.join(rows)

    def la(d): return json.dumps(d['labels']), json.dumps(d['values'])
    POS_L, POS_D = la(D['positions'])
    ETH_L, ETH_D = la(D['ethnicity'])
    GEN_L, GEN_D = la(D['gender'])

    replacements = {
        '__SERIES_JSON__':        json.dumps(D['series']['labels']),
        '__SERIES_DATA__':        json.dumps(D['series']['values']),
        '__SESSIONS_JSON__':      json.dumps([s['label'][:45] for s in D['sessions']]),
        '__SESSIONS_DATA__':      json.dumps([s['n'] for s in D['sessions']]),
        '__DEPTS_JSON__':         json.dumps(D['depts']['labels']),
        '__DEPTS_DATA__':         json.dumps(D['depts']['values']),
        '__RANKS_JSON__':         json.dumps(D['ranks']['labels']),
        '__RANKS_DATA__':         json.dumps(D['ranks']['values']),
        '__POS_LABELS__':         POS_L, '__POS_DATA__': POS_D,
        '__ETH_LABELS__':         ETH_L, '__ETH_DATA__': ETH_D,
        '__GEN_LABELS__':         GEN_L, '__GEN_DATA__': GEN_D,
        '__KPI_RECORDS__':        str(D['kpis']['total_records']),
        '__KPI_PEOPLE__':         str(D['kpis']['unique_people']),
        '__KPI_MEETINGS__':       str(D['kpis']['unique_meetings']),
        '__KPI_DEPT__':           str(D['kpis']['dept_pct']),
        '__DIR_ROWS__':           dir_rows(),
        '__TOP_TABLE_ROWS__':     top_table_rows(),
        '__DEPT_OPTS__':          '\n'.join(['<option value="">All Departments</option>'] +
                                  [f'<option value="{d}">{d}</option>'
                                   for d in sorted(D['dept_list']) if d]),
        '__PERSON_OPTS__':        '\n'.join(['<option value="">Search or Select Attendee...</option>'] +
                                  [f'<option value="{p}">{p}</option>'
                                   for p in sorted(D.get('person_list', [])) if p]),
        '__DIVISION_OPTS__':      '\n'.join(['<option value="">All Divisions</option>'] +
                                  [f'<option value="{d}">{d}</option>'
                                   for d in sorted(D.get('division_list', [])) if d]),
        '__RANK_OPTS__':          '\n'.join(['<option value="">All Ranks</option>'] +
                                  [f'<option value="{r}">{r}</option>'
                                   for r in sorted(D['rank_list']) if r]),
        '__SERIES_AVG_JSON__':    json.dumps(D['series_avg']['labels']),
        '__SERIES_AVG_DATA__':    json.dumps(D['series_avg']['values']),
        '__SERIES_AVG_SESS__':    json.dumps(D['series_avg']['sessions']),
        '__SERIES_AVG_TOTALS__':  json.dumps(D['series_avg']['totals']),
        '__SESSIONS_BY_SERIES__': json.dumps(D['sessions_by_series']),
        '__SCATTER_PEOPLE__':     json.dumps(D['scatter_people']),
        '__DEPT_BUBBLES__':       json.dumps(D['dept_bubbles']),
        '__SERIES_REACH_JSON__':  json.dumps(list(D['series_reach'].keys())),
        '__SERIES_REACH_DATA__':  json.dumps(list(D['series_reach'].values())),
        '__LOYALTY_JSON__':       json.dumps(D['loyalty']['labels']),
        '__LOYALTY_DATA__':       json.dumps(D['loyalty']['values']),
        '__TIMELINE_JSON__':      json.dumps([s['label'] for s in D['timeline']]),
        '__TIMELINE_DATA__':      json.dumps([s['n'] for s in D['timeline']]),
        '__TIMELINE_DATES__':     json.dumps([s['date'] for s in D['timeline']]),
        '__TIMELINE_SERIES__':    json.dumps([s.get('series', '') for s in D['timeline']]),
        '__DEPT_SERIES_MAP__':    json.dumps(D.get('dept_series', {})),
        '__PERSON_SERIES_MAP__':  json.dumps(D.get('person_series', {})),
    }

    total_mins = sum([int(p.get('cumulative_minutes', 0)) for p in D['table']])
    avg_dur = round(total_mins / max(1, D['kpis']['total_records']))
    replacements['__KPI_AVG_DUR__'] = f"{avg_dur}m"

    if sum(D['loyalty']['values']) > 0:
        repeat_attendees = sum(D['loyalty']['values'][1:])
        repeat_pct = round((repeat_attendees / D['kpis']['unique_people']) * 100)
    else:
        repeat_pct = 0
    replacements['__KPI_REPEAT__'] = f"{repeat_pct}%"

    HTML = open(TEMPLATE).read()
    for k, v in replacements.items():
        HTML = HTML.replace(k, v)

    missed = [k for k in replacements if k in HTML]
    if missed:
        print(f"    ⚠️  Unfilled placeholders: {missed}")

    with open(OUTPUT, 'w', encoding='utf-8') as f:
        f.write(HTML)
    
    # Force auto-refresh of index.html so local users never hit stale files
    INDEX_FILE = os.path.join(FOLDER, "index.html")
    with open(INDEX_FILE, 'w', encoding='utf-8') as f:
        f.write(HTML)

    import subprocess
    size_kb = os.path.getsize(OUTPUT) / 1024
    print(f"    ✅  Saved → hucm_ofd_dashboard.html & index.html ({size_kb:.0f} KB)")

print(f"\n✅  Pipeline complete — {datetime.now().strftime('%B %d, %Y at %I:%M %p')}")
print(f"\n🚀  Triggering automatic Netlify deployment...")
print("-" * 58)
try:
    subprocess.run(["npx", "netlify-cli", "deploy", "--prod"], cwd=FOLDER, check=True)
    print("\n✅  Successfully deployed to Live Netlify Dashboard!")
except Exception as e:
    print(f"\n❌  Failed to deploy to Netlify automatically: {e}")
print("=" * 58)
