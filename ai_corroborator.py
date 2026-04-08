import os
import pandas as pd
import json

try:
    import google.generativeai as genai
except ImportError:
    print("Error: Missing google-generativeai module. Run: pip install google-generativeai")
    exit(1)

API_KEY = os.getenv("GEMINI_API_KEY")
if not API_KEY:
    print("Error: You must provide a Gemini API Key.")
    print("Run this command first: export GEMINI_API_KEY='your-key'")
    exit(1)

genai.configure(api_key=API_KEY)
model = genai.GenerativeModel(
    model_name="gemini-2.5-pro", 
    tools=[{"google_search_retrieval": {}}]  # Enables native Web Grounding to prevent hallucinations
)

print("🤖 Initializing AI Corroborator with Active Search Grounding...")
try:
    df_missing = pd.read_csv("missing_metadata_profiles.csv")
    names_to_search = df_missing['name'].dropna().unique().tolist()
except FileNotFoundError:
    print("✅ No missing_metadata_profiles.csv found. Nothing to corroborate!")
    exit(0)

if not names_to_search:
    print("✅ No unvetted names found in missing_metadata_profiles.csv!")
    exit(0)

# Batching to avoid context window overflow
batch = names_to_search[:50]

print(f"📡 Sending priority batch of {len(batch)} unknown faculty to Gemini for research...")

prompt = f"""You are an elite data scientist working for Howard University Hospital. 
You have access to Google Search tools. You MUST use them to verify your answers. Do NOT hallucinate.
I am giving you a list of medical professionals who attended an internal faculty development seminar. 

Your job is to search the live web to determine if they are affiliated with Howard University, Howard University Hospital, or a related regional organization (like GHUCCTS), and what their Medical Department and Rank is.
If you cannot verify them via Google Search, output 'Unknown'. Focus on accuracy over guessing.

Output your results strictly as a JSON list of dictionaries with these keys: 
"name", "dept", "rank". No markdown formatting blocks, no introductory text, just the raw JSON array.

List of Profile Names to Evaluate:
{json.dumps(batch)}
"""

try:
    response = model.generate_content(prompt)
    raw_text = response.text.replace("```json", "").replace("```", "").strip()
    
    data = json.loads(raw_text)
    
    ov_file = "directory_overrides.csv"
    ov_df = pd.read_csv(ov_file) if os.path.exists(ov_file) else pd.DataFrame(columns=[
        'name_n_canon', 'name', 'email', 'dept', 'division', 'rank', 'degree', 'pos',
        'admin_title', 'gender', 'ethnicity', 'sessions', 'first_seen', 'last_seen', 'action_delete', 'merge_target'
    ])
    
    existing_names = ov_df['name_n_canon'].dropna().tolist()
    append_rows = []
    
    import re
    # We map missing data back based on canonical names from the missing_metadata filter
    for row in data:
        dept_str = str(row.get('dept', '')).strip().lower()
        if not dept_str or dept_str == 'unknown' or dept_str == 'nan':
            continue
            
        name = row.get('name', '')
        if not name: continue
            
        name_n_canon = df_missing[df_missing['name'] == name]['name_n_canon'].iloc[0] if name in df_missing['name'].values else re.sub(r'[^a-zA-Z\s]', '', name).strip().lower()
        
        new_entry = {
            'name_n_canon': name_n_canon,
            'name': name,
            'dept': row.get('dept', ''),
            'rank': row.get('rank', ''),
            'email': '', 'division': '', 'degree': '', 'pos': '', 'admin_title': '',
            'gender': '', 'ethnicity': '', 'sessions': '', 'first_seen': '', 'last_seen': '',
            'action_delete': '', 'merge_target': ''
        }
        
        if name_n_canon in existing_names:
            ov_idx = ov_df[ov_df['name_n_canon'] == name_n_canon].index[-1]
            for col in ['dept', 'rank']:
                curr_val = str(ov_df.at[ov_idx, col]).strip().lower()
                if curr_val == '' or curr_val == 'unknown' or curr_val == 'nan':
                    ov_df.at[ov_idx, col] = new_entry[col]
                    print(f"    🔄 AI Auto-Fill -> [{name}] {col}: {new_entry[col]}")
        else:
            append_rows.append(new_entry)
            print(f"    ✨ AI Discovered -> [{name}] mapped to {row.get('dept', '')}")
            
    if append_rows:
        new_df = pd.DataFrame(append_rows)
        ov_df = pd.concat([ov_df, new_df], ignore_index=True)
        print(f"    📥 Merged {len(append_rows)} brand new AI profiles into directory_overrides.csv")
        
    ov_df.to_csv(ov_file, index=False)
    print("✅ AI Corroboration Complete.")
    
    print("🔄 Activating strict Dashboard regeneration protocol...")
    os.system("python3 preprocess.py")

except Exception as e:
    print(f"❌ AI Generation Failed: {str(e)}")
