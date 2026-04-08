import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import difflib
import os

url = "https://medicine.howard.edu/faculty"
print(f"📡 Fetching directory from: {url}")

try:
    headers = {'User-Agent': 'Mozilla/5.0'}
    response = requests.get(url, headers=headers, timeout=15)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')
    
    faculty_list = []
    
    # We need to find the profile cards. 
    # Usually they are inside list items or article tags.
    # Let's target the exact structure. Howard profiles usually use .profile-card or similar.
    # Actually, the markdown showed: H3 for name, H4 for title, H5 for department.
    
    # Let's extract all h3s that look like names.
    for h3 in soup.find_all('h3'):
        name = h3.get_text(strip=True)
        if not name or "Navigation" in name: continue
        
        # Traverse siblings
        container = h3.parent
        text_blocks = container.get_text(separator='|', strip=True).split('|')
        
        # We can also just find the associated department and title
        # Often H4 is Title, H5 is Dept.
        title = ""
        dept = ""
        email = ""
        
        h4 = h3.find_next_sibling('h4')
        if h4 and h4.parent == container:
            title = h4.get_text(strip=True)
            
        h5 = h3.find_next_sibling('h5')
        if h5 and h5.parent == container:
            dept = h5.get_text(strip=True)
            if dept.lower() in ['she/her', 'he/his', 'he, him']:
                # The first H5 was pronouns, the second might be dept
                h5_2 = h5.find_next_sibling('h5')
                if h5_2:
                    dept = h5_2.get_text(strip=True)
                    
        a = h3.find_next_sibling('a')
        while a and a.parent == container:
            if 'mailto:' in a.get('href', ''):
                email = a.get_text(strip=True)
                break
            a = a.find_next_sibling('a')
            
        if title or dept:
            faculty_list.append({
                'scraped_name': name,
                'rank/title': title,
                'dept': dept,
                'email': email
            })
            
    print(f"🔎 Extracted {len(faculty_list)} faculty members from webpage.")
    
    if os.path.exists("missing_metadata_profiles.csv"):
        missing_df = pd.read_csv("missing_metadata_profiles.csv")
        missing_names = missing_df['name'].dropna().tolist()
        
        scraped_names = [f['scraped_name'] for f in faculty_list]
        
        matches = []
        for miss in missing_names:
            # Clean missing name
            m_clean = re.sub(r'[^a-zA-Z\s]', '', miss).strip().lower()
            best_match = None
            best_score = 0
            
            for scrap in faculty_list:
                s_clean = re.sub(r'[^a-zA-Z\s]', '', scrap['scraped_name']).strip().lower()
                
                # Check for direct inclusion or high similarity
                if m_clean in s_clean or s_clean in m_clean:
                    best_match = scrap
                    break
                    
                score = difflib.SequenceMatcher(None, m_clean, s_clean).ratio()
                if score > 0.85 and score > best_score:
                    best_score = score
                    best_match = scrap
                    
            if best_match:
                matches.append({
                    'name_n_canon': missing_df[missing_df['name']==miss]['name_n_canon'].iloc[0],
                    'name': miss,
                    'dept': best_match['dept'],
                    'rank': best_match['rank/title'],
                    'email': best_match['email']
                })
                
        if matches:
            print(f"🔗 Successfully matched {len(matches)} missing profiles to the public directory!")
            out_df = pd.DataFrame(matches)
            # Ensure it fits overrides schema format
            ordered_cols = ['name_n_canon', 'name', 'email', 'dept', 'division', 'rank', 'degree', 'pos', 'admin_title', 'gender', 'ethnicity', 'sessions', 'first_seen', 'last_seen', 'action_delete', 'merge_target']
            for c in ordered_cols:
                if c not in out_df.columns:
                    out_df[c] = ''
            out_df = out_df[ordered_cols]
            out_df.to_csv('ai_suggested_overrides.csv', index=False)
            print("✅ Saved matches to 'ai_suggested_overrides.csv' for user review.")
        else:
            print("⚠️ No matches found between the active missing list and the directory URL.")
    else:
        print("❌ missing_metadata_profiles.csv not found.")

except Exception as e:
    import traceback
    traceback.print_exc()
