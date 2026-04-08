import os
import glob
import json
import re

FOLDER = os.path.dirname(os.path.abspath(__file__))
DOCS_DIR = os.path.join(FOLDER, "Context Documents")
OUT_FILE = os.path.join(FOLDER, "activities_master.json")

print("\n==========================================================")
print("  HUCM OFD Dashboard — AI Event Extraction Engine")
print("==========================================================")

# Look for PDFs
pdfs = glob.glob(os.path.join(DOCS_DIR, "*.pdf"))

if not pdfs:
    print(f"    ℹ️  No PDFs found in {DOCS_DIR}")
    print("       Drop your event brochures there and re-run!")
else:
    print(f"    🔎  Found {len(pdfs)} event brochure(s).")
    print("    [!] AI processing requires the 'google-generativeai' package and an API Key.")
    
    # We will scaffold the output dictionary
    events = {}
    if os.path.exists(OUT_FILE):
        try:
            with open(OUT_FILE, 'r') as f:
                events = json.load(f)
        except:
            events = {}

    for pdf in pdfs:
        fname = os.path.basename(pdf)
        print(f"      → Scanning {fname} ...")
        
        # NOTE: Once the Gemini API key is configured, the text extraction runs here:
        # 1. model = genai.GenerativeModel('gemini-1.5-flash')
        # 2. Extract: "Date", "Keynote Speaker", "Guest Org", "Sub-Theme"
        # 3. Add to events dictionary.

        # Example Output Scaffold:
        events["Sample Event Title"] = {
            "date": "2025-04-16",
            "speaker": "Pending AI Extraction",
            "topic": "Pending AI Extraction",
            "pdf_source": fname
        }

    with open(OUT_FILE, 'w') as f:
        json.dump(events, f, indent=4)
        
    print(f"\n    ✅  Event list successfully updated at: activities_master.json")

print("==========================================================\n")
