import http.server
import json
import os
import shutil
import glob
import urllib.parse
from datetime import datetime

# ==========================================
# CONFIGURATION
# Set exactly where you want the Kiosk to upload forms.
# By default, it builds the structure inside the current app workspace.
SHAREPOINT_SYNC_FOLDER = "OFD_CME_Records"
# ==========================================

PORT = 8080

class KioskUploadHandler(http.server.SimpleHTTPRequestHandler):
    def do_OPTIONS(self):
        self.send_response(200, "ok")
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
        
    def do_POST(self):
        if self.path == '/upload':
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            data = json.loads(post_data.decode('utf-8'))
            
            series = data.get("series", "General")
            event_name = data.get("eventName", "Event").replace(":", "").replace("/", "-")
            event_date = data.get("date", "YYYY-MM-DD")
            
            clean_event_name = "".join(x for x in event_name if x.isalnum() or x in " _-")
            clean_series = "".join(x for x in series if x.isalnum() or x in " _-")
            timestamp = str(int(datetime.now().timestamp() * 1000))
            
            # Formulate structured paths
            base_dir = os.path.abspath(SHAREPOINT_SYNC_FOLDER)
            print_dir = os.path.join(base_dir, "1_Printable_Audit_Logs")
            series_dir = os.path.join(print_dir, clean_series)
            csv_dir = os.path.join(base_dir, "2_Dashboard_Data_Pipelines")
            json_dir = os.path.join(base_dir, "3_ML_Biometric_Backups")
            
            for d in [series_dir, csv_dir, json_dir]:
                os.makedirs(d, exist_ok=True)
                
            # 1. Save Pipeline CSV
            csv_filename = os.path.join(csv_dir, f"meetinglistdetails_Kiosk_{timestamp}.csv")
            with open(csv_filename, "w", encoding="utf-8") as f:
                f.write(data.get("csvData", ""))
                
            # 2. Save ML JSON Backup
            json_filename = os.path.join(json_dir, f"Kiosk_Raw_Data_{timestamp}.json")
            with open(json_filename, "w", encoding="utf-8") as f:
                f.write(data.get("jsonData", ""))
                
            # 3. Trigger Biometric ML Engine (signature_ml.py)
            print(f"[*] Received payload for {clean_event_name}. Triggering Biometric Validation...")
            os.system("python3 signature_ml.py")
            
            # 4. Route and Rename Output HTML
            generated_html = json_filename.replace("Kiosk_Raw_Data_", "Certified_CME_Audit_Log_").replace(".json", ".html")
            
            final_html_name = f"{event_date}_{clean_series.replace(' ', '')}_{clean_event_name.replace(' ', '_')}.html"
            final_html_path = os.path.join(series_dir, final_html_name)
            
            if os.path.exists(generated_html):
                shutil.move(generated_html, final_html_path)
                print(f"[*] Certified Audit Log successfully routed to: {final_html_path}")
            else:
                print("[!] ML Engine failed to produce HTML. Saving raw uncertified copy as fallback.")
                final_html_path = os.path.join(series_dir, f"UNCERTIFIED_{final_html_name}")
                with open(final_html_path, "w", encoding="utf-8") as f:
                    f.write(data.get("htmlData", ""))
            
            # 5. Update Master Ledger
            self.generate_master_ledger(print_dir)
            
            # Respond to iPad
            self.send_response(200)
            self.send_header('Access-Control-Allow-Origin', '*')
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"status": "success"}).encode('utf-8'))
        else:
            super().do_POST()

    def generate_master_ledger(self, print_dir):
        """Builds an interactive index.html linking to all events"""
        ledger_path = os.path.join(print_dir, "MASTER_AUDIT_LEDGER.html")
        
        all_logs = glob.glob(os.path.join(print_dir, "**", "*.html"), recursive=True)
        # Sort files by filename reverse chronologically (assuming YYYY-MM-DD prefix)
        all_logs.sort(reverse=True)
        
        html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Master CME Audit Ledger</title>
<style>
    body {{ font-family: -apple-system, sans-serif; background: #f8fafc; padding: 40px; color: #1e293b; }}
    .container {{ max-width: 900px; margin: 0 auto; background: white; padding: 30px; border-radius: 12px; box-shadow: 0 4px 15px rgba(0,0,0,0.05); }}
    h1 {{ color: #097C87; border-bottom: 2px solid #e2e8f0; padding-bottom: 10px; margin-top: 0; }}
    ul {{ list-style-type: none; padding: 0; }}
    li {{ margin-bottom: 12px; padding: 15px; border: 1px solid #e2e8f0; border-radius: 8px; font-size: 1rem; transition: 0.1s; display:flex; align-items:center; }}
    li:hover {{ border-color: #097C87; transform: translateX(5px); }}
    a {{ text-decoration: none; color: #1e293b; font-weight: 600; width:100% }}
    .badge {{ background: #e0f2fe; color: #0369a1; padding: 4px 10px; border-radius: 20px; font-size: 0.8rem; margin-right: 15px; font-weight:bold;}}
</style>
</head><body>
<div class="container">
    <h1>Master CME Audit Ledger</h1>
    <p>Chronological index of all Certified Attendance Sheets. Last updated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
    <ul>"""
        
        count = 0
        for log in all_logs:
            if "MASTER_AUDIT_LEDGER" in log: continue
            rel_path = os.path.relpath(log, print_dir)
            
            # Best effort parsing of filename: YYYY-MM-DD_Series_Event.html
            parts = os.path.basename(log).split("_")
            date_str = parts[0] if len(parts) > 1 else "Unknown"
            rest = " ".join(parts[1:]).replace(".html", "")
            
            html += f'<li><span class="badge">{date_str}</span> <a href="{urllib.parse.quote(rel_path)}">{rest}</a></li>'
            count += 1
            
        html += """</ul>
    <p style="text-align:center; color:#94a3b8; margin-top:30px; font-size:0.9rem;">
        Automatically Generated by Kiosk ML Automation Suite
    </p>
</div></body></html>"""
        
        with open(ledger_path, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"[*] MASTER_AUDIT_LEDGER updated with {count} records.")

def start_server():
    server_address = ('0.0.0.0', PORT)
    httpd = http.server.HTTPServer(server_address, KioskUploadHandler)
    print(f"======================================================")
    print(f"🚀 KIOSK AUTOMATION SERVER RUNNING ON PORT {PORT}")
    print(f"📥 Routing uploads to: {os.path.abspath(SHAREPOINT_SYNC_FOLDER)}")
    print(f"======================================================")
    print("Press Ctrl+C to stop.")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    httpd.server_close()

if __name__ == '__main__':
    start_server()
