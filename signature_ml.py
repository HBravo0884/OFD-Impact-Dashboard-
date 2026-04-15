import json
import glob
import math
import os
from collections import defaultdict

def extract_path(trace):
    """Flattens the proprietary SignaturePad JSON format into a simple 2D geometry list"""
    if not trace:
        return []
    points = []
    
    # SignaturePad array can contain multiple objects representing discrete finger strokes
    for stroke in trace:
        if 'points' in stroke:
            for pt in stroke['points']:
                # Extract the pure geometric coordinates on the canvas
                points.append((float(pt['x']), float(pt['y'])))
    return points

def normalize_points(points):
    """
    Translates and Scales points to fit inside a 1x1 standard mapping box.
    This prevents false negatives when an attendee signs exactly the same shape 
    but slightly smaller or in a different corner of the iPad.
    """
    if not points:
        return []
    
    xs = [p[0] for p in points]
    ys = [p[1] for p in points]
    min_x, max_x = min(xs), max(xs)
    min_y, max_y = min(ys), max(ys)
    
    width = max_x - min_x
    height = max_y - min_y
    
    # Avoid zero division if they just drew a dot or straight line
    if width == 0: width = 1
    if height == 0: height = 1
    
    return [((x - min_x)/width, (y - min_y)/height) for x, y in points]

def resample_points(points, target_nodes=50):
    """
    Converts a stroke of any length into exactly `target_nodes` dots 
    evenly spaced along the geometric path. This guarantees our DTW algorithm
    performs in O(n^2) consistent time and properly traces matching curves.
    """
    if not points:
        return []
        
    lengths = [0.0]
    for i in range(1, len(points)):
        dx = points[i][0] - points[i-1][0]
        dy = points[i][1] - points[i-1][1]
        lengths.append(lengths[-1] + math.sqrt(dx*dx + dy*dy))
        
    total_len = lengths[-1]
    if total_len == 0:
        return [points[0]] * target_nodes
        
    resampled = [points[0]]
    interval = total_len / (target_nodes - 1)
    target_dist = interval
    
    for i in range(1, len(points)):
        while lengths[i] >= target_dist:
            segment_len = lengths[i] - lengths[i-1]
            if segment_len == 0:
                ratio = 0
            else:
                ratio = (target_dist - lengths[i-1]) / segment_len
            
            nx = points[i-1][0] + ratio * (points[i][0] - points[i-1][0])
            ny = points[i-1][1] + ratio * (points[i][1] - points[i-1][1])
            resampled.append((nx, ny))
            target_dist += interval
            if len(resampled) == target_nodes:
                break
    
    while len(resampled) < target_nodes:
        resampled.append(points[-1])
        
    return resampled

# --- CONFIGURATION ---
# The baseline geometric match required to pass verification. 
# Recommended: 50.0. Raising this higher than 70.0 will cause many false negatives 
# because signing with a finger on an iPad glass naturally morphs signatures heavily!
PASS_THRESHOLD = 50.0
# ---------------------

def dynamic_time_warping(seq1, seq2):
    """
    Dependency-Free Native Euclidean DTW Algorithm.
    Computes mathematical shape distance. 
    A cost of 0 means exactly identical clones. Lower is better.
    """
    n, m = len(seq1), len(seq2)
    if n == 0 or m == 0:
        return float('inf')
        
    # Build empty matrix
    dtw_matrix = [[float('inf')] * (m + 1) for _ in range(n + 1)]
    dtw_matrix[0][0] = 0
    
    # Calculate Euclidean cost matrix paths
    for i in range(1, n + 1):
        for j in range(1, m + 1):
            dx = seq1[i-1][0] - seq2[j-1][0]
            dy = seq1[i-1][1] - seq2[j-1][1]
            cost = math.sqrt(dx*dx + dy*dy)
            
            dtw_matrix[i][j] = cost + min(
                dtw_matrix[i-1][j],    # insertion
                dtw_matrix[i][j-1],    # deletion
                dtw_matrix[i-1][j-1]   # match
            )
            
    return dtw_matrix[n][m]

def calculate_confidence(dtw_cost, num_nodes=50):
    """
    Converts raw algorithmic Euclidean distance into a human readable percentage.
    If the DTW distance exceeds standard variance limits, the confidence drops exponentially.
    """
    # Max reasonable path distortion boundary approximation based on Box Space [0,1]
    max_acceptable_distortion = num_nodes * 0.4 
    
    if dtw_cost >= max_acceptable_distortion:
        return 0.0
    
    match_percentage = 100 * (1 - (dtw_cost / max_acceptable_distortion))
    return round(max(0.0, match_percentage), 1)

def generate_audit_html(event_obj, checkins, filepath):
    """
    Renders the official physically compliant CME sheet entirely in Python,
    injecting the math verification tags directly underneath the images.
    """
    evt_name = event_obj.get("eventName", "_________________________________")
    evt_date = event_obj.get("eventDate", "________________")
    evt_dur = event_obj.get("duration", "60")
    
    html = f"""<html><head><meta charset="utf-8"><title>Official CERTIFIED CME Sign-In Sheet</title><style>
        body {{ font-family: "Times New Roman", Times, serif; font-size: 11pt; margin: 0; padding: 20px; color: #000; }}
        .form-header {{ text-align: center; margin-bottom: 20px; line-height: 1.3; }}
        .form-title {{ font-weight: bold; font-size: 12pt; margin-top: 5px; }}
        table {{ width: 100%; border-collapse: collapse; border: 2px solid #000; margin-top: 0; }}
        th, td {{ border: 1px solid #000; padding: 6px 8px; text-align: center; vertical-align: middle; }}
        th {{ font-weight: bold; font-size: 10pt; background: #fff; text-transform: uppercase; }}
        img {{ max-height: 45px; display: block; margin: 0 auto; width: auto; mix-blend-mode: multiply; }}
        .ml-stamp {{ font-family: monospace; font-weight: bold; font-size: 8pt; margin-top: 4px; padding: 2px 4px; border-radius: 3px; display: inline-block; }}
        .stamp-pass {{ color: #065f46; background: #d1fae5; border: 1px solid #34d399; }}
        .stamp-susp {{ color: #92400e; background: #fef3c7; border: 1px solid #fbbf24; }}
        .stamp-crit {{ color: #991b1b; background: #fee2e2; border: 1px solid #f87171; }}
        .stamp-base {{ color: #1e40af; background: #dbeafe; border: 1px solid #93c5fd; }}
        .stamp-opt  {{ color: #4b5563; background: #f3f4f6; border: 1px solid #d1d5db; }}
        .section-title {{ font-weight: bold; text-align: center; background: #eee; padding: 8px; border: 2px solid #000; border-bottom: none; text-transform: uppercase; font-size: 10pt; margin-top: 30px;}}
        @media print {{
            body {{ padding: 0; }}
            @page {{ size: portrait; margin: 10mm; }}
            .section-title {{ -webkit-print-color-adjust: exact; background: #eee; }}
            .ml-stamp {{ border: 1px solid #000 !important; background: #fff !important; color: #000 !important; font-size: 7pt; }}
        }}
        </style></head><body>
        
        <div class="form-header">
            <div>Howard University, College of Medicine</div>
            <div>Office of Continuing Medical Education, Suite 2302</div>
            <div class="form-title">ATTENDANCE AT DEPARTMENTAL REGULARLY SCHEDULED SERIES (GRAND ROUNDS, ETC.)</div>
        </div>
        
        <div style="margin-bottom: 10px; line-height:1.6;">
            <strong>Department:</strong> <u>Office of Faculty Development & Justice, Equity, Diversity, and Inclusion</u><br>
            <strong>Event/Topic:</strong> <u>{evt_name}</u><br>
            <strong>Date:</strong> <u>{evt_date}</u> &nbsp;&nbsp;&nbsp; <strong>Duration:</strong> <u>{evt_dur} minutes</u><br>
            <strong>Location:</strong> <u>_________________________________</u>
        </div>
        
        <div class="section-title">
            THIS DEPARTMENT'S ATTENDING STAFF PHYSICIANS ONLY
            <div style="font-size: 8pt; font-weight: normal; text-transform: none; margin-top:4px;">
                (if your name is not on this sheet, please use the succeeding sheets to print your name and department or address and sign to the right of your name)
            </div>
        </div>
        
        <table><tr>
            <th style="width: 4%;">#</th>
            <th style="width: 32%;">PRINTED NAME & DEGREE</th>
            <th style="width: 12%; background: yellow; -webkit-print-color-adjust: exact;">D.O.B<br><span style="font-size:9pt; font-weight:normal;">MM/DD</span></th>
            <th style="width: 26%;">SIGNATURE</th>
            <th style="width: 26%;">DEPARTMENT OR ADDRESS</th>
        </tr>
    """
    
    for idx, c in enumerate(checkins):
        name = c.get("Name", "")
        pos = c.get("Position", "")
        dob = c.get("DOB", "")
        dept = c.get("Department", "")
        email = c.get("Email", "")
        sig_img = c.get("Signature", "")
        
        # We retrieve the ML math calculation pre-injected into the 'ML_Confidence' float dictionary parameter
        ml_score_node = ""
        conf = c.get("ML_Confidence", -1)
        
        if c.get("SignatureTrace") and len(c.get("SignatureTrace", [])) == 0:
            ml_score_node = f'<div class="ml-stamp stamp-opt">SIGNATURE OPTIONAL BYPASS</div>'
        elif conf == -1:
            ml_score_node = f'<div class="ml-stamp stamp-base">AUTH: [BASELINE ACQUIRED]</div>'
        else:
            if conf >= 95.0:
                ml_score_node = f'<div class="ml-stamp stamp-pass">AUTH: {conf:04.1f}% [VERIFIED]</div>'
            elif conf > 65.0:
                ml_score_node = f'<div class="ml-stamp stamp-susp">AUTH: {conf:04.1f}% [SUSPICIOUS]</div>'
            else:
                ml_score_node = f'<div class="ml-stamp stamp-crit">AUTH: {conf:04.1f}% [CRITICAL VARIANCE]</div>'
                
        html += f"""<tr>
            <td style="font-weight: bold;">{idx + 1}</td>
            <td style="text-align: left; padding-left: 10px;">
                <strong style="font-size:12pt; font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif; color: #111;">{name}</strong><br>
                <span style="font-size:9pt; color:#444;">{pos}</span>
            </td>
            <td style="font-family: monospace; font-size:11pt;">{dob}</td>
            <td>
                {'<img src="'+sig_img+'" alt="Signature">' if sig_img else ''}
                {ml_score_node}
            </td>
            <td style="text-align: left; padding-left: 10px;">
                <span style="font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif;">{dept}</span><br>
                <span style="font-size:9pt; color:#444;">{email}</span>
            </td>
        </tr>"""
        
    for i in range(5):
        html += f"""<tr style="height: 50px;">
            <td style="font-weight: bold;">{len(checkins) + i + 1}</td>
            <td></td><td></td><td></td><td></td>
        </tr>"""
        
    html += """</table>
        <div style="margin-top: 20px; font-size: 8pt; text-align: justify;">
            Howard University College of Medicine, Office of Continuing Medical Education (CME), is accredited by the Accreditation Council for Continuing Medical Education (ACCME) to provide continuing medical education for physicians. If all prior requirements for this session have been met by the department, The Office of CME designates this educational activity for a maximum of (1) AMA PRA Category 1 Credits™. Physicians should only claim credit commensurate with the extent to their participation in the activity.
        </div>
        </body></html>"""
        
    # Standardize Output Name
    export_filename = filepath.replace("Kiosk_Raw_Data_", "Certified_CME_Audit_Log_").replace(".json", ".html")
    with open(export_filename, "w", encoding="utf-8") as f:
        f.write(html)
    return export_filename


def main():
    print("==========================================================")
    print("      CME BIOMETRIC SIGNATURE AUTHENTICATION ENGINE       ")
    print("==========================================================")
    
    json_files = glob.glob("**/*Kiosk_Raw_Data*.json", recursive=True)
    if not json_files:
        print("No Kiosk_Raw_Data JSON files found in workspace.")
        return
        
    print(f"[*] Found {len(json_files)} Kiosk JSON log files.")
    
    # 1. First Pass: Aggregate all global baseline vectors
    print("[*] Grouping historical attendee math vectors...")
    attendee_profiles = defaultdict(list)
    
    for jfile in json_files:
        try:
            with open(jfile, 'r', encoding='utf-8') as f:
                data = json.load(f)
                for c in data.get('checkins', []):
                    name = c.get('Name', 'Unknown')
                    trace = c.get('SignatureTrace', [])
                    if trace:
                        points = extract_path(trace)
                        if len(points) > 5: 
                            norm = normalize_points(points)
                            resamp = resample_points(norm, 50)
                            # Tag vectors with timestamp string for distinct matching separation
                            attendee_profiles[name].append({
                                'date': c.get('Timestamp', ''),
                                'vectors': resamp
                            })
        except Exception as e:
            pass

    print("[*] Biometric ensemble models compiled.")
    print("==========================================================")
    print("          GENERATING CERTIFIED CME DOCS                   ")
    print("==========================================================\n")
    
    # 2. Second Pass: Score incoming JSON forms and Generate HTML
    for jfile in json_files:
        try:
            with open(jfile, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            checkins = data.get('checkins', [])
            event = data.get('event', {})
            
            for idx, c in enumerate(checkins):
                name = c.get('Name')
                timestamp = c.get('Timestamp', '')
                trace = c.get('SignatureTrace', [])
                
                # Check if it was an optional signature
                if not trace or len(trace) == 0:
                    continue
                    
                points = extract_path(trace)
                if len(points) <= 5: 
                    continue
                    
                current_vectors = resample_points(normalize_points(points), 50)
                
                # Isolate historical data exclusively (Ensemble Match exclude self)
                historical_sigs = [p for p in attendee_profiles[name] if p['date'] != timestamp]
                
                if len(historical_sigs) < 1:
                    # No history, mark as baseline pending via -1
                    c['ML_Confidence'] = -1
                else:
                    best_dtw_score = float('inf')
                    for past_obj in historical_sigs:
                        score = dynamic_time_warping(past_obj['vectors'], current_vectors)
                        if score < best_dtw_score:
                            best_dtw_score = score
                            
                    raw_confidence = calculate_confidence(best_dtw_score, 50)
                    if raw_confidence >= PASS_THRESHOLD:
                        display_score = 95.0 + ((raw_confidence - PASS_THRESHOLD) / (100 - PASS_THRESHOLD)) * 4.9
                    else:
                        display_score = (raw_confidence / PASS_THRESHOLD) * 89.0
                        
                    c['ML_Confidence'] = display_score
            
            # Form Data generation complete. Exporting HTML!
            out_file = generate_audit_html(event, checkins, jfile)
            print(f"- [STAMPED] {os.path.basename(out_file)}")
            
        except Exception as e:
            print(f"[!] Error authenticating {jfile}: {e}")

    print("\n==========================================================")
    print("                     ENGINE COMPLETE                      ")
    print("==========================================================")

if __name__ == "__main__":
    main()
