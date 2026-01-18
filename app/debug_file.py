import sys
import subprocess
import json

def test_file(path):
    print(f"--- DEBUGGING: {path} ---")
    
    if not os.path.exists(path):
        print("❌ File not found.")
        return

    # 1. Run FFPROBE exactly like the analyzer does
    cmd = ['ffprobe', '-v', 'quiet', '-select_streams', 'v:0', 
           '-show_entries', 'stream=side_data_list,codec_name,profile', 
           '-of', 'json', path]
    
    print("1. Running FFPROBE...")
    p = subprocess.run(cmd, capture_output=True)
    
    if p.returncode != 0:
        print(f"❌ FFprobe Error: {p.stderr.decode()}")
        return

    try:
        data = json.loads(p.stdout.decode('utf-8', 'replace'))
        streams = data.get('streams', [])
        if not streams:
            print("❌ No streams found in JSON.")
            return
            
        stream = streams[0]
        raw_side_data = stream.get('side_data_list', [])
        
        # 2. Simulate the Header Search
        print(f"2. Side Data Entries Found: {len(raw_side_data)}")
        
        dv_header = next((x for x in raw_side_data if 'DOVI configuration record' in x.get('side_data_type', '')), None)
        
        if dv_header:
            print("\n✅ DOVI HEADER FOUND:")
            print(json.dumps(dv_header, indent=4))
            
            # 3. Test Key Extraction
            p_prof = dv_header.get('profile') or dv_header.get('dv_profile')
            p_comp = dv_header.get('compatibility_id') or dv_header.get('dv_bl_signal_compatibility_id')
            
            print(f"\n--- LOGIC CHECK ---")
            print(f"Raw Profile Value: {p_prof} (Type: {type(p_prof)})")
            print(f"Raw Compat ID Value: {p_comp} (Type: {type(p_comp)})")
            
            res_prof = str(p_prof)
            if res_prof == "8":
                print("-> Detected Profile 8. Checking Compatibility ID...")
                if str(p_comp) == "1":
                    print("SUCCESS: Logic switches to '8.1'")
                elif str(p_comp) == "4":
                    print("SUCCESS: Logic switches to '8.4'")
                else:
                    print(f"FAILURE: ID '{p_comp}' did not match '1' or '4'. Keeping '8'.")
            else:
                print(f"-> Profile is {res_prof}, not 8. Logic skipped.")
        else:
            print("\n❌ No 'DOVI configuration record' found in side_data_list.")
            print("Dump of all side_data:", json.dumps(raw_side_data, indent=2))

    except Exception as e:
        print(f"❌ Crash during analysis: {e}")

if __name__ == "__main__":
    import os
    if len(sys.argv) < 2:
        print("Usage: python3 debug_file.py /path/to/video.mkv")
    else:
        test_file(sys.argv[1])