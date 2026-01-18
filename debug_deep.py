import subprocess
import json
import sys
import os

def check_rpu(path):
    print(f"🚀 Analyzing: {path}")
    
    if not os.path.exists(path):
        print("❌ File not found")
        return

    # Use /tmp or /output directory for writable location
    import tempfile
    rpu_file = os.path.join(tempfile.gettempdir(), f"debug_rpu_{os.getpid()}.bin")
    if os.path.exists(rpu_file): os.remove(rpu_file)

    # 1. EXTRACT RPU (Simulating the script's fallback)
    print("1. Extracting RPU via ffmpeg -> dovi_tool...")
    
    ffmpeg_cmd = ['ffmpeg', '-i', path, '-c:v', 'copy', '-to', '2', '-f', 'hevc', '-y', '-']
    dovi_extract_cmd = ['dovi_tool', 'extract-rpu', '-', '-o', rpu_file]

    try:
        p1 = subprocess.Popen(ffmpeg_cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        p2 = subprocess.run(dovi_extract_cmd, stdin=p1.stdout, capture_output=True)
        p1.stdout.close()
        
        if p2.returncode != 0:
            print(f"❌ Extraction Failed: {p2.stderr.decode()}")
            return
            
        print(f"✅ RPU Extracted ({os.path.getsize(rpu_file)} bytes)")

    except Exception as e:
        print(f"❌ Execution Error: {e}")
        return

    # 2. READ INFO
    print("2. Reading RPU Info...")
    info_cmd = ['dovi_tool', 'info', '-i', rpu_file, '-f', '0']
    p_info = subprocess.run(info_cmd, capture_output=True)
    
    output = p_info.stdout.decode('utf-8', 'replace')
    print("\n--- RAW DOVI_TOOL OUTPUT START ---")
    print(output)
    print("--- RAW DOVI_TOOL OUTPUT END ---\n")

    # 3. TEST PARSING LOGIC
    try:
        json_start = output.find('{')
        if json_start != -1:
            data = json.loads(output[json_start:])
            
            print("--- PARSED DATA ---")
            print(f"dovi_profile: {data.get('dovi_profile')}")
            print(f"bl_compatibility_id: {data.get('bl_compatibility_id')}")
            
            # SIMULATE LOGIC
            prof = str(data.get('dovi_profile'))
            cid = str(data.get('bl_compatibility_id'))
            
            if prof == "8":
                if cid == "1": print("✅ RESULT: 8.1 (Matched ID 1)")
                elif cid == "4": print("✅ RESULT: 8.4 (Matched ID 4)")
                else: print(f"❌ RESULT: Still 8 (ID '{cid}' did not match 1 or 4)")
        else:
            print("❌ Could not find JSON start '{' in output")
            
    except Exception as e:
        print(f"❌ JSON Parse Error: {e}")

    if os.path.exists(rpu_file): os.remove(rpu_file)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 debug_deep.py /path/to/video.mkv")
    else:
        check_rpu(sys.argv[1])