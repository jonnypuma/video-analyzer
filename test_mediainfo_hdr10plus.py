#!/usr/bin/env python3
"""
Test script to check MediaInfo output for HDR10+ detection
Usage: python3 test_mediainfo_hdr10plus.py <file_path>
"""
import sys
import json
import subprocess
import os

def run_mediainfo(path):
    """Run MediaInfo and return parsed JSON"""
    if not os.path.exists(path):
        print(f"❌ File not found: {path}")
        return None
    
    try:
        result = subprocess.run(
            ['mediainfo', '--Output=JSON', path],
            capture_output=True,
            text=True,
            check=True
        )
        return json.loads(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"❌ MediaInfo failed: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"❌ Failed to parse MediaInfo JSON: {e}")
        return None

def analyze_hdr10plus(mi_data):
    """Extract HDR10+ related information from MediaInfo output"""
    if not mi_data:
        return
    
    tracks = mi_data.get('media', {}).get('track', [])
    
    print("\n" + "="*80)
    print("MEDIAINFO HDR10+ ANALYSIS")
    print("="*80)
    
    for track in tracks:
        ttype = track.get('@type', '')
        if ttype == 'Video':
            print(f"\n📹 VIDEO TRACK:")
            print("-" * 80)
            
            # Check all fields that might contain HDR info
            hdr_fields = [
                'HDR_Format',
                'HDR_Format_Compatibility',
                'HDR_Format_Version',
                'HDR_Format_Profile',
                'colour_primaries',
                'transfer_characteristics',
                'matrix_coefficients',
                'MasteringDisplay_ColorPrimaries',
                'MasteringDisplay_Luminance',
                'MaxCLL',
                'MaxFALL',
                'Format',
                'Format_Profile',
                'CodecID',
                'CodecID_Info'
            ]
            
            hdr10plus_indicators = []
            
            for field in hdr_fields:
                value = track.get(field)
                if value:
                    value_str = str(value).upper()
                    print(f"  {field:35} = {value}")
                    
                    # Check for HDR10+ indicators
                    if 'HDR10+' in value_str or 'HDR10PLUS' in value_str:
                        hdr10plus_indicators.append(f"{field}: {value}")
                    elif '2094' in value_str and 'SMPTE' in value_str:
                        hdr10plus_indicators.append(f"{field}: {value} (SMPTE 2094 = HDR10+)")
            
            # Check all fields for any mention of HDR10+
            print(f"\n  🔍 Checking all fields for HDR10+ indicators...")
            for key, value in track.items():
                if value and isinstance(value, str):
                    value_upper = value.upper()
                    if 'HDR10+' in value_upper or 'HDR10PLUS' in value_upper or ('2094' in value_upper and 'SMPTE' in value_upper):
                        if key not in hdr_fields:
                            print(f"  ⚠️  Found in unexpected field: {key} = {value}")
                            hdr10plus_indicators.append(f"{key}: {value}")
            
            if hdr10plus_indicators:
                print(f"\n  ✅ HDR10+ DETECTED in MediaInfo:")
                for indicator in hdr10plus_indicators:
                    print(f"     - {indicator}")
            else:
                print(f"\n  ❌ No HDR10+ indicators found in MediaInfo")
            
            # Also check for HDR10 (non-plus)
            hdr10_indicators = []
            for key, value in track.items():
                if value and isinstance(value, str):
                    value_upper = value.upper()
                    if 'HDR10' in value_upper and 'HDR10+' not in value_upper and 'HDR10PLUS' not in value_upper:
                        hdr10_indicators.append(f"{key}: {value}")
            
            if hdr10_indicators:
                print(f"\n  📌 HDR10 (non-plus) indicators:")
                for indicator in hdr10_indicators[:5]:  # Limit to first 5
                    print(f"     - {indicator}")

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 test_mediainfo_hdr10plus.py <file_path>")
        print("\nExample:")
        print("  python3 test_mediainfo_hdr10plus.py /path/to/hdr10plus_file.mkv")
        sys.exit(1)
    
    file_path = sys.argv[1]
    
    print(f"🚀 Analyzing: {os.path.basename(file_path)}")
    print(f"📁 Full path: {file_path}")
    
    mi_data = run_mediainfo(file_path)
    if mi_data:
        analyze_hdr10plus(mi_data)
    else:
        print("❌ Failed to get MediaInfo data")
        sys.exit(1)

if __name__ == "__main__":
    main()
