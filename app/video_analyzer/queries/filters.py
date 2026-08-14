"""SQL filter construction for the videos table and exports."""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

def parse_advanced_search(search_query: str) -> Tuple[str, Dict[str, Any]]:
    """
    Parse advanced search syntax to extract field:value patterns.
    
    Supports patterns like:
    - field:value (e.g., year:2020, codec:HEVC)
    - field:>value (e.g., size:>10GB, year:>2020)
    - field:<value (e.g., size:<5GB)
    - field:>=value, field:<=value, field:!=value
    
    Args:
        search_query: Search query string that may contain field:value patterns
        
    Returns:
        Tuple of (remaining_search_text, extracted_filters_dict)
        
    Examples:
        text, filters = parse_advanced_search("year:2020 codec:HEVC some movie")
        # Returns: ("some movie", {'year': '2020', 'video_codec': 'HEVC'})
        
        text, filters = parse_advanced_search("size:>10GB year:>=2020")
        # Returns: ("", {'size_op': '>', 'size_val': '10GB', 'year': '>=2020'})
    """
    if not search_query:
        return '', {}
    
    extracted_filters = {}
    remaining_parts = []
    
    # Pattern to match field:operator?value (e.g., year:2020, size:>10GB, codec:HEVC)
    # Matches: field_name, optional operator (>, <, >=, <=, !=), value (supports quoted strings)
    # Allows optional whitespace around the operator/value.
    pattern = r'\b(\w+):\s*(>=|<=|!=|>|<)?\s*("[^"]+"|\'[^\']+\'|[^\s]+)'
    matches = re.finditer(pattern, search_query)
    
    # Field name mapping from search syntax to filter parameter names
    field_map = {
        'year': 'year',
        'codec': 'video_codec',
        'source': 'video_source',
        'format': 'source_format',
        'resolution': 'resolution',
        'res': 'resolution',
        'profile': 'profile',
        'prof': 'profile',
        'volume': 'volume',
        'vol': 'volume',
        'category': 'category',
        'cat': 'category',
        'container': 'container',
        'cont': 'container',
        'size': 'size',
        'bitrate': 'bitrate',
        'bit': 'bitrate',
        'edition': 'edition',
        'hybrid': 'source_hybrid',
        'dual': 'is_hybrid',
        'dual_hdr': 'is_hybrid',
        'source_hybrid': 'source_hybrid',
        'hybrid_src': 'source_hybrid',
        '3d': 'is_3d',
        'nfo': 'nfo_missing',
        'nfo_missing': 'nfo_missing',
        'missing': 'missing',
    }
    
    # Collect all matches with their positions
    match_positions = []
    for match in matches:
        field_name = match.group(1).lower()
        operator = match.group(2) or ''
        value = match.group(3)
        if value and ((value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'"))):
            value = value[1:-1]
        start_pos = match.start()
        end_pos = match.end()
        match_positions.append((start_pos, end_pos, field_name, operator, value))
    
    # If no advanced filters were found, keep the original search text
    if not match_positions:
        return search_query.strip(), {}

    # Remove matched patterns from search query and build remaining text
    if match_positions:
        last_pos = 0
        for start, end, field_name, operator, value in sorted(match_positions):
            # Add text before this match
            remaining_parts.append(search_query[last_pos:start])
            last_pos = end
            
            # Process the field:value pattern
            if field_name in field_map:
                filter_key = field_map[field_name]
                
                # Handle size and bitrate with operators
                if filter_key == 'size':
                    if operator:
                        extracted_filters['size_op'] = operator
                        extracted_filters['size_val'] = value
                    else:
                        # Default to = if no operator
                        extracted_filters['size_op'] = '='
                        extracted_filters['size_val'] = value
                elif filter_key == 'bitrate':
                    if operator:
                        extracted_filters['bit_op'] = operator
                        extracted_filters['bit_val'] = value
                    else:
                        extracted_filters['bit_op'] = '='
                        extracted_filters['bit_val'] = value
                # Handle year with or without operators
                elif filter_key == 'year':
                    if operator:
                        extracted_filters['year_op'] = operator
                        extracted_filters['year_val'] = value
                    else:
                        extracted_filters['year'] = value
                elif filter_key == 'is_hybrid':
                    # Convert boolean-like values
                    if value.lower() in ('1', 'true', 'yes', 'y'):
                        extracted_filters['is_hybrid'] = '1'
                    elif value.lower() in ('0', 'false', 'no', 'n'):
                        extracted_filters['is_hybrid'] = '0'
                elif filter_key == 'source_hybrid':
                    if value.lower() in ('1', 'true', 'yes', 'y'):
                        extracted_filters['source_hybrid'] = '1'
                    elif value.lower() in ('0', 'false', 'no', 'n'):
                        extracted_filters['source_hybrid'] = '0'
                elif filter_key == 'is_3d':
                    if value.lower() in ('1', 'true', 'yes', 'y'):
                        extracted_filters['is_3d'] = '1'
                    else:
                        extracted_filters['is_3d'] = '0'
                else:
                    # Regular field:value
                    extracted_filters[filter_key] = value
        
        # Add remaining text after last match
        remaining_parts.append(search_query[last_pos:])
    
    # Join remaining parts and clean up whitespace
    remaining_text = ' '.join(remaining_parts).strip()
    
    return remaining_text, extracted_filters

def build_filter_query(args: Dict[str, Any], exclude_key: Optional[str] = None) -> Tuple[str, List[Any]]:
    """
    Build SQL WHERE clause and parameters from filter arguments.
    
    Constructs a SQL WHERE clause with placeholders and corresponding parameter list
    based on the provided filter arguments. Supports various filter types including
    search, category, volume, profile, resolution, status, and custom size/bitrate operators.
    
    Args:
        args: Dictionary of filter arguments (typically from request.args or request.json)
        exclude_key: Optional key to exclude from the filter query (useful for nested queries)
        
    Returns:
        Tuple of (WHERE clause string, parameter list)
        
    Example:
        where, params = build_filter_query({'category': 'dovi', 'resolution': '4K'})
        # Returns: ("1=1 AND category = ? AND resolution = ?", ['dovi', '4K'])
    """
    conditions = ["1=1"]; params = []
    
    # Parse advanced search syntax if search parameter exists
    # Create a copy of args to avoid modifying the original dict
    args = dict(args)
    search_query = args.get('search', '').strip()
    if search_query:
        remaining_search, advanced_filters = parse_advanced_search(search_query)
        # Merge advanced filters into args (advanced filters take precedence)
        args.update(advanced_filters)
        # Update search with remaining text
        args['search'] = remaining_search
    
    blank_token = '__blank__'
    mappings = [('search', 'filename'), ('category', 'category'), ('volume', 'source_vol'), ('profile', 'profile'), ('el', 'el_type'), ('container', 'container'), ('resolution', 'resolution'), ('status', 'scan_error'), ('audio', 'audio_codecs'), ('video_codec', 'video_codec'), ('video_source', 'video_source'), ('source_format', 'source_format'), ('edition', 'edition'), ('media_type', 'media_type'), ('nfo_missing', 'nfo_missing'), ('missing', 'missing'), ('anomaly', 'quality_anomaly'), ('quality_anomaly', 'quality_anomaly')]
    for key, col in mappings:
        if key == exclude_key: continue
        val = args.get(key, '').strip()
        if val:
            if key == 'search':
                conditions.append(f"(LOWER({col}) LIKE ? OR LOWER(full_path) LIKE ?)")
                params.extend([f"%{val.lower()}%", f"%{val.lower()}%"])
            elif key == 'status': 
                values = [v.strip().lower() for v in val.split(',') if v.strip()]
                has_ok = 'ok' in values
                has_failed = 'failed' in values
                if has_ok and has_failed:
                    pass  # both selected = no status filter
                elif has_failed and not has_ok:
                    conditions.append("scan_error IS NOT NULL AND scan_error != ''")
                elif has_ok and not has_failed:
                    conditions.append("(scan_error IS NULL OR scan_error = '')")
            elif key == 'audio':
                values = [v.strip() for v in val.split(',') if v.strip()]
                if blank_token in values:
                    values = [v for v in values if v != blank_token]
                    blank_clause = f"({col} IS NULL OR {col} = '')"
                    if values:
                        like_clauses = [f"LOWER({col}) LIKE ?" for _ in values]
                        params.extend([f"%{v.lower()}%" for v in values])
                        conditions.append(f"({ ' OR '.join(like_clauses + [blank_clause]) })")
                    else:
                        conditions.append(blank_clause)
                else:
                    conditions.append(f"LOWER({col}) LIKE ?"); params.append(f"%{val.lower()}%")
            elif key == 'video_codec':
                values = [v.strip() for v in val.split(',') if v.strip()]
                if blank_token in values:
                    values = [v for v in values if v != blank_token]
                    blank_clause = f"({col} IS NULL OR {col} = '')"
                    if values:
                        placeholders = ','.join('?' * len(values))
                        conditions.append(f"(LOWER({col}) IN ({placeholders}) OR {blank_clause})")
                        params.extend([v.lower() for v in values])
                    else:
                        conditions.append(blank_clause)
                elif len(values) > 1:
                    placeholders = ','.join('?' * len(values))
                    conditions.append(f"LOWER({col}) IN ({placeholders})")
                    params.extend([v.lower() for v in values])
                else:
                    conditions.append(f"LOWER({col}) = ?"); params.append(val.lower())
            elif key == 'nfo_missing':
                values = [v.strip().lower() for v in val.split(',') if v.strip()]
                want_missing = any(v in ('missing', 'none', '1', 'true', 'yes') for v in values)
                want_found = any(v in ('found', '0', 'false', 'no') for v in values)
                if want_missing and want_found:
                    pass
                elif want_missing:
                    conditions.append(f"{col} = 1")
                elif want_found:
                    conditions.append(f"{col} = 0")
            elif key == 'missing':
                values = [v.strip().lower() for v in val.split(',') if v.strip()]
                want_yes = any(v in ('yes', '1', 'true', 'y') for v in values)
                want_no = any(v in ('no', '0', 'false', 'n') for v in values)
                if want_yes and want_no:
                    pass
                elif want_yes:
                    conditions.append(f"{col} = 1")
                elif want_no:
                    conditions.append(f"{col} = 0")
            elif key in ('anomaly', 'quality_anomaly'):
                values = [v.strip().lower() for v in val.split(',') if v.strip()]
                want_yes = any(v in ('yes', '1', 'true', 'y') for v in values)
                want_no = any(v in ('no', '0', 'false', 'n') for v in values)
                if want_yes and want_no:
                    pass
                elif want_yes:
                    conditions.append("(quality_anomaly IS NOT NULL AND quality_anomaly != '')")
                elif want_no:
                    conditions.append("(quality_anomaly IS NULL OR quality_anomaly = '')")
            elif ',' in val or val == blank_token:
                # Handle multiple values (comma-separated) for any filter type, including blanks
                values = [v.strip() for v in val.split(',') if v.strip()]
                if blank_token in values:
                    values = [v for v in values if v != blank_token]
                    blank_clause = f"({col} IS NULL OR {col} = '')"
                    if values:
                        placeholders = ','.join('?' * len(values))
                        conditions.append(f"(LOWER({col}) IN ({placeholders}) OR {blank_clause})")
                        params.extend([v.lower() for v in values])
                    else:
                        conditions.append(blank_clause)
                elif values:
                    placeholders = ','.join('?' * len(values))
                    conditions.append(f"LOWER({col}) IN ({placeholders})")
                    params.extend([v.lower() for v in values])
            else:
                conditions.append(f"LOWER({col}) = ?"); params.append(val.lower())
    if exclude_key != 'secondary_hdr':
        sec = args.get('secondary_hdr', '').strip()
        if sec:
            values = [v.strip() for v in sec.split(',') if v.strip()]
            if blank_token in values or sec == 'none':
                values = [v for v in values if v != blank_token and v != 'none']
                blank_clause = "(secondary_hdr IS NULL OR secondary_hdr = '')"
                if values:
                    placeholders = ','.join('?' * len(values))
                    conditions.append(f"(LOWER(secondary_hdr) IN ({placeholders}) OR {blank_clause})")
                    params.extend([v.lower() for v in values])
                else:
                    conditions.append(blank_clause)
            elif ',' in sec:
                placeholders = ','.join('?' * len(values))
                conditions.append(f"LOWER(secondary_hdr) IN ({placeholders})")
                params.extend([v.lower() for v in values])
            else:
                conditions.append("LOWER(secondary_hdr) = ?"); params.append(sec.lower())
    if exclude_key != 'is_hybrid':
        hyb = args.get('is_hybrid', '').strip()
        hyb_vals = [v.strip() for v in hyb.split(',') if v.strip()]
        if hyb_vals == ['1'] or hyb == "1":
            conditions.append("is_hybrid = 1")
        elif hyb_vals == ['0'] or hyb == "0":
            conditions.append("is_hybrid = 0 AND category != 'sdr_only'")
    if exclude_key != 'source_hybrid':
        src_hyb = args.get('source_hybrid', '').strip()
        src_vals = [v.strip() for v in src_hyb.split(',') if v.strip()]
        if src_vals == ['1'] or src_hyb == "1":
            conditions.append("is_source_hybrid = 1")
        elif src_vals == ['0'] or src_hyb == "0":
            conditions.append("is_source_hybrid = 0")
    
    # Handle size filtering with operators
    if exclude_key != 'size':
        size_op = args.get('size_op', '').strip()
        size_val = args.get('size_val', '').strip()
        if size_op and size_val:
            try:
                # Parse value - handle GB, MB, etc.
                size_val_clean = size_val.upper().replace('GB', '').replace('MB', '').replace(' ', '').strip()
                size_bytes = float(size_val_clean)
                if 'GB' in size_val.upper():
                    size_bytes = size_bytes * 1024 * 1024 * 1024
                elif 'MB' in size_val.upper():
                    size_bytes = size_bytes * 1024 * 1024
                elif 'KB' in size_val.upper():
                    size_bytes = size_bytes * 1024
                
                if size_op == '>':
                    conditions.append("file_size > ?")
                elif size_op == '<':
                    conditions.append("file_size < ?")
                elif size_op == '=' or size_op == '==':
                    conditions.append("file_size = ?")
                elif size_op == '>=':
                    conditions.append("file_size >= ?")
                elif size_op == '<=':
                    conditions.append("file_size <= ?")
                params.append(int(size_bytes))
            except (ValueError, TypeError):
                pass  # Ignore invalid size values
    
    # Handle bitrate filtering with operators
    if exclude_key != 'bitrate':
        bit_op = args.get('bit_op', '').strip()
        bit_val = args.get('bit_val', '').strip()
        if bit_op and bit_val:
            try:
                # Parse value - handle Mbps, etc.
                bit_val_clean = bit_val.upper().replace('MBPS', '').replace('MBIT/S', '').replace(' ', '').strip()
                bitrate_val = float(bit_val_clean)
                
                if bit_op == '>':
                    conditions.append("bitrate_mbps > ?")
                elif bit_op == '<':
                    conditions.append("bitrate_mbps < ?")
                elif bit_op == '=' or bit_op == '==':
                    conditions.append("bitrate_mbps = ?")
                elif bit_op == '>=':
                    conditions.append("bitrate_mbps >= ?")
                elif bit_op == '<=':
                    conditions.append("bitrate_mbps <= ?")
                params.append(bitrate_val)
            except (ValueError, TypeError):
                pass  # Ignore invalid bitrate values
    
    # Handle year filtering with operators
    if exclude_key != 'year':
        year_op = args.get('year_op', '').strip()
        year_val = args.get('year_val', '').strip()
        year = args.get('year', '').strip()
        if year_op and year_val:
            try:
                year_int = int(year_val)
                if year_op == '>':
                    conditions.append("year > ?")
                elif year_op == '<':
                    conditions.append("year < ?")
                elif year_op == '>=':
                    conditions.append("year >= ?")
                elif year_op == '<=':
                    conditions.append("year <= ?")
                elif year_op == '!=':
                    conditions.append("year != ?")
                params.append(year_int)
            except (ValueError, TypeError):
                pass
        elif year:
            try:
                year_int = int(year)
                conditions.append("year = ?")
                params.append(year_int)
            except (ValueError, TypeError):
                pass
    
    # Handle is_3d filtering
    if exclude_key != 'is_3d':
        is_3d_val = args.get('is_3d', '').strip()
        if is_3d_val:
            vals = [v.strip() for v in is_3d_val.split(',') if v.strip()]
            if vals == ['1'] or is_3d_val == '1':
                conditions.append("is_3d = 1")
            elif vals == ['0'] or is_3d_val == '0':
                conditions.append("is_3d = 0")
    
    return " AND ".join(conditions), params

def parse_sort_order(value: Any, default: str = "desc") -> str:
    """Return a safe SQL sort direction."""
    order = str(value or default).strip().lower()
    return "ASC" if order == "asc" else "DESC"

def parse_positive_int(value: Any, default: int, max_value: int) -> int:
    """Parse a positive integer request value and clamp it."""
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return min(max(1, parsed), max_value)
