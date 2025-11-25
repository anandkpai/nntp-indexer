from create_db_basic import get_config, upsert_headers,ensure_db, get_nntp_client, TMP_ROWS_PATH_BASE
from email.utils import parsedate_to_datetime
from configparser import ConfigParser
from find_date_range import find_article_range_by_dates
from concurrent.futures import ThreadPoolExecutor, as_completed
import sqlite3, json
import time 
import os
import threading
import orjson

# globals 
config = get_config()
# Thread-local storage for NNTP clients
_thread_local = threading.local()


def get_thread_nntp_client(config: ConfigParser):
    """Get or create an NNTP client for the current thread."""
    if not hasattr(_thread_local, 'client'):
        _thread_local.client = get_nntp_client(config)
    return _thread_local.client

def get_article_range(config: ConfigParser, group: str, local_min: int, local_max: int) -> tuple[int, int]:
    """
    Get article range based on date filters in config, or return server defaults.
    
    Args:
        config: Configuration object
        group: Newsgroup name
        srv_first: Server's first article number
        srv_last: Server's last article number
    
    Returns:
        Tuple of (local_min, local_max) article numbers
    """
    
    if config.has_option('filters', 'local_min') and config.has_option('filters', 'local_max'):
        cfg_local_min = config.getint('filters', 'local_min')
        cfg_local_max = config.getint('filters', 'local_max')
        
        if cfg_local_min > 0 and cfg_local_max > 0 and cfg_local_min < cfg_local_max:
            print(f"Using configured local_min {cfg_local_min} and local_max {cfg_local_max}")
            return cfg_local_min, cfg_local_max


    # Check for date-based filtering
    if config.has_option('filters', 'min_days') and config.has_option('filters', 'max_days'):
        min_days = config.getint('filters', 'min_days')
        max_days = config.getint('filters', 'max_days')
        
        if min_days > 0 and max_days > 0 and min_days < max_days:
            print(f"Finding articles between {min_days} and {max_days} days old...")
            result = find_article_range_by_dates(group, min_days, max_days)
            
            if result:
                lower_artnum, upper_artnum, lower_age, upper_age = result
                print(f"Found range: articles {lower_artnum} ({lower_age:.1f} days) to {upper_artnum} ({upper_age:.1f} days)")
                
                # Ensure min < max regardless of article age ordering
                local_min = min(lower_artnum, upper_artnum)
                local_max = max(lower_artnum, upper_artnum)
            else:
                print(f"Could not find articles in specified date range, using full range")
    
    return local_min, local_max

def clean_text(s: str) -> str:
    return s.encode("utf-8", "ignore").decode("utf-8")

def sanitize_row(row):
    return {k: clean_text(v) if isinstance(v, str) else v for k, v in row.items()}

def to_iso(dt_str: str | None) -> str | None:
    if not dt_str:
        return None
    try:
        return parsedate_to_datetime(dt_str).astimezone(tz=None).isoformat()
    except Exception:
        return None

def fetch_rows_xzver(nntp_client, group: str, start: int, end: int):
    rows = []
    start_time = time.time()
    nntp_client.group(group)
    
    resp, overviews = nntp_client.xover(int(start), int(end))
    
    if not overviews:
        return rows
    
    # Build key mapping once from first overview
    first_artnum, first_ov = overviews[0]
    key_map = {}
    field_names = ['message-id', 'subject', 'from', 'date', 'references', 'bytes', 'lines', 'xref']
    
    for field_name in field_names:
        field_lower = field_name.lower()
        for key in first_ov.keys():
            key_lower = key.lower()
            if field_lower in key_lower:
                key_map[field_name] = key
                break
    
    print(f"\nDEBUG - Key mapping: {key_map}")
    
    # Process all overviews using the key map
    for artnum, ov in overviews:
        if artnum is None or not isinstance(ov, dict):
            continue
        rows.append(row_from_overview(group, int(artnum), ov, key_map))
    
    end_time = time.time()
    payload_MB = sum(len(str(r).encode("utf-8")) for r in rows)/1_000_000.0      
    mbps = payload_MB /(end_time - start_time)
    print(f"Retrieved {len(rows):,} from {group}, Elapsed = {end_time - start_time:.4f} seconds at mbps {mbps} ")

    return rows

def row_from_overview(group: str, artnum: int, ov: dict, key_map: dict) -> dict:
    """Convert NNTP overview dict to our row format using pre-built key mapping."""
    
    def get_field(field_name):
        key = key_map.get(field_name)
        if key and key in ov:
            value = ov[key]
            if isinstance(value, str):
                return clean_text(value)
            return value
        return None
    
    return {
        "message_id": get_field('message-id'),
        "group_name": group,
        "artnum": int(artnum),
        "subject": get_field('subject') or "",
        "from_addr": get_field('from') or "",
        "date_utc": to_iso(get_field('date')),
        "refs": get_field('references'),
        "bytes": int(get_field('bytes') or 0),
        "lines": int(get_field('lines') or 0),
        "xref": get_field('xref'),
    }

def fetch_headers_chunked(config:ConfigParser, group:str, limit: int = 5_000_000, chunk_size: int = 100_000, start: int = 0, back_filled_up_to:int = -1):
    """
    Fetch headers in chunks from back_filled_up_to (local_min) to start (local_max).
    
    Args:
        config: ConfigParser object
        group: NNTP group name
        limit: number of headers to fetch (<=0 means "all available")
        chunk_size: max articles per XOVER range
        newest_first: ignored, always fetches from oldest to newest
        start: local_max (upper bound)
        back_filled_up_to: local_min (lower bound)
    
    Returns:
        List[dict]: parsed overview rows
    """
    # Get group info with temporary connection
    temp_client = get_nntp_client(config)
    nntp_max, nntp_min = temp_client.group(group)[1:3]
    temp_client.quit()

    # Use passed-in parameters as the range if they have been set
    local_min = back_filled_up_to or nntp_min
    local_max = start or nntp_max
    
    print(f"Fetching from {local_min:,} to {local_max:,}")
    
    rows = []
    current = local_min
    
    # Calculate total articles to fetch
    total_articles = local_max - local_min + 1
    want = total_articles if (limit is None or limit <= 0) else min(int(limit), total_articles)
    
    print(f"Will fetch up to {want:,} articles in chunks of {chunk_size:,}")

    # Build list of chunks to fetch
    chunks = []
    current = local_min
    while current <= local_max and (current - local_min) < want:
        chunk_start = current
        chunk_end = min(local_max, current + chunk_size - 1)
        
        # Adjust last chunk to not exceed limit
        remaining = want - (current - local_min)
        if (chunk_end - chunk_start + 1) > remaining:
            chunk_end = chunk_start + remaining - 1
        
        chunks.append((chunk_start, chunk_end))
        current = chunk_end + 1
    
    max_workers = config.getint('servers', 'max_workers', fallback=5)
    print(f"Fetching {len(chunks)} chunks in parallel with {max_workers} workers...")    
    
    def fetch_chunk_with_client(chunk_start, chunk_end):
        """Worker function that creates its own NNTP client."""
        client = get_nntp_client(config)
        try:
            return fetch_rows_xzver(client, group=group, start=chunk_start, end=chunk_end)
        finally:
            client.quit()
    
    # Fetch chunks in parallel
    all_rows = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all chunks
        future_to_chunk = {
            executor.submit(fetch_chunk_with_client, chunk_start, chunk_end): (chunk_start, chunk_end)
            for chunk_start, chunk_end in chunks
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_chunk):
            chunk_start, chunk_end = future_to_chunk[future]
            try:
                chunk_rows = future.result()
                all_rows.extend(chunk_rows)
                print(f"Completed {chunk_start:,}-{chunk_end:,}: Total so far {len(all_rows):,}/{want:,}")
            except Exception as e:
                print(f"ERROR: Chunk {chunk_start:,}-{chunk_end:,} failed: {e}")
    
    # Sort by article number since parallel fetching may return out of order
    all_rows.sort(key=lambda x: x['artnum'])
    
    return all_rows




if __name__ == '__main__':
    groups = config['groups']['names'].split(',')    
    DB_BASE_PATH = config['db']['DB_BASE_PATH']

    for group in groups:
        db_path = f"{DB_BASE_PATH}/{group}.sqlite"
        conn = sqlite3.connect(db_path)
        
        ensure_db(conn)
        cur = conn.cursor()
        cur.execute("SELECT COALESCE(MAX(artnum), 0), COALESCE(MIN(artnum), 0) FROM articles WHERE group_name = ?", (group,))
        local_max, local_min = cur.fetchone()

        # if a filter limit has been set,  use that over the local max and local min. 
        local_min, local_max = get_article_range(config, group, local_min, local_max)
        print(f'making nntp connection to server with max artnum :{local_max} and min artnum {local_min}')            

        start_time = time.time()
        rows = fetch_headers_chunked(config, group=group, start=local_max, back_filled_up_to = local_min)
        end_time = time.time()
        print(f"Retrieved {len(rows):,} from {group}, Elapsed time for whole group = {end_time - start_time:.4f} seconds")        
        if rows:
            cached_headers_file = f"{TMP_ROWS_PATH_BASE}/{group}.json"
            os.makedirs(TMP_ROWS_PATH_BASE, exist_ok=True)
            start_time = time.time()
            with open(cached_headers_file, "wb") as f:  # Open in binary mode for orjson
                # Write all rows as NDJSON in one operation
                f.write(b'\n'.join(orjson.dumps(row) for row in rows))
                f.write(b'\n')        
            end_time = time.time()
            print(f"wrote {len(rows):,} to {cached_headers_file} in {end_time - start_time:.4f} seconds")
            start_time = time.time()
            upsert_headers(conn, group, rows)
            end_time = time.time()
            print(f"Upserted {len(rows):,} headers into {db_path} in {end_time - start_time:.4f} seconds")