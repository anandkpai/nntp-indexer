"""NNTP fetching operations."""

import nntplib
import time
from configparser import ConfigParser
from concurrent.futures import ThreadPoolExecutor, as_completed
from .utils import clean_text, to_iso

def get_nntp_client(config: ConfigParser) -> nntplib.NNTP_SSL:
    """Create an NNTP SSL connection from config."""
    host = config['servers']['host']
    port = config.getint('servers', 'port')
    username = config['servers']['username']
    password = config['servers']['password']
    timeout = config.getint('servers', 'timeout', fallback=60)
    
    client = nntplib.NNTP_SSL(
        host=host,
        port=port,
        user=username,
        password=password,
        timeout=timeout
    )
    return client

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

def fetch_rows_xover(nntp_client, group: str, start: int, end: int) -> list[dict]:
    """Fetch headers for a single range using XOVER."""
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
    
    # Process all overviews using the key map
    for artnum, ov in overviews:
        if artnum is None or not isinstance(ov, dict):
            continue
        rows.append(row_from_overview(group, int(artnum), ov, key_map))
    
    end_time = time.time()
    payload_MB = sum(len(str(r).encode("utf-8")) for r in rows) / 1_000_000.0
    mbps = payload_MB / (end_time - start_time) if (end_time - start_time) > 0 else 0
    print(f"Retrieved {len(rows):,} from {group} ({start:,}-{end:,}), "
          f"Elapsed = {end_time - start_time:.4f}s at {mbps:.2f} MB/s")

    return rows

def fetch_headers_chunked(config: ConfigParser, group: str, 
                          start: int, back_filled_up_to: int,
                          limit: int = 0, chunk_size: int = 100_000) -> list[dict]:
    """
    Fetch headers in chunks using multithreading.
    
    Args:
        config: ConfigParser object
        group: NNTP group name
        start: local_max (upper bound)
        back_filled_up_to: local_min (lower bound)
        limit: number of headers to fetch (<=0 means "all available")
        chunk_size: max articles per XOVER range
    
    Returns:
        List[dict]: parsed overview rows sorted by article number
    """
    # Get group info with temporary connection
    temp_client = get_nntp_client(config)
    nntp_max, nntp_min = temp_client.group(group)[1:3]
    temp_client.quit()

    # Use passed-in parameters as the range
    local_min = back_filled_up_to or nntp_min
    local_max = start or nntp_max
    
    print(f"Fetching from {local_min:,} to {local_max:,}")
    
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
            return fetch_rows_xover(client, group=group, start=chunk_start, end=chunk_end)
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
