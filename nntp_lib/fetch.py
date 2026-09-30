"""NNTP fetching operations."""

import nntplib
import time
from configparser import ConfigParser
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, wait, FIRST_COMPLETED
from multiprocessing import get_context
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
    start_time = time.perf_counter()
    nntp_client.group(group)
    
    xover_started = time.perf_counter()
    resp, overviews = nntp_client.xover(int(start), int(end))
    xover_elapsed = time.perf_counter() - xover_started
    cleanup_started = time.perf_counter()
    
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
    
    # Sorting each chunk preserves the public ordering guarantee, even for a
    # server that returns overviews out of order. Chunks themselves never overlap.
    rows.sort(key=lambda row: row['artnum'])
    elapsed = time.perf_counter() - start_time
    rate = len(rows) / elapsed if elapsed else 0
    print(f"Retrieved {len(rows):,} from {group} ({start:,}-{end:,}), "
          f"Elapsed = {elapsed:.4f}s at {rate:,.0f} headers/s "
          f"(XOVER + overview parsing: {xover_elapsed:.2f}s; "
          f"cleanup + sorting: {time.perf_counter() - cleanup_started:.2f}s)", flush=True)

    return rows

def _fetch_chunk_with_client(config, group, chunk_start, chunk_end):
    """Return parsed rows and a timestamp before executor result transfer."""
    client = get_nntp_client(config)
    try:
        rows = fetch_rows_xover(client, group, chunk_start, chunk_end)
    finally:
        client.quit()
    return rows, time.perf_counter()


def fetch_headers_chunked(config: ConfigParser, group: str, 
                          start: int, back_filled_up_to: int,
                          limit: int = 0, chunk_size: int = 100_000) -> list[dict]:
    """
    Fetch headers concurrently, one connection per task.

    Threads are the default. Set servers.fetch_backend = processes to opt into
    CPU parallelism, including the cost of transferring rows between processes.
    Process mode requires an importable script and an ``if __name__ == '__main__'``
    guard. max_workers limits workers and simultaneous NNTP connections.
    
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
    fetch_started = time.perf_counter()
    backend = config.get('servers', 'fetch_backend', fallback='threads').strip().lower()
    if backend not in ('threads', 'processes'):
        raise ValueError('fetch_backend must be threads or processes')
    max_workers = config.getint('servers', 'max_workers', fallback=5)
    if max_workers < 1 or chunk_size < 1:
        raise ValueError('max_workers and chunk_size must be at least 1')

    # Get group info with temporary connection
    temp_client = get_nntp_client(config)
    try:
        _, _, nntp_min, nntp_max, _ = temp_client.group(group)
    finally:
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
    
    if not chunks:
        return []
    max_workers = min(max_workers, len(chunks))
    print(f"Fetching {len(chunks)} chunks in parallel with {max_workers} {backend}...")

    # Bound outstanding tasks so completed Futures do not retain another full
    # set of results. Keep results by chunk index, regardless of finish order.
    chunk_rows_by_index = [None] * len(chunks)
    total_received = 0
    remaining_chunks = iter(enumerate(chunks))
    result_wait_total = 0.0
    result_wait_max = 0.0
    completed_chunks = 0
    executor_class = ThreadPoolExecutor if backend == 'threads' else ProcessPoolExecutor
    # Spawn avoids inheriting the caller's SQLite connections and other resources.
    executor_options = {} if backend == 'threads' else {'mp_context': get_context('spawn')}
    with executor_class(max_workers=max_workers, **executor_options) as executor:
        pending = {}

        def submit_next():
            item = next(remaining_chunks, None)
            if item is None:
                return
            index, (chunk_start, chunk_end) = item
            future = executor.submit(_fetch_chunk_with_client, config, group,
                                     chunk_start, chunk_end)
            pending[future] = (index, chunk_start, chunk_end)

        for _ in range(max_workers):
            submit_next()
        while pending:
            done, _ = wait(pending, return_when=FIRST_COMPLETED)
            for future in done:
                index, chunk_start, chunk_end = pending.pop(future)
                try:
                    chunk_rows, worker_finished = future.result()
                    result_wait = time.perf_counter() - worker_finished
                    result_wait_total += result_wait
                    result_wait_max = max(result_wait_max, result_wait)
                    completed_chunks += 1
                    chunk_rows_by_index[index] = chunk_rows
                    total_received += len(chunk_rows)
                    print(f"Completed {chunk_start:,}-{chunk_end:,}: "
                          f"Total so far {total_received:,}/{want:,}; "
                          f"result wait = {result_wait:.2f}s")
                except Exception as e:
                    print(f"ERROR: Chunk {chunk_start:,}-{chunk_end:,} failed: {e}")
                submit_next()

    # Concatenation is linear; no global sort of millions of rows is needed.
    assembly_started = time.perf_counter()
    all_rows = []
    for index, chunk_rows in enumerate(chunk_rows_by_index):
        if chunk_rows:
            all_rows.extend(chunk_rows)
        chunk_rows_by_index[index] = None
    assembly_elapsed = time.perf_counter() - assembly_started
    elapsed = time.perf_counter() - fetch_started
    average_wait = result_wait_total / completed_chunks if completed_chunks else 0
    rate = len(all_rows) / elapsed if elapsed else 0
    print(f"Fetch summary ({backend}, {max_workers} workers): "
          f"{len(all_rows):,} headers in {elapsed:.2f}s ({rate:,.0f} headers/s); "
          f"assembly = {assembly_elapsed:.2f}s; "
          f"result wait mean/max = {average_wait:.2f}/{result_wait_max:.2f}s", flush=True)
    return all_rows
