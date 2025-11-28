from nntp_lib.utils import get_config
from nntp_lib.db import ensure_db, upsert_headers
from nntp_lib.fetch import fetch_headers_chunked, get_nntp_client
from find_date_range import find_article_range_by_dates
from configparser import ConfigParser
import sqlite3
import time 
import os
import orjson

# globals 
config = get_config()
ARCHIVE_ROWS_PATH_BASE = f"{config.get('db', 'DB_BASE_PATH', fallback='/tmp/nntp_archive')}/headers-archive"

def get_article_range(config: ConfigParser, group: str, local_min: int, local_max: int) -> tuple[int, int]:
    """
    Get article range based on date filters in config, or return server defaults.
    
    Args:
        config: Configuration object
        group: Newsgroup name
        local_min: Current minimum article number in database
        local_max: Current maximum article number in database
    
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


if __name__ == '__main__':
    print("="*80)
    print("NNTP Indexer - Creating/Updating Article Database")
    print("="*80)
    
    groups = config['groups']['names'].split(',')    
    DB_BASE_PATH = config['db']['DB_BASE_PATH']
    
    print(f"\nGroups to process: {', '.join(groups)}")
    print(f"Database path: {DB_BASE_PATH}\n")

    for group in groups:
        print(f"\n{'='*80}")
        print(f"Processing group: {group}")
        print(f"{'='*80}")

        db_path = f"{DB_BASE_PATH}/{group}.sqlite"
        disk_db_exists = os.path.exists(db_path)
        if disk_db_exists:
            print(f"Disk DB exists at {db_path}, upserting directly into disk DB...")
            conn = sqlite3.connect(db_path)
            ensure_db(conn)
            # Get current max/min from disk DB
            cur = conn.cursor()
            cur.execute("SELECT COALESCE(MAX(artnum), 0), COALESCE(MIN(artnum), 0) FROM articles WHERE group_name = ?", (group,))
            db_max, db_min = cur.fetchone()
            print(f"Current disk DB range: {db_min:,} to {db_max:,}")
            cur.close()
        else:
            print(f"No disk DB found for {group}, using in-memory DB and backing up to disk after upsert...")
            conn = sqlite3.connect(':memory:')
            ensure_db(conn)
            db_max, db_min = 0, 0

        # Get server's current article range
        print("Connecting to NNTP server to check available articles...")
        temp_client = get_nntp_client(config)
        server_max, server_min = temp_client.group(group)[1:3]
        temp_client.quit()
        print(f"Server article range: {server_min:,} to {server_max:,}")

        # Default: fetch new articles only (from db_max+1 to server_max)
        local_min = db_max + 1 if db_max > 0 else server_min
        local_max = server_max

        # Apply filter overrides if configured
        local_min, local_max = get_article_range(config, group, local_min, local_max)

        # Check if there's anything to fetch
        if local_min > local_max:
            print(f"\n Database is up to date. No new articles to fetch.")
            conn_mem.close()
            continue

        total_to_fetch = local_max - local_min + 1
        print(f"\nArticle range to fetch: {local_min:,} to {local_max:,}")
        print(f"Total headers to retrieve: {total_to_fetch:,}")
        print(f"Starting parallel fetch...\n")

        start_time = time.time()
        rows = fetch_headers_chunked(
            config=config,
            if rows:
                cached_headers_file = f"{ARCHIVE_ROWS_PATH_BASE}/{group}.json"
                os.makedirs(ARCHIVE_ROWS_PATH_BASE, exist_ok=True)

                print(f"\nArchiving headers to JSON...")
                start_time = time.time()
                with open(cached_headers_file, "wb") as f:  # Open in binary mode for orjson
                    f.write(b'\n'.join(orjson.dumps(row) for row in rows))
                    f.write(b'\n')
                end_time = time.time()
                print(f"\u2713 Wrote {len(rows):,} rows to {cached_headers_file} in {end_time - start_time:.4f} seconds")

                print(f"\nInserting headers into database...")
                start_time = time.time()
                upsert_headers(conn, group, rows)
                end_time = time.time()
                print(f"\u2713 Upserted {len(rows):,} headers into DB in {end_time - start_time:.4f} seconds")

                if not disk_db_exists:
                    print(f"\nBacking up in-memory DB to disk DB at {db_path}...")
                    start_time = time.time()
                    conn_disk = sqlite3.connect(db_path)
                    conn.backup(conn_disk)
                    conn_disk.close()
                    end_time = time.time()
                    print(f"\u2713 Backed up in-memory DB to disk DB in {end_time - start_time:.4f} seconds")
            else:
                print("\nNo new headers to process.")

            conn.close()
            print(f"\n\u2713 Completed processing for {group}")
            end_time = time.time()
            print(f" Backed up in-memory DB to disk DB in {end_time - start_time:.4f} seconds")
        else:
            print("\nNo new headers to process.")

        conn_mem.close()
        print(f"\n Completed processing for {group}")