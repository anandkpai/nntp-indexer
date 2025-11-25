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
    groups = config['groups']['names'].split(',')    
    DB_BASE_PATH = config['db']['DB_BASE_PATH']

    for group in groups:
        db_path = f"{DB_BASE_PATH}/{group}.sqlite"
        conn = sqlite3.connect(db_path)
        
        ensure_db(conn)
        cur = conn.cursor()
        cur.execute("SELECT COALESCE(MAX(artnum), 0), COALESCE(MIN(artnum), 0) FROM articles WHERE group_name = ?", (group,))
        local_max, local_min = cur.fetchone()

        # if a filter limit has been set, use that over the local max and local min. 
        local_min, local_max = get_article_range(config, group, local_min, local_max)
        print(f'Fetching headers from server with max artnum: {local_max} and min artnum {local_min}')            

        start_time = time.time()
        rows = fetch_headers_chunked(
            config=config,
            group=group,
            start=local_max,
            back_filled_up_to=local_min
        )
        end_time = time.time()
        print(f"Retrieved {len(rows):,} from {group}, Elapsed time for whole group = {end_time - start_time:.4f} seconds")        
        
        if rows:
            cached_headers_file = f"{ARCHIVE_ROWS_PATH_BASE}/{group}.json"
            os.makedirs(ARCHIVE_ROWS_PATH_BASE, exist_ok=True)
            start_time = time.time()
            with open(cached_headers_file, "wb") as f:  # Open in binary mode for orjson
                # Write all rows as NDJSON in one operation
                f.write(b'\n'.join(orjson.dumps(row) for row in rows))
                f.write(b'\n')        
            end_time = time.time()
            print(f"Wrote {len(rows):,} to {cached_headers_file} in {end_time - start_time:.4f} seconds")
            
            start_time = time.time()
            upsert_headers(conn, group, rows)
            end_time = time.time()
            print(f"Upserted {len(rows):,} headers into {db_path} in {end_time - start_time:.4f} seconds")
        
        conn.close()