"""Example: Fetch NNTP headers and store in SQLite database."""

import sqlite3
import time
from pathlib import Path

from nntp_lib import get_config, ensure_db, upsert_headers, fetch_headers_chunked

def main():
    """Main function to fetch and store NNTP headers."""
    config = get_config('config.ini')
    groups = config['groups']['names'].split(',')
    DB_BASE_PATH = config['db']['DB_BASE_PATH']
    
    Path(DB_BASE_PATH).mkdir(parents=True, exist_ok=True)

    for group in groups:
        group = group.strip()
        db_path = f"{DB_BASE_PATH}/{group}.sqlite"
        conn = sqlite3.connect(db_path)
        
        ensure_db(conn)
        cur = conn.cursor()
        cur.execute(
            "SELECT COALESCE(MAX(artnum), 0), COALESCE(MIN(artnum), 0) "
            "FROM articles WHERE group_name = ?", 
            (group,)
        )
        local_max, local_min = cur.fetchone()

        print(f'Fetching headers: max={local_max}, min={local_min}')

        start_time = time.time()
        rows = fetch_headers_chunked(
            config, 
            group=group, 
            start=local_max or 0,
            back_filled_up_to=local_min or 0
        )
        fetch_time = time.time() - start_time
        print(f"Fetched {len(rows):,} in {fetch_time:.2f}s")
        
        if rows:
            start_time = time.time()
            upsert_headers(conn, group, rows)
            db_time = time.time() - start_time
            print(f"Inserted {len(rows):,} in {db_time:.2f}s")
        
        conn.close()

if __name__ == '__main__':
    main()
