"""Example: List all newsgroups available on the NNTP server."""

import sqlite3
from nntp_lib import get_config, get_nntp_client

def ensure_groups_table(conn: sqlite3.Connection):
    """Create newsgroups table if it doesn't exist."""
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS newsgroups (
        group_name TEXT PRIMARY KEY,
        first_article INTEGER,
        last_article INTEGER,
        article_count INTEGER,
        status_flag TEXT,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    conn.commit()

def list_all_groups(save_to_db: bool = True):
    """
    List all newsgroups available on the server and optionally save to database.
    
    Args:
        save_to_db: If True, save groups to database
    """
    config = get_config()
    client = get_nntp_client(config)
    
    try:
        print("Fetching list of newsgroups from server...")
        
        # Get list of all groups
        resp, groups = client.list()
        
        print(f"\nFound {len(groups)} newsgroups:\n")
        print(f"{'Group Name':<50} {'Count':>12} {'First':>12} {'Last':>12} Status")
        print("=" * 100)
        
        groups_data = []
        for group_info in groups:
            name, last, first, flag = group_info
            count = int(last) - int(first) + 1 if last and first else 0
            
            print(f"{name:<50} {count:>12,} {int(first):>12,} {int(last):>12,} {flag:>6}")
            
            groups_data.append({
                'group_name': name,
                'first_article': int(first),
                'last_article': int(last),
                'article_count': count,
                'status_flag': flag
            })
        
        print(f"\nTotal groups: {len(groups):,}")
        
        if save_to_db and groups_data:
            DB_BASE_PATH = config.get('db', 'DB_BASE_PATH', fallback='/tmp/nntp-index')
            db_path = f"{DB_BASE_PATH}/newsgroups.sqlite"
            
            conn = sqlite3.connect(db_path)
            ensure_groups_table(conn)
            
            cur = conn.cursor()
            cur.executemany("""
                INSERT OR REPLACE INTO newsgroups 
                (group_name, first_article, last_article, article_count, status_flag, last_updated)
                VALUES (:group_name, :first_article, :last_article, :article_count, :status_flag, CURRENT_TIMESTAMP)
            """, groups_data)
            
            conn.commit()
            conn.close()
            
            print(f"\nSaved {len(groups_data):,} groups to: {db_path}")
        
    finally:
        client.quit()

if __name__ == '__main__':
    list_all_groups()
