"""Database operations for NNTP library."""

import sqlite3
import time

def ensure_db(conn: sqlite3.Connection):
    """Create tables and indexes if they don't exist."""
    cur = conn.cursor()
    
    # Set performance PRAGMAs
    cur.execute("PRAGMA synchronous = OFF")
    cur.execute("PRAGMA journal_mode = WAL")
    cur.execute("PRAGMA temp_store = MEMORY")
    cur.execute("PRAGMA cache_size = -64000")  # 64MB cache
    
    # Base table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS articles (
        message_id   TEXT PRIMARY KEY,
        group_name   TEXT NOT NULL,
        artnum       INTEGER NOT NULL,
        subject      TEXT,
        from_addr    TEXT,
        date_utc     TEXT,
        refs         TEXT,
        bytes        INTEGER,
        lines        INTEGER,
        xref         TEXT
    );
    """)
    
    # Indexes for text searches and common queries
    cur.execute("CREATE INDEX IF NOT EXISTS idx_articles_subject ON articles(subject);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_articles_from ON articles(from_addr);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_articles_group_artnum ON articles(group_name, artnum);")

    conn.commit()

def upsert_headers(conn: sqlite3.Connection, group: str, rows: list[dict]):
    """Insert or ignore headers into database."""
    to_bind = []
    for r in rows:
        b = r.copy()
        b["group_name"] = group
        to_bind.append(b)

    cur = conn.cursor()
    start_time = time.time()
    
    cur.executemany(
        """
        INSERT OR IGNORE INTO articles (
            message_id, group_name, artnum, subject, from_addr, date_utc, refs, bytes, lines, xref
        ) VALUES (
            :message_id, :group_name, :artnum, :subject, :from_addr, :date_utc, :refs, :bytes, :lines, :xref
        )
        """,
        to_bind,
    )
    
    conn.commit()
    end_time = time.time()
    print(f"Wrote to db {len(rows):,} for {group}, Elapsed = {end_time - start_time:.4f} seconds")
