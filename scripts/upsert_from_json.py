
import sqlite3
import orjson
import time
import os
from nntp_lib import get_config
from nntp_lib.db import ensure_db, upsert_headers

# Read config and group name

config = get_config()
DB_BASE_PATH = config.get('db', 'DB_BASE_PATH', fallback='/mnt/r/tmp/nzbindex')
GROUP = config['groups']['names'].split(',')[0].strip()
DB_PATH = f"{DB_BASE_PATH}/{GROUP}.sqlite"
JSON_PATH = f"{DB_BASE_PATH}/headers-archive/{GROUP}_array.json"

# Load JSON array

print(f"Loading JSON array from {JSON_PATH}...")
start_time = time.time()
with open(JSON_PATH, 'rb') as f:
    rows = orjson.loads(f.read())
end_time = time.time()
print(f"Loaded {len(rows):,} rows in {end_time - start_time:.4f} seconds.")

# Create in-memory DB and insert

print("Creating in-memory SQLite DB and upserting rows...")
conn_mem = sqlite3.connect(':memory:')
ensure_db(conn_mem)

# Bulk upsert using the same logic as create_db.py
start_time = time.time()
upsert_headers(conn_mem, GROUP, rows)
end_time = time.time()
print(f"Upserted {len(rows):,} rows in {end_time - start_time:.4f} seconds.")

# Backup to disk DB
print(f"Backing up in-memory DB to disk DB at {DB_PATH}...")
conn_disk = sqlite3.connect(DB_PATH)
start_time = time.time()
conn_mem.backup(conn_disk)
conn_disk.close()
end_time = time.time()
print(f"Backup completed in {end_time - start_time:.4f} seconds.")

conn_mem.close()
print("Done.")
