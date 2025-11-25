# NNTP Indexer

A Python library for fetching, storing, and searching Usenet (NNTP) article headers.

## Features

- **Fast parallel header fetching** using multithreading
- **SQLite storage** with optimized indexes for text search
- **NZB file generation** from stored articles
- **Smart multipart grouping** for yEnc-encoded posts
- **Flexible filtering** by subject, poster, date range
- **Configurable** via INI files

## Installation

```bash
pip install -e .
```

## Quick Start

### 1. Create a config file

```bash
cp examples/nzbindex.ini.example nzbindex.ini
# Edit nzbindex.ini with your NNTP server details
```

### 2. Fetch headers

```python
from nntp_lib import get_config, fetch_headers_chunked, ensure_db, upsert_headers
import sqlite3

config = get_config()
group = 'alt.binaries.test'

# Setup database
conn = sqlite3.connect(f'{group}.sqlite')
ensure_db(conn)

# Fetch headers
rows = fetch_headers_chunked(
    config, 
    group=group, 
    start=100000,  # Upper article number
    back_filled_up_to=90000  # Lower article number
)

# Store in database
upsert_headers(conn, group, rows)
conn.close()
```

### 3. Create NZB files

```python
from nntp_lib import create_nzb_from_db

nzb_xml = create_nzb_from_db(
    db_path='alt.binaries.test.sqlite',
    group='alt.binaries.test',
    subject_like='Ubuntu',
    require_complete_sets=True
)

with open('ubuntu.nzb', 'w') as f:
    f.write(nzb_xml)
```

## Configuration

See `examples/nzbindex.ini.example` for all available options.

Key settings:
- `max_workers`: Number of parallel NNTP connections (5-20 recommended)
- `chunk_size`: Articles per XOVER request (100,000 default)
- `subject_like`, `not_subject`: Filter patterns for subject matching

## Performance

Typical performance on modern hardware:
- **Fetch**: ~40 seconds for 1.5M headers (with 20 workers)
- **DB insert**: ~15 seconds for 1.5M headers
- **NZB creation**: < 5 seconds for typical query

## Requirements

- Python 3.10+
- orjson (for fast JSON parsing)
- SQLite 3.35+ (for INSERT OR IGNORE optimization)

## License

MIT
