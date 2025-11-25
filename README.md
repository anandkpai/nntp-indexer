# NNTP Indexer

A Python library for fetching, storing, and searching Usenet (NNTP) article headers.

## Features

- **Fast parallel header fetching** using multithreading
- **SQLite storage** with optimized indexes for text search
- **NZB file generation** from stored articles
- **Smart multipart grouping** for yEnc-encoded posts
- **Grouped NZB creation** by poster and collection name
- **Flexible filtering** by subject, poster, date range
- **Configurable** via INI files

## Installation

```bash
# Clone the repository
git clone <repo-url>
cd nntp-indexer

# Create virtual environment (recommended)
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install in editable mode
pip install -e .
```

## Quick Start

### 1. Create a config file

```bash
cp scripts/nzbindex.ini.example nzbindex.ini
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

**Option A: Single NZB with all matches**

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

**Option B: Grouped NZBs by poster and collection**

```python
from nntp_lib import create_grouped_nzbs_from_db

# Returns list of (filename, nzb_xml) tuples
nzbs = create_grouped_nzbs_from_db(
    db_path='alt.binaries.test.sqlite',
    group='alt.binaries.test',
    output_path='./nzbs',
    subject_like='Ubuntu',
    require_complete_sets=True
)

# Write all NZBs to disk
for filename, nzb_xml in nzbs:
    with open(f'./nzbs/{filename}', 'w') as f:
        f.write(nzb_xml)
```

Or use the script with `group_by_collection = true` in config:

```bash
python scripts/create_nzb.py
```

## Configuration

See `scripts/nzbindex.ini.example` for all available options.

Key settings:
- `max_workers`: Number of parallel NNTP connections (5-20 recommended)
- `chunk_size`: Articles per XOVER request (100,000 default)
- `subject_like`, `not_subject`: Filter patterns for subject matching
- `require_complete_sets`: Only include complete multi-part sets in NZBs
- `group_by_collection`: Create separate NZB per poster/collection (reduces file count by ~95%)

### NZB Grouping

When `group_by_collection = true`, the system intelligently groups articles by:
1. Poster (from_addr)
2. Normalized collection name (removes file numbers, extensions, common patterns)

This dramatically reduces the number of NZB files created (e.g., 107 NZBs instead of 3,315 for the same content), making them more manageable and organized by actual collections.

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
