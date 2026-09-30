# NNTP Indexer

A Python library for fetching, storing, and searching Usenet (NNTP) article headers.

## Features

- **Parallel header fetching** with optional process-based parsing
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

def main():
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


if __name__ == '__main__':
    main()
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
- `max_workers`: Maximum workers and simultaneous NNTP connections, capped by the number of available chunks.
- `fetch_backend`: `threads` (default) or `processes`. Threads avoid transferring parsed rows between processes; process mode allows parsing on multiple CPU cores but can be slower overall.
- `chunk_size`: Articles per XOVER request (100,000 default)
- `subject_like`, `not_subject`: Filter patterns for subject matching
- `require_complete_sets`: Only include complete multi-part sets in NZBs
- `group_by_collection`: Create separate NZB per poster/collection (reduces file count by ~95%)
- `fuzzy_grouping`: Defaults to true in the create-NZB script. Group similar filenames, then batch the nearest collection names from the same poster. Set false to restore the original grouping and sequential batching. Library callers enable it with `fuzzy_grouping=True`.
- `min_articles_per_nzb`: Accumulate whole collections until at least this many emitted article segments are included (default 20; use 1 for separate collections). Smaller remainders are also saved. Fuzzy batches retain poster and time restrictions; non-fuzzy batches can combine posters within the same newsgroup.

Combined NZBs are named after the collection contributing the most emitted article
segments, followed by the newsgroup. Names contain no batch label or other collection
names. A numeric suffix is added only to distinguish duplicate output names.

### NZB Grouping

When `group_by_collection = true` and `fuzzy_grouping = true`, all matching
headers are read before collections are chosen. Quoted filenames are preferred;
otherwise the subject is used. File extensions, archive/recovery-volume markers,
part counters, size markers, and yEnc metadata are removed, followed by **all
digits**, wherever they occur. Case, punctuation, and whitespace are normalized.
Within the same poster and 48 hours of the collection anchor, Levenshtein
similarity of these cleaned names determines grouping, using
`fuzzy_similarity_threshold` percent (default 80, configurable from 0 to 100
under `[nzb]`). Missing dates do not restrict matching. There are no numeric-identifier
or minimum name-length restrictions. For example, `0ewpdl2.jpg` and `0ewpdl3.jpg` both become
`ewpdl` and match even at 100. Names that become empty stay separate.
Every candidate is compared with a fixed collection anchor to avoid transitive
chains of progressively less similar matches. Original file subjects, posters,
and segments remain intact. Numbers such as years and episode identifiers no longer
separate collections; only the remaining name and configured similarity matter.

After completeness filtering, fuzzy collections are batched to reach
`min_articles_per_nzb` emitted segments. Each batch starts with the first remaining
collection in alphabetical name order and adds whole collections from the same
poster in ascending Levenshtein distance from that first collection's cleaned
name. Known dates in a combined batch must span no more than 48 hours. The
similarity threshold controls initial collection matching; batching can add more
distant names to reach the requested size. Collections are never split, and any
remainder without enough eligible articles is saved below the minimum. A minimum
of 1 disables this batching step.

## Performance

Fetching defaults to threads. To compare CPU parallelism on the same article
range and worker count, set `fetch_backend = processes` in `[servers]`.
Process mode transfers all parsed rows back to the parent, which adds serialization
and memory overhead; more CPU parallelism does not guarantee faster retrieval.
The existing `scripts/create_db.py` supports both modes. Custom scripts using
process mode must run from a file with an `if __name__ == '__main__':` guard.

Completed chunks are assembled in article order without a final global sort.
Progress reports headers/second and separates XOVER (network transfer **plus**
nntplib overview parsing) from cleanup/date conversion/sorting. The final summary
reports total throughput, assembly time, and mean/maximum result wait. Result wait
measures the time from worker completion to parent collection, including process
serialization/transfer and parent scheduling; it is not a pure IPC measurement.
JSON archiving and SQLite writes remain sequential.

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
