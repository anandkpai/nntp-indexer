"""NZB file creation from stored articles."""

import re
import sqlite3
import time
import xml.etree.ElementTree as ET
from xml.dom import minidom
from datetime import datetime

def normalize_subject_base(subject: str) -> str:
    """
    Remove part counters like (n/m), [n/m], {n/m}, 'part n of m', 'yEnc', etc.
    to produce a "base" subject for grouping multi-part posts.
    """
    s = subject
    s = re.sub(r'[(\[{]\d+/\d+[)\]}]', '', s)
    s = re.sub(r'\bpart\s+\d+\s+of\s+\d+\b', '', s, flags=re.IGNORECASE)
    s = re.sub(r'\byEnc\b', '', s, flags=re.IGNORECASE)
    return s.strip()

def extract_nm_leftmost(subject: str) -> tuple[int, int] | None:
    """Extract (n, m) from leftmost occurrence of (n/m) or [n/m] or {n/m}."""
    pattern = r'[(\[{](\d+)/(\d+)[)\]}]'
    match = re.search(pattern, subject)
    if match:
        return (int(match.group(1)), int(match.group(2)))
    match2 = re.search(r'\bpart\s+(\d+)\s+of\s+(\d+)\b', subject, flags=re.IGNORECASE)
    if match2:
        return (int(match2.group(1)), int(match2.group(2)))
    return None

def extract_nm_rightmost(subject: str) -> tuple[int, int] | None:
    """Extract (n, m) from rightmost occurrence of (n/m) or [n/m] or {n/m}."""
    pattern = r'[(\[{](\d+)/(\d+)[)\]}]'
    matches = list(re.finditer(pattern, subject))
    if matches:
        match = matches[-1]
        return (int(match.group(1)), int(match.group(2)))
    matches2 = list(re.finditer(r'\bpart\s+(\d+)\s+of\s+(\d+)\b', subject, flags=re.IGNORECASE))
    if matches2:
        match2 = matches2[-1]
        return (int(match2.group(1)), int(match2.group(2)))
    return None

def _group_with_picker(rows: list[dict], picker_fn) -> tuple[dict, list]:
    """
    Group rows by (base_subject, m, poster).
    Returns ({ (base, m, poster): [rows] }, [singles])
    """
    groups = {}
    singles = []
    for r in rows:
        subj = r["subject"] or ""
        poster = r["from_addr"] or ""
        nm = picker_fn(subj)
        if nm is None:
            singles.append(r)
            continue
        n, m = nm
        base = normalize_subject_base(subj)
        key = (base, m, poster)
        groups.setdefault(key, []).append(r)
    return groups, singles

def group_rows_auto(rows: list[dict]) -> tuple[dict, list]:
    """
    Auto-select leftmost or rightmost strategy for (n/m).
    Returns ({ (base, m, poster): [rows] }, [singles])
    """
    groups_left, singles_left = _group_with_picker(rows, extract_nm_leftmost)
    groups_right, singles_right = _group_with_picker(rows, extract_nm_rightmost)
    
    def score_groups(groups_dict):
        total_parts = sum(len(parts) for parts in groups_dict.values())
        multi_part = sum(1 for parts in groups_dict.values() if len(parts) > 1)
        return (total_parts, multi_part)
    
    score_left = score_groups(groups_left)
    score_right = score_groups(groups_right)
    
    if score_right > score_left:
        print(f"Using rightmost strategy: {len(groups_right)} groups")
        return groups_right, singles_right
    else:
        print(f"Using leftmost strategy: {len(groups_left)} groups")
        return groups_left, singles_left

def message_id_text(mid: str) -> str:
    """Format message ID for NZB (strip < > if present)."""
    if not mid:
        return ""
    mid = mid.strip()
    if mid.startswith("<") and mid.endswith(">"):
        return mid[1:-1]
    return mid

def build_nzb_xml(groups_dict: dict, singles: list[dict], group_name: str, 
                  require_complete_sets: bool = False) -> str:
    """Build NZB XML from grouped articles."""
    root = ET.Element("nzb", xmlns="http://www.newzbin.com/DTD/2003/nzb")
    
    for (base, m, poster), parts in groups_dict.items():
        # Check if set is complete (has all parts)
        if require_complete_sets and len(parts) < m:
            continue
        if base.lower().endswith('.exe'):
            continue
        
        # Extract actual part numbers and check for gaps
        part_numbers = set()
        parts_with_numbers = []
        for part in parts:
            subj = part["subject"] or ""
            # Try to extract part number from subject
            nm = extract_nm_rightmost(subj) or extract_nm_leftmost(subj)
            if nm:
                n, _ = nm
                part_numbers.add(n)
                parts_with_numbers.append((n, part))
        
        # Check for gaps in part numbers
        if part_numbers:
            expected_parts = set(range(1, m + 1))
            missing_parts = expected_parts - part_numbers
            if missing_parts and require_complete_sets:
                print(f"Skipping incomplete set '{base[:50]}...': missing parts {sorted(missing_parts)}")
                continue
        
        file_el = ET.SubElement(root, "file", {
            "poster": poster,
            "date": str(int(datetime.now().timestamp())),
            "subject": base
        })
        
        groups_el = ET.SubElement(file_el, "groups")
        ET.SubElement(groups_el, "group").text = group_name
        
        segs_el = ET.SubElement(file_el, "segments")
        
        # Sort by actual part number, not article number
        parts_with_numbers.sort(key=lambda x: x[0])
        
        for part_num, r in parts_with_numbers:
            ET.SubElement(segs_el, "segment", {
                "bytes": str(r["bytes"] or 0),
                "number": str(part_num)  # Use actual part number from subject
            }).text = message_id_text(r["message_id"])
    
    return _serialize_nzb(root)

def _serialize_nzb(root: ET.Element) -> str:
    xml_str = ET.tostring(root, encoding='unicode')
    dom = minidom.parseString(xml_str)
    pretty = dom.toprettyxml(indent="  ")
    
    lines = pretty.split('\n')
    lines.insert(1, '<!DOCTYPE nzb PUBLIC "-//newzbin//DTD NZB 1.1//EN" "http://www.newzbin.com/DTD/nzb/nzb-1.1.dtd">')
    return '\n'.join(lines)

def _batch_filename(root: ET.Element, group: str, used_names: set,
                    collection_counts=None) -> str:
    from .utils import normalize_subject_for_grouping, sanitize_filename

    counts = {}
    for file in root.findall('file'):
        subject = file.get('subject', '')
        name = normalize_subject_for_grouping(subject) or subject
        name = sanitize_filename(name).strip(' _')
        if name:
            counts[name] = counts.get(name, 0) + len(file.findall('segments/segment'))

    if collection_counts is not None:
        counts = collection_counts
    # Name the output after the collection contributing the most segments.
    names = sorted(counts, key=lambda name: (-counts[name], name))
    label = sanitize_filename(names[0]).strip(' _') if names else ''
    label = label.encode('utf-8')[:140].decode('utf-8', 'ignore').rstrip(' .')
    group_label = sanitize_filename(group).encode('utf-8')[:60].decode('utf-8', 'ignore')
    prefix = f'{label}_{group_label}' if label else group_label
    filename = f'{prefix}.nzb'
    suffix = 2
    while filename in used_names:
        filename = f'{prefix}_{suffix}.nzb'
        suffix += 1
    used_names.add(filename)
    return filename


def _batch_nzbs(nzbs: list[tuple[str, str]], group: str,
                min_articles: int) -> list[tuple[str, str]]:

    if min_articles <= 1:
        return nzbs

    namespace = '{http://www.newzbin.com/DTD/2003/nzb}'
    results = []
    used_names = set()
    root = ET.Element('nzb', xmlns=namespace[1:-1])
    article_count = 0
    for _, xml in nzbs:
        collection = ET.fromstring(xml)
        files = collection.findall(f'{namespace}file')
        article_count += sum(len(file.findall(f'{namespace}segments/{namespace}segment'))
                             for file in files)
        # Merge rendered files, preserving segment numbering and completeness filtering.
        for file in files:
            for element in file.iter():
                element.tag = element.tag.removeprefix(namespace)
            root.append(file)
        if article_count >= min_articles:
            filename = _batch_filename(root, group, used_names)
            results.append((filename, _serialize_nzb(root)))
            root = ET.Element('nzb', xmlns=namespace[1:-1])
            article_count = 0

    if len(root):
        filename = _batch_filename(root, group, used_names)
        results.append((filename, _serialize_nzb(root)))
    return results

def _batch_fuzzy_nzbs(collections, group, minimum):
    """Fill batches from one poster, nearest to a fixed cleaned-name anchor.

    Input entries are (poster, name, known timestamps, rendered XML). Count only
    emitted segments, retain whole collections, and keep known batch dates within
    48 hours. The similarity threshold controls initial grouping, not batching.
    """
    from collections import defaultdict
    from .collection_matching import levenshtein

    namespace = '{http://www.newzbin.com/DTD/2003/nzb}'
    posters = defaultdict(list)
    for poster, name, dates, xml in collections:
        root = ET.fromstring(xml)
        for element in root.iter():
            element.tag = element.tag.removeprefix(namespace)
        count = len(root.findall('file/segments/segment'))
        posters[poster].append((name, dates, root, count))

    results = []
    used_names = set()
    for poster in sorted(posters):
        pending = sorted(posters[poster], key=lambda entry: entry[0])
        while pending:
            anchor, dates, root, count = pending.pop(0)
            collection_counts = {anchor: count}
            dates = list(dates)
            # Compare every candidate with this batch's original anchor.
            ranked = sorted(range(len(pending)), key=lambda i: (
                levenshtein(anchor, pending[i][0]), pending[i][0], i)) if count < minimum else []
            selected = set()
            for i in ranked:
                if count >= minimum:
                    break
                candidate_name, candidate_dates, candidate_root, candidate_count = pending[i]
                combined_dates = dates + list(candidate_dates)
                if combined_dates and max(combined_dates) - min(combined_dates) > 48 * 3600:
                    continue
                root.extend(candidate_root.findall('file'))
                count += candidate_count
                collection_counts[candidate_name] = collection_counts.get(candidate_name, 0) + candidate_count
                dates = combined_dates
                selected.add(i)
            pending = [entry for i, entry in enumerate(pending) if i not in selected]
            root.set('xmlns', namespace[1:-1])
            results.append((_batch_filename(root, group, used_names, collection_counts),
                            _serialize_nzb(root)))
    return results


def create_nzb_from_db(db_path: str, group: str,
                       subject_like: str = None,
                       from_like: str = None,
                       not_subject: str = None,
                       not_from: str = None,
                       require_complete_sets: bool = False) -> str:
    """Query database and create NZB XML."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    where = ["group_name = ?"]
    params = [group]
    
    if subject_like:
        where.append("subject LIKE ? COLLATE NOCASE")
        params.append(f"%{subject_like}%")
    
    if from_like:
        where.append("from_addr LIKE ? COLLATE NOCASE")
        params.append(f"%{from_like}%")
    
    if not_subject:
        for term in not_subject.split('|'):
            term = term.strip()
            if term:
                where.append("subject NOT LIKE ? COLLATE NOCASE")
                params.append(f"%{term}%")
    
    if not_from:
        for term in not_from.split('|'):
            term = term.strip()
            if term:
                where.append("from_addr NOT LIKE ? COLLATE NOCASE")
                params.append(f"%{term}%")
    
    sql = f"""
        SELECT message_id, subject, from_addr, date_utc, bytes, artnum, group_name
        FROM articles
        WHERE {' AND '.join(where)}
        ORDER BY artnum
    """
    
    # Build actual SQL with parameters substituted for display
    display_sql = sql
    for param in params:
        # Format string params with quotes, numbers without
        if isinstance(param, str):
            display_sql = display_sql.replace('?', f"'{param}'", 1)
        else:
            display_sql = display_sql.replace('?', str(param), 1)
    
    print(f"\nExecuting SQL Query:")
    print(display_sql)
    print()
    
    start_time = time.time()
    cur.execute(sql, tuple(params))
    
    rows = [dict(r) for r in cur.fetchall()]
    query_time = time.time() - start_time
    conn.close()
    
    print(f"Query execution time: {query_time:.4f} seconds")
    
    if not rows:
        print(f"No rows found for group='{group}'")
        return ""
    
    print(f"Found {len(rows):,} articles matching filters")
    groups_dict, singles = group_rows_auto(rows)
    print(f"Grouped into {len(groups_dict)} multi-part sets and {len(singles)} singles")
    
    return build_nzb_xml(groups_dict, singles, group, require_complete_sets)

def create_grouped_nzbs_from_db(db_path: str, group: str, output_path: str,
                                subject_like: str = None, from_like: str = None,
                                not_subject: str = None, not_from: str = None,
                                require_complete_sets: bool = False,
                                min_articles_per_nzb: int = 1,
                                fuzzy_grouping: bool = False,
                                fuzzy_similarity_threshold: float = 80) -> list[tuple[str, str]]:
    """Create separate NZB files grouped by poster and collection name.
    
    Collections accumulate until the minimum number of emitted article segments
    is reached. Collections are never split; the final remainder is also saved.
    With fuzzy_grouping, batch nearest collection names within the same poster
    and 48-hour window after rendering and completeness filtering.

    Returns:
        List of (filename, nzb_xml) tuples for created NZBs
    """
    if min_articles_per_nzb < 1:
        raise ValueError('min_articles_per_nzb must be at least 1')
    if not 0 <= fuzzy_similarity_threshold <= 100:
        raise ValueError('fuzzy_similarity_threshold must be between 0 and 100')

    from collections import defaultdict
    from .utils import normalize_subject_for_grouping, sanitize_filename
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    where = ["group_name = ?"]
    params = [group]
    
    if subject_like:
        for term in subject_like.split('%'):
            if term:
                where.append("subject LIKE ? COLLATE NOCASE")
                params.append(f"%{term}%")
    
    if from_like:
        for term in from_like.split('%'):
            if term:
                where.append("from_addr LIKE ? COLLATE NOCASE")
                params.append(f"%{term}%")
    
    if not_subject:
        for term in not_subject.split('|'):
            term = term.strip()
            if term:
                where.append("subject NOT LIKE ? COLLATE NOCASE")
                params.append(f"%{term}%")
    
    if not_from:
        for term in not_from.split('|'):
            term = term.strip()
            if term:
                where.append("from_addr NOT LIKE ? COLLATE NOCASE")
                params.append(f"%{term}%")
    
    where_clause = " AND ".join(where)
    sql = f"""
        SELECT message_id, subject, from_addr, date_utc, bytes, artnum, group_name
        FROM articles
        WHERE {where_clause}
        ORDER BY from_addr, subject, artnum
    """
    
    # Build actual SQL with parameters substituted for display
    display_sql = sql
    for param in params:
        # Format string params with quotes, numbers without
        if isinstance(param, str):
            display_sql = display_sql.replace('?', f"'{param}'", 1)
        else:
            display_sql = display_sql.replace('?', str(param), 1)
    
    print(f"\nExecuting SQL Query:")
    print(display_sql)
    print()
    
    start_time = time.time()
    print(f"Querying database...")
    cur.execute(sql, params)
    rows = [dict(r) for r in cur.fetchall()]
    query_time = time.time() - start_time
    conn.close()
    
    print(f"Query execution time: {query_time:.4f} seconds")
    
    if not rows:
        print(f"No articles found")
        return []
    
    print(f"Found {len(rows):,} articles")
    
    # Group by poster and normalized collection name
    collections = defaultdict(list)
    
    if fuzzy_grouping:
        from .collection_matching import match_collections
        collection_items = match_collections(rows, fuzzy_similarity_threshold)
    else:
        for row in rows:
            poster = row['from_addr']
            normalized = normalize_subject_for_grouping(row['subject'], subject_like)
            collections[(poster, normalized)].append(row)
        collection_items = list(collections.items())

    print(f"Grouped into {len(collection_items)} collections")
    
    # Create NZB for each collection
    results = []
    fuzzy_batches = []
    filename_counts = defaultdict(int)
    skipped_count = 0
    
    for (poster, collection_name), articles in collection_items:
        # Group articles within this collection
        groups_dict, singles = group_rows_auto(articles)
        
        if not groups_dict and not singles:
            skipped_count += 1
            continue
        
        # Build NZB (this will print messages about skipped incomplete sets)
        nzb_xml = build_nzb_xml(groups_dict, singles, group, require_complete_sets)
        
        if not nzb_xml or '<file' not in nzb_xml:
            skipped_count += 1
            continue
        
        # Create filename from collection name and poster
        poster_clean = sanitize_filename(poster[:30])
        collection_clean = sanitize_filename(collection_name[:50]) if collection_name else "misc"
        
        base_filename = f"{poster_clean}_{collection_clean}"
        filename_counts[base_filename] += 1
        
        # Add counter if duplicate
        if filename_counts[base_filename] > 1:
            filename = f"{base_filename}_{filename_counts[base_filename]}.nzb"
        else:
            filename = f"{base_filename}.nzb"
        
        results.append((filename, nzb_xml))
        if fuzzy_grouping and min_articles_per_nzb > 1:
            from .collection_matching import _timestamp
            dates = [date for article in articles
                     if (date := _timestamp(article.get('date_utc'))) is not None]
            fuzzy_batches.append((poster, collection_name, dates, nzb_xml))
        if min_articles_per_nzb == 1:
            print(f"  Created: {filename} ({len(articles)} articles)")
    
    if skipped_count > 0:
        print(f"\nSkipped {skipped_count} collections (empty or all incomplete sets)")
    if fuzzy_grouping and min_articles_per_nzb > 1:
        results = _batch_fuzzy_nzbs(fuzzy_batches, group, min_articles_per_nzb)
    elif not fuzzy_grouping:
        results = _batch_nzbs(results, group, min_articles_per_nzb)
    print(f"\nTotal NZBs created: {len(results)}")
    return results
