"""Filename similarity matching without changing file/segment identities."""

import re
from collections import defaultdict
from datetime import datetime, timezone


def collection_name(subject):
    quoted = re.search(r'"([^"]+)"', subject)
    name = quoted.group(1) if quoted else subject
    name = re.sub(r'[(\[{]\d+/\d+[)\]}]', ' ', name)
    name = re.sub(r'\b(?:file|part)\s+\d+\s+of\s+\d+\b', ' ', name, flags=re.I)
    name = re.sub(r'\b\d+(?:\.\d+)?\s*(?:bytes?|[kmgt]i?b)\b', ' ', name, flags=re.I)
    name = re.sub(r'\byenc\b.*$', '', name, flags=re.I).strip()
    # Strip recovery-volume and archive-volume suffixes before punctuation.
    name = re.sub(r'\.vol\d+\+\d+\.par2$', '', name, flags=re.I)
    name = re.sub(r'(?:\.(?:par2?|rar|zip|7z|r\d{2,3}|nfo|sfv|txt|diz|mkv|mp4|avi|wmv|mov|mpg|mpeg|flv|webm|m4v|jpg|jpeg|png|gif|bmp|tiff?|webp|bin))+$', '', name, flags=re.I)
    name = re.sub(r'\.(?:part\d+|\d{3})$', '', name, flags=re.I)
    # Strip standalone posting markers before removing their numeric suffixes.
    name = re.sub(r'\b(?:par\d*|rar|yenc|vol\d+(?:\+\d+)?|part\d+)\b', ' ', name, flags=re.I)
    name = re.sub(r'\d+', '', name)
    return re.sub(r'[\W_]+', ' ', name.casefold()).strip()


def levenshtein(left, right):
    if len(left) < len(right):
        left, right = right, left
    previous = list(range(len(right) + 1))
    for i, a in enumerate(left, 1):
        current = [i]
        for j, b in enumerate(right, 1):
            current.append(min(current[-1] + 1, previous[j] + 1,
                               previous[j - 1] + (a != b)))
        previous = current
    return previous[-1]


def _timestamp(value):
    try:
        if not isinstance(value, str):
            return None
        date = datetime.fromisoformat(value.replace('Z', '+00:00'))
        return date.replace(tzinfo=date.tzinfo or timezone.utc).timestamp()
    except ValueError:
        return None


def match_collections(rows, fuzzy_similarity_threshold=80):
    """Match to fixed anchors at a configurable similarity percentage.

    Compare cleaned filenames within the same poster and 48 hours of a fixed
    anchor when dates are known. Original file identities are assembled first,
    so matching never combines segments from different filenames. Empty cleaned
    names stay separate.
    """
    from .nzb import normalize_subject_base

    if not 0 <= fuzzy_similarity_threshold <= 100:
        raise ValueError('fuzzy_similarity_threshold must be between 0 and 100')

    files = defaultdict(list)
    for row in rows:
        files[(row['from_addr'] or '', normalize_subject_base(row['subject'] or ''))].append(row)

    anchors = defaultdict(list)
    collections = {}
    for (poster, subject), articles in sorted(files.items()):
        name = collection_name(subject)
        dates = [date for row in articles
                 if (date := _timestamp(row.get('date_utc'))) is not None]
        posted = min(dates) if dates else None
        best = None
        best_score = -1
        for anchor, anchor_date, key in anchors[poster] if name else ():
            if posted is not None and anchor_date is not None and abs(posted - anchor_date) > 48 * 3600:
                continue
            if name == anchor:
                score = 100.0
            else:
                length = max(len(name), len(anchor))
                if abs(len(name) - len(anchor)) * 100 > length * (100 - fuzzy_similarity_threshold):
                    continue
                score = 100 * (length - levenshtein(name, anchor)) / length
            if score >= fuzzy_similarity_threshold and score > best_score:
                best, best_score = key, score
        if best is None:
            best = (poster, name or 'misc', len(collections))
            if name:
                anchors[poster].append((name, posted, best))
            collections[best] = []
        collections[best].extend(articles)
    return [((poster, name), articles)
            for (poster, name, _), articles in collections.items()]
