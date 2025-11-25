"""NNTP library for fetching and storing Usenet headers."""

__version__ = '1.0.0'

from .db import ensure_db, upsert_headers
from .fetch import fetch_headers_chunked, get_nntp_client
from .utils import get_config, clean_text, to_iso, sanitize_filename
from .nzb import create_nzb_from_db, build_nzb_xml

__all__ = [
    'ensure_db',
    'upsert_headers',
    'fetch_headers_chunked',
    'get_nntp_client',
    'get_config',
    'clean_text',
    'to_iso',
    'sanitize_filename',
    'create_nzb_from_db',
    'build_nzb_xml',
]
