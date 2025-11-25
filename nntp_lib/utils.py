"""Utility functions for NNTP library."""

from configparser import ConfigParser
from email.utils import parsedate_to_datetime

def get_config(path: str = "config.ini") -> ConfigParser:
    """Load configuration from INI file."""
    config = ConfigParser()
    config.read(path)
    return config

def clean_text(s: str) -> str:
    """Remove invalid UTF-8 characters from string."""
    if not isinstance(s, str):
        return s
    return s.encode("utf-8", "ignore").decode("utf-8")

def to_iso(dt_str: str | None) -> str | None:
    """Convert date string to ISO format."""
    if not dt_str:
        return None
    try:
        return parsedate_to_datetime(dt_str).astimezone(tz=None).isoformat()
    except Exception:
        return None
