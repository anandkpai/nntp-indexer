"""Utility functions for NNTP library."""

from configparser import ConfigParser
from email.utils import parsedate_to_datetime
import os

# Configuration constants
CONFIG_BASE_PATH = os.getenv('CONFIG_BASE_PATH', '/mnt/r/tmp/nzbindex')

def get_config() -> ConfigParser:
    """Load configuration from nzbindex.ini file."""
    config = ConfigParser()
    config_path = f"{CONFIG_BASE_PATH}/nzbindex.ini"
    config.read(config_path)
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

def sanitize_filename(s: str) -> str:
    """Make a string safe for use as a filename."""
    return "".join(c if c.isalnum() or c in (' ', '-', '_') else '_' for c in s)
