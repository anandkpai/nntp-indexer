"""Utility functions for NNTP library."""

from configparser import ConfigParser
from email.utils import parsedate_to_datetime
import os
import re
import xml.etree.ElementTree as ET
from pathlib import Path

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

def normalize_subject_for_grouping(subject: str, search_term: str = None) -> str:
    """Normalize subject for grouping - aggressive normalization."""
    s = subject
    
    # Remove [nnn/nnn] and (nnn/nnn) file number patterns
    s = re.sub(r'\s*[\[\(]\d+/\d+[\]\)]\s*', ' ', s)
    
    # Remove numbers in brackets like [000], [001], etc.
    s = re.sub(r'\s*\[\d+\]\s*', ' ', s)
    s = re.sub(r'\s*\(\d+\)\s*', ' ', s)
    
    # Remove quoted filenames entirely (e.g., "filename.jpg")
    s = re.sub(r'"[^"]*"', '', s)
    
    # Remove file extensions (before splitting)
    s = re.sub(r'\.(jpg|jpeg|png|gif|bmp|tif|tiff|rar|zip|r\d+|par2?|nfo|sfv|txt|diz|mkv|avi|mp4|wmv|mov|mpg|mpeg|flv|webm|m4v)(\s|$)', ' ', s, flags=re.IGNORECASE)
    
    # Remove size indicators like "308.31 kB"
    s = re.sub(r'\d+\.?\d*\s*(kb|mb|gb|bytes?)\b', '', s, flags=re.IGNORECASE)
    
    # Split by " - " or " . " and take only the first part (the collection name)
    s = re.split(r'\s+[-\.]\s+', s, maxsplit=1)[0].strip()
    
    # Remove search term if provided
    if search_term:
        # Split by % and remove each term
        for term in search_term.split('%'):
            term = term.strip()
            if term:
                s = re.sub(re.escape(term), '', s, flags=re.IGNORECASE)
    
    # Remove quotes
    s = re.sub(r'["\']', '', s)
    
    # Remove yEnc and similar markers
    s = re.sub(r'\s*yEnc\s*', ' ', s, flags=re.IGNORECASE)
    
    # Remove "File X of Y" patterns
    s = re.sub(r'\s*-?\s*File\s+\d+\s+of\s+\d+\s*-?\s*', ' ', s, flags=re.IGNORECASE)
    
    # Remove special characters: &, -, \, /, etc.
    s = re.sub(r'[&\-\\/,.:;!?(){}[\]]', ' ', s)
    
    # Remove underscores from the last 10 characters
    if len(s) > 10:
        s = s[:-10] + s[-10:].replace('_', ' ')
    else:
        s = s.replace('_', ' ')
    
    # Remove trailing numbers and spaces from the end
    s = re.sub(r'[\s\d_]+$', '', s)
    
    # Collapse whitespace and remove leading/trailing spaces
    s = re.sub(r'\s+', ' ', s).strip()
    
    # Truncate to 100 characters
    s = s[:100].strip()
    
    return s

def split_nzb(input_nzb: str, output_dir: str) -> int:
    """Split an NZB file into individual files, one per <file> element.
    
    Returns:
        Number of files created
    """
    tree = ET.parse(input_nzb)
    root = tree.getroot()
    
    # Find all <file> elements
    namespace = {'nzb': 'http://www.newzbin.com/DTD/2003/nzb'}
    files = root.findall('nzb:file', namespace)
    
    if not files:
        return 0
    
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    for idx, file_elem in enumerate(files, 1):
        # Create a new NZB with just this one file
        new_root = ET.Element("nzb", xmlns="http://www.newzbin.com/DTD/2003/nzb")
        new_root.append(file_elem)
        
        # Get subject for filename
        subject = file_elem.get('subject', f'file_{idx}')
        # Sanitize filename
        safe_name = "".join(c for c in subject[:80] if c.isalnum() or c in (' ', '_', '-')).strip()
        safe_name = safe_name or f'file_{idx}'
        
        output_file = output_path / f"{idx:05d}_{safe_name}.nzb"
        
        # Write to file
        tree = ET.ElementTree(new_root)
        tree.write(output_file, encoding='unicode', xml_declaration=True)
    
    return len(files)
