from nntp_lib.utils import get_config
from nntp_lib.fetch import get_nntp_client
from email.utils import parsedate_to_datetime
from datetime import datetime, timedelta, timezone
import time

def get_article_date(nntp_client, group: str, artnum: int) -> datetime | None:
    """Fetch a single article's date by article number."""
    try:
        # Use xover to get overview data for a single article
        resp, overviews = nntp_client.over((artnum, artnum))
        if not overviews:
            return None
        
        # overviews is a list of (artnum, overview_dict) tuples
        _, ov = overviews[0]
        date_str = ov.get('date') or ov.get('Date')
        if not date_str:
            return None
        
        return parsedate_to_datetime(date_str).astimezone(timezone.utc)
    except Exception as e:
        print(f"Error fetching article {artnum}: {e}")
        return None

def days_old(dt: datetime) -> float:
    """Calculate how many days old a datetime is from now."""
    now = datetime.now(timezone.utc)
    return (now - dt).total_seconds() / 86400

def binary_search_date_boundary(nntp_client, group: str, low: int, high: int, 
                                target_days: int, find_lower: bool = True) -> int:
    """
    Binary search to find article number closest to target_days old.
    
    Args:
        nntp_client: NNTP connection
        group: newsgroup name
        low: lowest article number
        high: highest article number
        target_days: target age in days
        find_lower: if True, find article just OLDER than target_days
                   if False, find article just YOUNGER than target_days
    
    Returns:
        Article number closest to the boundary
    """
    result = low if find_lower else high
    
    while low <= high:
        mid = (low + high) // 2
        dt = get_article_date(nntp_client, group, mid)
        
        if dt is None:
            # Article not found, try nearby
            if find_lower:
                high = mid - 1
            else:
                low = mid + 1
            continue
        
        age = days_old(dt)
        print(f"  Article {mid}: {age:.1f} days old ({dt.date()})")
        
        if find_lower:
            # Looking for articles >= target_days (older)
            if age >= target_days:
                result = mid
                low = mid + 1  # Search for even older
            else:
                high = mid - 1
        else:
            # Looking for articles <= target_days (younger)
            if age <= target_days:
                result = mid
                high = mid - 1  # Search for even younger
            else:
                low = mid + 1
        
        time.sleep(0.1)  # Be nice to the server
    
    return result

def find_article_range_by_dates(group: str, min_days: int, max_days: int) -> tuple[int, int] | None:
    """
    Find article number range for articles between min_days and max_days old.
    
    Args:
        group: newsgroup name
        min_days: minimum age in days (lower bound)
        max_days: maximum age in days (upper bound)
    
    Returns:
        Tuple of (lower_artnum, upper_artnum) or None if range not found
    """
    config = get_config()
    nntp_client = get_nntp_client(config)
    
    try:
        # Get group info
        resp, count, first, last, name = nntp_client.group(group)
        
        # Check if date range is valid
        newest_date = get_article_date(nntp_client, group, last)
        if newest_date and days_old(newest_date) > max_days:
            return None
        
        oldest_date = get_article_date(nntp_client, group, first)
        if oldest_date and days_old(oldest_date) < min_days:
            return None
        
        # Binary search for bounds
        lower_artnum = binary_search_date_boundary(
            nntp_client, group, first, last, min_days, find_lower=True
        )
        
        upper_artnum = binary_search_date_boundary(
            nntp_client, group, first, last, max_days, find_lower=False
        )
        
        # Get the actual ages
        lower_date = get_article_date(nntp_client, group, lower_artnum)
        upper_date = get_article_date(nntp_client, group, upper_artnum)
        
        lower_age = days_old(lower_date) if lower_date else None
        upper_age = days_old(upper_date) if upper_date else None
        
        return (lower_artnum, upper_artnum, lower_age, upper_age)

        
    finally:
        nntp_client.quit()

if __name__ == "__main__":
    config = get_config()
    
    # Read group from config
    group = config['groups']['names'].split(',')[0]   
    
    # Read date ranges from [filters] section
    min_days = config.getint('filters', 'min_days', fallback=1000)
    max_days = config.getint('filters', 'max_days', fallback=1025)
    
    print(f"Reading configuration:")
    print(f"  Group: {group}")
    print(f"  Date range: {min_days} to {max_days} days old")
    
    result = find_article_range_by_dates(group, min_days, max_days)
    
    if result:
        lower_artnum, upper_artnum , lower_age, upper_age = result
        print(f"\n{'='*60}")
        print(f"RESULTS for {group}:")
        print(f"  Lower bound article: {lower_artnum} (>= {lower_age} days)")
        print(f"  Upper bound article: {upper_artnum} (<= {upper_age} days)")
        print(f"  Article range to fetch: {lower_artnum} to {upper_artnum}")
        print(f"  Estimated articles: {upper_artnum - lower_artnum + 1}")
        print(f"{'='*60}")
    else:
        print(f"\nERROR: Could not find articles in the specified date range for {group}")