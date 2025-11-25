"""Example: Create NZB files from stored database."""

from pathlib import Path
from nntp_lib import get_config, create_nzb_from_db

def sanitize_filename(s: str) -> str:
    """Make a string safe for use as a filename."""
    return "".join(c if c.isalnum() or c in (' ', '-', '_') else '_' for c in s)

def main():
    """Create NZB files based on config filters."""
    config = get_config()
    
    groups = config['groups']['names'].split(',')
    DB_BASE_PATH = config['db']['DB_BASE_PATH']
    NZB_OUTPUT_PATH = config.get('nzb', 'output_path', fallback=f'{DB_BASE_PATH}/nzbs')
    
    subject_like = config.get('filters', 'subject_like', fallback=None)
    from_like = config.get('filters', 'from_like', fallback=None)
    not_subject = config.get('filters', 'not_subject', fallback=None)
    not_from = config.get('filters', 'not_from', fallback=None)
    require_complete = config.getboolean('nzb', 'require_complete_sets', fallback=False)
    
    Path(NZB_OUTPUT_PATH).mkdir(parents=True, exist_ok=True)
    
    for group in groups:
        group = group.strip()
        print(f"\nProcessing: {group}")
        
        db_path = f"{DB_BASE_PATH}/{group}.sqlite"
        
        nzb_xml = create_nzb_from_db(
            db_path=db_path,
            group=group,
            subject_like=subject_like,
            from_like=from_like,
            not_subject=not_subject,
            not_from=not_from,
            require_complete_sets=require_complete
        )
        
        if nzb_xml:
            filename_parts = [group]
            if subject_like:
                filename_parts.append(sanitize_filename(subject_like[:30]))
            
            filename = '_'.join(filename_parts) + '.nzb'
            output_file = f"{NZB_OUTPUT_PATH}/{filename}"
            
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(nzb_xml)
            
            print(f"Created: {output_file}")
        else:
            print(f"No results for {group}")

if __name__ == '__main__':
    main()
