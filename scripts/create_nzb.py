"""Example: Create NZB files from stored database."""

from pathlib import Path
from nntp_lib import get_config, create_nzb_from_db, create_grouped_nzbs_from_db, sanitize_filename

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
    group_by_collection = config.getboolean('nzb', 'group_by_collection', fallback=False)
    
    Path(NZB_OUTPUT_PATH).mkdir(parents=True, exist_ok=True)
    
    for group in groups:
        group = group.strip()
        print(f"\nProcessing: {group}")
        
        db_path = ":memory:"
        
        if group_by_collection:
            # Create separate NZBs grouped by poster and collection
            nzb_list = create_grouped_nzbs_from_db(
                db_path=db_path,
                group=group,
                output_path=NZB_OUTPUT_PATH,
                subject_like=subject_like,
                from_like=from_like,
                not_subject=not_subject,
                not_from=not_from,
                require_complete_sets=require_complete
            )
            
            # Write all NZBs to disk
            for filename, nzb_xml in nzb_list:
                output_file = f"{NZB_OUTPUT_PATH}/{filename}"
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(nzb_xml)
        else:
            # Create single NZB with all matching articles
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
                filename_parts = []
                if subject_like:
                    filename_parts.append(sanitize_filename(subject_like[:30]))
                
                filename = '_'.join(filename_parts) + '.nzb' if filename_parts else 'output.nzb'
                output_file = f"{NZB_OUTPUT_PATH}/{filename}"
                
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(nzb_xml)
                
                print(f"Created: {output_file}")
            else:
                print(f"No results for {group}")

if __name__ == '__main__':
    main()
