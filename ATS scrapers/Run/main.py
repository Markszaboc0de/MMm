import os
import sys
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

import runpy
import importlib.util
import sqlite3
import csv
from datetime import datetime

# Add root directory to path to import postgres_export
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from postgres_export import push_to_postgres

def find_scrapers():
    scrapers_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scrapers")
    if not os.path.exists(scrapers_dir):
        return {}
    
    scraper_files = {}
    for file in os.listdir(scrapers_dir):
        if file.endswith(".py") and not file.startswith("__"):
            # Use the filename without .py as the scraper name
            name = file[:-3]
            scraper_files[name] = os.path.join(scrapers_dir, file)
            
    return scraper_files

def run_scraper(scraper_name, scraper_path):
    print(f"\n{'='*50}")
    print(f"--- Running {scraper_name} ---")
    print(f"{'='*50}")
    
    # We need to add scrapers to sys.path so it can find things if it imports relatively
    scrapers_dir = os.path.dirname(scraper_path)
    if scrapers_dir not in sys.path:
        sys.path.insert(0, scrapers_dir)
        
    # Also add the root directory to sys.path so they can find 'core'
    root_dir = os.path.dirname(os.path.abspath(__file__))
    if root_dir not in sys.path:
        sys.path.insert(0, root_dir)
        
    # Change current working directory to scrapers_dir so relative paths inside scrapers work correctly
    os.chdir(scrapers_dir)
        
    try:
        runpy.run_path(scraper_path, run_name="__main__")
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"Error running {scraper_name}: {e}")

    # Change back
    os.chdir(root_dir)

def export_unified_data():
    """Reads all SQLite databases in the data folder and exports them to a unified CSV."""
    print(f"\n{'='*50}")
    print("--- Generating Unified Export ---")
    print(f"{'='*50}")
    
    root_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(root_dir, "data")
    
    if not os.path.exists(data_dir):
        print("No data directory found.")
        return
        
    unified_csv_path = os.path.join(data_dir, "unified_jobs.csv")
    
    # Track unique URLs to deduplicate
    seen_urls = set()
    all_jobs = []
    
    # Define the expected columns based on core.base_scraper
    columns = ["url", "title", "company", "location_raw", "city", "country", "description", "scraped_at"]
    
    db_found = False
    for file in os.listdir(data_dir):
        if file.endswith(".db"):
            db_found = True
            db_path = os.path.join(data_dir, file)
            print(f"   Reading from {file}...")
            
            try:
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                
                # Check if jobs table exists
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='jobs'")
                if not cursor.fetchone():
                    continue
                    
                # Fetch all jobs
                cursor.execute(f"SELECT {', '.join(columns)} FROM jobs")
                rows = cursor.fetchall()
                
                for row in rows:
                    url = row[0]
                    if url not in seen_urls:
                        seen_urls.add(url)
                        all_jobs.append({
                            'url': row[0],
                            'job_title': row[1],
                            'company': row[2],
                            'location_raw': row[3],
                            'city': row[4],
                            'country': row[5],
                            'job_description': row[6],
                            'date': row[7]
                        })
                
                conn.close()
            except Exception as e:
                print(f"   ❌ Error reading {file}: {e}")

    if not db_found:
        print("No database files found in the data directory.")
        return

    # Write unified CSV
    if not all_jobs:
        print("No jobs found in any databases.")
        return
        
    try:
        push_to_postgres(all_jobs)
    except Exception as e:
        print(f"❌ Error writing unified Postgres: {e}")


if __name__ == "__main__":
    scrapers = find_scrapers()
    args = sys.argv[1:]
    
    if len(args) == 0 or args[0].lower() == "all":
        print("Running ALL scrapers...")
        # Sort them to be deterministic
        for scraper_name in sorted(scrapers.keys()):
            run_scraper(scraper_name, scrapers[scraper_name])
            
        export_unified_data()
    else:
        target = args[0]
        
        if target not in scrapers:
            matches = [s for s in scrapers if target.lower() in s.lower()]
            if len(matches) == 1:
                target = matches[0]
            elif len(matches) > 1:
                print(f"Multiple scrapers match '{target}': {', '.join(matches)}")
                sys.exit(1)
                
        if target in scrapers:
            run_scraper(target, scrapers[target])
            export_unified_data()
        else:
            print(f"Scraper '{target}' not found. Available scrapers:")
            for s in sorted(scrapers.keys()):
                print(f"  - {s}")
