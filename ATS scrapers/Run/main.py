import os
import sys
import subprocess
import threading
import time

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

import importlib.util
import sqlite3
import csv
from datetime import datetime

# Add root directory to path to import postgres_export
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from postgres_export import push_to_postgres

SCRAPER_IDLE_TIMEOUT = 1200  # 20 minutes allowed for silent heavy processing
ABSOLUTE_TIMEOUT = 10800     # 3 hours absolute hard kill limit for massive ATS runs

def find_scrapers():
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    scrapers_dir = os.path.join(root_dir, "scrapers")
    if not os.path.exists(scrapers_dir):
        print(f"❌ Error: Scrapers directory not found at {scrapers_dir}")
        return {}
    
    scraper_files = {}
    for file in os.listdir(scrapers_dir):
        if file.endswith(".py") and not file.startswith("__"):
            # Use the filename without .py as the scraper name
            name = file[:-3]
            scraper_files[name] = os.path.join(scrapers_dir, file)
            
    return scraper_files

def run_scraper(scraper_name, scraper_path):
    print(f"\n▶️ Starting: {scraper_name}...", flush=True)
    
    scrapers_dir = os.path.dirname(scraper_path)
    root_dir = os.path.dirname(os.path.abspath(__file__))
    
    # We must pass the correct PYTHONPATH so the subprocess can resolve imports natively
    env = os.environ.copy()
    existing_pythonpath = env.get("PYTHONPATH", "")
    new_paths = f"{scrapers_dir}:{root_dir}"
    env["PYTHONPATH"] = f"{new_paths}:{existing_pythonpath}" if existing_pythonpath else new_paths

    try:
        proc = subprocess.Popen(
            [sys.executable, scraper_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            cwd=scrapers_dir,
            env=env
        )
        
        start_time_proc = time.time()
        last_output_time = [time.time()]
        
        def read_stdout():
            for line in proc.stdout:
                # Prefix the line with the module name to avoid interleaving confusion
                print(f"[{scraper_name}] {line}", end='', flush=True)
                last_output_time[0] = time.time()
                
        t = threading.Thread(target=read_stdout)
        t.daemon = True
        t.start()
        
        timeout_expired = False
        while True:
            if proc.poll() is not None:
                break
                
            now = time.time()
            if now - start_time_proc > ABSOLUTE_TIMEOUT:
                print(f"\n   ⏰ [{scraper_name}] ABSOLUTE TIMEOUT: Sequence halted after {ABSOLUTE_TIMEOUT}s max limit. Killing infinite loop process.", flush=True)
                proc.kill()
                proc.wait() # Reap zombie
                timeout_expired = True
                break
                
            if now - last_output_time[0] > SCRAPER_IDLE_TIMEOUT:
                print(f"\n   ⏰ [{scraper_name}] IDLE TIMEOUT: Sequence halted after {SCRAPER_IDLE_TIMEOUT}s of zero output. Killing hung process.", flush=True)
                proc.kill()
                proc.wait() # Reap zombie
                timeout_expired = True
                break
            time.sleep(1)
            
        t.join(timeout=2)
        
        if timeout_expired:
            print(f"\n❌ [{scraper_name}] was forcefully terminated due to hanging.")
        elif proc.returncode != 0:
            print(f"\n❌ [{scraper_name}] crashed with exit code {proc.returncode}.")
        else:
            print(f"\n✅ [{scraper_name}] finished successfully.")
            
    except Exception as e:
        print(f"\n❌ Error running {scraper_name}: {e}")

def export_unified_data():
    """Reads all SQLite databases in the data folder and exports them to a unified CSV."""
    print(f"\n{'='*50}")
    print("--- [ATS SYSTEM] Generating Unified Postgres Export ---")
    print(f"{'='*50}")
    
    root_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(os.path.dirname(root_dir), "data")
    
    print(f"📂 Searching for ATS databases in: {data_dir}")
    if not os.path.exists(data_dir):
        print(f"❌ No data directory found at {data_dir}.")
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
            print(f"   🔍 Reading payload from {file}...")
            
            try:
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                
                # Check if jobs table exists
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='jobs'")
                if not cursor.fetchone():
                    print(f"      ⚠️ No 'jobs' table found in {file}.")
                    continue
                    
                # Fetch all jobs
                cursor.execute(f"SELECT {', '.join(columns)} FROM jobs")
                rows = cursor.fetchall()
                print(f"      ✅ Extracted {len(rows)} raw rows from {file}.")
                
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
                print(f"   ❌ FATAL SQLite Error extracting {file}: {type(e).__name__} -> {e}")

    if not db_found:
        print("❌ No database files found in the data directory.")
        return

    print(f"\n📊 Total deduplicated ATS jobs ready for PostgreSQL sync: {len(all_jobs)}")

    # Write unified Postgres
    if not all_jobs:
        print("⚠️ No jobs found to export.")
        return
        
    print(f"🚀 [ATS SYSTEM] Delegating {len(all_jobs)} jobs to postgres_export.py...")
    try:
        push_to_postgres(all_jobs)
        print("✅ Postgres export fully completed.")
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"❌ FATAL Error writing unified Postgres: {e}")


def get_cpu_utilization():
    """Returns total CPU utilization as a percentage (e.g., 100.0 means 1 core fully loaded)."""
    try:
        import psutil
        return psutil.cpu_percent(interval=0.1) * psutil.cpu_count()
    except Exception:
        pass
    try:
        import subprocess
        output = subprocess.check_output(['ps', '-A', '-o', '%cpu']).decode('utf-8')
        total = 0.0
        for line in output.splitlines()[1:]:
            line = line.strip()
            if line:
                try:
                    total += float(line)
                except ValueError:
                    pass
        return total
    except Exception:
        return 0.0


if __name__ == "__main__":
    scrapers = find_scrapers()
    args = sys.argv[1:]
    
    if len(args) == 0 or args[0].lower() == "all":
        print("Running ALL ATS scrapers in parallel (10 workers, 60s stagger)...")
        import concurrent.futures
        MAX_WORKERS = 10
        db_lock = threading.Lock()
        
        launch_lock = threading.Lock()
        last_launch_time = [0.0]
        
        def run_and_export(scraper_name):
            scraper_path = scrapers[scraper_name]
            
            with launch_lock:
                # Dynamic stagger based on CPU: Only slow down if CPU > 196%
                while True:
                    cpu_usage = get_cpu_utilization()
                    if cpu_usage > 194.0:
                        print(f"\n⏳ [{scraper_name}] CPU load high ({cpu_usage:.1f}%). Delaying launch 10s...", flush=True)
                        time.sleep(10)
                    else:
                        break
                        
                # Minimal 2-second stagger to prevent instant I/O race conditions
                now = time.time()
                elapsed = now - last_launch_time[0]
                if elapsed < 2.0 and last_launch_time[0] > 0:
                    time.sleep(2.0 - elapsed)
                last_launch_time[0] = time.time()
                
            run_scraper(scraper_name, scraper_path)
            
            with db_lock:
                print(f"\n🔄 [{scraper_name}] Executing intermediate pipeline push...", flush=True)
                export_unified_data()
                
        # Execute concurrently
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            executor.map(run_and_export, sorted(scrapers.keys()))
            
    elif args[0].lower() == "export":
        print("Manual export requested. Skipping scraping...")
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
