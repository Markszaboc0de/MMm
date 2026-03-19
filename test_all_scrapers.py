import os
import sys
import glob
import time
import csv
import sqlite3
import subprocess
from datetime import datetime

# Root directory of the project
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Define the folders to scan for scraper modules
TARGETS = [
    {
        "name": "Magyar",
        "modules_path": os.path.join(BASE_DIR, "Magyar", "modules"),
        "data_path": os.path.join(BASE_DIR, "Magyar", "data"),
        "cwd": os.path.join(BASE_DIR, "Magyar")
    },
    {
        "name": "Manual",
        "modules_path": os.path.join(BASE_DIR, "Manual", "modules"),
        "data_path": os.path.join(BASE_DIR, "Manual", "data"),
        "cwd": os.path.join(BASE_DIR, "Manual")
    },
    {
        "name": "ATS",
        "modules_path": os.path.join(BASE_DIR, "ATS scrapers", "scrapers"),
        "data_path": os.path.join(BASE_DIR, "ATS scrapers", "Run", "data"),
        "cwd": os.path.join(BASE_DIR, "ATS scrapers", "Run")
    }
]

TIMEOUT_SECONDS = 180  # 3 minutes max per scraper
RESULTS_CSV = os.path.join(BASE_DIR, "scraper_health_results.csv")

def clean_data_folder(data_path):
    """Deletes all .db and .sqlite files in the targeted data directory."""
    if not os.path.exists(data_path):
        os.makedirs(data_path, exist_ok=True)
        return
    
    for f in glob.glob(os.path.join(data_path, "*.db")) + glob.glob(os.path.join(data_path, "*.sqlite")):
        try:
            os.remove(f)
        except:
            pass

def extract_first_job(db_path):
    """Safely attempts to read the first job from an SQLite database."""
    try:
        conn = sqlite3.connect(db_path, timeout=1)
        cursor = conn.cursor()
        
        # Check if table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='jobs'")
        if not cursor.fetchone():
            conn.close()
            return None
            
        # Get column names to handle schema variations (title vs job_title)
        cursor.execute("PRAGMA table_info(jobs)")
        cols = [r[1] for r in cursor.fetchall()]
        
        if not cols:
            conn.close()
            return None
            
        title_col = 'title' if 'title' in cols else ('job_title' if 'job_title' in cols else None)
        desc_col = 'description' if 'description' in cols else ('job_description' if 'job_description' in cols else None)
        
        if not title_col or not desc_col:
            conn.close()
            return None
            
        cursor.execute(f"SELECT company, {title_col}, city, country, {desc_col}, url FROM jobs LIMIT 1")
        row = cursor.fetchone()
        conn.close()
        
        if row:
            return {
                'company':         row[0] or 'Unknown',
                'job_title':       row[1] or 'Unknown',
                'city':            row[2] or '',
                'country':         row[3] or 'Hungary',
                'job_description': row[4] or '',
                'url':             row[5] or '',
                'date':            datetime.now().strftime('%Y-%m-%d')
            }
    except Exception:
        pass
    
    return None

import threading
import concurrent.futures
import urllib.request

def send_notification(successful, total):
    try:
        topic_url = "https://ntfy.sh/resumatch_scraper_alerts"
        message = f"✅ Scraper Health Check Complete!\n{successful}/{total} modules successfully scraped a validation job."
        req = urllib.request.Request(
            topic_url,
            data=message.encode('utf-8'),
            headers={
                "Title": "Resumatch Tester",
                "Tags": "mag,robot"
            }
        )
        urllib.request.urlopen(req, timeout=5)
        print("📲 Push notification sent!")
    except Exception as e:
        pass

def run_test(target, module, csv_lock):
    """Executes a single scraper test in an isolated thread context."""
    module_path = os.path.join(target["modules_path"], module)
    print(f"▶️ Testing [{target['name']}] -> {module}")
    
    start_time = time.time()
    job_found = None
    
    # Heuristically determine what the db file might be named
    base_name = module.replace("module_", "").replace("scrape_", "").replace(".py", "").lower()
    
    try:
        process = subprocess.Popen(
            [sys.executable, module_path],
            cwd=target["cwd"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        
        while True:
            elapsed = time.time() - start_time
            if elapsed > TIMEOUT_SECONDS:
                print(f"   ⏰ TIMEOUT ({TIMEOUT_SECONDS}s) for {module}. Killing process.")
                process.terminate()
                break
                
            db_files = glob.glob(os.path.join(target["data_path"], "*.db")) + glob.glob(os.path.join(target["data_path"], "*.sqlite"))
            
            for db_file in db_files:
                # Thread Safety: Only read from a db file if its name roughly matches our module
                if base_name in os.path.basename(db_file).lower():
                    job_found = extract_first_job(db_file)
                    if job_found:
                        break
                        
            if job_found:
                print(f"   🎯 BINGO! Got 1 job for {module} at {elapsed:.1f}s!")
                process.terminate()
                break
                
            if process.poll() is not None:
                print(f"   ❌ Process {module} exited prematurely without finding jobs.")
                break
                
            time.sleep(1)
            
    except Exception as e:
        print(f"   ❌ Error starting process {module}: {e}")
        
    try:
        process.kill()
    except:
        pass
        
    # Log results thread-safely
    with csv_lock:
        with open(RESULTS_CSV, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if job_found:
                writer.writerow([
                    target["name"], 
                    module, 
                    "SUCCESS", 
                    job_found.get("company", ""), 
                    job_found.get("job_title", ""), 
                    job_found.get("url", ""), 
                    f"{time.time() - start_time:.1f}"
                ])
                return True
            else:
                writer.writerow([target["name"], module, "FAILED/TIMEOUT", "", "", "", f"{time.time() - start_time:.1f}"])
                return False

def main():
    print("🚀 Starting Specialized Scraper Health Check (PARALLEL MODE)\n")
    
    with open(RESULTS_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["Module Category", "Module Name", "Status", "Company Scraped", "Job Title Scraped", "URL", "Time Taken (s)"])

    total_scrapers = 0
    successful_scrapers = 0
    failed_scrapers = []
    
    # Pre-clean all data folders once
    for target in TARGETS:
        clean_data_folder(target["data_path"])

    csv_lock = threading.Lock()
    
    tasks = []
    for target in TARGETS:
        if not os.path.exists(target["modules_path"]):
            continue
        modules = sorted([f for f in os.listdir(target["modules_path"]) if f.endswith('.py') and not f.startswith('__')])
        for module in modules:
            tasks.append((target, module))
            
    total_scrapers = len(tasks)
    
    # Execute 5 scrapers simultaneously!
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(run_test, t, m, csv_lock): (t, m) for t, m in tasks}
        
        for future in concurrent.futures.as_completed(futures):
            t, m = futures[future]
            try:
                success = future.result()
                if success:
                    successful_scrapers += 1
                else:
                    failed_scrapers.append(f"[{t['name']}] {m}")
            except Exception as exc:
                print(f"   ❌ Thread crash for {m}: {exc}")
                failed_scrapers.append(f"[{t['name']}] {m}")

    print("\n" + "="*50)
    print("🏁 MULTI-THREADED HEALTH CHECK COMPLETE")
    print("="*50)
    print(f"✅ Successful: {successful_scrapers}/{total_scrapers}")
    if failed_scrapers:
        print("❌ Failed Scrapers:")
        for fail in failed_scrapers:
            print(f"  - {fail}")
    print(f"\n📂 A detailed report has been saved to: {RESULTS_CSV}")
    
    send_notification(successful_scrapers, total_scrapers)

if __name__ == "__main__":
    main()
