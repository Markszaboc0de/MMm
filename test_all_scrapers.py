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
        "data_path": os.path.join(BASE_DIR, "ATS scrapers", "data"),
        "cwd": os.path.join(BASE_DIR, "ATS scrapers", "Run")
    }
]

TIMEOUT_SECONDS = 300  # Increased to 5 minutes for slow VM performance
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
import re

def get_expected_db_filename(module_path):
    try:
        with open(module_path, 'r', encoding='utf-8') as f:
            content = f.read()
            
        # Extract COMPANY_NAME for f-string resolution
        company_match = re.search(r'COMPANY_NAME\s*=\s*["\']([^"\']+)["\']', content)
        company_name = company_match.group(1) if company_match else ""

        # Pattern 1: Regex for literal or f-string in DB_PATH
        m1 = re.search(r'DB_PATH\s*=\s*.*?["\']([^"\']+\.(?:db|sqlite))["\']', content)
        if m1: 
            raw_path = m1.group(1)
            # Resolve common f-string placeholders
            if "{COMPANY_NAME.lower()}" in raw_path:
                raw_path = raw_path.replace("{COMPANY_NAME.lower()}", company_name.lower())
            elif "{COMPANY_NAME}" in raw_path:
                raw_path = raw_path.replace("{COMPANY_NAME}", company_name)
            return os.path.basename(raw_path).lower()
        
        m2 = re.search(r'db_filename\s*=\s*["\'].*?([^/"\']+\.(?:db|sqlite))["\']', content)
        if m2: return os.path.basename(m2.group(1)).lower()

        m3 = re.search(r'sqlite3\.connect\(\s*["\']([^"\']+\.(?:db|sqlite))["\']', content)
        if m3: return os.path.basename(m3.group(1)).lower()
        
        m4 = re.search(r'DB_FILE\s*=\s*["\'].*?([^/"\']+\.(?:db|sqlite))["\']', content)
        if m4: return os.path.basename(m4.group(1)).lower()
    except:
        pass
    return None

def send_notification(successful, total, runtime_seconds):
    try:
        topic_url = "https://ntfy.sh/resumatch_scraper_alerts"
        
        minutes = int(runtime_seconds // 60)
        seconds = int(runtime_seconds % 60)
        time_str = f"{minutes}m {seconds}s" if minutes > 0 else f"{seconds}s"

        message = f"Scraper Health Check Complete!\n{successful}/{total} modules successful.\nTotal Runtime: {time_str}"
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
    
    # Safely extract the exact database filename from the python source code!
    expected_db = get_expected_db_filename(module_path)
    base_name = module.replace("module_", "").replace("scrape_", "").replace(".py", "").lower()
    
    try:
        env = os.environ.copy()
        # Ensure ATS scrapers can resolve `core.base_scraper` by adding their root to PYTHONPATH
        target_root = os.path.dirname(target["cwd"]) if "ATS" in target["cwd"] else target["cwd"]
        env["PYTHONPATH"] = f"{target_root}{os.pathsep}{BASE_DIR}{os.pathsep}{env.get('PYTHONPATH', '')}"

        process = subprocess.Popen(
            [sys.executable, module_path],
            cwd=target["cwd"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            text=True
        )
        
        stdout_data = []
        stderr_data = []
        last_output_time = [time.time()]
        
        # Start threads to read pipes without blocking
        def read_pipe(pipe, data_list):
            for line in pipe:
                data_list.append(line)
                last_output_time[0] = time.time()
                
        t1 = threading.Thread(target=read_pipe, args=(process.stdout, stdout_data))
        t2 = threading.Thread(target=read_pipe, args=(process.stderr, stderr_data))
        t1.start()
        t2.start()

        while True:
            elapsed_idle = time.time() - last_output_time[0]
            if elapsed_idle > TIMEOUT_SECONDS:
                print(f"   ⏰ IDLE TIMEOUT ({TIMEOUT_SECONDS}s of no output) for {module}. Killing process.")
                process.terminate()
                break
                
            db_files = glob.glob(os.path.join(target["data_path"], "*.db")) + glob.glob(os.path.join(target["data_path"], "*.sqlite"))
            
            for db_file in db_files:
                actual_name = os.path.basename(db_file).lower()
                # Thread Safety: Only read from a db file if its name exactly matches the expected DB_PATH
                if (expected_db and expected_db == actual_name) or (not expected_db and base_name in actual_name):
                    job_found = extract_first_job(db_file)
                    if job_found:
                        break
                        
            if job_found:
                print(f"   🎯 BINGO! Got 1 job for {module} at {time.time() - start_time:.1f}s!")
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
                # Log errors for debugging
                log_dir = os.path.join(BASE_DIR, "logs")
                os.makedirs(log_dir, exist_ok=True)
                with open(os.path.join(log_dir, f"{module}.log"), "w", encoding='utf-8') as log_file:
                    log_file.write("--- STDOUT ---\n")
                    log_file.writelines(stdout_data)
                    log_file.write("\n--- STDERR ---\n")
                    log_file.writelines(stderr_data)
                
                writer.writerow([target["name"], module, "FAILED/TIMEOUT", "", "", "", f"{time.time() - start_time:.1f}"])
                return False

def main():
    print("🚀 Starting Specialized Scraper Health Check (PARALLEL MODE)\n")
    start_time_all = time.time()
    
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
    
    # Execute 3 scrapers simultaneously to prevent CPU/RAM exhaustion on standard VMs
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
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

    total_time_all = time.time() - start_time_all
    print("\n" + "="*50)
    print("🏁 MULTI-THREADED HEALTH CHECK COMPLETE")
    print("="*50)
    print(f"✅ Successful: {successful_scrapers}/{total_scrapers}")
    if failed_scrapers:
        print("❌ Failed Scrapers:")
        for fail in failed_scrapers:
            print(f"  - {fail}")
    print(f"\n📂 A detailed report has been saved to: {RESULTS_CSV}")
    print(f"⏱️ Total Runtime: {total_time_all:.1f}s")
    
    send_notification(successful_scrapers, total_scrapers, total_time_all)

if __name__ == "__main__":
    main()
