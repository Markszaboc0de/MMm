import os
import sys
import glob
import time
import csv
import sqlite3
import subprocess
from datetime import datetime
import threading
import concurrent.futures
import re
import urllib.request

# Root directory of the project (parent of Check folder)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

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

TIMEOUT_SECONDS = 180  # 3 minutes maximum timeout for each website/scraper
RESULTS_CSV = os.path.join(BASE_DIR, "Check", "health_check_results.csv")

def send_notification(successful, total, runtime_seconds):
    try:
        topic_url = "https://ntfy.sh/resumatch_scraper_alerts"
        minutes = int(runtime_seconds // 60)
        seconds = int(runtime_seconds % 60)
        time_str = f"{minutes}m {seconds}s" if minutes > 0 else f"{seconds}s"
        
        status = "SUCCESS" if successful == total else "OBSTACLES DETECTED"
        message = f"Health Check Complete: {status}\n{successful}/{total} scrapers working.\nRuntime: {time_str}"
        
        req = urllib.request.Request(
            topic_url,
            data=message.encode('utf-8'),
            headers={
                "Title": "Scraper Health Check",
                "Tags": "stethoscope,robot"
            }
        )
        urllib.request.urlopen(req, timeout=5)
        print("📲 Push notification sent to your phone/browser!")
    except Exception as e:
        print(f"   ❌ Failed to send push notification: {e}")

def clean_data_folder(data_path):
    """Deletes all .db and .sqlite files in the targeted data directory."""
    if not os.path.exists(data_path):
        os.makedirs(data_path, exist_ok=True)
        return
    for f in glob.glob(os.path.join(data_path, "*.db")) + glob.glob(os.path.join(data_path, "*.sqlite")):
        try: os.remove(f)
        except: pass

def extract_first_job(db_path):
    """Safely attempts to read the first job from an SQLite database."""
    try:
        conn = sqlite3.connect(db_path, timeout=1)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='jobs'")
        if not cursor.fetchone():
            conn.close()
            return None
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
            
        cursor.execute(f"SELECT company, {title_col}, url FROM jobs LIMIT 1")
        row = cursor.fetchone()
        conn.close()
        
        if row: return True
    except:
        pass
    return None

def get_expected_db_filename(module_path):
    try:
        with open(module_path, 'r', encoding='utf-8') as f:
            content = f.read()
        company_match = re.search(r'COMPANY_NAME\s*=\s*["\']([^"\']+)["\']', content)
        company_name = company_match.group(1) if company_match else ""

        m1 = re.search(r'DB_PATH\s*=\s*.*?["\']([^"\']+\.(?:db|sqlite))["\']', content)
        if m1: 
            raw_path = m1.group(1)
            if "{COMPANY_NAME.lower()}" in raw_path: raw_path = raw_path.replace("{COMPANY_NAME.lower()}", company_name.lower())
            elif "{COMPANY_NAME}" in raw_path: raw_path = raw_path.replace("{COMPANY_NAME}", company_name)
            return os.path.basename(raw_path).lower()
        
        m2 = re.search(r'db_filename\s*=\s*["\'].*?([^/"\']+\.(?:db|sqlite))["\']', content)
        if m2: return os.path.basename(m2.group(1)).lower()

        m3 = re.search(r'sqlite3\.connect\(\s*["\']([^"\']+\.(?:db|sqlite))["\']', content)
        if m3: return os.path.basename(m3.group(1)).lower()
        
        m4 = re.search(r'DB_FILE\s*=\s*["\'].*?([^/"\']+\.(?:db|sqlite))["\']', content)
        if m4: return os.path.basename(m4.group(1)).lower()
    except: pass
    return None

def run_test(target, module, csv_lock):
    """Executes a single scraper test in an isolated thread context."""
    module_path = os.path.join(target["modules_path"], module)
    print(f"▶️ Testing [{target['name']}] -> {module}")
    
    start_time = time.time()
    job_found = False
    is_ats = target["name"] == "ATS"
    
    expected_db = get_expected_db_filename(module_path)
    base_name = module.replace("module_", "").replace("scrape_", "").replace(".py", "").lower()
    
    error_reason = ""
    
    try:
        env = os.environ.copy()
        target_root = os.path.dirname(target["cwd"]) if is_ats else target["cwd"]
        env["PYTHONPATH"] = f"{target_root}{os.pathsep}{BASE_DIR}{os.pathsep}{env.get('PYTHONPATH', '')}"
        
        if is_ats:
            env["HEALTH_CHECK_MODE"] = "1"

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
        
        def read_pipe(pipe, data_list):
            for line in pipe:
                data_list.append(line)
                last_output_time[0] = time.time()
                
        t1 = threading.Thread(target=read_pipe, args=(process.stdout, stdout_data))
        t2 = threading.Thread(target=read_pipe, args=(process.stderr, stderr_data))
        t1.start()
        t2.start()

        while True:
            elapsed_total = time.time() - start_time
            if elapsed_total > 180:
                print(f"   ⏰ TIMEOUT (3 minutes reached) for {module}. Killing process.")
                error_reason = "Timeout (> 180s)"
                process.terminate()
                break

            elapsed_idle = time.time() - last_output_time[0]
            if elapsed_idle > TIMEOUT_SECONDS:
                print(f"   ⏰ IDLE TIMEOUT ({TIMEOUT_SECONDS}s of no output) for {module}. Killing process.")
                error_reason = f"Idle Timeout (> {TIMEOUT_SECONDS}s)"
                process.terminate()
                break
                
            db_files = glob.glob(os.path.join(target["data_path"], "*.db")) + glob.glob(os.path.join(target["data_path"], "*.sqlite"))
            
            for db_file in db_files:
                actual_name = os.path.basename(db_file).lower()
                if (expected_db and expected_db == actual_name) or (not expected_db and base_name in actual_name):
                    if extract_first_job(db_file):
                        job_found = True
                        break
                        
            # For Magyar/Manual, forcefully terminate as soon as 1 job is found
            if not is_ats and job_found:
                print(f"   🎯 BINGO! Got 1 job for {module} at {time.time() - start_time:.1f}s!")
                process.terminate()
                break
                
            if process.poll() is not None:
                if is_ats and job_found:
                    print(f"   🎯 ATS {module} finished successfully!")
                elif process.poll() != 0:
                    print(f"   ❌ Process {module} exited with code {process.poll()}.")
                    err_lines = [line.strip() for line in stderr_data if line.strip()]
                    if err_lines:
                        # Grab the last non-empty line from stderr as error reason
                        error_reason = f"Exit {process.poll()}: {err_lines[-1][:200]}"
                    else:
                        error_reason = f"Exit Code {process.poll()}"
                else:
                    if not job_found:
                        print(f"   ❌ {module} finished normally but 0 jobs found.")
                        error_reason = "Finished normally, 0 jobs found"
                break
                
            time.sleep(1)
            
    except Exception as e:
        print(f"   ❌ Error starting process {module}: {e}")
        error_reason = f"Error: {str(e)[:100]}"
        
    try: process.kill()
    except: pass
        
    with csv_lock:
        with open(RESULTS_CSV, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if job_found:
                writer.writerow([target["name"], module, "SUCCESS", f"{time.time() - start_time:.1f}", ""])
                return True
            else:
                writer.writerow([target["name"], module, "FAILED", f"{time.time() - start_time:.1f}", error_reason])
                return False

def main():
    if not os.path.exists(os.path.dirname(RESULTS_CSV)):
        os.makedirs(os.path.dirname(RESULTS_CSV), exist_ok=True)

    print("🚀 Starting Specialized Scraper Health Check\n")
    start_time_all = time.time()
    
    with open(RESULTS_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["Module Category", "Module Name", "Status", "Time Taken (s)", "Error Log"])

    for target in TARGETS:
        clean_data_folder(target["data_path"])

    csv_lock = threading.Lock()
    tasks = []
    
    for target in TARGETS:
        if not os.path.exists(target["modules_path"]):
            continue
        modules = sorted([
            f for f in os.listdir(target["modules_path"]) 
            if f.endswith('.py') 
            and not f.startswith('__') 
            and "core" not in f.lower() 
            and "base" not in f.lower()
            and f != "extract!!.py"
        ])
        for module in modules:
            tasks.append((target, module))
            
    total_scrapers = len(tasks)
    successful_scrapers = 0
    failed_scrapers = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        futures = {executor.submit(run_test, t, m, csv_lock): (t, m) for t, m in tasks}  # type: ignore
        for future in concurrent.futures.as_completed(futures):
            t, m = futures[future]
            try:
                if future.result():
                    successful_scrapers += 1  # type: ignore
                else:
                    failed_scrapers.append(f"[{t['name']}] {m}")
            except Exception:
                failed_scrapers.append(f"[{t['name']}] {m}")

    total_time = time.time() - start_time_all
    print("\n" + "="*50)
    print("🏁 MULTI-THREADED HEALTH CHECK COMPLETE")
    print(f"✅ Successful: {successful_scrapers}/{total_scrapers}")
    if failed_scrapers:
        print("❌ Failed:")
        for fail in failed_scrapers:
            print(f"  - {fail}")
            
    send_notification(successful_scrapers, total_scrapers, total_time)
            
if __name__ == "__main__":
    main()
