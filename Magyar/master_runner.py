import os
import subprocess
import sys
import time
import sqlite3
import glob
from datetime import datetime

MODULES_FOLDER = "modules"
# data/ folder is a sibling of the modules/ folder
DATA_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

# Add project root to path
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)
from postgres_export import push_to_postgres

# Max seconds a single scraper is allowed to run without printing
SCRAPER_TIMEOUT = 1200  # 20 minutes per module
ABSOLUTE_TIMEOUT = 14400 # 4 hours absolute max limit


def get_all_jobs_from_sqlite():
    """Read all jobs from all SQLite .db files in the data/ directory."""
    all_jobs = []
    seen_urls = set()
    if not os.path.exists(DATA_FOLDER):
        print(f"   ⚠️  data/ folder not found: {DATA_FOLDER}")
        return all_jobs

    db_files = glob.glob(os.path.join(DATA_FOLDER, "*.db"))
    if not db_files:
        print(f"   ⚠️  No .db files found in {DATA_FOLDER}")
        return all_jobs

    scrape_date = datetime.now().strftime('%Y-%m-%d')
    for db_path in db_files:
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(jobs)")
            cols = [r[1] for r in cursor.fetchall()]
            # Determine which column holds the job title
            title_col = 'title' if 'title' in cols else (cols[1] if len(cols) > 1 else None)
            if not title_col:
                conn.close()
                continue
                
            # Safely build query dynamically
            select_cols = []
            for col_name in ['company', title_col, 'city', 'country', 'description', 'url']:
                if col_name in cols:
                    select_cols.append(col_name)
                else:
                    select_cols.append("''")
                    
            query = f"SELECT {', '.join(select_cols)} FROM jobs"
            cursor.execute(query)
            
            for row in cursor.fetchall():
                url = row[5] or ''
                # Postgres throws error if the same batch contains duplicate ON CONFLICT keys
                if not url or url in seen_urls:
                    continue
                seen_urls.add(url)
                
                all_jobs.append({
                    'company':         row[0] or '',
                    'job_title':       row[1] or '',
                    'city':            row[2] or '',
                    'country':         row[3] or 'Hungary',
                    'job_description': row[4] or '',
                    'url':             url,
                    'date':            scrape_date,
                })
            conn.close()
        except Exception as e:
            print(f"   ⚠️  Error reading {os.path.basename(db_path)}: {e}")

    return all_jobs


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


def run_all_modules():
    print("🚀 Starting the Master Scraper Pipeline...\n")

    if not os.path.exists(MODULES_FOLDER):
        print(f"❌ Error: '{MODULES_FOLDER}' folder not found.")
        return

    modules = sorted(f for f in os.listdir(MODULES_FOLDER) if f.endswith('.py'))

    if not modules:
        print(f"⚠️ No scraper modules found in '{MODULES_FOLDER}/'.")
        return

    MAX_WORKERS = 4
    STAGGER_SECONDS = 60  # (Legacy)
    print(f"📊 Found {len(modules)} modules to execute. Beginning parallel run ({MAX_WORKERS} workers, {STAGGER_SECONDS}s stagger)...\n")
    print("=" * 50)

    import concurrent.futures
    import threading

    success_count = 0
    fail_count = 0
    db_lock = threading.Lock()
    count_lock = threading.Lock()
    launch_lock = threading.Lock()
    last_launch_time = [0.0]

    def process_module(module):
        nonlocal success_count, fail_count
        module_path = os.path.join(MODULES_FOLDER, module)
        
        with launch_lock:
            # Dynamic stagger based on CPU: Limit to max 60s delay so we never halt indefinitely
            for _ in range(6):
                cpu_usage = get_cpu_utilization()
                if cpu_usage > 196.0:
                    print(f"\n⏳ [{module}] CPU load high ({cpu_usage:.1f}%). Delaying launch 10s...", flush=True)
                    time.sleep(10)
                else:
                    break
                    
            # Minimal 2-second stagger to prevent instant I/O race conditions
            now = time.time()
            elapsed = now - last_launch_time[0]
            if elapsed < 2.0 and last_launch_time[0] > 0:
                time.sleep(2.0 - elapsed)
            last_launch_time[0] = time.time()
            
        print(f"\n▶️ Starting: {module}...", flush=True)

        # Stream output directly so errors are visible, with an IDLE timeout (sliding window)
        try:
            proc = subprocess.Popen(
                [sys.executable, module_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                start_new_session=True
            )
            
            start_time_proc = time.time()
            last_output_time = [time.time()]
            
            def read_stdout():
                for line in proc.stdout:
                    # Prefix the line with the module name to avoid interleaving confusion
                    print(f"[{module}] {line}", end='', flush=True)
                    last_output_time[0] = time.time()
                    
            t = threading.Thread(target=read_stdout)
            t.daemon = True
            t.start()
            
            timeout_expired = False
            while True:
                if proc.poll() is not None:
                    break
                    
                now = time.time()
                import signal
                
                # Absolute max timeout
                if now - start_time_proc > ABSOLUTE_TIMEOUT:
                    print(f"\n   ⏰ [{module}] ABSOLUTE TIMEOUT after {ABSOLUTE_TIMEOUT}s running — killing heavily hanging process.", flush=True)
                    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                    proc.wait() # Reap zombie
                    timeout_expired = True
                    break
                    
                # Idle timeout
                if now - last_output_time[0] > SCRAPER_TIMEOUT:
                    print(f"\n   ⏰ [{module}] IDLE TIMEOUT after {SCRAPER_TIMEOUT}s of zero output — killing process.", flush=True)
                    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                    proc.wait() # Reap zombie
                    timeout_expired = True
                    break
                time.sleep(1)
                
            t.join(timeout=2)

            with count_lock:
                if timeout_expired:
                    fail_count += 1
                elif proc.returncode == 0:
                    success_count += 1
                else:
                    print(f"\n   ❌ [{module}] Failed (exit code {proc.returncode}).", flush=True)
                    fail_count += 1

            if proc.returncode == 0 and not timeout_expired:
                with db_lock:
                    print(f"\n   ✅ [{module}] Success! Pushing to PostgreSQL...", flush=True)
                    jobs = get_all_jobs_from_sqlite()
                    if jobs:
                        push_to_postgres(jobs)
                        print(f"   📤 [{module}] Pushed {len(jobs)} total jobs to PostgreSQL.", flush=True)
                    else:
                        print(f"   ⚠️  [{module}] No jobs found in SQLite to push.", flush=True)
                        
        except Exception as e:
            with count_lock:
                print(f"\n   ❌ [{module}] Critical error: {e}", flush=True)
                fail_count += 1

    # Execute all modules using a constrained ThreadPool
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        executor.map(process_module, modules)

    print("\n" + "=" * 50, flush=True)
    time.sleep(1)

    print("\n🏁 PIPELINE COMPLETE")
    print(f"📈 Successful: {success_count}")
    print(f"📉 Failed/Timeout: {fail_count}")


if __name__ == "__main__":
    run_all_modules()