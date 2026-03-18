import os
import subprocess
import sys
import time
import sqlite3
import glob
from datetime import datetime

MODULES_FOLDER = "modules"
DATA_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)
from postgres_export import push_to_postgres

SCRAPER_TIMEOUT = 180  # 3 minutes per module


def get_all_jobs_from_sqlite():
    all_jobs = []
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
            title_col = 'title' if 'title' in cols else (cols[1] if len(cols) > 1 else None)
            if not title_col:
                conn.close()
                continue
            cursor.execute(f"SELECT company, {title_col}, city, country, description, url FROM jobs")
            for row in cursor.fetchall():
                all_jobs.append({
                    'company':         row[0] or '',
                    'job_title':       row[1] or '',
                    'city':            row[2] or '',
                    'country':         row[3] or 'Hungary',
                    'job_description': row[4] or '',
                    'url':             row[5] or '',
                    'date':            scrape_date,
                })
            conn.close()
        except Exception as e:
            print(f"   ⚠️  Error reading {os.path.basename(db_path)}: {e}")

    return all_jobs


def run_all_modules():
    print("🚀 Starting the Master Scraper Pipeline...\n")

    if not os.path.exists(MODULES_FOLDER):
        print(f"❌ Error: '{MODULES_FOLDER}' folder not found.")
        return

    modules = sorted(f for f in os.listdir(MODULES_FOLDER) if f.endswith('.py'))

    if not modules:
        print(f"⚠️ No scraper modules found in '{MODULES_FOLDER}/'.")
        return

    print(f"📊 Found {len(modules)} modules to execute. Beginning run...\n")
    print("=" * 50)

    success_count = 0
    fail_count = 0

    for module in modules:
        module_path = os.path.join(MODULES_FOLDER, module)
        print(f"▶️ Running: {module}...", flush=True)

        try:
            result = subprocess.run(
                [sys.executable, module_path],
                timeout=SCRAPER_TIMEOUT,
            )

            if result.returncode == 0:
                print(f"   ✅ Success! Pushing to PostgreSQL...", flush=True)
                jobs = get_all_jobs_from_sqlite()
                if jobs:
                    push_to_postgres(jobs)
                    print(f"   📤 Pushed {len(jobs)} total jobs to PostgreSQL.", flush=True)
                else:
                    print(f"   ⚠️  No jobs found in SQLite to push.", flush=True)
                success_count += 1
            else:
                print(f"   ❌ Failed (exit code {result.returncode}).", flush=True)
                fail_count += 1

        except subprocess.TimeoutExpired:
            print(f"   ⏰ TIMEOUT after {SCRAPER_TIMEOUT}s — moving on.", flush=True)
            fail_count += 1
        except Exception as e:
            print(f"   ❌ Critical error: {e}", flush=True)
            fail_count += 1

        print("-" * 50, flush=True)
        time.sleep(1)

    print("\n🏁 PIPELINE COMPLETE")
    print(f"📈 Successful: {success_count}")
    print(f"📉 Failed/Timeout: {fail_count}")


if __name__ == "__main__":
    run_all_modules()