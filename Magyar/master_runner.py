import os
import subprocess
import sys
import time
import sqlite3
import glob
from datetime import datetime

MODULES_FOLDER = "modules"
DATA_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

# Add root directory to path to import postgres_export
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT_DIR)
from postgres_export import push_to_postgres


def get_jobs_from_db(db_path):
    """Read jobs from a single SQLite database file and return as list of dicts."""
    jobs = []
    db_name = os.path.basename(db_path)

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Ensure country column exists
        cursor.execute("PRAGMA table_info(jobs)")
        columns = [info[1] for info in cursor.fetchall()]
        if 'country' not in columns:
            conn.execute("ALTER TABLE jobs ADD COLUMN country TEXT DEFAULT 'Hungary'")
            conn.commit()
        if 'company' not in columns:
            guessed = db_name.replace("_jobs.db", "").upper()
            conn.execute(f"ALTER TABLE jobs ADD COLUMN company TEXT DEFAULT '{guessed}'")
            conn.commit()

        scrape_date = datetime.now().strftime('%Y-%m-%d')
        cursor.execute("SELECT company, title, city, country, description, url FROM jobs")
        for row in cursor.fetchall():
            jobs.append({
                'company':         row[0],
                'job_title':       row[1],
                'city':            row[2],
                'country':         row[3],
                'job_description': row[4],
                'url':             row[5],
                'date':            scrape_date,
            })
        conn.close()
    except Exception as e:
        print(f"   ⚠️ Could not read {db_name}: {e}")

    return jobs


def export_new_jobs_to_postgres():
    """Read all SQLite dbs in data/ and push their contents to PostgreSQL."""
    if not os.path.exists(DATA_FOLDER):
        return
    db_files = glob.glob(os.path.join(DATA_FOLDER, "*.db"))
    all_jobs = []
    for db_path in db_files:
        all_jobs.extend(get_jobs_from_db(db_path))
    if all_jobs:
        push_to_postgres(all_jobs)
    else:
        print("   ℹ️  No jobs found in SQLite dbs to export.")


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
        print(f"▶️ Running: {module}...")

        try:
            result = subprocess.run(
                [sys.executable, module_path],
                capture_output=True, text=True
            )

            if result.returncode == 0:
                print(f"   ✅ Success!")
                success_count += 1
                # 🔴 Export immediately to PostgreSQL after each successful scraper
                print(f"   📤 Pushing to PostgreSQL...")
                export_new_jobs_to_postgres()
            else:
                print(f"   ❌ Failed. Error log:")
                print(f"      {result.stderr.strip()}")
                fail_count += 1

        except Exception as e:
            print(f"   ❌ Critical failure running {module}: {e}")
            fail_count += 1

        print("-" * 50)
        time.sleep(2)

    print("\n🏁 PIPELINE COMPLETE")
    print(f"📈 Successful Modules: {success_count}")
    print(f"📉 Failed Modules: {fail_count}")


if __name__ == "__main__":
    run_all_modules()