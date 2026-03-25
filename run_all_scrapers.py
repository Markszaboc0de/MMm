import os
import subprocess
import sys
import time

def run_script(script_path, cwd):
    print(f"--- Running {script_path} in {cwd} ---")
    try:
        # Run the script, passing the current Python executable
        subprocess.run([sys.executable, script_path], cwd=cwd, check=True)
        print(f"--- Successfully finished {script_path} ---\n")
    except subprocess.CalledProcessError as e:
        print(f"--- Error running {script_path} (Exit code: {e.returncode}) ---\n")

import urllib.request

def send_notification(runtime_seconds):
    try:
        # A unique topic name for your scraper notifications
        topic_url = "https://ntfy.sh/resumatch_scraper_alerts"
        
        minutes = int(runtime_seconds // 60)
        seconds = int(runtime_seconds % 60)
        time_str = f"{minutes}m {seconds}s" if minutes > 0 else f"{seconds}s"

        message = f"All structured scrapers have completed executing!\nTotal Runtime: {time_str}"
        
        req = urllib.request.Request(
            topic_url,
            data=message.encode('utf-8'),
            headers={
                "Title": "Resumatch Scraper",
                "Tags": "white_check_mark,robot"
            }
        )
        urllib.request.urlopen(req, timeout=5)
        print("📲 Push notification sent to your phone/browser!")
    except Exception as e:
        print(f"Failed to send push notification: {e}")

def clear_local_databases(base_dir):
    import glob
    data_dirs = [
        os.path.join(base_dir, "ATS scrapers", "data"),
        os.path.join(base_dir, "Magyar", "data"),
        os.path.join(base_dir, "Manual", "data")
    ]
    for d in data_dirs:
        if os.path.exists(d):
            targets = glob.glob(os.path.join(d, "*.db")) + glob.glob(os.path.join(d, "*.sqlite"))
            for f in targets:
                try:
                    os.remove(f)
                    print(f"🧹 Deleted old cache: {f}")
                except Exception as e:
                    pass

def main():
    # Base directory of this script (Magyar-Manual-main)
    base_dir = os.path.dirname(os.path.abspath(__file__))
    
    # 🧹 Clear PostgreSQL and local SQLite caches before starting so jobs aren't continuously duplicated
    from postgres_export import clear_postgres_table
    print("\n" + "="*50)
    print("🧹 PRE-FLIGHT: Wiping databases for a fresh run...")
    print("="*50)
    clear_postgres_table()
    clear_local_databases(base_dir)
    print("✅ Databases cleared!\n")
    
    # 💿 Auto disk cleanup — clears Chromium tmp, journal logs, old snap revisions
    try:
        import cleanup_disk
        cleanup_disk.main()
    except Exception as e:
        print(f"⚠️ Disk cleanup skipped: {e}")
    
    # Define the scripts to run and their desired working directories
    scripts_to_run = [
        {"script": "main.py", "cwd": os.path.join(base_dir, "ATS scrapers", "Run")},
        {"script": "master_runner.py", "cwd": os.path.join(base_dir, "Magyar")},
        {"script": "master_runner.py", "cwd": os.path.join(base_dir, "Manual")},
    ]

    start_time = time.time()
    for item in scripts_to_run:
        script_path = item["script"]
        cwd = item["cwd"]
        
        # Check if the directory and script exist before running
        full_script_path = os.path.join(cwd, script_path)
        if not os.path.exists(full_script_path):
            print(f"Error: Script not found at {full_script_path}")
            continue
            
        run_script(script_path, cwd)

    total_time = time.time() - start_time
    print(f"All structured scrapers have completed executing. (Took {total_time:.1f}s)")
    send_notification(total_time)

if __name__ == "__main__":
    main()
