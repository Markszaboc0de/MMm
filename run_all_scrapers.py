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
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def get_job_count():
    try:
        conn = psycopg2.connect(
            host=os.getenv("PG_HOST", "localhost"),
            port=os.getenv("PG_PORT", "5432"),
            dbname=os.getenv("PG_DATABASE", "raw_db"),
            user=os.getenv("PG_USER", "postgres"),
            password=os.getenv("PG_PASSWORD")
        )
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM scraped_jobs")
        count = cursor.fetchone()[0]
        conn.close()
        return count
    except Exception as e:
        print(f"Failed to get job count: {e}")
        return None

def send_notification(runtime_seconds, job_count):
    try:
        # A unique topic name for your scraper notifications
        topic_url = "https://ntfy.sh/resumatch_scraper_alerts"
        
        minutes = int(runtime_seconds // 60)
        seconds = int(runtime_seconds % 60)
        time_str = f"{minutes}m {seconds}s" if minutes > 0 else f"{seconds}s"

        if job_count is not None:
            message = f"✅ All structured scrapers have finished executing!\n📊 Total Jobs in Database: {job_count}\n⏱️ Total Runtime: {time_str}"
        else:
            message = f"✅ All structured scrapers have finished executing!\n⏱️ Total Runtime: {time_str}"
            
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

def main():
    # Base directory of this script (Magyar-Manual-main)
    base_dir = os.path.dirname(os.path.abspath(__file__))
    
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
    final_job_count = get_job_count()
    print(f"All structured scrapers have completed executing. (Took {total_time:.1f}s)")
    send_notification(total_time, final_job_count)

if __name__ == "__main__":
    main()
