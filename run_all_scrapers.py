import os
import subprocess
import sys

def run_script(script_path, cwd):
    print(f"--- Running {script_path} in {cwd} ---")
    try:
        # Run the script, passing the current Python executable
        subprocess.run([sys.executable, script_path], cwd=cwd, check=True)
        print(f"--- Successfully finished {script_path} ---\n")
    except subprocess.CalledProcessError as e:
        print(f"--- Error running {script_path} (Exit code: {e.returncode}) ---\n")

import urllib.request

def send_notification():
    try:
        # A unique topic name for your scraper notifications
        topic_url = "https://ntfy.sh/resumatch_scraper_alerts"
        
        req = urllib.request.Request(
            topic_url,
            data="✅ All structured scrapers have completed executing!".encode('utf-8'),
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

    for item in scripts_to_run:
        script_path = item["script"]
        cwd = item["cwd"]
        
        # Check if the directory and script exist before running
        full_script_path = os.path.join(cwd, script_path)
        if not os.path.exists(full_script_path):
            print(f"Error: Script not found at {full_script_path}")
            continue
            
        run_script(script_path, cwd)

    print("All structured scrapers have completed executing.")
    send_notification()

if __name__ == "__main__":
    main()
