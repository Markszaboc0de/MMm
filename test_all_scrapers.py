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
import postgres_export

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

def main():
    print("🚀 Starting Specialized Scraper Health Check\n")
    
    # Initialize the results CSV
    with open(RESULTS_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["Module Category", "Module Name", "Status", "Company Scraped", "Job Title Scraped", "URL", "Time Taken (s)"])

    total_scrapers = 0
    successful_scrapers = 0
    failed_scrapers = []

    for target in TARGETS:
        if not os.path.exists(target["modules_path"]):
            continue
            
        modules = sorted([f for f in os.listdir(target["modules_path"]) if f.endswith('.py') and not f.startswith('__')])
        
        for module in modules:
            total_scrapers += 1
            module_path = os.path.join(target["modules_path"], module)
            print(f"\n▶️ Testing [{target['name']}] -> {module}")
            
            # Clean databases before starting
            clean_data_folder(target["data_path"])
            
            start_time = time.time()
            job_found = None
            
            # Launch scraper silently
            try:
                process = subprocess.Popen(
                    [sys.executable, module_path],
                    cwd=target["cwd"],
                    stdout=subprocess.DEVNULL,  # Hide normal output
                    stderr=subprocess.DEVNULL
                )
                
                # Monitor data folder
                while True:
                    elapsed = time.time() - start_time
                    
                    if elapsed > TIMEOUT_SECONDS:
                        print(f"   ⏰ TIMEOUT ({TIMEOUT_SECONDS}s). Killing process.")
                        process.terminate()
                        break
                        
                    # Did process crash/exit early without creating db?
                    if process.poll() is not None:
                        # Process finished. Final check on DB.
                        pass # proceed to check DB one last time below
                    
                    # Scan for sqlite files
                    db_files = glob.glob(os.path.join(target["data_path"], "*.db")) + glob.glob(os.path.join(target["data_path"], "*.sqlite"))
                    
                    for db_file in db_files:
                        job_found = extract_first_job(db_file)
                        if job_found:
                            break
                            
                    if job_found:
                        print(f"   🎯 BINGO! Got exactly 1 job. Snipping process at {elapsed:.1f}s!")
                        process.terminate()
                        break
                        
                    if process.poll() is not None:
                        print(f"   ❌ Process exited prematurely without saving any viable jobs (Code {process.returncode}).")
                        break
                        
                    time.sleep(1) # Poll interval
                    
            except Exception as e:
                print(f"   ❌ Error starting process: {e}")
                
            # Log results
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
                    # Push this single job to Postgres for verification testing
                    try:
                        postgres_export.push_to_postgres([job_found])
                    except:
                        pass
                    successful_scrapers += 1
                else:
                    writer.writerow([target["name"], module, "FAILED/TIMEOUT", "", "", "", f"{time.time() - start_time:.1f}"])
                    failed_scrapers.append(f"[{target['name']}] {module}")
                    
            # Ensure process is dead
            try:
                process.kill()
            except:
                pass
                
            time.sleep(2) # Cooldown before next scraper

    print("\n" + "="*50)
    print("🏁 HEALTH CHECK COMPLETE")
    print("="*50)
    print(f"✅ Successful: {successful_scrapers}/{total_scrapers}")
    if failed_scrapers:
        print("❌ Failed Scrapers:")
        for fail in failed_scrapers:
            print(f"  - {fail}")
    print(f"\n📂 A detailed report has been saved to: {RESULTS_CSV}")

if __name__ == "__main__":
    main()
