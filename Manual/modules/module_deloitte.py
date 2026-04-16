import sys as _sys
import os as _os
_sys.path.append(_os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))
from driver_setup import get_chrome_driver
from selenium.webdriver.common.by import By
import sqlite3
import os
import sys
import time
import re

# Force UTF-8 encoding for Windows terminals
sys.stdout.reconfigure(encoding='utf-8')

# ==========================================
# ⚙️ CONFIGURATION
# ==========================================
COMPANY_NAME = "Deloitte"
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Manual\data"
DB_PATH = os.path.join(DATA_FOLDER, "deloitte_jobs.db")


def init_db():
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)
        print(f"   📁 Created new data directory: {DATA_FOLDER}")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        url TEXT UNIQUE NOT NULL,
        title TEXT NOT NULL,
        company TEXT NOT NULL,
        location_raw TEXT,
        city TEXT,
        country TEXT,
        description TEXT,
        category TEXT,
        date_found TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    conn.commit()
    conn.close()


def create_driver():
    """Creates a high-speed, auto-updating headless-ready Chrome instance."""
    # Disable images for lightning-fast navigation
    return get_chrome_driver()


def run_scraper():
    init_db()
    print(
        f"🚀 Starting {COMPANY_NAME} Scraper (Infinite Offset Pagination Mode)...")

    driver = create_driver()
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        # --- PHASE 1: Infinite URL Offset Pagination ---
        target_jobs = []
        offset = 0
        records_per_page = 10  # Strictly 10 to match server limits!

        while True:
            # Construct the dynamic URL with the current offset
            page_url = f"https://apply.deloittece.com/en_US/careers/SearchJobs/?522=%5B2437%2C2438%5D&522_format=1484&listFilterMode=1&jobRecordsPerPage={records_per_page}&jobOffset={offset}"

            print(f"   📄 Scanning page (Offset: {offset})...")
            driver.get(page_url)
            time.sleep(1)  # Wait for page to render

            # Accept Cookies if present (only needed on the first hit)
            if offset == 0:
                try:
                    cookie_btn = driver.find_element(By.ID, "cookie-accept")
                    driver.execute_script("arguments[0].click();", cookie_btn)
                    time.sleep(0.5)
                except:
                    pass

            # Extract jobs from current page
            jobs_on_page = driver.execute_script("""
                let results = [];
                let links = document.querySelectorAll('a[href*="/careers/JobDetail/"]');
                links.forEach(a => {
                    let title = a.innerText.trim();
                    if (title && a.href) {
                        results.push({ title: title, url: a.href });
                    }
                });
                return results;
            """)

            # Exit loop if the page returns absolutely nothing
            if not jobs_on_page:
                print("   🏁 No more jobs found. Pagination complete.")
                break

            new_jobs_found = 0
            for job in jobs_on_page:
                if not any(j['url'] == job['url'] for j in target_jobs):
                    target_jobs.append(job)
                    new_jobs_found += 1

            print(
                f"      -> Found {len(jobs_on_page)} jobs on this page ({new_jobs_found} new).")

            # Exit loop if we're just seeing the exact same jobs again (site looped back)
            if new_jobs_found == 0:
                print("   🏁 No new jobs found on this page. Pagination complete.")
                break

            # Increase offset strictly by 10 for the next loop
            offset += records_per_page

        print(
            f"\n✅ Found {len(target_jobs)} total jobs across all pages. Starting deep extraction...")

        if not target_jobs:
            return

        # --- PHASE 2: Deep Extraction ---
        saved_count = 0

        for idx, job in enumerate(target_jobs, 1):
            cursor.execute("SELECT id FROM jobs WHERE url = ?", (job['url'],))
            if cursor.fetchone():
                print(
                    f"   [{idx}/{len(target_jobs)}] ⏩ Skipping (Already in DB): {job['title']}")
                continue

            print(f"   [{idx}/{len(target_jobs)}] Extracting: {job['title']}")
            driver.get(job['url'])
            time.sleep(0.5)

            # Upgraded JS DOM Walker tailored for precise City/Country extraction
            page_data = driver.execute_script("""
                function walk(el) {
                    let text = "";
                    if (!el) return "";
                    if (el.nodeType === 3) {
                        let val = el.nodeValue.replace(/\\s+/g, ' '); 
                        if (val !== ' ') text += val;
                    } else if (el.nodeType === 1) {
                        let tag = el.tagName.toUpperCase();
                        let cls = (el.className && typeof el.className === 'string') ? el.className.toLowerCase() : "";
                        
                        if (['SCRIPT','STYLE','NAV','FOOTER','BUTTON','SVG','IFRAME'].includes(tag)) return "";
                        if (cls.includes('share') || cls.includes('apply') || cls.includes('video')) return "";
                        if (tag === 'LI') text += "- ";
                        
                        for (let child of el.childNodes) text += walk(child);
                        
                        if (['H1','H2','H3','H4'].includes(tag)) text += "\\n\\n";
                        else if (['P','DIV','BR', 'ARTICLE'].includes(tag)) text += "\\n";
                        else if (tag === 'LI') text += "\\n";
                    }
                    return text;
                }

                // 1. Extract Description
                let fullDesc = "";
                let mainContent = document.querySelector('.section__content');
                if (mainContent) {
                    fullDesc = walk(mainContent).trim();
                }
                
                // 2. Extract Precise Location (City & Country)
                let city = "Unknown";
                let country = "Unknown";
                
                let fields = document.querySelectorAll('.article__content__view__field');
                fields.forEach(field => {
                    let labelEl = field.querySelector('.article__content__view__field__label');
                    let valueEl = field.querySelector('.article__content__view__field__value');
                    
                    if (labelEl && valueEl) {
                        let label = labelEl.innerText.trim().toLowerCase();
                        let val = valueEl.innerText.trim();
                        
                        if (label === 'city') city = val;
                        if (label === 'country') country = val;
                    }
                });
                
                return {
                    description: fullDesc || 'Description could not be loaded.',
                    city: city,
                    country: country
                };
            """)

            clean_desc = re.sub(r' +', ' ', page_data['description'])
            clean_desc = re.sub(r'\n[ \t]+\n', '\n\n', clean_desc)
            clean_desc = re.sub(r'\n{3,}', '\n\n', clean_desc).strip()

            city = page_data['city']
            country = page_data['country']
            raw_loc = f"{city} - {country}" if city != "Unknown" else "Unknown"

            try:
                cursor.execute('''
                    INSERT INTO jobs (url, title, company, location_raw, city, country, description, category)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (job['url'], job['title'], COMPANY_NAME, raw_loc, city, country, clean_desc, "Junior/Intern/Program"))

                if cursor.rowcount > 0:
                    saved_count += 1
            except Exception as e:
                print(f"      ⚠️ DB Error for {job['title']}: {e}")

            conn.commit()

        print(
            f"\n🏁 Deloitte scrape completed. {saved_count} new jobs added to the database.")

    except Exception as e:
        print(f"❌ Critical error during Deloitte scrape: {e}")

    finally:
        driver.quit()
        conn.close()


if __name__ == "__main__":
    run_scraper()
