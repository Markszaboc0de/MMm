from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import sqlite3
import os
import sys
import time
import re

sys.stdout.reconfigure(encoding='utf-8')

# --- CONFIGURATION ---
COMPANY_NAME = "Continental"
BASE_URL = "https://jobs.continental.com/en/#/?entryLevel_stringS=c4a378c8-234a-473b-bf18-1c139155575f,2ca3c248-1ffe-482c-b94a-1da19f0c857d,5e8bd685-42a9-4a67-af8c-3d4aed822c76,0131ce3c-8b5b-4b63-bbb6-24d80fb3d76e,8bbd3b6d-969a-4b6f-ab96-53be00ff4681,5d63c039-eb80-4f3c-b55e-bf71f84cae68,ef2d950f-3f99-4019-9d2c-868558a3688e"
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Manual\data"
DB_PATH = os.path.join(DATA_FOLDER, "continental_jobs.db")


def init_db():
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)
    conn = sqlite3.connect(DB_PATH)

    conn.execute('''CREATE TABLE IF NOT EXISTS jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT, url TEXT UNIQUE, title TEXT, 
        company TEXT, location_raw TEXT, city TEXT, country TEXT, description TEXT, category TEXT,
        date_found TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')

    conn.commit()
    conn.close()


def run_scraper():
    init_db()
    print(
        f"🚀 Starting {COMPANY_NAME} Scraper (Angular SPA & Dynamic Location Parse)...")

    options = Options()
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.binary_location = "/usr/bin/chromium-browser"
    _service = Service(executable_path="/usr/bin/chromedriver")
    driver = webdriver.Chrome(service=_service, options=options)

    job_links = []
    unique_urls = set()

    try:
        # --- PHASE 1: GATHERING LINKS ---
        print("📂 Opening Continental career site...")
        driver.get(BASE_URL)
        # Wait extra time for the Angular SPA and jobs to fully render
        time.sleep(8)

        # Handle Cookie Banner (if present)
        try:
            cookie_btn = driver.find_element(
                By.XPATH, "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'accept') or contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'allow') or contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'agree')]")
            driver.execute_script("arguments[0].click();", cookie_btn)
            time.sleep(1)
        except:
            pass

        page_num = 1

        while True:

            print(f"📄 Scraping data from page {page_num}...")

            try:
                WebDriverWait(driver, 15).until(EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "a.c-jobs-list__link")))
            except:
                print("⚠️ No job cards found on this page.")
                break

            # Extract links and raw locations via JS
            jobs_on_page = driver.execute_script("""
                let results = [];
                let links = document.querySelectorAll('a.c-jobs-list__link');
                
                links.forEach(a => {
                    if (a.href) {
                        let loc = "N/A";
                        // Find the title (excluding the Job ID span if possible)
                        let titleEl = a.querySelector('span.d-block:not(.c-jobs-list__jobid)');
                        let title = titleEl ? titleEl.innerText.trim() : a.innerText.split('Job ID')[0].trim();

                        // Traverse up to find the container holding the location marker
                        let parent = a.parentElement;
                        for(let i=0; i<5; i++) {
                            if(parent) {
                                // Search for the column that contains the 'marker' SVG icon
                                let markerCol = Array.from(parent.querySelectorAll('.c-jobs-list__col')).find(col => col.innerHTML.includes('marker'));
                                if (markerCol) {
                                    loc = markerCol.innerText.trim();
                                    break;
                                }
                                parent = parent.parentElement;
                            }
                        }

                        results.push({
                            url: a.href,
                            title: title,
                            location_raw: loc,
                            category: 'Engineering / Manufacturing' // Default based on context
                        });
                    }
                });
                return results;
            """)

            if not jobs_on_page:
                break

            for job in jobs_on_page:
                if job['url'] not in unique_urls:
                    unique_urls.add(job['url'])
                    job_links.append(job)

            # PAGINATION: Find the 'Next' button and click it via JS
            has_next = driver.execute_script("""
                let nextBtn = document.querySelector('li.c-pagination__item--next');
                // Check if it exists and is not disabled
                if (nextBtn && !nextBtn.classList.contains('c-pagination__item--disabled') && !nextBtn.classList.contains('disabled')) {
                    nextBtn.click();
                    return true;
                }
                return false;
            """)

            if has_next:
                page_num += 1
                # Wait for the Angular router to load the next set of jobs via AJAX
                time.sleep(4)
            else:
                print(
                    "🏁 No 'Next' button found or it is disabled. Reached the end of the list.")
                break

        print(
            f"✅ Identified {len(job_links)} unique jobs. Starting deep scrape...")

        if not job_links:
            return

        # --- PHASE 2: DEEP SCRAPING DESCRIPTIONS ---
        conn = sqlite3.connect(DB_PATH)
        wait = WebDriverWait(driver, 10)

        for idx, job in enumerate(job_links, 1):
            try:
                conn.execute("DELETE FROM jobs WHERE url = ?", (job['url'],))

                print(f"   [{idx}/{len(job_links)}] {job['title']}")
                driver.get(job['url'])

                try:
                    wait.until(EC.presence_of_element_located(
                        (By.CSS_SELECTOR, ".c-jobdetails__section-wrapper")))
                    time.sleep(1)
                except:
                    time.sleep(2)

                # DOM Walker specifically targeting the details section
                description = driver.execute_script("""
                    function walk(el) {
                        let text = "";
                        if (!el) return "";
                        if (el.nodeType === 3) {
                            let val = el.nodeValue.trim();
                            if (val) text += val + " ";
                        } else if (el.nodeType === 1) {
                            let tag = el.tagName.toUpperCase();
                            let cls = (el.className && typeof el.className === 'string') ? el.className.toLowerCase() : "";

                            if (['SCRIPT','STYLE','NAV','FOOTER','HEADER','BUTTON','SVG'].includes(tag)) return "";
                            
                            // Continental specific exclusions (Print hidden elements, apply buttons, share buttons, readmore toggles)
                            if (cls.includes('share') || cls.includes('apply') || cls.includes('u-hide@print') || cls.includes('c-readmore__button')) return "";

                            if (tag === 'LI') text += "• ";
                            for (let child of el.childNodes) text += walk(child); 
                            
                            if (['H1','H2','H3','H4'].includes(tag)) text += "\\n\\n";
                            else if (['B','STRONG'].includes(tag) && text.length > 10) text += "\\n\\n";
                            else if (['P','DIV','BR','LI'].includes(tag)) text += "\\n";
                        }
                        return text;
                    }

                    let mainContent = document.querySelector('.c-jobdetails__section-wrapper');
                    return mainContent ? walk(mainContent) : walk(document.body);
                """)

                clean_desc = re.sub(r'\n{3,}', '\n\n', description).strip()

                # --- DYNAMIC LOCATION, CITY AND COUNTRY PARSING ---
                location_raw = job.get('location_raw', 'N/A')
                city = "N/A"
                country = "N/A"

                if location_raw and location_raw != 'N/A':
                    parts = [p.strip() for p in location_raw.split(',')]
                    if len(parts) > 1:
                        # The last element is usually the country (e.g. "Mexico")
                        country = parts[-1]
                        # The first element is the city + business unit (e.g. "San Luis Potosí - Tires")
                        city_raw = parts[0]
                        # Clean up trailing business units if separated by a hyphen
                        city = city_raw.split(
                            '-')[0].strip() if '-' in city_raw else city_raw
                    else:
                        # Fallback if no comma exists
                        city = parts[0].split(
                            '-')[0].strip() if '-' in parts[0] else parts[0]

                conn.execute('''INSERT INTO jobs (url, title, company, location_raw, city, country, description, category) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                             (job['url'], job['title'], COMPANY_NAME, location_raw, city, country, clean_desc, job['category']))
                conn.commit()

            except Exception as e:
                print(f"      ⚠️ Error processing job {idx}: {e}")

        conn.close()
        print(f"\n✨ SUCCESS! {COMPANY_NAME} jobs saved successfully.")

    finally:
        try:
            driver.quit()
        except:
            pass


if __name__ == "__main__":
    run_scraper()
