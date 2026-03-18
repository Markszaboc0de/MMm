import undetected_chromedriver as uc
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
COMPANY_NAME = "MET Group"
# We will use URL formatting to handle pagination cleanly
BASE_URL_TEMPLATE = "https://met.com/en/people-and-career/career-portal/?country=all&page={}"
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Manual\data"
DB_PATH = os.path.join(DATA_FOLDER, "metgroup_jobs.db")


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
        f"🚀 Starting {COMPANY_NAME} Scraper (SmartRecruiters API Bypass Mode)...")

    options = uc.ChromeOptions()
    options.add_argument("--window-size=1920,1080")
    driver = uc.Chrome(options=options, version_main=145)

    job_links = []
    unique_urls = set()

    try:
        # --- PHASE 1: GATHERING LINKS ---
        page_num = 1

        while True:

            target_url = BASE_URL_TEMPLATE.format(page_num)
            print(f"📂 Opening MET Group career site - Page {page_num}...")
            driver.get(target_url)
            time.sleep(5)  # Wait for the internal API to populate the list

            # Handle Cookie Banner (if present on first load)
            if page_num == 1:
                try:
                    cookie_btn = driver.find_element(
                        By.XPATH, "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'accept') or contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'agree')]")
                    driver.execute_script("arguments[0].click();", cookie_btn)
                    time.sleep(1)
                except:
                    pass

            try:
                WebDriverWait(driver, 10).until(EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "a.recruitment-page-result")))
            except:
                print("⚠️ No job cards found, list might be empty or fully scraped.")
                break

            # Extract SmartRecruiters IDs, titles, and raw locations via JS
            jobs_on_page = driver.execute_script("""
                let results = [];
                let links = document.querySelectorAll('a.recruitment-page-result');
                
                links.forEach(a => {
                    let onclickText = a.getAttribute('onclick') || '';
                    // Extract the Job ID from the API string (e.g., .../postings/744000114649842)
                    let idMatch = onclickText.match(/postings\\/(\\d+)/);
                    
                    if (idMatch && idMatch[1]) {
                        let jobId = idMatch[1];
                        // Construct the direct SmartRecruiters URL to bypass clunky redirects
                        let directUrl = 'https://jobs.smartrecruiters.com/METGroup/' + jobId;
                        
                        let titleEl = a.querySelector('.title');
                        let title = titleEl ? titleEl.innerText.trim() : 'MET Group Position';
                        
                        let params = a.querySelectorAll('.params');
                        let category = "Energy / Utilities";
                        let loc = "N/A";
                        
                        // The params spans usually contain: [Company, Department, Location, Type]
                        if (params.length >= 3) {
                            category = params[1].innerText.trim();
                            loc = params[2].innerText.trim();
                        }
                        
                        results.push({
                            url: directUrl,
                            title: title,
                            location_raw: loc,
                            category: category
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

            # PAGINATION CHECK: Look at the pagination UL to see if the 'Next' arrow is disabled
            is_last_page = driver.execute_script("""
                let nextSpan = document.querySelector('span.next');
                if (nextSpan) {
                    let parentLi = nextSpan.closest('li');
                    if (parentLi && parentLi.classList.contains('disabled')) {
                        return true;
                    }
                    return false;
                }
                return true; // If there is no pagination, it's the last page
            """)

            if is_last_page:
                print("🏁 Reached the last page of the list.")
                break
            else:
                page_num += 1

        print(
            f"✅ Identified {len(job_links)} unique jobs. Starting deep scrape...")

        if not job_links:
            return

        # --- PHASE 2: DEEP SCRAPING DESCRIPTIONS & EXACT LOCATIONS ---
        conn = sqlite3.connect(DB_PATH)
        wait = WebDriverWait(driver, 10)

        for idx, job in enumerate(job_links, 1):
            try:
                conn.execute("DELETE FROM jobs WHERE url = ?", (job['url'],))

                print(f"   [{idx}/{len(job_links)}] {job['title']}")
                driver.get(job['url'])

                try:
                    # Wait for the SmartRecruiters description container
                    wait.until(EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "div[itemprop='description']")))
                    time.sleep(1)
                except:
                    time.sleep(2)

                # DOM Walker specifically for SmartRecruiters + Precise Location Extraction
                details = driver.execute_script("""
                    // 1. Extract precise location from SmartRecruiters specific tag
                    let exactLoc = "";
                    let locContainer = document.querySelector('.c-spl-job-location__place');
                    if (locContainer) {
                        exactLoc = locContainer.innerText.trim();
                    }

                    // 2. DOM Walker for the description
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
                            if (cls.includes('share') || cls.includes('apply') || cls.includes('back')) return "";

                            if (tag === 'LI') text += "• ";
                            for (let child of el.childNodes) text += walk(child); 
                            
                            if (['H1','H2','H3','H4'].includes(tag)) text += "\\n\\n";
                            else if (['B','STRONG'].includes(tag) && text.length > 10) text += "\\n\\n";
                            else if (['P','DIV','BR','LI', 'SECTION'].includes(tag)) text += "\\n";
                        }
                        return text;
                    }

                    let mainContent = document.querySelector('div[itemprop="description"]');
                    let descText = mainContent ? walk(mainContent) : walk(document.body);
                    
                    return { description: descText, location: exactLoc };
                """)

                clean_desc = re.sub(r'\n{3,}', '\n\n',
                                    details['description']).strip()

                # --- DYNAMIC LOCATION, CITY AND COUNTRY PARSING ---
                # SmartRecruiters format is typically: "Street, City, State, Country" or "City, Country"
                location_raw = details['location'] if details['location'] else job.get(
                    'location_raw', 'N/A')
                city = "N/A"
                country = "N/A"

                if location_raw and location_raw != 'N/A':
                    parts = [p.strip() for p in location_raw.split(',')]

                    if len(parts) >= 2:
                        # For detail page format: "139 rue Vendôme, Lyon, France" -> country is last, city is second to last
                        country = parts[-1]
                        city = parts[-2]
                    else:
                        city = parts[0]

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
