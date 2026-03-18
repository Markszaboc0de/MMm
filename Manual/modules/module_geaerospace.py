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
COMPANY_NAME = "GE Aerospace"
BASE_URL = "https://careers.geaerospace.com/global/en/search-results"
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Manual\data"
DB_PATH = os.path.join(DATA_FOLDER, "geaerospace_jobs.db")


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
    print(f"🚀 Starting {COMPANY_NAME} Scraper (Multi-Location Master Mode)...")

    options = uc.ChromeOptions()
    options.add_argument("--window-size=1920,1080")
    driver = uc.Chrome(options=options, version_main=145)

    job_links = []
    unique_urls = set()

    try:
        # --- PHASE 1: GATHERING LINKS & APPLYING FILTERS ---
        print("📂 Opening GE Aerospace career site...")
        driver.get(BASE_URL)
        time.sleep(6)

        # Handle Cookie Banner
        try:
            cookie_btn = driver.find_element(
                By.XPATH, "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'accept') or contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'allow')]")
            driver.execute_script("arguments[0].click();", cookie_btn)
            time.sleep(1)
        except:
            pass

        # Apply specific filters sequentially
        print("🎛️ Applying requested Experience Level filters...")
        filters_to_apply = ['Apprentice', 'Co-op/Intern', 'Early Career']

        for f in filters_to_apply:
            clicked = driver.execute_script(f"""
                let label = Array.from(document.querySelectorAll('label.phw-check-label')).find(l => l.innerText.includes('{f}'));
                if (label) {{
                    let inputId = label.getAttribute('for');
                    let input = document.getElementById(inputId);
                    if (input && (input.getAttribute('aria-checked') === 'false' || !input.checked)) {{
                        label.click();
                        return true;
                    }}
                }}
                return false;
            """)

            if clicked:
                print(
                    f"   ☑️ Applied filter: {f}, waiting for list to update...")
                time.sleep(4)

        page_num = 1

        while True:

            print(f"📄 Scraping data from page {page_num}...")

            # Robust wait loop
            cards_found = False
            for _ in range(15):
                count = driver.execute_script(
                    "return document.querySelectorAll('a[data-ph-at-id=\"job-link\"], a[phw-tk=\"job_click\"]').length;")
                if count > 0:
                    cards_found = True
                    break
                time.sleep(1)

            if not cards_found:
                print(
                    "⚠️ No job cards found, list might be empty or filters returned 0 results.")
                break

            # Extract links
            jobs_on_page = driver.execute_script("""
                let results = [];
                let links = document.querySelectorAll('a[data-ph-at-id="job-link"], a[phw-tk="job_click"]');
                
                links.forEach(a => {
                    if (a.href) {
                        let loc = "N/A";
                        let parent = a.closest('li, .phw-card, [data-ph-at-id="facet-results-item"]');
                        if (parent) {
                            let locEl = parent.querySelector('.job-location, [data-ph-at-id="job-location"]');
                            if (locEl) {
                                loc = locEl.innerText.trim();
                            } else {
                                let textBlocks = parent.querySelectorAll('span.phw-line-clamp');
                                if (textBlocks.length > 1) {
                                    loc = textBlocks[textBlocks.length - 1].innerText.trim();
                                }
                            }
                        }
                        
                        loc = loc.replace(/^Location:\\s*/i, '');

                        results.push({
                            url: a.href,
                            title: a.innerText.trim(),
                            location_raw: loc,
                            category: 'Aviation / Engineering'
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

            # PAGINATION
            has_next = driver.execute_script("""
                let nextSpan = document.querySelector('span[data-ph-at-id="pagination-next-text"]');
                if (nextSpan) {
                    let btn = nextSpan.closest('a, button, li');
                    if (btn && !btn.classList.contains('disabled') && btn.getAttribute('aria-disabled') !== 'true') {
                        btn.click();
                        return true;
                    }
                }
                return false;
            """)

            if has_next:
                page_num += 1
                time.sleep(4)
            else:
                print(
                    "🏁 No next button found or it is disabled, reached the end of the list.")
                break

        print(
            f"✅ Identified {len(job_links)} unique jobs. Starting deep scrape...")

        if not job_links:
            return

        # --- PHASE 2: DEEP SCRAPING DESCRIPTIONS & MULTI-LOCATIONS ---
        conn = sqlite3.connect(DB_PATH)
        wait = WebDriverWait(driver, 10)

        for idx, job in enumerate(job_links, 1):
            try:
                conn.execute("DELETE FROM jobs WHERE url = ?", (job['url'],))

                print(f"   [{idx}/{len(job_links)}] {job['title']}")
                driver.get(job['url'])

                try:
                    wait.until(EC.presence_of_element_located(
                        (By.CSS_SELECTOR, ".phw-job-description")))
                    time.sleep(1)
                except:
                    time.sleep(2)

                # 💡 THE MASTER MOVE: Click "See all" if it exists to open the modal!
                try:
                    see_all_btn = driver.find_element(
                        By.XPATH, "//span[contains(text(), 'See all')]")
                    driver.execute_script("arguments[0].click();", see_all_btn)
                    time.sleep(1)  # Give the modal 1 second to pop up
                except:
                    pass  # If there is no "See all" button, just proceed normally

                # DOM Walker for Phenom ATS + Modal Location Extraction
                details = driver.execute_script("""
                    let exactLoc = "";
                    
                    // First, try to grab the list from the Modal (if "See all" was clicked)
                    let multiLocs = document.querySelectorAll('[data-ph-at-id="multi-feild-block"] span');
                    if (multiLocs && multiLocs.length > 0) {
                        let locArray = [];
                        multiLocs.forEach(span => {
                            if (span.innerText.trim()) locArray.push(span.innerText.trim());
                        });
                        exactLoc = locArray.join(' | '); // Join multiple locations with a pipe
                    } else {
                        // Fallback to the standard single location tag
                        let locContainer = document.querySelector('span[data-ph-at-text="location"]');
                        if (locContainer) {
                            exactLoc = locContainer.innerText.replace(/Location\\s*:/i, '').replace('See all', '').trim();
                        }
                    }

                    // DOM Walker for the description
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
                            else if (['P','DIV','BR','LI'].includes(tag)) text += "\\n";
                        }
                        return text;
                    }

                    let mainContent = document.querySelector('.phw-job-description');
                    let descText = mainContent ? walk(mainContent) : walk(document.body);
                    
                    return { description: descText, location: exactLoc };
                """)

                clean_desc = re.sub(r'\n{3,}', '\n\n',
                                    details['description']).strip()

                # GE Aerospace specific footer cleanup
                truncation_markers = [
                    "Additional Information",
                    "Relocation Assistance Provided",
                    "GE offers a great work environment"
                ]
                for marker in truncation_markers:
                    if marker in clean_desc:
                        clean_desc = clean_desc.split(marker)[0].strip()

                # --- DYNAMIC LOCATION PARSING ---
                location_raw = details['location'] if details['location'] else job.get(
                    'location_raw', 'N/A')
                city = "N/A"
                country = "N/A"

                if location_raw and location_raw != 'N/A':
                    # If there are multiple locations (separated by |), we parse the FIRST one for city/country
                    primary_loc = location_raw.split('|')[0].strip()
                    parts = [p.strip() for p in primary_loc.split(',')]
                    city = parts[0]
                    country = parts[-1]

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
