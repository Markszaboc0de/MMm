from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

import sys as _sys
import os as _os
_sys.path.append(_os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))
from driver_setup import get_chrome_driver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import sqlite3
import os
import sys
import time
import re
from markdownify import markdownify as md

sys.stdout.reconfigure(encoding='utf-8')

# --- CONFIGURATION ---
COMPANY_NAME = "Allianz"
BASE_URL = "https://careers.allianz.com/global/en/search-results"
DATA_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
DB_PATH = os.path.join(DATA_FOLDER, "allianz_jobs.db")

FILTERS_TO_APPLY = [
    # Countries
    "Austria", "Belgium", "Bulgaria", "Croatia", "Czech Republic", "Denmark",
    "Estonia", "Finland", "France", "Germany", "Greece", "Hungary",
    "Ireland", "Italy", "Latvia", "Lithuania", "Netherlands",
    "Poland", "Portugal", "Romania", "Slovakia", "Spain", "Sweden",
    # Job Levels
    "Apprenticeship / Dual Studies", "Entry Level", "Student"
]


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


def create_driver():
    """Creates fresh options for every launch to prevent 'options reuse' crashes"""
    return webdriver.Chrome(options=options)


def run_scraper():
    init_db()
    print(
        f"🚀 Starting {COMPANY_NAME} Scraper (GE Aerospace UI Filtering Mode)...")

    driver = create_driver()
    job_links = []
    unique_urls = set()

    try:
        # --- PHASE 1: GATHERING LINKS & APPLYING FILTERS ---
        print("📂 Opening Allianz career site...")
        driver.get(BASE_URL)
        time.sleep(6)

        # Handle Cookie Banner
        try:
            cookie_btn = driver.find_element(
                By.XPATH, "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'accept') or contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'allow')]")
            driver.execute_script("arguments[0].click();", cookie_btn)
            time.sleep(1.5)
        except:
            pass

        # Apply specific filters sequentially via JavaScript
        print("🎛️ Applying requested EU Country and Job Level filters...")

        for f in FILTERS_TO_APPLY:
            clicked = driver.execute_script(f"""
                // Expand "Show More" buttons just in case the filter is hidden
                let moreBtns = document.querySelectorAll('button.phw-show-more-text');
                moreBtns.forEach(b => b.click());
                
                // Find the specific label by textContent
                let label = Array.from(document.querySelectorAll('label.phw-check-label')).find(l => (l.textContent || "").trim().startsWith('{f}'));
                if (label) {{
                    let inputId = label.getAttribute('for');
                    let input = document.getElementById(inputId);
                    if (input && (input.getAttribute('aria-checked') === 'false' || !input.checked)) {{
                        label.click(); // Click the label to trigger the search update
                        return true;
                    }}
                }}
                return false;
            """)

            if clicked:
                print(
                    f"   ☑️ Applied filter: {f}, waiting for list to update...")
                time.sleep(3)

        page_num = 1

        while True:
            print(f"📄 Scraping data from page {page_num}...")

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

            jobs_on_page = driver.execute_script("""
                let results = [];
                let links = document.querySelectorAll('a[data-ph-at-id="job-link"], a[phw-tk="job_click"]');
                
                links.forEach(a => {
                    if (a.href) {
                        let loc = "N/A";
                        let parent = a.closest('.phw-card-block, .ph-card, .jobs-list-item') || a.parentElement.parentElement;
                        
                        if (parent) {
                            let locEl = parent.querySelector('.job-location, [data-ph-at-id="job-location"]');
                            if (locEl) {
                                let rawText = locEl.innerText || locEl.textContent || "";
                                loc = rawText.replace(/Location\\s*:/i, '').trim();
                            } else {
                                let textBlocks = parent.querySelectorAll('span.phw-line-clamp');
                                if (textBlocks.length > 1) {
                                    loc = textBlocks[textBlocks.length - 1].innerText.trim();
                                }
                            }
                        }
                        
                        results.push({
                            url: a.href,
                            title: a.innerText.trim(),
                            location_raw: loc,
                            category: 'Insurance / Finance'
                        });
                    }
                });
                return results;
            """)

            if not jobs_on_page:
                break

            # 🛑 INFINITE LOOP BREAKER 🛑
            new_jobs_this_page = 0
            for job in jobs_on_page:
                if job['url'] not in unique_urls:
                    unique_urls.add(job['url'])
                    job_links.append(job)
                    new_jobs_this_page += 1

            if new_jobs_this_page == 0:
                print("🏁 No new jobs found on this page. Reached the end of the list.")
                break

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
            f"✅ Identified {len(job_links)} unique jobs. Rebooting browser to prevent memory leaks...")

        if not job_links:
            return

        # --- PRE-PHASE 2: BROWSER REBOOT ---
        try:
            driver.quit()
        except:
            pass
        time.sleep(2)

        driver = create_driver()
        wait = WebDriverWait(driver, 10)

        # --- PHASE 2: DEEP SCRAPING DESCRIPTIONS & MULTI-LOCATIONS ---
        conn = sqlite3.connect(DB_PATH)

        for idx, job in enumerate(job_links, 1):
            try:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT id FROM jobs WHERE url = ?", (job['url'],))
                if cursor.fetchone():
                    print(
                        f"   [{idx}/{len(job_links)}] {job['title']} (Already in DB, skipping...)")
                    continue

                print(f"   [{idx}/{len(job_links)}] Formatting: {job['title']}")

                # AUTO-RECOVERY BLOCK
                try:
                    driver.get(job['url'])
                except Exception as e:
                    print("   🔄 Connection lost. Auto-recovering browser...")
                    try:
                        driver.quit()
                    except:
                        pass
                    time.sleep(2)
                    driver = create_driver()
                    wait = WebDriverWait(driver, 10)
                    driver.get(job['url'])

                try:
                    wait.until(EC.presence_of_element_located(
                        (By.CSS_SELECTOR, ".phw-job-description, [itemprop='description']")))
                    time.sleep(1)
                except:
                    time.sleep(2)

                # 💡 MULTI-LOCATION MODAL CLICKER
                try:
                    multi_loc_btn = driver.find_element(
                        By.CSS_SELECTOR, "button[data-ph-at-id='job-multi_location']")
                    driver.execute_script(
                        "arguments[0].click();", multi_loc_btn)
                    # Give the modal 1 second to render the locations
                    time.sleep(1)
                except:
                    pass

                # EXTRACT DESCRIPTION AND LOCATIONS ARRAY
                details = driver.execute_script("""
                    let result = { locations: [], description: "" };
                    
                    // Grab all location items from the modal (if open)
                    let modalLocs = document.querySelectorAll('[data-ph-at-id="location-list"] li, .job-location-list li, [data-ph-at-id="multi-feild-block"] span');
                    if (modalLocs.length > 0) {
                        modalLocs.forEach(el => {
                            let txt = el.innerText.replace(/Location\\s*:/i, '').trim();
                            if (txt) result.locations.push(txt);
                        });
                    } else {
                        // Fallback to standard single location
                        let singleLoc = document.querySelector('.job-location, [data-ph-at-id="job-location"]');
                        if (singleLoc) {
                            let txt = singleLoc.innerText.replace(/Location\\s*:/i, '').trim();
                            if (txt) result.locations.push(txt);
                        }
                    }
                    
                    let desc = document.querySelector('.phw-job-description') || document.querySelector('[itemprop="description"]');
                    if (desc) result.description = desc.innerHTML;
                    
                    return result;
                """)

                # --- MULTI-LOCATION PARSER ---
                raw_locs = details.get('locations', [])
                valid_locs = []
                primary_city = "N/A"
                primary_country = "N/A"

                for loc in raw_locs:
                    # Ignore the generic "Available in X locations" text if it sneaks in
                    if "available in" in loc.lower() or "locations" in loc.lower():
                        continue

                    parts = [p.strip() for p in loc.split(',')]
                    if not parts:
                        continue

                    city = parts[0]
                    country = "N/A"

                    if len(parts) > 1:
                        # Strip zip codes from the country identifier
                        if any(c.isdigit() for c in parts[-1]):
                            country = parts[-2] if len(
                                parts) >= 3 else parts[1]
                        else:
                            country = parts[-1]
                    else:
                        country = parts[0]

                    valid_locs.append(loc)
                    if primary_country == "N/A":
                        primary_city = city
                        primary_country = country

                # Fallback to Phase 1 location if Phase 2 extraction failed completely
                final_location_raw = " | ".join(
                    valid_locs) if valid_locs else job.get('location_raw', 'N/A')

                if primary_country == "N/A":
                    loc_fallback = job.get('location_raw', 'N/A')
                    loc_fallback = re.sub(
                        r'(?i)^Location\s*:\s*', '', loc_fallback).strip()
                    parts = [p.strip() for p in loc_fallback.split(',')]
                    primary_city = parts[0]
                    if len(parts) > 1:
                        if any(c.isdigit() for c in parts[-1]):
                            primary_country = parts[-2] if len(
                                parts) >= 3 else parts[1]
                        else:
                            primary_country = parts[-1]

                # --- FORMAT DESCRIPTION ---
                if details['description']:
                    clean_desc = md(details['description'], heading_style="ATX",
                                    bullets="-", strip=['img', 'script', 'style', 'a'])
                    clean_desc = clean_desc.replace(
                        '•', '-').replace('·', '-').replace('\xa0', ' ')
                    clean_desc = re.sub(r'\n{3,}', '\n\n', clean_desc).strip()
                else:
                    clean_desc = "Description could not be loaded."

                # Trim marketing footers
                truncation_markers = [
                    "Let's care for tomorrow.", "Allianz Group is one of the most trusted"]
                for marker in truncation_markers:
                    if marker in clean_desc:
                        clean_desc = clean_desc.split(marker)[0].strip()

                conn.execute('''INSERT INTO jobs (url, title, company, location_raw, city, country, description, category) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                             (job['url'], job['title'], COMPANY_NAME, final_location_raw, primary_city, primary_country, clean_desc, job['category']))
                conn.commit()

            except Exception as e:
                print(f"      ⚠️ Error on job {idx}: {e}")

        conn.close()
        print(
            f"\n✨ SUCCESS! {COMPANY_NAME} jobs safely saved with full Multi-Location data.")

    finally:
        try:
            driver.quit()
        except:
            pass


if __name__ == "__main__":
    run_scraper()
