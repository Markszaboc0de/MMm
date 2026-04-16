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
COMPANY_NAME = "EPAM"
BASE_URL = "https://careers.epam.com/en/jobs?seniority=junior"
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "epam_jobs.db")


def init_db():
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)
    conn = sqlite3.connect(DB_PATH)

    conn.execute('''CREATE TABLE IF NOT EXISTS jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT, url TEXT UNIQUE, title TEXT, 
        company TEXT, location_raw TEXT, city TEXT, country TEXT, description TEXT, category TEXT,
        date_found TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')

    try:
        conn.execute("ALTER TABLE jobs ADD COLUMN location_raw TEXT")
        conn.execute("ALTER TABLE jobs ADD COLUMN country TEXT")
    except:
        pass

    conn.commit()
    conn.close()


def create_driver():
    return get_chrome_driver()


def run_scraper():
    init_db()
    print(f"🚀 Starting {COMPANY_NAME} Scraper (React SPA Mode)...")

    driver = create_driver()
    job_links = []
    unique_urls = set()

    try:
        # --- PHASE 1: GATHERING LINKS & PAGINATING ---
        print(f"📂 Opening EPAM careers page: {BASE_URL}")
        driver.get(BASE_URL)
        time.sleep(8)

        # Handle typical OneTrust Cookie Banner
        try:
            cookie_btn = driver.find_element(
                By.XPATH, "//button[@id='onetrust-accept-btn-handler'] | //button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'accept')]")
            driver.execute_script("arguments[0].click();", cookie_btn)
            time.sleep(1.5)
        except:
            pass

        clicks = 0
        while True:
            # Wait for the job list to load
            try:
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, 'a[data-testid="job-card-link"]'))
                )
            except:
                print("⚠️ No job cards found on this page. Halting pagination.")
                break

            # Track the first job URL to ensure the page actually refreshes after clicking 'Next'
            first_job_url = driver.execute_script(
                "return document.querySelector('a[data-testid=\"job-card-link\"]').href;")

            # Extract job cards on the current page
            jobs_on_page = driver.execute_script("""
                let results = [];
                let links = document.querySelectorAll('a[data-testid="job-card-link"]');
                
                links.forEach(a => {
                    let titleSpan = a.querySelector('[data-testid="job-card-title"]');
                    let title = titleSpan ? titleSpan.innerText.trim() : a.innerText.trim();
                    
                    if (a.href) {
                        results.push({
                            url: a.href,
                            title: title,
                            category: 'IT / Tech' // EPAM is strictly IT
                        });
                    }
                });
                return results;
            """)

            # Add to our unique list
            for job in jobs_on_page:
                if job['url'] not in unique_urls:
                    unique_urls.add(job['url'])
                    job_links.append(job)

            # Try to click the "Next" pagination arrow
            try:
                next_btn = driver.find_element(
                    By.CSS_SELECTOR, 'button[aria-label="next page"], .PaginationArrow_paginationArrow__EEK_r')

                if not next_btn.is_displayed() or "disabled" in next_btn.get_attribute("class") or next_btn.get_attribute("disabled"):
                    print("\n🏁 Reached the last page.")
                    break

                clicks += 1
                sys.stdout.write(
                    f"\r   🔄 Clicked 'Next Page' {clicks} times... (Found {len(job_links)} jobs so far)")
                sys.stdout.flush()

                driver.execute_script(
                    "arguments[0].scrollIntoView({block: 'center'});", next_btn)
                time.sleep(1)
                driver.execute_script("arguments[0].click();", next_btn)

                # Wait for React to update the DOM (the first URL should change)
                success = False
                for _ in range(15):
                    time.sleep(1)
                    new_first_url = driver.execute_script(
                        "let a = document.querySelector('a[data-testid=\"job-card-link\"]'); return a ? a.href : '';")
                    if new_first_url and new_first_url != first_job_url:
                        success = True
                        break

                if not success:
                    print(
                        "\n⚠️ Page did not refresh after clicking next. Exiting pagination.")
                    break

            except Exception as e:
                print("\n🏁 No 'Next' button found. Pagination complete.")
                break

        print(
            f"\n✅ Total {len(job_links)} unique jobs identified. Rebooting browser for deep extraction...")

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

        # --- PHASE 2: DEEP SCRAPING DESCRIPTIONS & LOCATIONS ---
        conn = sqlite3.connect(DB_PATH)
        saved_count = 0

        for idx, job in enumerate(job_links, 1):
            try:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT id FROM jobs WHERE url = ?", (job['url'],))
                if cursor.fetchone():
                    print(
                        f"   [{idx}/{len(job_links)}] {job['title']} (Already in DB, skipping...)")
                    continue

                # AUTO-RECOVERY BLOCK
                try:
                    driver.get(job['url'])
                except:
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
                        (By.CSS_SELECTOR, '[data-testid="description-container"], .Description_container__VxWne')))
                    time.sleep(1)
                except:
                    time.sleep(2)

                # Click open all accordions to ensure content is fully loaded into the DOM
                try:
                    driver.execute_script("""
                        let accordions = document.querySelectorAll('[data-testid="accordion-section-container"] [tabindex="-1"], .AccordionSection_header__kp8GP');
                        accordions.forEach(acc => {
                            try { acc.click(); } catch(e) {}
                        });
                    """)
                    time.sleep(1)
                except:
                    pass

                details = driver.execute_script("""
                    let result = { location: "N/A", description: "" };
                    
                    // 1. Extract exact Location from EPAM's specific top bar
                    let bullets = document.querySelectorAll('[data-testid="icon-bullet-list-container"] [data-testid="icon-bullet-item"]');
                    for (let i = 0; i < bullets.length; i++) {
                        let txt = bullets[i].innerText.trim();
                        if (txt.toLowerCase().includes('office in')) {
                            // The actual location is usually in the next span
                            let nextSpan = bullets[i + 1];
                            if (nextSpan && !nextSpan.innerText.toLowerCase().includes('office in')) {
                                result.location = nextSpan.innerText.trim();
                            } else {
                                // Fallback if they are in the same string
                                result.location = txt.replace(/office in( the)?/i, '').trim();
                            }
                            break;
                        }
                    }
                    
                    // 2. Extract Description from the specific React container
                    let descContainer = document.querySelector('[data-testid="description-container"]');
                    if (descContainer) {
                        result.description = descContainer.innerHTML;
                    }
                    
                    return result;
                """)

                # --- 3. DYNAMIC LOCATION PARSING ---
                location_raw = details['location']
                city = "N/A"
                country = "N/A"

                if location_raw and location_raw != 'N/A':
                    # EPAM format: "The United Kingdom: Newry" or "Hungary: Budapest"
                    if ':' in location_raw:
                        parts = location_raw.split(':')
                        # Clean up "The United Kingdom" -> "United Kingdom"
                        country = parts[0].replace('The ', '').strip()
                        city = parts[1].strip()
                    else:
                        city = location_raw
                        country = location_raw

                print(
                    f"   [{idx}/{len(job_links)}] Formatting: {job['title']} | Location: {city}, {country}")

                # --- 4. FORMAT DESCRIPTION (Markdownify) ---
                if details['description']:
                    # Convert to Markdown while stripping React-specific tags and buttons
                    clean_desc = md(details['description'], heading_style="ATX", bullets="-", strip=[
                                    'img', 'script', 'style', 'a', 'button', 'svg'])

                    # Fix artifacts and spacing
                    clean_desc = clean_desc.replace(
                        '•', '-').replace('·', '-').replace('\xa0', ' ').replace('&nbsp;', ' ')
                    clean_desc = re.sub(r'\n{3,}', '\n\n', clean_desc).strip()
                else:
                    clean_desc = "Description could not be loaded."

                conn.execute('''INSERT INTO jobs (url, title, company, location_raw, city, country, description, category) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                             (job['url'], job['title'], COMPANY_NAME, location_raw, city, country, clean_desc, job['category']))
                conn.commit()
                saved_count += 1

            except Exception as e:
                print(f"      ⚠️ Error on job {idx}: {e}")

        conn.close()
        print(f"\n✨ SUCCESS! {saved_count} {COMPANY_NAME} jobs safely saved.")

    finally:
        try:
            driver.quit()
        except:
            pass


if __name__ == "__main__":
    run_scraper()
