import undetected_chromedriver as uc
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
COMPANY_NAME = "Philip Morris International"
BASE_URL = "https://join.pmicareers.com/earlycareers/gb/en/search-results"
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Manual\data"
DB_PATH = os.path.join(DATA_FOLDER, "pmi_jobs.db")

# Complete list of EU countries to filter by
EU_COUNTRIES = [
    'Austria', 'Belgium', 'Bulgaria', 'Croatia', 'Cyprus', 'Czechia', 'Czech Republic',
    'Denmark', 'Estonia', 'Finland', 'France', 'Germany', 'Greece', 'Hungary', 'Ireland',
    'Italy', 'Latvia', 'Lithuania', 'Luxembourg', 'Malta', 'Netherlands', 'Poland',
    'Portugal', 'Romania', 'Slovakia', 'Slovenia', 'Spain', 'Sweden'
]


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
    options = uc.ChromeOptions()
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    return uc.Chrome(options=options, version_main=145)


def run_scraper():
    init_db()
    print(f"🚀 Starting {COMPANY_NAME} Scraper (Phenom SPA Mode)...")

    driver = create_driver()
    job_links = []
    unique_urls = set()

    try:
        # --- PHASE 1: GATHERING LINKS & APPLYING EU FILTER ---
        print(f"📂 Opening PMI careers page...")
        driver.get(BASE_URL)
        time.sleep(8)

        # Handle Cookie Banner
        print("🍪 Handling cookies...")
        try:
            cookie_btn = driver.find_element(
                By.XPATH, "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'accept')] | //button[@id='button-accept-all']")
            driver.execute_script("arguments[0].click();", cookie_btn)
            time.sleep(1.5)
        except:
            pass

        print("🎛️ Injecting EU Country Filters...")

        # We inject a JavaScript function that clicks the country accordion (if closed)
        # and then checks every box that matches an EU country.
        driver.execute_script(f"""
            // 1. Open the Country Accordion if it's closed
            let countryBtn = document.getElementById('CountryAccordion');
            if (countryBtn && countryBtn.getAttribute('aria-expanded') === 'false') {{
                countryBtn.click();
            }}
            
            // 2. Tick the EU checkboxes
            setTimeout(() => {{
                const euCountries = {EU_COUNTRIES};
                let checkboxes = document.querySelectorAll('input[data-ph-at-facetkey="facet-country"]');
                let clickedAny = false;
                
                checkboxes.forEach(c => {{
                    let countryName = c.getAttribute('data-ph-at-text');
                    if (euCountries.includes(countryName) && !c.checked) {{
                        c.click();
                        clickedAny = true;
                    }}
                }});
            }}, 1000);
        """)

        print("⏳ Waiting for the filtered job list to load...")
        # Give the Aurelia framework time to fetch the filtered jobs
        time.sleep(6)

        clicks = 0
        while True:
            # Extract job cards on the current page
            jobs_on_page = driver.execute_script("""
                let results = [];
                // Phenom uses data-ph-at-id="job-link" or generic anchor links to job pages
                let links = document.querySelectorAll('a[data-ph-at-id="job-link"], li.jobs-list-item a[href*="/job/"]');
                
                links.forEach(a => {
                    if (a.href) {
                        results.push({
                            url: a.href,
                            category: 'Early Careers / FMCG'
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

            # Try to paginate (Phenom typically uses 'li.next a' or 'a.next')
            try:
                # Scroll down to the bottom
                driver.execute_script(
                    "window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1.5)

                next_btn = driver.find_element(
                    By.CSS_SELECTOR, 'li.next a, a.next, button.next-btn, a[data-ph-at-id="pagination-next-link"]')

                if not next_btn.is_displayed() or "disabled" in next_btn.get_attribute("class"):
                    print("\n🏁 Reached the last page.")
                    break

                clicks += 1
                sys.stdout.write(
                    f"\r   🔄 Clicked 'Next Page' {clicks} times... (Collected {len(job_links)} jobs so far)")
                sys.stdout.flush()

                driver.execute_script("arguments[0].click();", next_btn)
                time.sleep(4)  # Wait for AJAX to swap the jobs

            except Exception as e:
                print(
                    "\n🏁 No 'Next' button found or list is fully expanded. Pagination complete.")
                break

        print(
            f"\n✅ Total {len(job_links)} unique EU jobs identified. Rebooting browser for deep extraction...")

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
                        f"   [{idx}/{len(job_links)}] Skipped (Already in DB)")
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
                        (By.CSS_SELECTOR, '.jd-info, .job-description')))
                    time.sleep(1)
                except:
                    time.sleep(2)

                details = driver.execute_script("""
                    let result = { title: "N/A", location: "N/A", description: "" };
                    
                    // 1. Extract Title
                    let titleEl = document.querySelector('.job-title span') || document.querySelector('h1.job-title');
                    if (titleEl) result.title = titleEl.innerText.trim();
                    
                    // 2. Extract Location
                    let locEl = document.querySelector('.job-location');
                    if (locEl) {
                        // Remove the hidden "Location" screen reader text if it exists
                        let cleanLoc = locEl.innerText.replace(/Location\\s*/i, '').trim();
                        result.location = cleanLoc;
                    }
                    
                    // 3. Extract Description
                    let descContainer = document.querySelector('.jd-info, [data-ph-at-id="jobdescription-text"]');
                    if (descContainer) {
                        result.description = descContainer.innerHTML;
                    }
                    
                    return result;
                """)

                title = details['title'] if details['title'] != 'N/A' else job['url'].split(
                    '/')[-1].replace('-', ' ').title()

                # --- 3. DYNAMIC LOCATION PARSING ---
                location_raw = details['location']
                city = "N/A"
                country = "N/A"

                if location_raw and location_raw != 'N/A':
                    # Format: "Budapest, Hungary"
                    parts = [p.strip() for p in location_raw.split(',')]
                    city = parts[0]
                    country = parts[-1] if len(parts) > 1 else parts[0]

                print(
                    f"   [{idx}/{len(job_links)}] Formatting: {title} | Location: {city}, {country}")

                # --- 4. FORMAT DESCRIPTION (Markdownify) ---
                if details['description']:
                    clean_desc = md(details['description'], heading_style="ATX", bullets="-", strip=[
                                    'img', 'script', 'style', 'a', 'button', 'svg'])

                    # Fix artifacts and spacing
                    clean_desc = clean_desc.replace(
                        '•', '-').replace('·', '-').replace('\xa0', ' ').replace('&nbsp;', ' ')
                    clean_desc = re.sub(r'\n{3,}', '\n\n', clean_desc).strip()
                else:
                    clean_desc = "Description could not be loaded."

                # Trim marketing footers
                truncation_markers = ["Apply now",
                                      "Where will your ambition take you next?"]
                for marker in truncation_markers:
                    if marker in clean_desc:
                        clean_desc = clean_desc.split(marker)[0].strip()

                conn.execute('''INSERT INTO jobs (url, title, company, location_raw, city, country, description, category) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                             (job['url'], title, COMPANY_NAME, location_raw, city, country, clean_desc, job['category']))
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
