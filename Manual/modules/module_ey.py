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
import datetime
from markdownify import markdownify as md

sys.stdout.reconfigure(encoding='utf-8')

# --- CONFIGURATION ---
COMPANY_NAME = "EY"

# The specific EY Yello Board URL
BASE_URL = "https://eyglobal.yello.co/job_boards/c1riT--B2O-KySgYWsZO1Q?locale=en"

DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Manual\data"
DB_PATH = os.path.join(DATA_FOLDER, "ey_jobs.db")

# Time Constraint: 2 Months (60 days)
CUTOFF_DATE = datetime.datetime.now() - datetime.timedelta(days=60)

# EY 3-Letter Country Code Dictionary
EY_COUNTRIES = {
    'AUT': 'Austria', 'BEL': 'Belgium', 'BGR': 'Bulgaria', 'HRV': 'Croatia',
    'CYP': 'Cyprus', 'CZE': 'Czech Republic', 'DNK': 'Denmark', 'EST': 'Estonia',
    'FIN': 'Finland', 'FRA': 'France', 'DEU': 'Germany', 'GER': 'Germany',
    'GRC': 'Greece', 'HUN': 'Hungary', 'IRL': 'Ireland', 'ITA': 'Italy',
    'LVA': 'Latvia', 'LTU': 'Lithuania', 'LUX': 'Luxembourg', 'MLT': 'Malta',
    'NLD': 'Netherlands', 'POL': 'Poland', 'PRT': 'Portugal', 'ROU': 'Romania',
    'SVK': 'Slovakia', 'SVN': 'Slovenia', 'ESP': 'Spain', 'SWE': 'Sweden',
    'GBR': 'United Kingdom', 'UK': 'United Kingdom', 'CHE': 'Switzerland',
    'SWI': 'Switzerland'
}


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
    options = Options()
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    return webdriver.Chrome(options=options)


def parse_job_date(text):
    text = text.lower().replace(',', '')
    now = datetime.datetime.now()

    if 'today' in text:
        return now
    if 'yesterday' in text:
        return now - datetime.timedelta(days=1)

    rel_match = re.search(r'(\d+)\s+(day|week|month|year)s?\s+ago', text)
    if rel_match:
        val = int(rel_match.group(1))
        unit = rel_match.group(2)
        if unit == 'day':
            return now - datetime.timedelta(days=val)
        if unit == 'week':
            return now - datetime.timedelta(weeks=val)
        if unit == 'month':
            return now - datetime.timedelta(days=val*30)
        if unit == 'year':
            return now - datetime.timedelta(days=val*365)

    months = ['jan', 'feb', 'mar', 'apr', 'may', 'jun',
              'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
    for month in months:
        m = re.search(fr'({month}[a-z]*)\s+(\d{{1,2}})\s+(\d{{4}})', text)
        if m:
            month_str, day_str, year_str = m.groups()
            month_idx = next(i for i, x in enumerate(months)
                             if month_str.startswith(x)) + 1
            try:
                return datetime.datetime(int(year_str), month_idx, int(day_str))
            except:
                pass

    return now


def run_scraper():
    init_db()
    print(
        f"🚀 Starting {COMPANY_NAME} Scraper (Time-Traveling DOM & EY Format Mode)...")
    print(
        f"⏰ Strict Cutoff Date: {CUTOFF_DATE.strftime('%Y-%m-%d')} (Jobs older than 2 months will be ignored)")

    driver = create_driver()
    job_links = []
    unique_urls = set()

    try:
        # --- PHASE 1: GATHERING LINKS & APPLYING FILTERS ---
        print("📂 Opening EY careers page...")
        driver.get(BASE_URL)
        time.sleep(8)

        print("🍪 Handling cookies...")
        try:
            driver.execute_script("""
                let buttons = document.querySelectorAll('button, a');
                for (let btn of buttons) {
                    let text = (btn.innerText || "").toLowerCase();
                    if (text.includes('accept') || text.includes('allow') || text.includes('agree')) {
                        btn.click();
                    }
                }
            """)
            time.sleep(2)
        except:
            pass

        print("🎛️ Injecting EU Country Filters via exact HTML Values...")

        eu_filter_values = [
            '29953', '29960', '29968', '29980', '29982', '29983', '29984', '29990',
            '29993', '29994', '29997', '30000', '30007', '30012', '30015', '30026',
            '30030', '30031', '30037', '30047', '30061', '30062', '30064', '30072',
            '30073', '30076', '30079'
        ]

        driver.execute_script(f"""
            let filterPanel = document.querySelector('a[href="#collapse-245"]');
            if (filterPanel && filterPanel.classList.contains('collapsed')) {{
                filterPanel.click();
            }}
            
            setTimeout(() => {{
                const euVals = {eu_filter_values};
                let checkboxes = document.querySelectorAll('#collapse-245 input[type="checkbox"]');
                checkboxes.forEach(c => {{
                    if (euVals.includes(c.value) && !c.checked) {{
                        c.click();
                    }}
                }});
            }}, 500);
        """)

        print("⏳ Waiting for the filtered list to load...")
        time.sleep(6)

        print("⏳ Scrolling and searching for jobs posted within the last 60 days...")

        reached_old_jobs = False
        scroll_attempts = 0
        last_job_count = 0

        while not reached_old_jobs:
            driver.execute_script(
                "window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2.5)

            driver.execute_script("""
                let btn = document.querySelector('button.load-more, .pagination-next, a.next');
                if (btn && !btn.disabled) btn.click();
            """)
            time.sleep(2)

            jobs_on_page = driver.execute_script("""
                let results = [];
                let links = document.querySelectorAll('a.search-results__req_title, a[href*="/jobs/"]');
                
                links.forEach(aTag => {
                    if (aTag.href) {
                        let card = aTag.closest('div[class*="item"], li, tr, .search-results__list-item') || aTag.parentElement.parentElement;
                        results.push({
                            url: aTag.href,
                            title: aTag.innerText.trim(),
                            raw_text: card ? card.innerText : aTag.innerText 
                        });
                    }
                });
                return results;
            """)

            if len(jobs_on_page) == last_job_count:
                scroll_attempts += 1
                if scroll_attempts > 3:
                    print("🏁 Reached the absolute bottom of the page.")
                    break
            else:
                scroll_attempts = 0
                last_job_count = len(jobs_on_page)

            for jd in jobs_on_page:
                if jd['url'] not in unique_urls:
                    unique_urls.add(jd['url'])

                    job_date = parse_job_date(jd['raw_text'])

                    if job_date >= CUTOFF_DATE:
                        job_links.append(jd)
                    else:
                        print(
                            f"\n🛑 Detected a job older than 60 days (Posted approx: {job_date.strftime('%Y-%m-%d')}). Halting scroll!")
                        reached_old_jobs = True
                        break

            sys.stdout.write(
                f"\r   🔄 Scrolling... Collected {len(job_links)} recent jobs so far.")
            sys.stdout.flush()

        print(
            f"\n✅ Successfully harvested {len(job_links)} jobs posted in the last 2 months. Rebooting browser for deep extraction...")

        if not job_links:
            return

        # --- PRE-PHASE 2: BROWSER REBOOT ---
        try:
            driver.quit()
        except Exception:
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

                try:
                    driver.get(job['url'])
                except Exception:
                    print("   🔄 Connection lost. Auto-recovering browser...")
                    try:
                        driver.quit()
                    except Exception:
                        pass
                    time.sleep(2)
                    driver = create_driver()
                    wait = WebDriverWait(driver, 10)
                    driver.get(job['url'])

                try:
                    wait.until(EC.presence_of_element_located(
                        (By.CSS_SELECTOR, ".job-details__description, .job-show__description, .job-description, article")))
                    time.sleep(1)
                except:
                    time.sleep(2)

                details = driver.execute_script("""
                    let result = { location: "N/A", description: "" };
                    
                    // 💡 THE SPAN HUNTER: Look specifically for "ITA-Milano" format via Regex
                    let spans = document.querySelectorAll('span, li, div');
                    for (let el of spans) {
                        let txt = el.innerText.trim();
                        // Matches 2-3 uppercase letters, a hyphen, and a city name (e.g. ITA-Milano)
                        if (/^[A-Z]{2,3}-[A-Za-z\\s\\-\']+$/.test(txt)) {
                            result.location = txt;
                            break;
                        }
                    }
                    
                    // Fallback to standard Yello classes just in case
                    if (result.location === "N/A") {
                        let locElements = document.querySelectorAll('.job-details__location, .job-show__location, .location, [data-ph-at-id="job-location"]');
                        for (let el of locElements) {
                            let txt = el.innerText.trim();
                            if (txt) {
                                result.location = txt.replace(/Location\\s*:/i, '').trim();
                                break;
                            }
                        }
                    }
                    
                    // Extract Description utilizing the EY class
                    let desc = document.querySelector('.job-details__description, .job-show__description, .job-description, article');
                    if (desc) result.description = desc.innerHTML;
                    
                    return result;
                """)

                # --- EY DICTIONARY LOCATION PARSER ---
                location_raw = details['location'] if details['location'] != 'N/A' else 'N/A'
                city = "N/A"
                country = "N/A"

                if location_raw and location_raw != 'N/A':
                    # If it matches the EY format (e.g., "ITA-Milano")
                    if '-' in location_raw and len(location_raw.split('-')[0].strip()) in [2, 3]:
                        parts = location_raw.split('-', 1)
                        code = parts[0].strip().upper()

                        city = parts[1].strip()
                        # Translate the 3-letter code using our dictionary!
                        country = EY_COUNTRIES.get(code, code)
                    else:
                        # Fallback parsing for normal strings
                        primary_loc = location_raw.split(
                            '|')[0].split(';')[0].strip()
                        parts = [p.strip() for p in primary_loc.split(',')]

                        city = parts[0]
                        if len(parts) > 1:
                            if any(c.isdigit() for c in parts[-1]):
                                country = parts[-2] if len(
                                    parts) >= 3 else parts[1]
                            else:
                                country = parts[-1]
                        else:
                            country = parts[0]

                print(
                    f"   [{idx}/{len(job_links)}] Formatting: {job['title']} in {city}, {country}")

                # --- FORMAT DESCRIPTION ---
                if details['description']:
                    clean_desc = md(details['description'], heading_style="ATX",
                                    bullets="-", strip=['img', 'script', 'style', 'a', 'svg'])
                    clean_desc = clean_desc.replace(
                        '•', '-').replace('·', '-').replace('\xa0', ' ')
                    clean_desc = re.sub(r'\n{3,}', '\n\n', clean_desc).strip()
                else:
                    clean_desc = "Description could not be loaded."

                conn.execute('''INSERT INTO jobs (url, title, company, location_raw, city, country, description, category) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                             (job['url'], job['title'], COMPANY_NAME, location_raw, city, country, clean_desc, 'Consulting / Finance'))
                conn.commit()
                saved_count += 1

            except Exception as e:
                print(f"      ⚠️ Error on job {idx}: {e}")

        conn.close()
        print(f"\n✨ SUCCESS! {saved_count} {COMPANY_NAME} jobs safely saved.")

    finally:
        try:
            driver.quit()
        except Exception:
            pass


if __name__ == "__main__":
    run_scraper()
