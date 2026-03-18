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
from urllib.parse import urljoin

sys.stdout.reconfigure(encoding='utf-8')

# --- CONFIGURATION ---
COMPANY_NAME = "GE Vernova"
DOMAIN_URL = "https://careers.gevernova.com"
# Filtered URL from the prompt (Europe + Early Career/Interns)
BASE_URL = "https://careers.gevernova.com/jobs?filter%5Bcf_custom_mapping%5D%5B0%5D=Apprentice&filter%5Bcf_custom_mapping%5D%5B1%5D=Co-op%2FIntern&filter%5Bcf_custom_mapping%5D%5B2%5D=Development%20Program&filter%5Bcf_custom_mapping%5D%5B3%5D=Early%20Career&filter%5Bcountry%5D%5B0%5D=Austria&filter%5Bcountry%5D%5B1%5D=France&filter%5Bcountry%5D%5B2%5D=Germany&filter%5Bcountry%5D%5B3%5D=Greece&filter%5Bcountry%5D%5B4%5D=Hungary&filter%5Bcountry%5D%5B5%5D=Italy&filter%5Bcountry%5D%5B6%5D=Norway&filter%5Bcountry%5D%5B7%5D=Poland&filter%5Bcountry%5D%5B8%5D=Romania&filter%5Bcountry%5D%5B9%5D=Spain&filter%5Bcountry%5D%5B10%5D=Switzerland"
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Manual\data"
DB_PATH = os.path.join(DATA_FOLDER, "gevernova_jobs.db")

# Country code mapper based on typical European ATS data
COUNTRY_MAP = {
    "AT": "Austria", "FR": "France", "DE": "Germany", "GR": "Greece",
    "HU": "Hungary", "IT": "Italy", "NO": "Norway", "PL": "Poland",
    "RO": "Romania", "ES": "Spain", "CH": "Switzerland", "CZ": "Czechia",
    "GB": "United Kingdom", "UK": "United Kingdom", "US": "United States"
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


def run_scraper():
    init_db()
    print(
        f"🚀 Starting {COMPANY_NAME} Scraper (URL Pagination & Country Parsing Mode)...")

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
        print("📂 Opening GE Vernova career site...")
        driver.get(BASE_URL)
        time.sleep(5)

        # Handle Cookie Banner (if present)
        try:
            cookie_btn = driver.find_element(
                By.XPATH, "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'accept') or contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'allow')]")
            driver.execute_script("arguments[0].click();", cookie_btn)
            time.sleep(1)
        except:
            pass

        page_num = 1

        while True:

            print(f"📄 Scraping data from page {page_num}...")

            try:
                WebDriverWait(driver, 10).until(EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "a.results-list__item-title--link")))
            except:
                print("⚠️ No job cards found, list might be empty.")
                break

            # Extract links
            jobs_on_page = driver.execute_script("""
                let results = [];
                let links = document.querySelectorAll('a.results-list__item-title--link');
                
                links.forEach(a => {
                    if (a.href) {
                        results.push({
                            url: a.href,
                            title: a.innerText.trim(),
                            category: 'Engineering / Technology'
                        });
                    }
                });
                return results;
            """)

            if not jobs_on_page:
                break

            for job in jobs_on_page:
                full_url = urljoin(DOMAIN_URL, job['url'])
                if full_url not in unique_urls:
                    unique_urls.add(full_url)
                    job['url'] = full_url
                    job_links.append(job)

            # PAGINATION: Check for next page URL
            try:
                next_button = driver.find_element(
                    By.CSS_SELECTOR, "a.page-link-next.selectable")
                next_url = next_button.get_attribute("href")

                if next_url:
                    full_next_url = urljoin(DOMAIN_URL, next_url)
                    driver.get(full_next_url)
                    page_num += 1
                    time.sleep(4)  # Wait for next page to load
                else:
                    print("🏁 Next button has no URL, reached the end of the list.")
                    break
            except:
                print(
                    "🏁 No 'Next' button found or it is disabled. Reached the end of the list.")
                break

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
                    wait.until(EC.presence_of_element_located(
                        (By.CSS_SELECTOR, ".description")))
                    time.sleep(1)
                except:
                    time.sleep(2)

                # DOM Walker + Precise Location Extractor from the dl/dt/dd structure
                details = driver.execute_script("""
                    // 1. Extract location from the summary list
                    let exactLoc = "N/A";
                    let listItems = document.querySelectorAll('.summary-list .summary-list-item');
                    listItems.forEach(item => {
                        let label = item.querySelector('.summary-label');
                        if(label && label.innerText.toLowerCase().includes('location')) {
                            let val = item.querySelector('.summary-value');
                            if(val) exactLoc = val.innerText.trim();
                        }
                    });

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
                            else if (['P','DIV','BR','LI'].includes(tag)) text += "\\n";
                        }
                        return text;
                    }

                    let mainContent = document.querySelector('.description');
                    let descText = mainContent ? walk(mainContent) : walk(document.body);
                    
                    return { description: descText, location: exactLoc };
                """)

                clean_desc = re.sub(r'\n{3,}', '\n\n',
                                    details['description']).strip()

                # GE Vernova specific footer cleanup (PR templates)
                truncation_markers = [
                    "Additional Information",
                    "Relocation Assistance Provided"
                ]
                for marker in truncation_markers:
                    if marker in clean_desc:
                        clean_desc = clean_desc.split(marker)[0].strip()

                # --- DYNAMIC LOCATION PARSING (City and Country Code) ---
                location_raw = details['location']
                city = location_raw
                country = "N/A"

                if location_raw and location_raw != 'N/A':
                    # Parse formats like "Villeurbanne FR 2" or "Baden CH"
                    tokens = location_raw.split()

                    # Scan backwards to find the 2-letter uppercase country code
                    for i in range(len(tokens)-1, -1, -1):
                        token = tokens[i]
                        # Clean token of commas just in case
                        clean_token = token.replace(',', '').strip()
                        if len(clean_token) == 2 and clean_token.isupper():
                            # We found the country code!
                            country_code = clean_token
                            country = COUNTRY_MAP.get(
                                country_code, country_code)
                            # Everything before this code is the city
                            city = " ".join(tokens[:i]).replace(
                                ',', '').strip()
                            break

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
