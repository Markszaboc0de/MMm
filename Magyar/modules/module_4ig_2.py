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
from urllib.parse import urljoin

sys.stdout.reconfigure(encoding='utf-8')

# ==========================================
# ⚙️ KONFIGURÁCIÓ
# ==========================================
COMPANY_NAME = "4iG_2"
BASE_URL = "https://karrier.4iggroup.hu/go/%C3%81ll%C3%A1saink/9360455/"
DOMAIN_URL = "https://karrier.4iggroup.hu"
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, f"{COMPANY_NAME.lower()}_jobs.db")
CHROME_VERSION = 145


def init_db():
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)
    conn = sqlite3.connect(DB_PATH)
    # Kibővített séma a city és country mezőkkel
    conn.execute('''CREATE TABLE IF NOT EXISTS jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT, 
        url TEXT UNIQUE, 
        title TEXT, 
        company TEXT, 
        location_raw TEXT,
        city TEXT, 
        country TEXT,
        description TEXT, 
        date_found TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    conn.close()


def run_scraper():
    print(f"🚀 {COMPANY_NAME} Scraper indítása (Fix város: Budapest)...")
    init_db()

    options = Options()
    options.add_argument("--window-size=1280,1024")
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.binary_location = "/usr/bin/chromium-browser"
    _service = Service(executable_path="/usr/bin/chromedriver")
    driver = webdriver.Chrome(service=_service, options=options)
    try:
        driver.get(BASE_URL)
        time.sleep(5)

        # --- FÁZIS 0: ÖSSZES ÁLLÁS BETÖLTÉSE ---
        while True:
            try:
                load_more_btn = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.ID, "tile-more-results"))
                )
                if not load_more_btn.is_displayed():
                    break
                driver.execute_script(
                    "arguments[0].scrollIntoView({block: 'center'});", load_more_btn)
                time.sleep(1)
                driver.execute_script("arguments[0].click();", load_more_btn)
                time.sleep(3)
            except:
                break

        # --- 1. FÁZIS: LINKEK GYŰJTÉSE ---
        job_list = []
        links = driver.find_elements(By.CSS_SELECTOR, "a.jobTitle-link")
        for link in links:
            href = link.get_attribute("href")
            title = link.text.strip()
            if href and title:
                url = urljoin(DOMAIN_URL, href)
                if url not in [j['url'] for j in job_list]:
                    job_list.append({"url": url, "title": title})

        print(f"🔍 Talált állások: {len(job_list)}")

        # --- 2. FÁZIS: ADATGYŰJTÉS ---
        conn = sqlite3.connect(DB_PATH)
        for idx, job in enumerate(job_list, 1):
            print(f"   [{idx}/{len(job_list)}] {job['title']}")
            driver.get(job['url'])
            time.sleep(5)

            description = driver.execute_script("""
                var content = document.querySelector('.jobdescription') || 
                               document.querySelector('.jobDescription') || 
                               document.querySelector('.joqReqDescription') ||
                               document.body;
                return content.innerText;
            """)

            # Mentés fix Budapest adatokkal
            conn.execute('''INSERT OR IGNORE INTO jobs 
                            (url, title, company, location_raw, city, country, description)
                            VALUES (?, ?, ?, ?, ?, ?, ?)''',
                         (job['url'],
                          job['title'],
                          "4iG Group",
                          "Budapest, Magyarország",
                          "Budapest",
                          "Magyarország",
                          description.strip()))
            conn.commit()

        conn.close()
        print(f"\n✨ SIKER! Az adatok mentve Budapest várossal ide: {DB_PATH}")

    finally:
        driver.quit()


if __name__ == "__main__":
    run_scraper()
