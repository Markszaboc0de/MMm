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
COMPANY_NAME = "4iG_3"
BASE_URL = "https://karrier.4iggroup.hu/it/go/%C3%81ll%C3%A1saink-%28IT%29/9375555/"
DOMAIN_URL = "https://karrier.4iggroup.hu"
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, f"{COMPANY_NAME.lower()}_jobs.db")
CHROME_VERSION = 145


def init_db():
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)
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
        date_found TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    conn.commit()
    conn.close()


def run_scraper():
    print(f"🚀 {COMPANY_NAME} Scraper indítása (Duplikáció szűréssel)...")
    init_db()

    options = Options()
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    driver = webdriver.Chrome(options=options)
    try:
        driver.get(BASE_URL)
        time.sleep(5)

        # 0. "További találatok" gomb kezelése
        print("⏳ Állások kibontása...")
        while True:
            try:
                btn = driver.find_elements(By.ID, "tile-more-results")
                if btn and btn[0].is_displayed():
                    driver.execute_script("arguments[0].click();", btn[0])
                    time.sleep(3)
                else:
                    break
            except:
                break

        # 1. Linkek gyűjtése SZŰRÉSSEL
        # Halmazt (set) használunk, hogy az azonos URL-ek ne kerüljenek be többször
        unique_jobs = {}

        links = driver.find_elements(By.CSS_SELECTOR, "a.jobTitle-link")
        for link in links:
            url = urljoin(DOMAIN_URL, link.get_attribute("href"))
            title = link.text.strip()

            if url and title and url not in unique_jobs:
                unique_jobs[url] = title

        # Átalakítjuk listává a feldolgozáshoz
        job_list = [{"url": u, "title": t} for u, t in unique_jobs.items()]

        print(
            f"🔍 Talált egyedi állások száma: {len(job_list)} (Szűrés előtt: {len(links)})")

        # 2. Adatgyűjtés
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        for idx, job in enumerate(job_list, 1):
            print(f"   [{idx}/{len(job_list)}] Feldolgozás: {job['title']}")
            driver.get(job['url'])
            time.sleep(6)

            # Mély leírás kinyerése
            description = driver.execute_script("""
                var selectors = ['.jobdescription', '.jobDescription', '.joqReqDescription', 'div.content'];
                var target = null;
                for (var s of selectors) {
                    var el = document.querySelector(s);
                    if (el && el.innerText.length > 100) { target = el; break; }
                }
                if (!target) target = document.body;
                
                var clone = target.cloneNode(true);
                var junk = clone.querySelectorAll('button, nav, footer, script, style, .header');
                junk.forEach(j => j.remove());
                return clone.innerText;
            """)

            cursor.execute('''
                INSERT OR IGNORE INTO jobs (url, title, company, location_raw, city, country, description)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (job['url'], job['title'], "4iG Group", "Budapest, HU", "Budapest", "Magyarország", description.strip()))

            conn.commit()

        conn.close()
        print(f"\n✨ KÉSZ! Összesen {len(job_list)} egyedi állás mentve.")

    finally:
        driver.quit()


if __name__ == "__main__":
    run_scraper()
