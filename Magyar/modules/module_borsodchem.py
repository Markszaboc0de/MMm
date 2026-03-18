from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
import sqlite3
import os
import sys
import time
import re
from urllib.parse import urljoin

# UTF-8 kódolás kényszerítése a konzolhoz
sys.stdout.reconfigure(encoding='utf-8')

# --- KONFIGURÁCIÓ ---
COMPANY_NAME = "BorsodChem"
BASE_URL = "https://karrier.borsodchem.com/search/"
DOMAIN_URL = "https://karrier.borsodchem.com"
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "borsodchem_jobs.db")
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
    print(f"✅ Adatbázis inicializálva: {DB_PATH}")


def run_scraper():
    init_db()
    print(f"🚀 {COMPANY_NAME} Scraper indítása...")

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

        # --- 0. FÁZIS: ÖSSZES ÁLLÁS BETÖLTÉSE ---
        print("⏳ Álláslista kibontása (További találatok gomb)...")
        while True:
            try:
                load_more_btn = driver.find_elements(
                    By.ID, "tile-more-results")
                if load_more_btn and load_more_btn[0].is_displayed():
                    driver.execute_script(
                        "arguments[0].scrollIntoView({block: 'center'});", load_more_btn[0])
                    time.sleep(1)
                    driver.execute_script(
                        "arguments[0].click();", load_more_btn[0])
                    time.sleep(3)  # Várunk a betöltésre
                else:
                    break
            except:
                break

        # --- 1. FÁZIS: LINKEK ÉS LOKÁCIÓK GYŰJTÉSE ---
        job_list = []
        # A desktop verziójú kártyákat célozzuk meg
        job_cells = driver.find_elements(By.CSS_SELECTOR, ".job-tile-cell")

        for cell in job_cells:
            try:
                link_el = cell.find_element(By.CSS_SELECTOR, "a.jobTitle-link")
                url = urljoin(DOMAIN_URL, link_el.get_attribute("href"))
                title = link_el.text.strip()

                # Lokáció kinyerése a kártyáról (pl. Kazincbarcika)
                # A megadott HTML alapján a várost a desktop szekcióból vesszük
                city = "Ismeretlen"
                try:
                    city_div = cell.find_element(
                        By.CSS_SELECTOR, "[id*='desktop-section-city-value']")
                    city = city_div.text.strip()
                except:
                    pass

                job_list.append({
                    "url": url,
                    "title": title,
                    "city": city
                })
            except:
                continue

        print(f"🔍 Talált egyedi állások: {len(job_list)}")

        # --- 2. FÁZIS: RÉSZLETEK KINYERÉSE MÉLYFÚRÁSSAL ---
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        for idx, job in enumerate(job_list, 1):
            try:
                print(
                    f"   [{idx}/{len(job_list)}] {job['title']} ({job['city']})")
                driver.get(job['url'])
                time.sleep(6)

                # Speciális JS a széttördelt SuccessFactors leírásokhoz
                raw_description = driver.execute_script("""
                    function getDeepCleanText() {
                        let text = "";
                        let container = document.querySelector('.jobdescription') || 
                                        document.querySelector('.jobDescription') || 
                                        document.querySelector('.joqReqDescription') ||
                                        document.body;

                        function walk(el) {
                            if (el.nodeType === 3) {
                                let val = el.nodeValue.trim();
                                if (val) text += val + " ";
                            } else if (el.nodeType === 1) {
                                let tag = el.tagName;
                                if (['SCRIPT', 'STYLE', 'BUTTON', 'NAV', 'FOOTER'].includes(tag)) return;
                                for (let child of el.childNodes) { walk(child); }
                                if (['P', 'DIV', 'BR', 'LI', 'TR', 'H1', 'H2', 'H3'].includes(tag)) text += "\\n";
                            }
                        }
                        walk(container);
                        return text;
                    }
                    return getDeepCleanText();
                """)

                # Tisztítás
                clean_desc = re.sub(r'[ \t]+', ' ', raw_description)
                clean_desc = re.sub(r'\n\s*\n', '\n\n', clean_desc).strip()

                # Sallangok (pl. gomb szövege) eltávolítása a biztonság kedvéért
                clean_desc = clean_desc.split("További találatok")[0].strip()

                cursor.execute('''
                    INSERT OR IGNORE INTO jobs (url, title, company, location_raw, city, country, description)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (job['url'], job['title'], "BorsodChem", job['city'], job['city'], "Magyarország", clean_desc))
                conn.commit()
            except Exception as e:
                print(f"      ⚠️ Hiba a részleteknél: {e}")

        conn.close()
        print(f"\n✨ KÉSZ! BorsodChem állások mentve.")

    finally:
        driver.quit()


if __name__ == "__main__":
    run_scraper()
