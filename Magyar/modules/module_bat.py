import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
import sqlite3
import os
import sys
import time
import re
from urllib.parse import urljoin

# UTF-8 kódolás kényszerítése
sys.stdout.reconfigure(encoding='utf-8')

# --- KONFIGURÁCIÓ ---
COMPANY_NAME = "BAT_Hungary"
BASE_URL = "https://karrier.bat.hu/Datacenter/Batkarrier/Nyitottpoziciok"
DOMAIN_URL = "https://karrier.bat.hu"
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "bat_jobs.db")
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

    options = uc.ChromeOptions()
    options.add_argument("--window-size=1920,1080")

    options.add_argument("--headless=new")
    driver = uc.Chrome(options=options)

    try:
        driver.get(BASE_URL)
        time.sleep(5)

        # --- 1. FÁZIS: LINKEK ÉS LOKÁCIÓK GYŰJTÉSE ---
        job_list = []
        # A megadott HTML struktúra alapján keressük a munkákat
        job_elements = driver.find_elements(
            By.CSS_SELECTOR, "a.batPositions__job")

        for el in job_elements:
            try:
                url = urljoin(DOMAIN_URL, el.get_attribute("href"))
                title = el.find_element(
                    By.CSS_SELECTOR, ".position-name").text.strip()

                # DINAMIKUS LOKÁCIÓ KINYERÉSE
                # Megkeressük a lokáció szekciót a kártyán belül
                loc_desc = el.find_element(
                    By.CSS_SELECTOR, ".batPositions__item--location .batPositions__item-desc").text.strip()

                job_list.append({
                    "url": url,
                    "title": title,
                    "city": loc_desc
                })
            except Exception as e:
                continue

        print(f"🔍 Talált nyitott pozíciók: {len(job_list)}")

        # --- 2. FÁZIS: RÉSZLETEK KINYERÉSE MÉLYFÚRÁSSAL ---
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        for idx, job in enumerate(job_list, 1):
            try:
                print(
                    f"   [{idx}/{len(job_list)}] {job['title']} ({job['city']})")
                driver.get(job['url'])
                time.sleep(5)

                # Robusztus JS leírás kinyerő (bejárja az összes szöveges node-ot)
                full_description = driver.execute_script("""
                    function getDeepCleanText() {
                        let text = "";
                        // A BAT oldalán gyakran a 'batJob' vagy 'content' osztályban van a lényeg
                        let container = document.querySelector('.batJob__details') || 
                                        document.querySelector('.batJob') || 
                                        document.querySelector('main') || 
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

                # Szöveg tisztítása
                clean_desc = re.sub(r'[ \t]+', ' ', full_description)
                clean_desc = re.sub(r'\n\s*\n', '\n\n', clean_desc).strip()

                cursor.execute('''
                    INSERT OR IGNORE INTO jobs (url, title, company, location_raw, city, country, description)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (job['url'], job['title'], "BAT Hungary", job['city'], job['city'], "Magyarország", clean_desc))
                conn.commit()

            except Exception as e:
                print(f"      ⚠️ Hiba a részletek kinyerésekor: {e}")

        conn.close()
        print(f"\n✨ KÉSZ! A BAT állások mentve (dinamikus lokációval).")

    finally:
        driver.quit()


if __name__ == "__main__":
    run_scraper()
