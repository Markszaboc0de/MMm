import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import sqlite3
import os
import sys
import time
import re
from urllib.parse import urljoin

# UTF-8 kódolás kényszerítése
sys.stdout.reconfigure(encoding='utf-8')

# --- KONFIGURÁCIÓ ---
COMPANY_NAME = "Alfa_Biztosito"
BASE_URL = "https://karrier.alfa.hu/allasok"
DOMAIN_URL = "https://karrier.alfa.hu"
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "alfa_jobs.db")
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
    wait = WebDriverWait(driver, 15)

    try:
        driver.get(BASE_URL)
        time.sleep(5)

        # 1. LÉPÉS: Cookie ablak elfogadása
        try:
            cookie_btn = wait.until(EC.element_to_be_clickable(
                (By.ID, "CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll")))
            cookie_btn.click()
            print("✅ Cookie-k elfogadva.")
            time.sleep(2)
        except:
            print("ℹ️ Cookie ablak nem jelent meg vagy már el lett fogadva.")

        # 2. LÉPÉS: Álláslinkek gyűjtése
        job_links = []
        job_elements = driver.find_elements(
            By.CSS_SELECTOR, "a[href*='/allas/']")

        for el in job_elements:
            try:
                url = el.get_attribute("href")
                title = el.text.strip()
                if url and title and url not in [j['url'] for j in job_links]:
                    job_links.append({'url': url, 'title': title})
            except:
                continue

        print(f"📊 Talált állások száma: {len(job_links)}")

        # 3. LÉPÉS: Részletek kinyerése
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        for idx, job in enumerate(job_links, 1):
            try:
                cursor.execute(
                    "SELECT 1 FROM jobs WHERE url = ?", (job['url'],))
                if cursor.fetchone():
                    continue

                print(f"   [{idx}/{len(job_links)}] {job['title']}")
                driver.get(job['url'])
                time.sleep(3)

                # JAVÍTOTT JavaScript: Hibatűrőbb osztály- és ID ellenőrzés
                raw_description = driver.execute_script("""
                    function getCleanJobText() {
                        let container = document.querySelector('.vacancy-details') || 
                                        document.querySelector('article') || 
                                        document.querySelector('.content') || 
                                        document.body;

                        let text = "";
                        function walk(el) {
                            if (!el) return;
                            
                            if (el.nodeType === 3) {
                                text += el.nodeValue.trim() + " ";
                            } else if (el.nodeType === 1) {
                                let tag = el.tagName.toUpperCase();
                                
                                // Biztonságos ellenőrzés ID-ra és ClassName-re
                                let elId = el.id ? String(el.id).toLowerCase() : "";
                                let elClass = el.className && typeof el.className === 'string' ? el.className.toLowerCase() : "";
                                
                                if (['SCRIPT', 'STYLE', 'BUTTON', 'NAV', 'FOOTER', 'NOSCRIPT'].includes(tag)) return;
                                if (elId.includes('cookie') || elClass.includes('cookie')) return;
                                
                                for (let child of el.childNodes) { walk(child); }
                                if (['P', 'DIV', 'BR', 'LI', 'TR', 'H1', 'H2', 'H3'].includes(tag)) text += "\\n";
                            }
                        }
                        walk(container);
                        return text;
                    }
                    return getCleanJobText();
                """)

                # Szöveg tisztítása
                clean_desc = re.sub(r'[ \t]+', ' ', raw_description)
                clean_desc = re.sub(r'\n\s*\n', '\n\n', clean_desc)
                clean_desc = clean_desc.split("Megnézem")[0].strip()

                cursor.execute('''
                    INSERT OR IGNORE INTO jobs (url, title, company, location_raw, city, country, description)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (job['url'], job['title'], "Alfa Biztosító", "Budapest, Magyarország", "Budapest", "Magyarország", clean_desc))
                conn.commit()
            except Exception as e:
                print(f"      ⚠️ Hiba ennél az állásnál: {e}")

        conn.close()
        print(f"\n✨ KÉSZ! Az Alfa Biztosító állások elmentve.")

    finally:
        driver.quit()


if __name__ == "__main__":
    run_scraper()
