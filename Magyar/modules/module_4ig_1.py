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

# UTF-8 kódolás kényszerítése
sys.stdout.reconfigure(encoding='utf-8')

# --- KONFIGURÁCIÓ ---
COMPANY_NAME = "4iG_1"
BASE_URL = "https://karrier.4ig.hu/allasok"
DOMAIN_URL = "https://karrier.4ig.hu"
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "4ig_jobs.db")
CHROME_VERSION = 145


def init_db():
    """Létrehozza a táblát. Ha már létezik, nem nyúl hozzá."""
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    # Fontos: Minden oszlopnak szerepelnie kell itt!
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
    print(f"✅ Adatbázis ellenőrizve: {DB_PATH}")


def run_scraper():
    # 1. LÉPÉS: Ez KELL az elejére, hogy ne legyen 'no such table' hiba
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
        # --- 1. FÁZIS: LINKEK GYŰJTÉSE ---
        driver.get(BASE_URL)
        job_list = []
        unique_urls = set()
        current_page = 1

        while True:
            print(f"📄 {current_page}. oldal linkjeinek gyűjtése...")
            time.sleep(5)

            links = driver.find_elements(
                By.CSS_SELECTOR, "a.jobList__item__title")
            for link in links:
                url = urljoin(DOMAIN_URL, link.get_attribute("href"))
                title = link.text.strip()
                if url and url not in unique_urls:
                    unique_urls.add(url)
                    job_list.append({"url": url, "title": title})

            try:
                next_page = current_page + 1
                next_btn = driver.find_elements(
                    By.XPATH, f"//button[text()='{next_page}']")
                if next_btn:
                    driver.execute_script(
                        "arguments[0].scrollIntoView({block: 'center'});", next_btn[0])
                    time.sleep(1)
                    driver.execute_script("arguments[0].click();", next_btn[0])
                    current_page += 1
                else:
                    break
            except:
                break

        print(f"🔍 Talált egyedi állások: {len(job_list)}")

        # --- 2. FÁZIS: ADATGYŰJTÉS MÉLYFÚRÁSSAL ---
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        for idx, job in enumerate(job_list, 1):
            try:
                print(f"   [{idx}/{len(job_list)}] {job['title']}")
                driver.get(job['url'])
                time.sleep(7)  # Több idő a Rheinmetall-os JS-nek

                # SPECIÁLIS MÉLYSZÖVEG KINYERŐ (Minden egyes span/div tartalmát kimenti)
                raw_desc = driver.execute_script("""
                    function getDeepText() {
                        let text = "";
                        let container = document.querySelector('.jobdescription') || 
                                        document.querySelector('.job-description') || 
                                        document.querySelector('article') || 
                                        document.body;

                        function walk(el) {
                            if (el.nodeType === 3) { // Szöveg csomópont
                                let val = el.nodeValue.trim();
                                if (val) text += val + " ";
                            } else if (el.nodeType === 1) { // Elem csomópont
                                if (['SCRIPT', 'STYLE', 'BUTTON', 'NAV', 'FOOTER'].includes(el.tagName)) return;
                                for (let child of el.childNodes) { walk(child); }
                                if (['P', 'DIV', 'BR', 'LI', 'H1', 'H2', 'H3'].includes(el.tagName)) text += "\\n";
                            }
                        }
                        walk(container);
                        return text;
                    }
                    return getDeepText();
                """)

                # TISZTÍTÁS (Hatalmas üres helyek és sortörések javítása)
                clean_desc = re.sub(r'[ \t]+', ' ', raw_desc)
                clean_desc = re.sub(r'\n\s*\n', '\n\n', clean_desc)
                # A "Részletek" gomb levágása, ha benne maradt
                clean_desc = clean_desc.split("Részletek")[0].strip()

                cursor.execute('''
                    INSERT OR IGNORE INTO jobs (url, title, company, location_raw, city, country, description)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (job['url'], job['title'], "4iG", "Budapest, Magyarország", "Budapest", "Magyarország", clean_desc))
                conn.commit()
            except Exception as e:
                print(f"      ⚠️ Hiba ennél az állásnál: {e}")

        conn.close()
        print(f"\n✨ KÉSZ! Az összes állás elmentve ide: {DB_PATH}")

    finally:
        driver.quit()


if __name__ == "__main__":
    run_scraper()
