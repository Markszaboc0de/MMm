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
COMPANY_NAME = "Fundamenta"
BASE_URL = "https://karrier.fundamenta.hu/allasok"
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "fundamenta_jobs.db")
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
    init_db()
    print(f"🚀 {COMPANY_NAME} Scraper indítása (Targeted JobEnd mód)...")

    options = uc.ChromeOptions()
    options.add_argument("--window-size=1920,1080")
    driver = uc.Chrome(options=options, version_main=CHROME_VERSION)
    wait = WebDriverWait(driver, 15)

    try:
        driver.get(BASE_URL)
        time.sleep(5)

        # 1. LÉPÉS: Cookie ablak elfogadása
        try:
            cookie_btn = wait.until(EC.element_to_be_clickable(
                (By.XPATH, "//button[contains(text(),'Elfogadom') or contains(text(),'ELFOGADOM')]")))
            cookie_btn.click()
            time.sleep(2)
        except:
            pass

        # 2. LÉPÉS: Álláslinkek gyűjtése
        job_links = []
        job_elements = driver.find_elements(
            By.CSS_SELECTOR, "a[href*='/allas/']")
        for el in job_elements:
            url = el.get_attribute("href")
            title = el.text.strip()
            if url and title and "/allas/" in url and url not in [j['url'] for j in job_links]:
                job_links.append({'url': url, 'title': title})

        print(f"📊 {len(job_links)} állás azonosítva. Részletes kinyerés indítása...")

        # 3. LÉPÉS: Adatgyűjtés
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        for idx, job in enumerate(job_links, 1):
            try:
                # Régi, hibás adatok törlése a felülíráshoz
                cursor.execute("DELETE FROM jobs WHERE url = ?", (job['url'],))

                print(f"   [{idx}/{len(job_links)}] {job['title']}")
                driver.get(job['url'])
                time.sleep(3)

                # JAVÍTOTT JavaScript: A beküldött HTML struktúrára optimalizálva
                raw_description = driver.execute_script("""
                    function getJobDescription() {
                        // Kifejezetten a hirdetés végén lévő feladatokat és leírást tartalmazó részt keressük
                        let container = document.querySelector('.jobEnd__tasks') || 
                                        document.querySelector('.jobEnd') || 
                                        document.querySelector('.vacancy-details') || 
                                        document.querySelector('article');
                        
                        if (!container) return "Hiba: A hirdetési konténer nem található a DOM-ban.";

                        let text = "";
                        function walk(el) {
                            if (!el) return;
                            
                            // Szöveges csomópont
                            if (el.nodeType === 3) {
                                let val = el.nodeValue.trim();
                                if (val) text += val + " ";
                            } 
                            // HTML elem
                            else if (el.nodeType === 1) {
                                let tag = el.tagName.toUpperCase();
                                let cls = (el.className && typeof el.className === 'string') ? el.className.toLowerCase() : "";
                                
                                // Kizárjuk a navigációt, láblécet és a listákat (hasonló állások)
                                if (['SCRIPT', 'STYLE', 'NAV', 'FOOTER', 'HEADER', 'BUTTON'].includes(tag)) return;
                                if (cls.includes('joblist') || cls.includes('similar')) return;

                                // Listaelemek jelölése kis golyóval a jobb olvashatóságért
                                if (tag === 'LI') text += "• ";

                                for (let child of el.childNodes) { walk(child); }
                                
                                // Sortörések kezelése a struktúra megőrzéséhez
                                if (['P', 'DIV', 'BR', 'LI', 'TR', 'H1', 'H2', 'H3', 'H4'].includes(tag)) {
                                    text += "\\n";
                                }
                            }
                        }
                        walk(container);
                        return text;
                    }
                    return getJobDescription();
                """)

                # Szövegtisztítás
                clean_desc = re.sub(r'[ \t]+', ' ', raw_description)
                clean_desc = re.sub(r'\n\s*\n', '\n\n', clean_desc).strip()

                # Mentés az adatbázisba
                cursor.execute('''
                    INSERT INTO jobs (url, title, company, location_raw, city, country, description)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (job['url'], job['title'], "Fundamenta", "Budapest", "Budapest", "Magyarország", clean_desc))
                conn.commit()

            except Exception as e:
                print(f"      ⚠️ Hiba az adatgyűjtés során: {e}")

        conn.close()
        print(f"\n✨ SIKER! A Fundamenta leírások frissítve.")

    finally:
        driver.quit()


if __name__ == "__main__":
    run_scraper()
