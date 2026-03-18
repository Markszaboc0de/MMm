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

sys.stdout.reconfigure(encoding='utf-8')

# --- KONFIGURÁCIÓ ---
COMPANY_NAME = "RSM"
BASE_URL = "https://www.rsm.hu/karrier/csatlakozz-hozzank"
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "rsm_jobs.db")


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


def run_scraper():
    init_db()
    print(f"🚀 {COMPANY_NAME} Scraper indítása (Static List & DOM Walker mód)...")

    options = Options()
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 10)

    job_links = []
    unique_urls = set()

    try:
        # --- 1. FÁZIS: LINKEK GYŰJTÉSE ---
        print("📂 RSM karrieroldal megnyitása...")
        driver.get(BASE_URL)
        time.sleep(4)

        # Cookie ablak bezárása (ha van)
        try:
            cookie_btn = driver.find_element(
                By.XPATH, "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'elfogad')] | //a[contains(text(), 'Elfogadom')]")
            driver.execute_script("arguments[0].click();", cookie_btn)
            time.sleep(1)
        except:
            pass

        print("🔍 Álláskártyák beolvasása...")

        # A megadott HTML alapján kinyerjük a linkeket a .bottom-container-ből
        jobs_on_page = driver.execute_script("""
            let results = [];
            let cards = document.querySelectorAll('.bottom-container');
            
            cards.forEach(card => {
                let linkEl = card.querySelector('.node-title a');
                
                if (linkEl && linkEl.href) {
                    let title = linkEl.innerText.trim();
                    
                    // Kinyerhetjük a munkaidőt is, ha szükség lenne rá, de most a lokációt fixáljuk
                    results.push({
                        url: linkEl.href,
                        title: title,
                        location_raw: 'Budapest' // Az RSM irodája Budapesten van
                    });
                }
            });
            return results;
        """)

        for job in jobs_on_page:
            if job['url'] not in unique_urls:
                unique_urls.add(job['url'])
                job_links.append(job)

        print(f"✅ {len(job_links)} állás azonosítva. Kezdődik a mélyfúrás...")

        # --- 2. FÁZIS: LEÍRÁSOK SCRAPELÉSE ---
        conn = sqlite3.connect(DB_PATH)
        for idx, job in enumerate(job_links, 1):
            try:
                conn.execute("DELETE FROM jobs WHERE url = ?", (job['url'],))

                print(f"   [{idx}/{len(job_links)}] {job['title']}")
                driver.get(job['url'])

                try:
                    # Megvárjuk az egyedi Drupal osztályt
                    wait.until(EC.presence_of_element_located(
                        (By.CSS_SELECTOR, ".field--name--field_text")))
                    time.sleep(1.5)
                except:
                    time.sleep(3)

                # CÉLZOTT DOM BEJÁRÓ A FORMÁZOTT SZÖVEGÉRT
                description = driver.execute_script("""
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
                            
                            for (let child of el.childNodes) { 
                                text += walk(child); 
                            }
                            
                            if (['H1','H2','H3','H4'].includes(tag)) text += "\\n\\n";
                            else if (['P','DIV','BR','LI'].includes(tag)) text += "\\n";
                        }
                        return text;
                    }

                    // Kifejezetten a beküldött szöveg konténerét célozzuk
                    let mainContent = document.querySelector('.field--name--field_text');
                                      
                    if (mainContent) {
                        return walk(mainContent);
                    } else {
                        // Tartalék
                        let fallback = document.querySelector('article') || document.querySelector('main');
                        return fallback ? walk(fallback) : walk(document.body);
                    }
                """)

                # Felesleges sortörések eltávolítása
                clean_desc = re.sub(r'\n{3,}', '\n\n', description).strip()

                # Vágjuk le a "Jelentkezés" gomb instrukcióit és a közösségi média linkeket a végéről
                if "Önéletrajzod a pozíció megjelölésével" in clean_desc:
                    clean_desc = clean_desc.split(
                        "Önéletrajzod a pozíció megjelölésével")[0].strip()
                elif "Jelentkezés gombra kattintva" in clean_desc:
                    clean_desc = clean_desc.split(
                        "Jelentkezés gombra kattintva")[0].strip()

                # RSM alapértelmezett adatok
                location_raw = "Budapest"
                city = "Budapest"
                country = "Magyarország"
                category = "Pénzügy / Adótanácsadás / Jog"

                conn.execute('''INSERT INTO jobs (url, title, company, location_raw, city, country, description, category) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                             (job['url'], job['title'], "RSM", location_raw, city, country, clean_desc, category))
                conn.commit()

            except Exception as e:
                print(f"      ⚠️ Hiba a(z) {idx}. állásnál: {e}")

        conn.close()
        print(f"\n✨ SIKER! Az RSM pozíciók elmentve.")

    finally:
        driver.quit()


if __name__ == "__main__":
    run_scraper()
