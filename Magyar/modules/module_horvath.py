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
COMPANY_NAME = "IFUA_Horvath"
BASE_URL = "https://www.horvath-partners.com/hu/karrier/allaspalyazat/allasajanlatok/diakoknak"
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "ifuahorvath_jobs.db")


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
    print(f"🚀 {COMPANY_NAME} Scraper indítása (DOM Walker formázó mód)...")

    options = Options()
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.binary_location = "/usr/bin/chromium-browser"
    _service = Service(executable_path="/usr/bin/chromedriver")
    driver = webdriver.Chrome(service=_service, options=options)
    wait = WebDriverWait(driver, 10)

    job_links = []
    unique_urls = set()

    try:
        # --- 1. FÁZIS: LINKEK GYŰJTÉSE ---
        print("📂 IFUA Horváth karrieroldal megnyitása...")
        driver.get(BASE_URL)
        time.sleep(4)

        try:
            cookie_btn = driver.find_element(
                By.XPATH, "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'elfogad')] | //button[contains(@class, 'uc-accept-all')]")
            driver.execute_script("arguments[0].click();", cookie_btn)
            time.sleep(1)
        except:
            pass

        print("🔍 Álláskártyák beolvasása...")

        jobs_on_page = driver.execute_script("""
            let results = [];
            let cards = document.querySelectorAll('a.component-teaserlist-text');
            
            cards.forEach(card => {
                if (card.href) {
                    let titleEl = card.querySelector('.text');
                    results.push({
                        url: card.href,
                        title: titleEl ? titleEl.innerText.trim() : 'Ismeretlen pozíció'
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
                    wait.until(EC.presence_of_element_located(
                        (By.CSS_SELECTOR, ".col-8")))
                    time.sleep(1.5)
                except:
                    time.sleep(3)

                # CÉLZOTT DOM BEJÁRÓ A TÖKÉLETES FORMÁZÁSÉRT
                description = driver.execute_script("""
                    function walk(el) {
                        let text = "";
                        if (!el) return "";
                        if (el.nodeType === 3) {
                            let val = el.nodeValue.trim();
                            if (val) text += val + " ";
                        } else if (el.nodeType === 1) {
                            let tag = el.tagName.toUpperCase();

                            // Szigorú kizárások (ne kerüljön bele rejtett script vagy gomb)
                            if (['SCRIPT','STYLE','BUTTON','SVG'].includes(tag)) return "";

                            // Ha lista elem jön, tegyünk elé egy pontot
                            if (tag === 'LI') text += "• ";
                            
                            // Bejárjuk a gyermek elemeket
                            for (let child of el.childNodes) { 
                                text += walk(child); 
                            }
                            
                            // Sortörések a bekezdéseknél és fejléceknél
                            if (['P','DIV','BR','LI','H1','H2','H3','H4'].includes(tag)) text += "\\n";
                        }
                        return text;
                    }

                    // A megadott HTML alapján a col-8 divet célozzuk, ami a col-container-ben van
                    let mainContent = document.querySelector('.col-container--66-25 .col-8') || 
                                      document.querySelector('.col-8.col-1-before');
                                      
                    if (mainContent) {
                        return walk(mainContent);
                    } else {
                        // Tartalék
                        return walk(document.querySelector('main') || document.body);
                    }
                """)

                # Tisztítás: felesleges extra sortörések és szóközök eltávolítása
                clean_desc = re.sub(r'\n\s*\n', '\n\n', description).strip()

                # Vágjuk le a közösségi média hivatkozásokat a végéről, ha bekerültek
                if "Követessen minket" in clean_desc:
                    clean_desc = clean_desc.split(
                        "Követessen minket")[0].strip()
                elif "Kövessen minket" in clean_desc:
                    clean_desc = clean_desc.split("Kövessen minket")[0].strip()

                location_raw = "Budapest"
                city = "Budapest"
                country = "Magyarország"
                category = "Tanácsadás / Gyakornok"

                conn.execute('''INSERT INTO jobs (url, title, company, location_raw, city, country, description, category) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                             (job['url'], job['title'], "IFUA Horváth", location_raw, city, country, clean_desc, category))
                conn.commit()

            except Exception as e:
                print(f"      ⚠️ Hiba a(z) {idx}. állásnál: {e}")

        conn.close()
        print(f"\n✨ SIKER! Az IFUA Horváth pozíciók (formázott leírással) elmentve.")

    finally:
        driver.quit()


if __name__ == "__main__":
    run_scraper()
