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
COMPANY_NAME = "Bosch"
BASE_URL = "https://jobs.bosch.com/hu/?country=hu"
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "bosch_jobs.db")


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
    print(f"🚀 {COMPANY_NAME} Scraper indítása (A-Text-RichText beolvasó mód)...")

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
        print("📂 Bosch karrieroldal megnyitása...")
        driver.get(BASE_URL)
        time.sleep(6)

        # Cookie ablak bezárása
        try:
            cookie_btn = driver.find_element(
                By.XPATH, "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'elfogad')] | //button[contains(@class, 'privacy-settings-accept-all')] | //button[@id='onetrust-accept-btn-handler']")
            driver.execute_script("arguments[0].click();", cookie_btn)
            time.sleep(1.5)
        except:
            pass

        clicks = 0
        while True:

            try:
                load_more_btn = driver.find_element(
                    By.XPATH, "//button[.//span[contains(text(), 'Továbbiak betöltése')]]")

                if not load_more_btn.is_displayed() or load_more_btn.get_attribute("disabled"):
                    break

                print(
                    f"🔄 {clicks + 1}. alkalommal rákattintunk a 'Továbbiak betöltése' gombra...")
                driver.execute_script(
                    "arguments[0].scrollIntoView({block: 'center'});", load_more_btn)
                time.sleep(1)
                driver.execute_script("arguments[0].click();", load_more_btn)

                clicks += 1
                time.sleep(3.5)

            except:
                print("🏁 Nincs több 'Továbbiak betöltése' gomb, lista vége.")
                break

        print("🔍 Álláskártyák beolvasása az oldalról...")

        jobs_on_page = driver.execute_script("""
            let results = [];
            let cards = document.querySelectorAll('.A-JobPanel');
            
            cards.forEach(card => {
                let linkEl = card.querySelector('a.A-JobPanel__header');
                
                if (linkEl && linkEl.href) {
                    let title = linkEl.innerText.trim();
                    let category = "Egyéb";
                    let location = "Ismeretlen";
                    
                    let infoBlocks = card.querySelectorAll('dl.A-JobPanel__info');
                    infoBlocks.forEach(dl => {
                        let labelEl = dl.querySelector('dt.A-JobPanel__label');
                        let valueEl = dl.querySelector('dd.A-JobPanel__value');
                        
                        if (labelEl && valueEl) {
                            let labelText = labelEl.innerText.trim().toLowerCase();
                            let valueText = valueEl.innerText.trim();
                            
                            if (labelText.includes('munkaterületek')) {
                                category = valueText;
                            } else if (labelText.includes('telephelyek')) {
                                location = valueText;
                            }
                        }
                    });
                    
                    results.push({
                        url: linkEl.href,
                        title: title,
                        location_raw: location,
                        category: category
                    });
                }
            });
            return results;
        """)

        for job in jobs_on_page:
            if job['url'] not in unique_urls:
                unique_urls.add(job['url'])
                job_links.append(job)

        print(f"✅ {len(job_links)} egyedi állás azonosítva. Kezdődik a mélyfúrás...")

        # --- 2. FÁZIS: LEÍRÁSOK SCRAPELÉSE (A Helyes HTML alapján) ---
        conn = sqlite3.connect(DB_PATH)
        for idx, job in enumerate(job_links, 1):
            try:
                conn.execute("DELETE FROM jobs WHERE url = ?", (job['url'],))

                print(
                    f"   [{idx}/{len(job_links)}] {job['title']} ({job['location_raw']})")
                driver.get(job['url'])

                try:
                    # Megvárjuk amíg a szöveges szekciók betöltenek
                    wait.until(EC.presence_of_element_located(
                        (By.CSS_SELECTOR, ".a-text, .A-Text-RichText")))
                    time.sleep(1.5)
                except:
                    time.sleep(3)

                # JAVÍTOTT KINYERŐ (Kizárólag a beküldött .a-text struktúrát szedi)
                description = driver.execute_script("""
                    let desc = "";
                    
                    // Megkeressük az összes ".a-text" konténert, mert abban van a h2 (cím) és a .A-Text-RichText (törzs)
                    let textSections = document.querySelectorAll('.a-text');
                    
                    if (textSections.length > 0) {
                        textSections.forEach(sec => {
                            desc += sec.innerText.trim() + "\\n\\n";
                        });
                    } else {
                        // Ha ez az osztály mégsem létezik (mert mondjuk más rendszerű a hirdetés)
                        let fallback = document.querySelector('[itemprop="description"]') || 
                                       document.querySelector('.O-JobDescription') ||
                                       document.querySelector('.job-description') ||
                                       document.querySelector('main') ||
                                       document.querySelector('article');
                        if (fallback) {
                            desc = fallback.innerText;
                        } else {
                            desc = document.body.innerText;
                        }
                    }
                    
                    return desc;
                """)

                clean_desc = re.sub(r'\n\s*\n', '\n\n', description).strip()

                # Biztonsági vágás, ha a fallback opció (body) olvasta volna be az oldalt
                if "Jelentkezés most" in clean_desc:
                    clean_desc = clean_desc.split(
                        "Jelentkezés most")[0].strip()
                elif "Apply Now" in clean_desc:
                    clean_desc = clean_desc.split("Apply Now")[0].strip()

                # Helyszín formázása
                location_raw = job['location_raw']
                city = location_raw.split(',')[0].split('-')[0].strip()
                country = "Magyarország"

                conn.execute('''INSERT INTO jobs (url, title, company, location_raw, city, country, description, category) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                             (job['url'], job['title'], "Bosch", location_raw, city, country, clean_desc, job['category']))
                conn.commit()

            except Exception as e:
                print(f"      ⚠️ Hiba a(z) {idx}. állásnál: {e}")

        conn.close()
        print(f"\n✨ SIKER! A Bosch pozíciók (teszt mód, helyes leírásokkal) elmentve.")

    finally:
        driver.quit()


if __name__ == "__main__":
    run_scraper()
