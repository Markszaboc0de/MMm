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
COMPANY_NAME = "DT_ITS"
BASE_URL = "https://www.deutschetelekomitsolutions.hu/nyitott-poziciok/"
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "dtits_jobs.db")


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
    print(f"🚀 {COMPANY_NAME} Scraper indítása (SmartRecruiters & Load More mód)...")

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
        print("📂 DT-ITS karrieroldal megnyitása...")
        driver.get(BASE_URL)
        time.sleep(5)

        # Cookie ablak bezárása (ha van)
        try:
            cookie_btn = driver.find_element(
                By.XPATH, "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'elfogad')] | //a[contains(text(), 'Elfogadom')]")
            driver.execute_script("arguments[0].click();", cookie_btn)
            time.sleep(1)
        except:
            pass

        clicks = 0
        while True:

            try:
                # Keressük a megadott "Mutasd a többit is" gombot
                load_more_btn = driver.find_element(
                    By.CSS_SELECTOR, "a.show_more")

                # Ha a gomb nem látszik (mert elfogytak az állások), kilépünk
                if not load_more_btn.is_displayed():
                    break

                print(
                    f"🔄 {clicks + 1}. alkalommal rákattintunk a 'Mutasd a többit is' gombra...")
                driver.execute_script(
                    "arguments[0].scrollIntoView({block: 'center'});", load_more_btn)
                time.sleep(1)
                driver.execute_script("arguments[0].click();", load_more_btn)

                clicks += 1
                time.sleep(3)  # AJAX várakozás

            except:
                print("🏁 Nincs több 'Mutasd a többit is' gomb, lista vége.")
                break

        print("🔍 Álláskártyák beolvasása az oldalról...")

        # A megadott HTML alapján kinyerjük a linkeket a .position-card-ból
        jobs_on_page = driver.execute_script("""
            let results = [];
            let cards = document.querySelectorAll('.position-card');
            
            cards.forEach(card => {
                let links = card.querySelectorAll('a');
                
                // Az első link a cím, a második linkben (amiben a <small> van) a helyszín található
                if (links.length > 0 && links[0].href) {
                    let title = links[0].innerText.trim();
                    let locRaw = links.length > 1 ? links[1].innerText.trim() : 'Ismeretlen';
                    
                    results.push({
                        url: links[0].href,
                        title: title,
                        location_raw: locRaw,
                        category: 'IT / Telekommunikáció'
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

        # --- 2. FÁZIS: LEÍRÁSOK SCRAPELÉSE ---
        conn = sqlite3.connect(DB_PATH)
        for idx, job in enumerate(job_links, 1):
            try:
                conn.execute("DELETE FROM jobs WHERE url = ?", (job['url'],))

                print(
                    f"   [{idx}/{len(job_links)}] {job['title']} ({job['location_raw']})")
                driver.get(job['url'])

                try:
                    # Megvárjuk a SmartRecruiters specifikus itemprop leírást
                    wait.until(EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "[itemprop='description']")))
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
                            
                            // A szebb tagolásért a címsorok után dupla sortörést rakunk
                            if (['H1','H2','H3','H4'].includes(tag)) text += "\\n\\n";
                            else if (['P','DIV','BR','LI'].includes(tag)) text += "\\n";
                        }
                        return text;
                    }

                    // A SmartRecruiters a [itemprop="description"] tagbe rakja a teljes állásleírást
                    let mainContent = document.querySelector('[itemprop="description"]');
                                      
                    if (mainContent) {
                        return walk(mainContent);
                    } else {
                        // Tartalék
                        let fallback = document.querySelector('.job-sections') || document.querySelector('main');
                        return fallback ? walk(fallback) : walk(document.body);
                    }
                """)

                # Felesleges tripla sortörések eltávolítása
                clean_desc = re.sub(r'\n{3,}', '\n\n', description).strip()

                # Vágjuk le a gombokat és SmartRecruiters feleslegeket a tartalék esetére
                if "I'm interested" in clean_desc:
                    clean_desc = clean_desc.split("I'm interested")[0].strip()
                elif "Jelentkezem" in clean_desc:
                    clean_desc = clean_desc.split("Jelentkezem")[0].strip()

                # --- 3. FÁZIS: VÁROS ÉS ORSZÁG BONTÁSA ---
                location_raw = job['location_raw']
                parts = [p.strip() for p in location_raw.split(',')]

                # Pl. "Debrecen", "Budapest, HU"
                if len(parts) >= 2:
                    city = parts[0]
                    country = "Magyarország" if "HU" in parts[-1].upper(
                    ) else parts[-1]
                else:
                    city = location_raw
                    country = "Magyarország"

                conn.execute('''INSERT INTO jobs (url, title, company, location_raw, city, country, description, category) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                             (job['url'], job['title'], "Deutsche Telekom IT Solutions", location_raw, city, country, clean_desc, job['category']))
                conn.commit()

            except Exception as e:
                print(f"      ⚠️ Hiba a(z) {idx}. állásnál: {e}")

        conn.close()
        print(f"\n✨ SIKER! A DT-ITS pozíciók elmentve.")

    finally:
        driver.quit()


if __name__ == "__main__":
    run_scraper()
