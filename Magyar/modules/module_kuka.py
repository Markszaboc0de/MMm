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
COMPANY_NAME = "KUKA"
BASE_URL = "https://www.kuka.com/hu-hu/vállalat/karrier/állásajánlatok"
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "kuka_jobs.db")


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
    print(f"🚀 {COMPANY_NAME} Scraper indítása (Scroll & DOM Walker mód)...")

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
        print("📂 KUKA karrieroldal megnyitása...")
        driver.get(BASE_URL)
        time.sleep(6)

        # Cookie ablak bezárása (ha van)
        try:
            cookie_btn = driver.find_element(
                By.XPATH, "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'elfogad')] | //button[contains(@class, 'cmp-cookie__btn--accept-all')]")
            driver.execute_script("arguments[0].click();", cookie_btn)
            time.sleep(1)
        except:
            pass

        print("🔄 Lista betöltése...")
        scrolls = 0
        last_count = 0

        # Opcionális végtelen görgetés, ha az oldal lazy-loadot használ
        while True:

            items = driver.find_elements(
                By.CSS_SELECTOR, "a.m-results__anchor")
            current_count = len(items)

            if current_count == 0 or current_count == last_count:
                break

            last_count = current_count
            driver.execute_script(
                "arguments[0].scrollIntoView({block: 'center'});", items[-1])
            scrolls += 1
            time.sleep(2)

        print("🔍 Álláskártyák beolvasása az oldalról...")

        # A megadott HTML alapján kinyerjük a linkeket, helyszínt és a tageket
        jobs_on_page = driver.execute_script("""
            let results = [];
            let cards = document.querySelectorAll('a.m-results__anchor');
            
            cards.forEach(card => {
                if (card.href) {
                    let titleEl = card.querySelector('h3');
                    let captionEls = card.querySelectorAll('.caption');
                    let tagEls = card.querySelectorAll('.mod-joblist__tag');
                    
                    // A helyszín általában az első caption-ben van (pl. "Budapest, Magyarország")
                    let location = captionEls.length > 0 ? captionEls[0].innerText.trim() : 'Budapest';
                    
                    // Kategória az első tag-ből (pl. "IT")
                    let category = tagEls.length > 0 ? tagEls[0].innerText.trim() : 'Robotika / Mérnöki';
                    
                    results.push({
                        url: card.href,
                        title: titleEl ? titleEl.innerText.trim() : 'Ismeretlen pozíció',
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

        print(f"✅ {len(job_links)} állás azonosítva. Kezdődik a mélyfúrás...")

        # --- 2. FÁZIS: LEÍRÁSOK SCRAPELÉSE ---
        conn = sqlite3.connect(DB_PATH)
        for idx, job in enumerate(job_links, 1):
            try:
                conn.execute("DELETE FROM jobs WHERE url = ?", (job['url'],))

                print(
                    f"   [{idx}/{len(job_links)}] {job['title']} ({job['location_raw']})")
                driver.get(job['url'])

                try:
                    # Megvárjuk amíg a "mod-jobprofile" modul betölt
                    wait.until(EC.presence_of_element_located(
                        (By.CSS_SELECTOR, ".mod-jobprofile")))
                    time.sleep(1.5)
                except:
                    time.sleep(3)

                # NATIVE INNERTEXT A PONTOS DOBOZOKBÓL (Mély bejáró)
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

                            // Szigorú kizárások
                            if (['SCRIPT','STYLE','NAV','FOOTER','HEADER','BUTTON','SVG'].includes(tag)) return "";
                            if (cls.includes('share') || cls.includes('apply') || cls.includes('back')) return "";

                            // Listaelemek formázása
                            if (tag === 'LI') text += "• ";
                            
                            for (let child of el.childNodes) { 
                                text += walk(child); 
                            }
                            
                            // Sortörések a bekezdéseknél és fejléceknél
                            if (['P','DIV','BR','LI','H1','H2','H3','H4'].includes(tag)) text += "\\n";
                        }
                        return text;
                    }

                    // A megadott HTML alapján a .mod-jobprofile dobozt célozzuk
                    let mainContent = document.querySelector('.mod-jobprofile');
                                      
                    if (mainContent) {
                        return walk(mainContent);
                    } else {
                        return walk(document.querySelector('main') || document.body);
                    }
                """)

                # Felesleges tripla sortörések eltávolítása
                clean_desc = re.sub(r'\n\s*\n', '\n\n', description).strip()

                # Vágjuk le az esetlegesen beolvasott jelentkezési gombokat a fallbacknél
                if "Apply now" in clean_desc:
                    clean_desc = clean_desc.split("Apply now")[0].strip()
                elif "Jelentkezés" in clean_desc:
                    clean_desc = clean_desc.split("Jelentkezés")[0].strip()

                # --- 3. FÁZIS: VÁROS ÉS ORSZÁG BONTÁSA ---
                # "Budapest, Magyarország" vagy "Taksony, Magyarország"
                location_raw = job['location_raw']
                parts = [p.strip() for p in location_raw.split(',')]

                if len(parts) >= 2:
                    city = parts[0]
                    country = parts[-1]
                else:
                    city = location_raw
                    country = "Magyarország"

                conn.execute('''INSERT INTO jobs (url, title, company, location_raw, city, country, description, category) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                             (job['url'], job['title'], "KUKA", location_raw, city, country, clean_desc, job['category']))
                conn.commit()

            except Exception as e:
                print(f"      ⚠️ Hiba a(z) {idx}. állásnál: {e}")

        conn.close()
        print(f"\n✨ SIKER! A KUKA pozíciók elmentve.")

    finally:
        driver.quit()


if __name__ == "__main__":
    run_scraper()
