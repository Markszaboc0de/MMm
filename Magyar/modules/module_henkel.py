import undetected_chromedriver as uc
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
COMPANY_NAME = "Henkel"
# Az új, bővebb keresési URL
BASE_URL = "https://www.henkel.hu/karrier/allasok-es-jelentkezes#selectFilterByParameter=Career_Level_18682=Students%3BGraduates%3BPupils&Locations_279384=Europe&startIndex=0&loadCount=10&"
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "henkel_jobs.db")


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
    print(f"🚀 {COMPANY_NAME} Scraper indítása (Load More & Native InnerText mód)...")

    options = uc.ChromeOptions()
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--headless=new")
    driver = uc.Chrome(options=options)
    wait = WebDriverWait(driver, 10)

    job_links = []
    unique_urls = set()

    try:
        # --- 1. FÁZIS: LINKEK GYŰJTÉSE ---
        print("📂 Henkel karrieroldal megnyitása...")
        driver.get(BASE_URL)
        time.sleep(6)

        # Cookie ablak bezárása (ha van)
        try:
            cookie_btn = driver.find_element(
                By.XPATH, "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'elfogad')] | //button[contains(@class, 'osano-cm-accept-all')]")
            driver.execute_script("arguments[0].click();", cookie_btn)
            time.sleep(1)
        except:
            pass

        clicks = 0
        while True:

            try:
                # Kifejezetten a megadott HTML gombot keressük
                load_more_btn = driver.find_element(
                    By.CSS_SELECTOR, "button.bab-filters__results-loadMore")

                # Ha a gomb láthatatlan (pl. elfogytak az állások), kilépünk
                if not load_more_btn.is_displayed():
                    break

                print(
                    f"🔄 {clicks + 1}. alkalommal rákattintunk a 'További állásajánlatok' gombra...")
                # Odatekerünk, hogy a böngésző biztosan kattinthatónak lássa
                driver.execute_script(
                    "arguments[0].scrollIntoView({block: 'center'});", load_more_btn)
                time.sleep(1)
                driver.execute_script("arguments[0].click();", load_more_btn)

                clicks += 1
                time.sleep(3.5)  # Várjuk meg az AJAX választ

            except:
                print("🏁 Nincs (több) 'További állásajánlatok' gomb, lista vége.")
                break

        print("🔍 Álláskártyák beolvasása az oldalról...")

        jobs_on_page = driver.execute_script("""
            let results = [];
            let cards = document.querySelectorAll('.bab-filters__results-list-result a');
            
            cards.forEach(aTag => {
                if (aTag && aTag.href) {
                    let titleEl = aTag.querySelector('.link-title');
                    let placeEl = aTag.querySelector('.place');
                    
                    // Cím végéről levágjuk a felesleges gondolatjelet (—)
                    let title = titleEl ? titleEl.innerText.replace(/—$/, '').trim() : 'Ismeretlen pozíció';
                    
                    results.push({
                        url: aTag.href,
                        title: title,
                        location_raw: placeEl ? placeEl.innerText.trim() : 'Ismeretlen',
                        category: 'FMCG / Kereskedelem'
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
                    # Megvárjuk amíg a tartalom betöltődik
                    wait.until(EC.presence_of_element_located(
                        (By.CSS_SELECTOR, ".job-detail__content")))
                    time.sleep(1.5)
                except:
                    time.sleep(3)

                # NATIVE INNERTEXT KINYERÉS A MEGADOTT HTML ALAPJÁN
                description = driver.execute_script("""
                    let desc = "";
                    let mainContent = document.querySelector('.job-detail__content');
                    
                    if (mainContent) {
                        desc = mainContent.innerText;
                    } else {
                        desc = document.body.innerText;
                    }
                    return desc;
                """)

                clean_desc = re.sub(r'\n\s*\n', '\n\n', description).strip()

                # Vágjuk le a kapcsolati infókat a legvégéről
                if "Contact information for application-related questions:" in clean_desc:
                    clean_desc = clean_desc.split(
                        "Contact information for application-related questions:")[0].strip()

                # --- 3. FÁZIS: VÁROS ÉS ORSZÁG BONTÁSA ---
                # A Henkelnél a formátum: "Hungary, Budapest, Henkel Consumer Brands" vagy "Germany, Düsseldorf, ..."
                location_raw = job['location_raw']
                parts = [p.strip() for p in location_raw.split(',')]

                if len(parts) >= 2:
                    country_raw = parts[0]
                    city = parts[1]
                    # Egységesítjük az adatbázishoz, ha magyar
                    country = "Magyarország" if country_raw.lower(
                    ) in ["hungary", "magyarország"] else country_raw
                else:
                    city = location_raw
                    country = "Ismeretlen"

                conn.execute('''INSERT INTO jobs (url, title, company, location_raw, city, country, description, category) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                             (job['url'], job['title'], "Henkel", location_raw, city, country, clean_desc, job['category']))
                conn.commit()

            except Exception as e:
                print(f"      ⚠️ Hiba a(z) {idx}. állásnál: {e}")

        conn.close()
        print(f"\n✨ SIKER! A Henkel pozíciók elmentve.")

    finally:
        driver.quit()


if __name__ == "__main__":
    run_scraper()
