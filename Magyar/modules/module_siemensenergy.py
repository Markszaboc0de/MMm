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
COMPANY_NAME = "Siemens_Energy"
BASE_URL = "https://siemens-energy.swicon.com/allasaink/"
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "siemens_energy_jobs.db")


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
        # --- 1. FÁZIS: LINKEK GYŰJTÉSE A "TOVÁBBI ÁLLÁSOK" GOMBBAL ---
        print("📂 Siemens Energy álláslista megnyitása...")
        driver.get(BASE_URL)
        time.sleep(6)

        # Cookie ablak bezárása (ha van)
        try:
            cookie_btn = driver.find_element(
                By.XPATH, "//a[contains(text(), 'Elfogadom') or contains(text(), 'Accept')] | //button[contains(text(), 'Elfogadom') or contains(text(), 'Accept')]")
            driver.execute_script("arguments[0].click();", cookie_btn)
            time.sleep(1)
        except:
            pass

        clicks = 0
        while True:

            try:
                # A megadott HTML alapján keressük a gombot
                load_more_btn = driver.find_element(
                    By.CSS_SELECTOR, "a.wpr-load-more-btn")

                # Ha a gomb már nem látható (eltűnt, mert nincs több állás), kilépünk a ciklusból
                if not load_more_btn.is_displayed():
                    break

                print(
                    f"🔄 {clicks + 1}. alkalommal rákattintunk a 'További állások' gombra...")
                driver.execute_script(
                    "arguments[0].scrollIntoView({block: 'center'});", load_more_btn)
                time.sleep(1)
                driver.execute_script("arguments[0].click();", load_more_btn)

                clicks += 1
                # Várjuk meg, amíg az AJAX behúzza az új kártyákat
                time.sleep(4)

            except:
                print("🏁 Nincs több 'További állások' gomb, lista vége.")
                break

        print("🔍 Álláskártyák beolvasása az oldalról...")

        # A megadott HTML struktúra alapján keressük a linkeket
        jobs_on_page = driver.execute_script("""
            let results = [];
            let cards = document.querySelectorAll('.wpr-grid-item-below-content');
            cards.forEach(card => {
                let linkEl = card.querySelector('h2.wpr-grid-item-title a');
                if (linkEl && linkEl.href) {
                    results.push({
                        url: linkEl.href,
                        title: linkEl.innerText.trim(),
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

                print(f"   [{idx}/{len(job_links)}] {job['title']}")
                driver.get(job['url'])
                time.sleep(4)  # Várakozás a részletek betöltésére

                # NATIVE INNERTEXT KINYERÉS (WordPress/Elementor specifikus konténerekkel)
                details = driver.execute_script("""
                    let res = { description: "" };

                    // Elementor és WordPress gyakori konténerei a bejegyzésekhez
                    let mainContent = document.querySelector('.elementor-widget-theme-post-content') || 
                                      document.querySelector('.elementor-post__content') || 
                                      document.querySelector('article') || 
                                      document.querySelector('main');
                                      
                    if (mainContent) {
                        res.description = mainContent.innerText;
                    } else {
                        // Végső tartalék, ha semmi sem stimmel
                        res.description = document.body.innerText;
                    }

                    return res;
                """)

                description = details.get('description', '')

                # Ha a végső tartalék (body) futott le, vágjuk le a fej- és láblécet
                if "Jelentkezés" in description:
                    description = description.split("Jelentkezés")[0]

                clean_desc = re.sub(r'\n\s*\n', '\n\n', description).strip()

                # --- 3. FÁZIS: HELYSZÍN KERESÉSE ---
                # Mivel a kártyán nem volt rajta, megpróbáljuk a szövegből kibányászni
                city = "Budapest"
                country = "Magyarország"
                location_raw = "Budapest"

                # Egyszerű regex a helyszín kinyerésére a szövegből (ha van ilyen sor)
                loc_match = re.search(
                    r'(Munkavégzés helye|Location):\s*([^\n]+)', clean_desc, re.IGNORECASE)
                if loc_match:
                    location_raw = loc_match.group(2).strip()
                    city = location_raw.split(',')[0].strip()

                conn.execute('''INSERT INTO jobs (url, title, company, location_raw, city, country, description, category) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                             (job['url'], job['title'], "Siemens Energy", location_raw, city, country, clean_desc, "Energia / Mérnöki"))
                conn.commit()

            except Exception as e:
                print(f"      ⚠️ Hiba a(z) {idx}. állásnál: {e}")

        conn.close()
        print(f"\n✨ SIKER! A Siemens Energy pozíciók (teszt mód) elmentve.")

    finally:
        driver.quit()


if __name__ == "__main__":
    run_scraper()
