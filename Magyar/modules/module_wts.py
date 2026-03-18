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
COMPANY_NAME = "WTS_Klient"
BASE_URL = "https://wtsklient.zohorecruit.eu/jobs/Careers"
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "wtsklient_jobs.db")


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
    print(f"🚀 {COMPANY_NAME} Scraper indítása (Zoho Recruit mód)...")

    options = uc.ChromeOptions()
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--headless=new")
    driver = uc.Chrome(options=options)
    wait = WebDriverWait(driver, 10)

    job_links = []

    try:
        # --- 1. FÁZIS: LINKEK GYŰJTÉSE ---
        print("📂 WTS Klient karrieroldal megnyitása...")
        driver.get(BASE_URL)
        time.sleep(5)  # Várjuk meg, amíg a Zoho JS moduljai betöltenek

        # Cookie ablak bezárása (ha van)
        try:
            cookie_btn = driver.find_element(
                By.XPATH, "//button[contains(text(), 'Elfogadom') or contains(text(), 'Accept')]")
            driver.execute_script("arguments[0].click();", cookie_btn)
            time.sleep(1)
        except:
            pass

        print("🔍 Álláskártyák beolvasása...")

        # A beküldött HTML struktúra alapján keressük a kártyákat
        jobs_on_page = driver.execute_script("""
            let results = [];
            let cards = document.querySelectorAll('.cw-filter-joblist');
            cards.forEach(card => {
                let linkEl = card.querySelector('.cw-3-title');
                if (linkEl && linkEl.href) {
                    let locEl = card.querySelector('.filter-subhead');
                    results.push({
                        url: linkEl.href,
                        title: linkEl.innerText.trim(),
                        location_raw: locEl ? locEl.innerText.trim() : 'Budapest'
                    });
                }
            });
            return results;
        """)

        job_links.extend(jobs_on_page)

        print(f"✅ {len(job_links)} állás azonosítva. Kezdődik a mélyfúrás...")

        # --- 2. FÁZIS: LEÍRÁSOK ÉS HELYSZÍN SCRAPELÉSE ---
        conn = sqlite3.connect(DB_PATH)
        for idx, job in enumerate(job_links, 1):
            try:
                conn.execute("DELETE FROM jobs WHERE url = ?", (job['url'],))

                print(
                    f"   [{idx}/{len(job_links)}] {job['title']} ({job['location_raw']})")
                driver.get(job['url'])
                time.sleep(4)  # Várjunk a részletek betöltésére

                # NATIVE INNERTEXT KINYERÉS (Zoho specifikus konténerekkel)
                description = driver.execute_script("""
                    let desc = "";
                    // A Zoho Recruit általában ezeket a konténereket használja a részletekre
                    let mainContent = document.querySelector('.cw-job-details') || 
                                      document.querySelector('.job-description') || 
                                      document.querySelector('article') ||
                                      document.querySelector('.zrec-job-details');
                                      
                    if (mainContent) {
                        desc = mainContent.innerText;
                    } else {
                        // Végső tartalék
                        desc = document.body.innerText;
                    }

                    return desc;
                """)

                clean_desc = re.sub(r'\n\s*\n', '\n\n', description).strip()

                # --- 3. FÁZIS: VÁROS ÉS ORSZÁG BONTÁSA ---
                # Nyers pl: "Budapest, Budapest, Hungary   1-3 év"
                location_raw = job['location_raw']

                # Darabolás vesszők mentén
                parts = [p.strip() for p in location_raw.split(',')]

                if len(parts) >= 3:
                    city = parts[0]
                    # Az utolsó elemből kivágjuk az "1-3 év" jellegű tapasztalati infókat
                    country_raw = parts[-1]
                    country = re.sub(
                        r'\s*[0-9\-]+\s*(év|years?).*', '', country_raw, flags=re.IGNORECASE).strip()
                elif len(parts) == 2:
                    city = parts[0]
                    country = re.sub(
                        r'\s*[0-9\-]+\s*(év|years?).*', '', parts[1], flags=re.IGNORECASE).strip()
                else:
                    city = parts[0] if parts else "Budapest"
                    country = "Magyarország"

                conn.execute('''INSERT INTO jobs (url, title, company, location_raw, city, country, description, category) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                             (job['url'], job['title'], "WTS Klient", location_raw, city, country, clean_desc, "Pénzügy / Számvitel"))
                conn.commit()

            except Exception as e:
                print(f"      ⚠️ Hiba a(z) {idx}. állásnál: {e}")

        conn.close()
        print(f"\n✨ SIKER! A WTS Klient pozíciók elmentve.")

    finally:
        driver.quit()


if __name__ == "__main__":
    run_scraper()
