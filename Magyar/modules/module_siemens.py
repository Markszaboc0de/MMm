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
COMPANY_NAME = "Siemens"
BASE_URL = "https://jobs.siemens.com/en_US/externaljobs/SearchJobs/?42386=%5B812003%2C812136%2C812123%2C812131%2C812146%2C812009%2C812145%2C811975%2C812141%2C812124%2C812008%2C812020%2C812134%2C812132%2C811990%2C812024%2C812120%2C812010%2C812137%2C812011%2C812133%2C812130%2C812027%2C812023%2C812097%2C812022%2C812007%2C812028%2C812026%2C812144%2C812119%2C812013%2C812129%2C812017%5D&42386_format=17546&42390=%5B102154%2C102155%2C102156%5D&42390_format=17550&listFilterMode=1&folderRecordsPerPage=6&"
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "siemens_jobs.db")


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
    print(f"🚀 {COMPANY_NAME} Scraper indítása (Native InnerText & Teszt mód)...")

    options = uc.ChromeOptions()
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--headless=new")
    driver = uc.Chrome(options=options)
    wait = WebDriverWait(driver, 15)

    job_links = []
    unique_urls = set()

    try:
        # --- 1. FÁZIS: LINKEK GYŰJTÉSE LAPOZÁSSAL ---
        print("📂 Siemens álláslista megnyitása...")
        driver.get(BASE_URL)
        time.sleep(8)

        try:
            cookie_btn = driver.find_element(
                By.XPATH, "//button[contains(text(), 'Accept') or contains(text(), 'Agree')]")
            driver.execute_script("arguments[0].click();", cookie_btn)
            time.sleep(1)
        except:
            pass

        current_page = 1
        while True:

            print(f"📄 {current_page}. oldal adatainak begyűjtése...")

            try:
                wait.until(EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "article")))
                time.sleep(2)
            except:
                break

            jobs_on_page = driver.execute_script("""
                let results = [];
                let cards = document.querySelectorAll('article');
                cards.forEach(card => {
                    let linkEl = card.querySelector('h3 a.link') || card.querySelector('a.link');
                    if (linkEl) {
                        let locEl = card.querySelector('.list-item-location');
                        let catEl = card.querySelector('.list-item-family');
                        results.push({
                            url: linkEl.href,
                            title: linkEl.innerText.trim(),
                            location_raw: locEl ? locEl.innerText.trim() : 'Ismeretlen',
                            category: catEl ? catEl.innerText.trim() : 'Egyéb'
                        });
                    }
                });
                return results;
            """)

            if not jobs_on_page:
                break

            for job in jobs_on_page:
                if job['url'] not in unique_urls:
                    unique_urls.add(job['url'])
                    job_links.append(job)

            try:
                next_btn = driver.find_element(
                    By.XPATH, "//a[contains(text(), 'Next')] | //button[contains(text(), 'Next')]")
                if "disabled" in next_btn.get_attribute("class").lower():
                    break

                driver.execute_script(
                    "arguments[0].scrollIntoView({block: 'center'});", next_btn)
                time.sleep(1)
                driver.execute_script("arguments[0].click();", next_btn)

                current_page += 1
                time.sleep(5)
            except:
                print("🏁 Nincs több oldal, lapozás befejezve.")
                break

        print(f"✅ {len(job_links)} állás azonosítva. Kezdődik a mélyfúrás...")

        # --- 2. FÁZIS: LEÍRÁSOK ÉS ORSZÁG SCRAPELÉSE ---
        conn = sqlite3.connect(DB_PATH)
        for idx, job in enumerate(job_links, 1):
            try:
                conn.execute("DELETE FROM jobs WHERE url = ?", (job['url'],))

                print(
                    f"   [{idx}/{len(job_links)}] {job['title']} ({job['location_raw']})")
                driver.get(job['url'])

                # Várakozás, amíg a kettes szekció (a tényleges hirdetés) be nem töltődik
                try:
                    wait.until(EC.presence_of_element_located(
                        (By.ID, "section1__content")))
                    time.sleep(1.5)
                except:
                    time.sleep(3)

                # NATIVE INNERTEXT KINYERÉS (Sokkal megbízhatóbb, mint a ciklusos bejárás)
                details = driver.execute_script("""
                    let res = { description: "", exact_location: "" };

                    // Szöveg kinyerése a beépített innerText funkcióval
                    let sec0 = document.getElementById('section0__content');
                    let sec1 = document.getElementById('section1__content');
                    
                    if (sec0) {
                        res.description += sec0.innerText + "\\n\\n--- RÉSZLETES LEÍRÁS ---\\n\\n";
                    }
                    if (sec1) {
                        res.description += sec1.innerText;
                    }

                    // Tartalék, ha nagyon máshogy töltődne be egyedi állásoknál
                    if (!sec0 && !sec1) {
                        let main = document.querySelector('.article--details') || document.querySelector('main') || document.body;
                        if (main) res.description = main.innerText;
                    }

                    // Pontos helyszín kikeresése a "Location(s)" listából
                    let locNode = document.querySelector('.tf_locations .list__item');
                    if (locNode) {
                        res.exact_location = locNode.innerText.trim();
                    }

                    return res;
                """)

                description = details.get('description', '')
                exact_location = details.get('exact_location', '')

                clean_desc = re.sub(r'\n\s*\n', '\n\n', description).strip()

                # --- 3. FÁZIS: VÁROS ÉS ORSZÁG BONTÁSA BIZTONSÁGOSAN ---
                # Ha a részletes adatok között talált egyedi várost, felülírja a "Multiple Locations"-t
                final_loc_raw = exact_location if exact_location else job['location_raw']
                final_loc_raw = re.sub(r'\s+', ' ', final_loc_raw).strip()

                # Darabolás a '-' vagy ',' mentén
                if "-" in final_loc_raw:
                    parts = [p.strip()
                             for p in final_loc_raw.split("-") if p.strip()]
                elif "," in final_loc_raw:
                    parts = [p.strip()
                             for p in final_loc_raw.split(",") if p.strip()]
                else:
                    parts = [final_loc_raw]

                if len(parts) >= 2:
                    city = parts[0]
                    country = parts[-1]
                elif len(parts) == 1:
                    city = parts[0]
                    country = "Ismeretlen"  # Ha csak egy adat van, azt betesszük városnak
                else:
                    city = "Ismeretlen"
                    country = "Ismeretlen"

                conn.execute('''INSERT INTO jobs (url, title, company, location_raw, city, country, description, category) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                             (job['url'], job['title'], "Siemens", final_loc_raw, city, country, clean_desc, job['category']))
                conn.commit()

            except Exception as e:
                print(f"      ⚠️ Hiba a(z) {idx}. állásnál: {e}")

        conn.close()
        print(f"\n✨ SIKER! A Siemens pozíciók teszt-futtatása befejeződött.")

    finally:
        driver.quit()


if __name__ == "__main__":
    run_scraper()
