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

sys.stdout.reconfigure(encoding='utf-8')

# --- KONFIGURÁCIÓ ---
COMPANY_NAME = "CIB_Bank"
BASE_URL = "https://jobs.intesasanpaolo.com/go/Jobs-CIB_HU/9669601/?locale=hu_HU&previewCategory=true&referrerSave=false"
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "cib_jobs.db")


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
    print(f"🚀 {COMPANY_NAME} Scraper indítása (Smart Pagination mód)...")

    options = uc.ChromeOptions()
    options.add_argument("--window-size=1920,1080")
    driver = uc.Chrome(options=options, version_main=145)
    wait = WebDriverWait(driver, 10)

    job_links = []
    unique_urls = set()

    try:
        # --- 1. FÁZIS: LINKEK GYŰJTÉSE LAPOZÁSSAL ---
        print("📂 CIB Bank karrieroldal megnyitása...")
        driver.get(BASE_URL)
        time.sleep(5)

        # Cookie ablak bezárása (ha van)
        try:
            cookie_btn = driver.find_element(
                By.XPATH, "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'elfogad')]")
            driver.execute_script("arguments[0].click();", cookie_btn)
            time.sleep(1)
        except:
            pass

        current_page = 1
        while True:

            print(f"📄 {current_page}. oldal adatainak begyűjtése...")

            try:
                # Várunk a táblázat betöltésére
                wait.until(EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "tr.data-row")))
            except:
                print("⚠️ Nem találhatók állások az oldalon.")
                break

            # Álláskártyák (táblázat sorok) beolvasása
            jobs_on_page = driver.execute_script("""
                let results = [];
                let rows = document.querySelectorAll('tr.data-row');
                
                rows.forEach(row => {
                    let linkEl = row.querySelector('a.jobTitle-link');
                    if (linkEl && linkEl.href) {
                        let title = linkEl.innerText.trim();
                        // Asztali (hidden-phone) és mobilos (visible-phone) nézetek kezelése
                        let locEl = row.querySelector('.colLocation .jobLocation') || row.querySelector('.jobLocation');
                        let catEl = row.querySelector('.colDepartment .jobDepartment') || row.querySelector('.jobFacility');
                        
                        results.push({
                            url: linkEl.href,
                            title: title,
                            location_raw: locEl ? locEl.innerText.trim() : 'Budapest',
                            category: catEl ? catEl.innerText.trim() : 'Pénzügy / Bank'
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

            # LAPOZÁS: Következő oldal URL-jének kinyerése
            next_url = driver.execute_script("""
                let activeLi = document.querySelector('ul.pagination li.active');
                if (activeLi) {
                    let nextLi = activeLi.nextElementSibling;
                    // Ha a következő li létezik, és benne van egy link (nem az "Utolsó" gomb ugrója, hanem a következő szám)
                    if (nextLi && nextLi.querySelector('a') && !nextLi.querySelector('a').classList.contains('paginationItemLast')) {
                        return nextLi.querySelector('a').href;
                    }
                }
                return null;
            """)

            if next_url:
                driver.get(next_url)
                current_page += 1
                time.sleep(3.5)  # Kis várakozás az új oldal betöltésére
            else:
                print("🏁 Nincs több oldal, lapozás befejezve.")
                break

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
                    # Az SAP SuccessFactors az [itemprop="description"] vagy a .jobdescription dobozt használja
                    wait.until(EC.presence_of_element_located(
                        (By.CSS_SELECTOR, ".jobdescription, [itemprop='description']")))
                    time.sleep(1.5)
                except:
                    time.sleep(3)

                # NATIVE INNERTEXT KINYERÉS
                description = driver.execute_script("""
                    let desc = "";
                    let mainContent = document.querySelector('.jobdescription') || 
                                      document.querySelector('[itemprop="description"]') || 
                                      document.querySelector('.job') || 
                                      document.querySelector('main');
                                      
                    if (mainContent) {
                        desc = mainContent.innerText;
                    } else {
                        desc = document.body.innerText;
                    }
                    return desc;
                """)

                clean_desc = re.sub(r'\n\s*\n', '\n\n', description).strip()

                # Biztonsági vágás (jelentkezés gomb levágása, ha belekerült volna)
                if "Jelentkezés most" in clean_desc:
                    clean_desc = clean_desc.split(
                        "Jelentkezés most")[0].strip()

                # --- 3. FÁZIS: VÁROS ÉS ORSZÁG BONTÁSA ---
                # A CIB-nél a formátum általában "Budapest, HU" vagy "Székesfehérvár, HU"
                location_raw = job['location_raw']

                if "," in location_raw:
                    parts = [p.strip() for p in location_raw.split(',')]
                    city = parts[0]
                    country = "Magyarország" if "HU" in parts[-1].upper(
                    ) else parts[-1]
                else:
                    city = location_raw
                    country = "Magyarország"

                conn.execute('''INSERT INTO jobs (url, title, company, location_raw, city, country, description, category) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                             (job['url'], job['title'], "CIB Bank", location_raw, city, country, clean_desc, job['category']))
                conn.commit()

            except Exception as e:
                print(f"      ⚠️ Hiba a(z) {idx}. állásnál: {e}")

        conn.close()
        print(f"\n✨ SIKER! A CIB Bank pozíciók (teszt mód) elmentve.")

    finally:
        driver.quit()


if __name__ == "__main__":
    run_scraper()
