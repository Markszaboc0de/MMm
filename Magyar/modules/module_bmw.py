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
COMPANY_NAME = "BMW_Group"
BASE_URL = "https://www.bmwgroup.jobs/hu/hu/jobs.html"
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "bmw_jobs.db")


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
        # --- 1. FÁZIS: LINKEK GYŰJTÉSE A "TÖBB ÁLLÁS" GOMBBAL ---
        print("📂 BMW Group karrieroldal megnyitása...")
        driver.get(BASE_URL)
        time.sleep(6)

        # Cookie ablak bezárása (A BMW overlay-e néha makacs, rányomunk az Elfogadásra)
        try:
            cookie_btn = driver.find_element(
                By.XPATH, "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'elfogad')] | //button[contains(@class, 'accept-all')]")
            driver.execute_script("arguments[0].click();", cookie_btn)
            time.sleep(1.5)
        except:
            pass

        clicks = 0
        while True:
            # ========================================================
            # 🛑 TESZT MÓD MEGSZAKÍTÓ
            # Ha már élesben futtatod, ezt a két sort töröld ki!
            if clicks >= 3:
                print(
                    "🛑 TESZT MÓD: 'Több állás' kattintás megállítva a 3. alkalom után!")
                break
            # ========================================================

            try:
                # Keresünk egy gombot, aminek a szövege "Több állás" vagy az aria-label "More"
                load_more_btn = driver.find_element(
                    By.XPATH, "//button[.//span[contains(text(), 'Több állás')]] | //button[@aria-label='More']")

                # Ha le van tiltva vagy eltűnt
                if not load_more_btn.is_displayed() or load_more_btn.get_attribute("disabled"):
                    break

                print(
                    f"🔄 {clicks + 1}. alkalommal rákattintunk a 'Több állás' gombra...")
                driver.execute_script(
                    "arguments[0].scrollIntoView({block: 'center'});", load_more_btn)
                time.sleep(1)
                driver.execute_script("arguments[0].click();", load_more_btn)

                clicks += 1
                # Várjuk meg, amíg az AEM (Adobe) betölti az új kártyákat
                time.sleep(4)

            except:
                print("🏁 Nincs több 'Több állás' gomb, lista vége.")
                break

        print("🔍 Álláskártyák beolvasása az oldalról...")

        # A megadott HTML struktúra alapján kikeresse az a taget, és a belső adatokat
        jobs_on_page = driver.execute_script("""
            let results = [];
            // Megkeressük a címeket
            let titleNodes = document.querySelectorAll('.grp-jobfinder-cell-job-title-group__title');
            
            titleNodes.forEach(node => {
                let aTag = node.closest('a'); // A BMW-nél a sor egy link
                let href = aTag ? aTag.href : null;
                
                if (href) {
                    let title = node.innerText.trim();
                    let groupNode = node.closest('.grp-jobfinder-cell-job-title-group');
                    
                    let locNode = groupNode ? groupNode.querySelector('.grp-jobfinder-cell-job-location') : null;
                    let catNode = groupNode ? groupNode.querySelector('.grp-jobfinder-cell-job-jobfield') : null;
                    
                    results.push({
                        url: href,
                        title: title,
                        location_raw: locNode ? locNode.innerText.trim() : 'Debrecen',
                        category: catNode ? catNode.innerText.trim() : 'Autóipar / Gyártás'
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
                    # Megvárjuk az AEM tartalom betöltését
                    wait.until(EC.presence_of_element_located(
                        (By.CSS_SELECTOR, ".cmp-job-details, main, article")))
                    time.sleep(2)
                except:
                    time.sleep(3)

                # NATIVE INNERTEXT KINYERÉS (BMW AEM specifikus konténerekkel)
                description = driver.execute_script("""
                    let desc = "";
                    let mainContent = document.querySelector('.cmp-job-details') || 
                                      document.querySelector('.cmp-experiencefragment') || 
                                      document.querySelector('main') || 
                                      document.querySelector('article');
                                      
                    if (mainContent) {
                        desc = mainContent.innerText;
                    } else {
                        desc = document.body.innerText;
                    }
                    return desc;
                """)

                clean_desc = re.sub(r'\n\s*\n', '\n\n', description).strip()

                # Vágjuk le a BMW fej- és lábléceit, ha a body-ból olvasott
                if "Jelentkezés most" in clean_desc:
                    clean_desc = clean_desc.split(
                        "Jelentkezés most")[0].strip()
                elif "Apply now" in clean_desc:
                    clean_desc = clean_desc.split("Apply now")[0].strip()

                # Helyszín formázása
                location_raw = job['location_raw']
                city = location_raw
                country = "Magyarország"

                conn.execute('''INSERT INTO jobs (url, title, company, location_raw, city, country, description, category) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                             (job['url'], job['title'], "BMW Group", location_raw, city, country, clean_desc, job['category']))
                conn.commit()

            except Exception as e:
                print(f"      ⚠️ Hiba a(z) {idx}. állásnál: {e}")

        conn.close()
        print(f"\n✨ SIKER! A BMW Group pozíciók (teszt mód) elmentve.")

    finally:
        driver.quit()


if __name__ == "__main__":
    run_scraper()
