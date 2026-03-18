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
COMPANY_NAME = "KPMG"
BASE_URL = "https://kpmg.hrfelho.hu/allasajanlataink/gyakornok-palyakezdo"
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "kpmg_jobs.db")


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
    print(f"🚀 {COMPANY_NAME} Scraper indítása (HRFelhő mód)...")

    options = uc.ChromeOptions()
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--headless=new")
    driver = uc.Chrome(options=options)

    job_links = []
    unique_urls = set()

    try:
        # --- 1. FÁZIS: LINKEK GYŰJTÉSE A TÁBLÁZATBÓL ---
        print("📂 KPMG karrieroldal megnyitása...")
        driver.get(BASE_URL)

        # Várunk a gombok megjelenésére
        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "a.viewjob")))
            time.sleep(2)
        except:
            print("⚠️ Nem töltődött be a lista. Ellenőrizd az oldalt!")
            return

        print("📄 Linkek és metadatok kinyerése a táblázatból...")

        jobs_on_page = driver.execute_script("""
            let results = [];
            // Megkeressük az összes gombot
            let btns = document.querySelectorAll('a.viewjob');
            
            btns.forEach(btn => {
                if (btn.href) {
                    // Megkeressük a gombhoz tartozó táblázat sort (tr)
                    let row = btn.closest('tr');
                    let title = "KPMG Pozíció";
                    let category = "Pénzügy / Tanácsadás";
                    
                    if (row) {
                        let cells = row.querySelectorAll('td');
                        // A kép alapján: 1. oszlop = Cím, 2. oszlop = Üzletág
                        if (cells.length > 0) title = cells[0].innerText.trim();
                        if (cells.length > 1) category = cells[1].innerText.trim();
                    }
                    
                    results.push({
                        url: btn.href,
                        title: title,
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

        if not job_links:
            return

        # --- 2. FÁZIS: LEÍRÁSOK SCRAPELÉSE ---
        conn = sqlite3.connect(DB_PATH)
        wait = WebDriverWait(driver, 10)

        for idx, job in enumerate(job_links, 1):
            try:
                conn.execute("DELETE FROM jobs WHERE url = ?", (job['url'],))

                print(
                    f"   [{idx}/{len(job_links)}] {job['title']} ({job['category']})")
                driver.get(job['url'])

                try:
                    # Várunk a megadott .job-col konténerre
                    wait.until(EC.presence_of_element_located(
                        (By.CSS_SELECTOR, ".job-col")))
                    time.sleep(1)
                except:
                    time.sleep(3)

                # DOM Walker bejáró
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

                            // Felesleges elemek kihagyása
                            if (['SCRIPT','STYLE','NAV','FOOTER','HEADER','BUTTON','SVG','HR'].includes(tag)) return "";
                            
                            // KPMG specifikus zajszűrés: az .outro konténer ignorálása (Miért a KPMG? rész)
                            if (cls.includes('outro')) return "";

                            if (tag === 'LI') text += "• ";
                            for (let child of el.childNodes) text += walk(child); 
                            
                            // Formázás
                            if (['H1','H2','H3','H4'].includes(tag)) text += "\\n\\n";
                            else if (['B','STRONG'].includes(tag) && text.length > 10) text += "\\n\\n";
                            else if (['P','DIV','BR','LI'].includes(tag)) text += "\\n";
                        }
                        return text;
                    }

                    // Fókuszálunk a tényleges leírás konténerére
                    let mainContent = document.querySelector('.col-md-8.job-col') || document.querySelector('.job-col');
                    return mainContent ? walk(mainContent) : walk(document.body);
                """)

                clean_desc = re.sub(r'\n{3,}', '\n\n', description).strip()

                # A KPMG központja Budapesten van, az esetek 99%-ában ide keresnek gyakornokot
                location_raw = "Budapest"
                city = "Budapest"

                conn.execute('''INSERT INTO jobs (url, title, company, location_raw, city, country, description, category) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                             (job['url'], job['title'], COMPANY_NAME, location_raw, city, "Magyarország", clean_desc, job['category']))
                conn.commit()

            except Exception as e:
                print(f"      ⚠️ Hiba a(z) {idx}. állásnál: {e}")

        conn.close()
        print(f"\n✨ SIKER! A {COMPANY_NAME} pozíciók mentve.")

    finally:
        try:
            driver.quit()
        except:
            pass


if __name__ == "__main__":
    run_scraper()
