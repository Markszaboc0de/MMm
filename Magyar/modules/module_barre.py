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
COMPANY_NAME = "Barre"
BASE_URL = "https://www.barre.hu/karrier/"
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "barre_jobs.db")


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
    print(f"🚀 {COMPANY_NAME} Scraper indítása (Targeted Grid-Column mód)...")

    options = uc.ChromeOptions()
    options.add_argument("--window-size=1920,1080")
    driver = uc.Chrome(options=options, version_main=145)

    job_links = []
    unique_urls = set()

    try:
        # --- 1. FÁZIS: LINKEK GYŰJTÉSE ---
        print("📂 Barre karrieroldal megnyitása...")
        driver.get(BASE_URL)
        time.sleep(5)

        try:
            cookie_btn = driver.find_element(
                By.XPATH, "//button[contains(text(), 'Elfogadom') or contains(text(), 'Accept')] | //a[contains(text(), 'Elfogadom')]")
            driver.execute_script("arguments[0].click();", cookie_btn)
            time.sleep(1)
        except:
            pass

        print("🔍 Álláskártyák beolvasása...")

        jobs_on_page = driver.execute_script("""
            let results = [];
            let links = document.querySelectorAll('.col-8 h3.h4 a');
            links.forEach(link => {
                if (link.href) {
                    results.push({
                        url: link.href,
                        title: link.innerText.trim()
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

                print(f"   [{idx}/{len(job_links)}] {job['title']}")
                driver.get(job['url'])
                time.sleep(4)

                # JAVÍTOTT: Célzott kinyerés a beküldött HTML alapján
                description = driver.execute_script("""
                    let result = "";
                    let columns = document.querySelectorAll('.col-md-6');
                    let hasRelevantContent = false;

                    // Végigmegyünk a hasábokon
                    columns.forEach(col => {
                        let titleEl = col.querySelector('h2.title-gr');
                        let listEl = col.querySelector('ul');
                        
                        // Ha van benne Címsor (pl. "Elvárások")
                        if (titleEl) {
                            hasRelevantContent = true;
                            result += titleEl.innerText.trim() + "\\n\\n";
                            
                            // Ha van benne lista
                            if (listEl) {
                                let items = listEl.querySelectorAll('li');
                                items.forEach(li => {
                                    result += "• " + li.innerText.trim() + "\\n";
                                });
                            }
                            result += "\\n";
                        }
                    });

                    // Ha véletlenül olyan állás van, ami nem ilyen grid-es struktúrájú
                    if (!hasRelevantContent) {
                        let mainContent = document.querySelector('.container') || document.body;
                        result = mainContent.innerText;
                    }

                    return result;
                """)

                clean_desc = re.sub(r'\n\s*\n', '\n\n', description).strip()

                # Vágjuk le a felesleget, ha a tartalék módszer futott le
                if "Jelentkezem" in clean_desc:
                    clean_desc = clean_desc.split("Jelentkezem")[0].strip()
                elif "Jelentkezés" in clean_desc:
                    clean_desc = clean_desc.split("Jelentkezés")[0].strip()

                location_raw = "Budapest"
                city = "Budapest"
                country = "Magyarország"
                category = "Informatika / IT"

                conn.execute('''INSERT INTO jobs (url, title, company, location_raw, city, country, description, category) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                             (job['url'], job['title'], "Barre", location_raw, city, country, clean_desc, category))
                conn.commit()

            except Exception as e:
                print(f"      ⚠️ Hiba a(z) {idx}. állásnál: {e}")

        conn.close()
        print(f"\n✨ SIKER! A Barre összes pozíciójának leírása célzottan letöltve.")

    finally:
        driver.quit()


if __name__ == "__main__":
    run_scraper()
