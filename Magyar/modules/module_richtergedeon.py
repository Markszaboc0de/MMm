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
COMPANY_NAME = "Richter_Gedeon"
BASE_URL = "https://careers.gedeonrichter.com/search/"
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "richter_jobs.db")


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
    print(
        f"🚀 {COMPANY_NAME} Scraper indítása (SAP SuccessFactors Table & DOM Walker mód)...")

    options = uc.ChromeOptions()
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--headless=new")
    driver = uc.Chrome(options=options)
    wait = WebDriverWait(driver, 10)

    job_links = []
    unique_urls = set()

    try:
        # --- 1. FÁZIS: LINKEK GYŰJTÉSE ---
        print("📂 Richter Gedeon karrieroldal megnyitása...")
        driver.get(BASE_URL)
        time.sleep(5)

        # Cookie ablak bezárása (ha van)
        try:
            cookie_btn = driver.find_element(
                By.XPATH, "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'elfogad') or contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'accept')]")
            driver.execute_script("arguments[0].click();", cookie_btn)
            time.sleep(1)
        except:
            pass

        current_page = 1
        while True:

            print(f"📄 {current_page}. oldal adatainak begyűjtése...")

            try:
                wait.until(EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "tr.data-row")))
                time.sleep(1.5)
            except:
                print("⚠️ Nem találtam állásokat az oldalon.")
                break

            # Táblázatsorok beolvasása a megadott HTML alapján
            jobs_on_page = driver.execute_script("""
                let results = [];
                let rows = document.querySelectorAll('tr.data-row');
                
                rows.forEach(row => {
                    let linkEl = row.querySelector('.jobTitle-link');
                    // A desktop verzióból szedjük a lokációt
                    let locEl = row.querySelector('.colLocation .jobLocation') || row.querySelector('.jobLocation');
                    
                    if (linkEl && linkEl.href) {
                        results.push({
                            url: linkEl.href,
                            title: linkEl.innerText.trim(),
                            location_raw: locEl ? locEl.innerText.trim() : 'Budapest, HU'
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

            # INTELLIGENS LAPOZÁS: Megkeressük az aktív oldalszámot, és a szomszédos (következő) linket olvassuk ki
            next_url = driver.execute_script("""
                let activeLi = document.querySelector('ul.pagination li.active');
                if (activeLi && activeLi.nextElementSibling) {
                    let nextA = activeLi.nextElementSibling.querySelector('a');
                    // Csak akkor megyünk tovább, ha ez nem az utolsó ugrógomb
                    if (nextA && !nextA.className.includes('paginationItemLast')) {
                        return nextA.href;
                    }
                }
                return null;
            """)

            if next_url:
                print("🔄 Ugrás a következő oldalra...")
                driver.get(next_url)
                current_page += 1
                time.sleep(3.5)
            else:
                print("🏁 Nincs több oldal, lista vége.")
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
                    # Megvárjuk a leírás konténerét
                    wait.until(EC.presence_of_element_located(
                        (By.CSS_SELECTOR, ".jobdescription, [itemprop='description']")))
                    time.sleep(1.5)
                except:
                    time.sleep(3)

                # CÉLZOTT DOM BEJÁRÓ
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
                            
                            if (['H1','H2','H3','H4','B','STRONG'].includes(tag) && text.length > 5) {
                                // A Richter szereti a B vagy STRONG tageket címsornak használni,
                                // de csak akkor törünk sort, ha nem egy mondaton belüli kiemelés
                                text += "\\n\\n";
                            }
                            else if (['P','DIV','BR','LI'].includes(tag)) text += "\\n";
                        }
                        return text;
                    }

                    let mainContent = document.querySelector('.jobdescription') || 
                                      document.querySelector('[itemprop="description"]');
                                      
                    if (mainContent) {
                        return walk(mainContent);
                    } else {
                        let fallback = document.querySelector('.joblayouttoken') || document.querySelector('main');
                        return fallback ? walk(fallback) : walk(document.body);
                    }
                """)

                clean_desc = re.sub(r'\n{3,}', '\n\n', description).strip()

                if "Jelentkezem" in clean_desc:
                    clean_desc = clean_desc.split("Jelentkezem")[0].strip()
                elif "Apply now" in clean_desc:
                    clean_desc = clean_desc.split("Apply now")[0].strip()

                # --- 3. FÁZIS: VÁROS ÉS ORSZÁG BONTÁSA ---
                # A Richter formátuma tipikusan: "Budapest, HU" vagy "Debrecen, HU"
                location_raw = job['location_raw']
                parts = [p.strip() for p in location_raw.split(',')]

                city = parts[0] if parts else "Budapest"
                country_raw = parts[-1].upper() if len(parts) > 1 else "HU"

                country = "Magyarország" if country_raw in [
                    "HU", "HUNGARY", "MAGYARORSZÁG"] else country_raw

                conn.execute('''INSERT INTO jobs (url, title, company, location_raw, city, country, description, category) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                             (job['url'], job['title'], "Richter Gedeon", location_raw, city, country, clean_desc, "Egészségügy / Gyógyszeripar"))
                conn.commit()

            except Exception as e:
                print(f"      ⚠️ Hiba a(z) {idx}. állásnál: {e}")

        conn.close()
        print(f"\n✨ SIKER! A Richter Gedeon pozíciók elmentve.")

    finally:
        driver.quit()


if __name__ == "__main__":
    run_scraper()
