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
COMPANY_NAME = "Raiffeisen_Bank"
BASE_URL = "https://raiffeisen.karrierportal.hu/allasok?q=bGV2ZWxzJTVCJTVEJTNER3lha29ybm9rJTJGUCVDMyVBMWx5YWtlemQlQzUlOTElMjYuuzzuuzz#!"
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "raiffeisen_jobs.db")


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
        f"🚀 {COMPANY_NAME} Scraper indítása (Smart Button Paginator & JobEnd Tasks mód)...")

    options = uc.ChromeOptions()
    options.add_argument("--window-size=1920,1080")
    driver = uc.Chrome(options=options, version_main=145)
    wait = WebDriverWait(driver, 10)

    job_links = []
    unique_urls = set()

    try:
        # --- 1. FÁZIS: LINKEK GYŰJTÉSE ---
        print("📂 Raiffeisen karrieroldal megnyitása...")
        driver.get(BASE_URL)
        time.sleep(5)

        # Cookie ablak bezárása (ha van)
        try:
            cookie_btn = driver.find_element(
                By.XPATH, "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'elfogad')] | //a[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'elfogad')]")
            driver.execute_script("arguments[0].click();", cookie_btn)
            time.sleep(1)
        except:
            pass

        current_page = 1
        while True:

            print(f"📄 {current_page}. oldal adatainak begyűjtése...")

            try:
                wait.until(EC.presence_of_element_located(
                    (By.CSS_SELECTOR, ".jobList__item")))
                time.sleep(1.5)
            except:
                print("⚠️ Nem találtam állásokat. Lehet, hogy üres az oldal.")
                break

            jobs_on_page = driver.execute_script("""
                let results = [];
                let cards = document.querySelectorAll('.jobList__item');
                
                cards.forEach(card => {
                    let linkEl = card.querySelector('.jobList__item__title a');
                    
                    if (linkEl && linkEl.href) {
                        let title = linkEl.innerText.trim();
                        let locEl = card.querySelector('.job_list_city');
                        let catEl = card.querySelector('.job_list_specialities');
                        
                        results.push({
                            url: linkEl.href,
                            title: title,
                            location_raw: locEl ? locEl.innerText.trim() : 'Ismeretlen',
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

            # LAPOZÁS
            try:
                next_page_num = current_page + 1
                next_btn = driver.find_element(
                    By.XPATH, f"//button[contains(@class, 'pager-element') and text()='{next_page_num}']")

                print(f"🔄 Lapozás a(z) {next_page_num}. oldalra...")
                driver.execute_script(
                    "arguments[0].scrollIntoView({block: 'center'});", next_btn)
                time.sleep(1)
                driver.execute_script("arguments[0].click();", next_btn)

                current_page += 1
                time.sleep(3.5)
            except:
                print("🏁 Nincs több lapozógomb, lista vége.")
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
                    # Kifejezetten a te általad küldött osztályt várjuk!
                    wait.until(EC.presence_of_element_located(
                        (By.CSS_SELECTOR, ".jobEnd__tasks")))
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

                            if (['SCRIPT','STYLE','NAV','FOOTER','HEADER','BUTTON','SVG'].includes(tag)) return "";
                            if (cls.includes('share') || cls.includes('apply') || cls.includes('back')) return "";

                            if (tag === 'LI') text += "• ";
                            
                            for (let child of el.childNodes) { 
                                text += walk(child); 
                            }
                            
                            // Ha H2 (cím), tegyünk utána két sortörést a szebb tagolásért
                            if (['H1','H2','H3','H4'].includes(tag)) text += "\\n\\n";
                            // Sima bekezdés és lista sortörés
                            else if (['P','DIV','BR','LI'].includes(tag)) text += "\\n";
                        }
                        return text;
                    }

                    // A megadott HTML alapján a .jobEnd__tasks divet célozzuk
                    let mainContent = document.querySelector('.jobEnd__tasks');
                                      
                    if (mainContent) {
                        return walk(mainContent);
                    } else {
                        // Tartalék, ha esetleg más sablont használna egy régebbi állás
                        let fallback = document.querySelector('.jobDetails') || document.querySelector('main');
                        return fallback ? walk(fallback) : walk(document.body);
                    }
                """)

                # Tisztítjuk a túl sok üres sort
                clean_desc = re.sub(r'\n{3,}', '\n\n', description).strip()

                # Vágjuk le a gombokat a tartalék esetére
                if "Jelentkezem" in clean_desc:
                    clean_desc = clean_desc.split("Jelentkezem")[0].strip()
                elif "Jelentkezés" in clean_desc:
                    clean_desc = clean_desc.split("Jelentkezés")[0].strip()

                # --- 3. FÁZIS: VÁROS ÉS ORSZÁG BONTÁSA ---
                location_raw = job['location_raw']
                parts = [p.strip() for p in location_raw.split(',')]

                city = parts[0] if parts else "Budapest"
                country = "Magyarország"

                conn.execute('''INSERT INTO jobs (url, title, company, location_raw, city, country, description, category) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                             (job['url'], job['title'], "Raiffeisen Bank", location_raw, city, country, clean_desc, job['category']))
                conn.commit()

            except Exception as e:
                print(f"      ⚠️ Hiba a(z) {idx}. állásnál: {e}")

        conn.close()
        print(f"\n✨ SIKER! A Raiffeisen Bank pozíciók elmentve.")

    finally:
        driver.quit()


if __name__ == "__main__":
    run_scraper()
