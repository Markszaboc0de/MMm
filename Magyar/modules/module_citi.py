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
COMPANY_NAME = "Citi"
# Megjegyzés: Ez az URL a globális kereső. Ha csak a magyar állások kellenek,
# érdemes lehet a böngészőből kimásolni a szűrt linket (pl. .../search-jobs/Hungary/...)
BASE_URL = "https://jobs.citi.com/search-jobs"
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "citi_jobs.db")


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
        f"🚀 {COMPANY_NAME} Scraper indítása (Smart Pagination & Universal Walker mód)...")

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
        # --- 1. FÁZIS: LINKEK GYŰJTÉSE ---
        print("📂 Citibank karrieroldal megnyitása...")
        driver.get(BASE_URL)
        time.sleep(5)

        # Cookie ablak bezárása (ha van)
        try:
            cookie_btn = driver.find_element(
                By.XPATH, "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'elfogad') or contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'accept')] | //button[@id='gdpr-button']")
            driver.execute_script("arguments[0].click();", cookie_btn)
            time.sleep(1)
        except:
            pass

        current_page = 1
        while True:

            print(f"📄 {current_page}. oldal adatainak begyűjtése...")

            try:
                wait.until(EC.presence_of_element_located(
                    (By.CSS_SELECTOR, ".sr-job-item")))
                time.sleep(1.5)
            except:
                print("⚠️ Nem találtam állásokat a megadott oldalon.")
                break

            # Álláskártyák beolvasása a megadott HTML alapján
            jobs_on_page = driver.execute_script("""
                let results = [];
                let cards = document.querySelectorAll('.sr-job-item');
                
                cards.forEach(card => {
                    let linkEl = card.querySelector('a.sr-job-item__link');
                    let locEl = card.querySelector('.sr-job-location');
                    
                    if (linkEl && linkEl.href) {
                        results.push({
                            url: linkEl.href,
                            title: linkEl.innerText.trim(),
                            location_raw: locEl ? locEl.innerText.trim() : 'Ismeretlen'
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

            # LAPOZÁS (Az a.next elem href attribútumának lekérése)
            try:
                next_btns = driver.find_elements(By.CSS_SELECTOR, "a.next")
                if next_btns and next_btns[0].is_displayed():
                    next_url = next_btns[0].get_attribute("href")
                    if next_url:
                        print("🔄 Ugrás a következő oldalra...")
                        driver.get(next_url)
                        current_page += 1
                        time.sleep(3.5)  # Várakozás a teljes oldalbetöltésre
                    else:
                        break
                else:
                    print("🏁 Nincs (több) 'Next' gomb, lista vége.")
                    break
            except Exception as e:
                print(f"🏁 Lapozás befejezve: {e}")
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
                    # Megvárjuk az általános leírás konténerek valamelyikét
                    wait.until(EC.presence_of_element_located(
                        (By.CSS_SELECTOR, ".job-description, .job-details, article, main")))
                    time.sleep(1.5)
                except:
                    time.sleep(3)

                # UNIVERZÁLIS DOM BEJÁRÓ (Mivel nem kaptunk HTML-t az aloldalhoz)
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
                            
                            if (['H1','H2','H3','H4'].includes(tag)) text += "\\n\\n";
                            else if (['P','DIV','BR','LI'].includes(tag)) text += "\\n";
                        }
                        return text;
                    }

                    // A Citi (és a hasonló ATS-ek) leggyakoribb szövegtartó div-jei
                    let mainContent = document.querySelector('.job-description') || 
                                      document.querySelector('.job-details') || 
                                      document.querySelector('.ats-description') ||
                                      document.querySelector('article');
                                      
                    if (mainContent) {
                        return walk(mainContent);
                    } else {
                        let fallback = document.querySelector('main');
                        return fallback ? walk(fallback) : walk(document.body);
                    }
                """)

                clean_desc = re.sub(r'\n{3,}', '\n\n', description).strip()

                # Biztonsági vágás a gombok mentén
                if "Apply Now" in clean_desc:
                    clean_desc = clean_desc.split("Apply Now")[0].strip()
                elif "Jelentkezem" in clean_desc:
                    clean_desc = clean_desc.split("Jelentkezem")[0].strip()

                # --- 3. FÁZIS: VÁROS ÉS ORSZÁG BONTÁSA ---
                # A Citi formátuma: "Budapest, Budapest, Hungary"
                location_raw = job['location_raw']
                parts = [p.strip() for p in location_raw.split(',')]

                if len(parts) >= 3:
                    city = parts[0]
                    country = "Magyarország" if parts[-1].lower(
                    ) in ["hungary", "magyarország"] else parts[-1]
                elif len(parts) == 2:
                    city = parts[0]
                    country = "Magyarország" if parts[1].lower(
                    ) in ["hungary", "magyarország"] else parts[1]
                else:
                    city = location_raw
                    country = "Ismeretlen"

                conn.execute('''INSERT INTO jobs (url, title, company, location_raw, city, country, description, category) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                             (job['url'], job['title'], "Citi", location_raw, city, country, clean_desc, "Pénzügy / Bank"))
                conn.commit()

            except Exception as e:
                print(f"      ⚠️ Hiba a(z) {idx}. állásnál: {e}")

        conn.close()
        print(f"\n✨ SIKER! A Citibank pozíciók elmentve.")

    finally:
        driver.quit()


if __name__ == "__main__":
    run_scraper()
