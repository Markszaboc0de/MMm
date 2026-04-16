import sys as _sys
import os as _os
_sys.path.append(_os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))
from driver_setup import get_chrome_driver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import sqlite3
import os
import sys
import time
import re
from markdownify import markdownify as md

sys.stdout.reconfigure(encoding='utf-8')

# --- KONFIGURÁCIÓ ---
COMPANY_NAME = "Carbyne"
BASE_URL = "https://carbyne.hu/vacancies-2/"
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "carbyne_jobs.db")


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


def create_driver():
    return get_chrome_driver()


def run_scraper():
    init_db()
    print(f"🚀 {COMPANY_NAME} Scraper indítása (Strict VC-Engine mód)...")

    driver = create_driver()
    job_links = []
    unique_urls = set()

    try:
        # --- 1. FÁZIS: LINKEK GYŰJTÉSE ---
        print("📂 Carbyne állásoldal megnyitása...")
        driver.get(BASE_URL)
        time.sleep(8)

        try:
            cookie_btn = driver.find_element(
                By.XPATH, "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'elfogad')] | //a[contains(@class, 'cookie') and contains(text(), 'Ok')]")
            driver.execute_script("arguments[0].click();", cookie_btn)
            time.sleep(1)
        except:
            pass

        print("🔍 Álláskártyák keresése...")
        jobs_on_page = driver.execute_script("""
            let results = [];
            let links = document.querySelectorAll('a.vc_gitem-link, .vc_grid-item a[href*="/job/"]');
            
            links.forEach(a => {
                if (a.href) {
                    results.push({
                        url: a.href,
                        raw_title: a.innerHTML.trim()
                    });
                }
            });
            return results;
        """)

        for job in jobs_on_page:
            if "/job/" in job['url'] and job['url'] not in unique_urls:
                unique_urls.add(job['url'])
                job_links.append(job)

        print(
            f"✅ Összesen {len(job_links)} egyedi állás azonosítva. Böngésző újraindítása...")

        if not job_links:
            return

        # --- PRE-PHASE 2: BROWSER REBOOT ---
        try:
            driver.quit()
        except:
            pass
        time.sleep(2)

        driver = create_driver()
        wait = WebDriverWait(driver, 10)

        # --- 2. FÁZIS: LEÍRÁSOK ÉS ADATOK TISZTÍTÁSA ---
        conn = sqlite3.connect(DB_PATH)
        saved_count = 0

        for idx, job in enumerate(job_links, 1):
            try:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT id FROM jobs WHERE url = ?", (job['url'],))
                if cursor.fetchone():
                    print(
                        f"   [{idx}/{len(job_links)}] {job['url'].split('/')[-2]} (Már az adatbázisban, ugrás...)")
                    continue

                # AUTO-RECOVERY BLOCK
                try:
                    driver.get(job['url'])
                except:
                    print(
                        "   🔄 Kapcsolat megszakadt. Böngésző automatikus újraindítása...")
                    try:
                        driver.quit()
                    except:
                        pass
                    time.sleep(2)
                    driver = create_driver()
                    wait = WebDriverWait(driver, 10)
                    driver.get(job['url'])

                try:
                    # Megvárjuk az egyedi .vc_acf konténert
                    wait.until(EC.presence_of_element_located(
                        (By.CSS_SELECTOR, ".vc_acf, .wpb_text_column")))
                    time.sleep(1)
                except:
                    time.sleep(2)

                details = driver.execute_script("""
                    let result = { title: "", description: "" };
                    
                    // 1. Cím kinyerése
                    let titleEl = document.querySelector('h1, .vc_custom_heading h1, .post-title');
                    if (titleEl) {
                        result.title = titleEl.innerText.trim();
                    }
                    
                    // 2. Leírás kinyerése Szigorúan az elküldött struktúra alapján (.vc_acf)
                    // Nincs több body.innerHTML fallback!
                    let acfBlocks = document.querySelectorAll('.vc_acf');
                    
                    if (acfBlocks.length > 0) {
                        acfBlocks.forEach(block => {
                            result.description += block.innerHTML + "<br><br>";
                        });
                    } else {
                        let wpbBlocks = document.querySelectorAll('.wpb_text_column .wpb_wrapper');
                        wpbBlocks.forEach(block => {
                            result.description += block.innerHTML + "<br><br>";
                        });
                    }
                    
                    return result;
                """)

                # --- 3. FÁZIS: ADATTISZTÍTÁS ---

                # Cím tisztítása a "NEW!" felirattól
                title_raw = details['title'] if details['title'] else job['url'].split(
                    '/')[-2].replace('-', ' ').title()
                clean_title = re.sub(r'(?i)NEW!?', '', title_raw).replace(
                    '  ', ' ').strip()
                # Olykor a cím elején maradhatnak speciális karakterek (pl. csillagok, kacsacsőrök), ha azok a NEW-hoz tartoztak
                clean_title = re.sub(
                    r'^[^a-zA-Z0-9]+', '', clean_title).strip()

                # Fix Lokáció beállítása
                city = "Budapest"
                country = "Hungary"
                location_raw = "Budapest, Hungary"
                category = "HR / Recruiter"

                print(f"   [{idx}/{len(job_links)}] Formázás: {clean_title}")

                # --- FORMAT DESCRIPTION (Markdownify) ---
                if details['description']:
                    clean_desc = md(details['description'], heading_style="ATX", bullets="-", strip=[
                                    'img', 'script', 'style', 'a', 'button', 'svg'])
                    clean_desc = clean_desc.replace(
                        '•', '-').replace('·', '-').replace('\xa0', ' ').replace('&nbsp;', ' ')
                    clean_desc = re.sub(r'\n{3,}', '\n\n', clean_desc).strip()
                else:
                    clean_desc = "Leírás nem található. (Lehet, hogy az oldal teljesen más szerkezetű.)"

                # Levágjuk a jelentkezési marketing szöveget a leírás végéről
                truncation_markers = ["Ha felkeltette érdeklődését a pozíció",
                                      "Töltse fel CV", "Apply Now", "Jelentkezés", "📩"]
                for marker in truncation_markers:
                    if marker in clean_desc:
                        clean_desc = clean_desc.split(marker)[0].strip()

                conn.execute('''INSERT INTO jobs (url, title, company, location_raw, city, country, description, category) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                             (job['url'], clean_title, COMPANY_NAME, location_raw, city, country, clean_desc, category))
                conn.commit()
                saved_count += 1

            except Exception as e:
                print(f"      ⚠️ Hiba a(z) {idx}. állásnál: {e}")

        conn.close()
        print(
            f"\n✨ SIKER! {saved_count} {COMPANY_NAME} állás biztonságosan mentve.")

    finally:
        try:
            driver.quit()
        except:
            pass


if __name__ == "__main__":
    run_scraper()
