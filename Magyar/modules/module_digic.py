import sys as _sys
import os as _os
_sys.path.append(_os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))
from driver_setup import get_chrome_driver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
import sqlite3
import os
import sys
import time
import re
from markdownify import markdownify as md

sys.stdout.reconfigure(encoding='utf-8')

# --- KONFIGURÁCIÓ ---
COMPANY_NAME = "Digic Pictures"
BASE_URL = "https://career.digicpictures.com/jobs"
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "digic_jobs.db")


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
    # Képek letiltása a villámgyors görgetésért és betöltésért
    return get_chrome_driver()


def run_scraper():
    init_db()
    print(f"🚀 {COMPANY_NAME} Scraper indítása (Full Selenium JS Render mód)...")

    driver = create_driver()
    job_links = []
    unique_urls = set()

    try:
        # --- 1. FÁZIS: LINKEK GYŰJTÉSE (Selenium Infinite Scroll) ---
        print("📂 Digic Pictures karrieroldal megnyitása...")
        driver.get(BASE_URL)
        time.sleep(3)

        print("🔄 Végtelenített oldal pörgetése az összes állásért...")
        last_height = driver.execute_script(
            "return document.body.scrollHeight")

        while True:
            driver.execute_script(
                "window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1.5)
            new_height = driver.execute_script(
                "return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

        # Kinyerjük a linkeket és a címet a kártyákról
        jobs_on_page = driver.execute_script("""
            let results = [];
            document.querySelectorAll('a.job-link').forEach(a => {
                if(a.href) {
                    results.push({
                        title: a.innerText.trim(),
                        url: a.href
                    });
                }
            });
            return results;
        """)

        for job in jobs_on_page:
            if job['url'] not in unique_urls:
                unique_urls.add(job['url'])
                job_links.append(job)

        print(
            f"✅ Összesen {len(job_links)} db állás azonosítva! Kezdődik a leírások kinyerése...")

        if not job_links:
            return

        # --- 2. FÁZIS: LEÍRÁSOK KINYERÉSE (Selenium Smart Wait) ---
        conn = sqlite3.connect(DB_PATH)
        saved_count = 0
        wait = WebDriverWait(driver, 10, poll_frequency=0.2)

        for idx, job in enumerate(job_links, 1):
            try:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT id FROM jobs WHERE url = ?", (job['url'],))
                if cursor.fetchone():
                    print(
                        f"   [{idx}/{len(job_links)}] Ugrás (Már az adatbázisban van)")
                    continue

                sys.stdout.write(
                    f"\r   [{idx}/{len(job_links)}] Fetching: {job['title'][:30]}...")
                sys.stdout.flush()

                driver.get(job['url'])

                # 💡 GARANCIA: Megvárjuk, amíg a .singleJobBlockHolder tartalommal frissül
                try:
                    wait.until(
                        lambda d: len(d.execute_script("""
                            let blocks = document.querySelectorAll('.singleJobBlockHolder');
                            return blocks.length > 0 ? blocks[0].innerText : '';
                        """).strip()) > 20
                    )
                except:
                    pass  # Ha lassan tölt be, megpróbáljuk kinyerni amink van

                # JS-sel kinyerjük a .col-md-8 konténer tartalmát, a felesleges gombok eltávolításával
                desc_html = driver.execute_script("""
                    let container = document.querySelector('.col-md-8');
                    if (!container) return '';
                    
                    // Lemásoljuk a konténert, hogy a DOM-ot ne rontsuk el
                    let clone = container.cloneNode(true);
                    
                    // Eltávolítjuk a felesleges gombokat és megosztási sávokat
                    let junks = clone.querySelectorAll('.row.pb-5.hide, .hidden-lg, #btnJobSinglejobHeaderApply, .ftco-footer-social');
                    junks.forEach(j => j.remove());
                    
                    return clone.innerHTML;
                """)

                # 💡 LOKÁCIÓ FIX (Kérésed alapján)
                city = "Budapest"
                country = "Hungary"
                location_raw = "Budapest, Hungary"

                clean_desc = "Leírás nem található."

                if desc_html:
                    clean_desc = md(desc_html, heading_style="ATX", bullets="-",
                                    strip=['img', 'script', 'style', 'a', 'button', 'svg'])
                    clean_desc = clean_desc.replace(
                        '•', '-').replace('·', '-').replace('\xa0', ' ')
                    clean_desc = re.sub(r'\n{3,}', '\n\n', clean_desc).strip()

                conn.execute('''INSERT INTO jobs (url, title, company, location_raw, city, country, description, category) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                             (job['url'], job['title'], COMPANY_NAME, location_raw, city, country, clean_desc, "Animation / Creative"))
                conn.commit()
                saved_count += 1

            except Exception as e:
                print(f"\n      ⚠️ Hiba a(z) {idx}. állásnál: {e}")

        conn.close()
        print(
            f"\n✨ SIKER! {saved_count} db {COMPANY_NAME} állás letöltve az adatbázisba.")

    except Exception as e:
        print(f"\n❌ Váratlan hiba történt a futás során: {e}")
    finally:
        try:
            driver.quit()
        except:
            pass


if __name__ == "__main__":
    run_scraper()
