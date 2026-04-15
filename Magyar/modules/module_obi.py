import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import sqlite3
import os
import sys
import time
import re
from markdownify import markdownify as md

sys.stdout.reconfigure(encoding='utf-8')

# --- KONFIGURÁCIÓ ---
COMPANY_NAME = "OBI"
BASE_SEARCH_URL = "https://karrier.obi.hu/gyakornoki-poziciok?utm_source=talents-connect&utm_medium=organic&utm_campaign=talents-connect#23064ec9-19f8-404a-902b-775566837dda"

# Mentés a Magyar mappába!
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "obi_jobs.db")


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
    options = uc.ChromeOptions()
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument('--blink-settings=imagesEnabled=false')
    return uc.Chrome(options=options, version_main=145)


def run_scraper():
    init_db()
    print(f"🚀 {COMPANY_NAME} Scraper indítása (Iframe & Talents Connect mód)...")

    driver = create_driver()
    job_links = []
    unique_urls = set()

    try:
        # --- 1. FÁZIS: LINKEK GYŰJTÉSE ---
        print("📂 Karrieroldal betöltése...")
        driver.get(BASE_SEARCH_URL)

        wait = WebDriverWait(driver, 15, poll_frequency=0.5)

        # 💡 A LÉNYEG: Átváltunk a beágyazott ablakba (Iframe), mert a widget abban él!
        try:
            # Várunk, amíg az iframe megjelenik a dom-ban
            iframe = wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, "iframe")))
            driver.switch_to.frame(iframe)
        except Exception:
            # Ha nincs iframe (esetleg kivették), próbáljuk meg a főoldalon
            pass

        # Várunk, amíg a Vue.js felépíti a kártyákat az iframe-ben
        for _ in range(20):
            if driver.execute_script("return document.querySelectorAll('a.result-item').length > 0"):
                break
            time.sleep(0.5)

        page_num = 1
        while True:
            sys.stdout.write(
                f"\r   🔄 {page_num}. oldal feldolgozása... (Eddig begyűjtve: {len(job_links)})")
            sys.stdout.flush()

            # Adatok kinyerése az aktuális oldalról JS-sel
            jobs_on_page = driver.execute_script("""
                let results = [];
                document.querySelectorAll('a.result-item').forEach(card => {
                    let url = card.href;
                    let titleEl = card.querySelector('h3.title');
                    let cityEl = card.querySelector('.location');
                    let catEl = card.querySelector('[data-testid="detail-department"] li');
                    
                    if(url && titleEl) {
                        results.push({
                            url: url, 
                            title: titleEl.getAttribute('title') || titleEl.innerText.trim(),
                            city: cityEl ? cityEl.innerText.trim() : 'N/A',
                            category: catEl ? catEl.innerText.trim() : 'Retail / Operations'
                        });
                    }
                });
                return results;
            """)

            for job in jobs_on_page:
                clean_job_url = job['url'].split('#')[0].rstrip('/')

                if clean_job_url not in unique_urls:
                    unique_urls.add(clean_job_url)

                    city = job['city']
                    country = "Hungary"
                    clean_location_raw = f"{city}, {country}" if city != "N/A" else country

                    job_links.append({
                        "url": clean_job_url,
                        "title": job['title'],
                        "location_raw": clean_location_raw,
                        "city": city,
                        "country": country,
                        "category": job['category']
                    })

            # Lapozás a "Következő" gombbal az Iframe-en belül
            try:
                next_btn = driver.find_element(
                    By.CSS_SELECTOR, 'button.is-next')

                if not next_btn.is_enabled() or 'disabled' in next_btn.get_attribute('class'):
                    break

                driver.execute_script(
                    "arguments[0].scrollIntoView({block: 'center'});", next_btn)
                time.sleep(0.2)
                driver.execute_script("arguments[0].click();", next_btn)
                time.sleep(1)
                page_num += 1
            except Exception:
                break

        print(
            f"\n✅ Összesen {len(job_links)} db állás azonosítva! Kezdődik a mélyfúrás...")

        if not job_links:
            return

        # --- 2. FÁZIS: LEÍRÁSOK KINYERÉSE (SELENIUMMAL) ---
        print("📄 Részletek letöltése (Átirányítások és Iframe lekezelése)...")
        conn = sqlite3.connect(DB_PATH)
        saved_count = 0

        for idx, job in enumerate(job_links, 1):
            try:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT id FROM jobs WHERE url = ?", (job['url'],))
                if cursor.fetchone():
                    sys.stdout.write(
                        f"\r   [{idx}/{len(job_links)}] Ugrás (Már az adatbázisban van)")
                    sys.stdout.flush()
                    continue

                sys.stdout.write(
                    f"\r   [{idx}/{len(job_links)}] Fetching: {job['title'][:30]}...")
                sys.stdout.flush()

                # Visszaváltunk a fő dokumentumra a biztonság kedvéért, mielőtt új URL-t nyitunk
                driver.switch_to.default_content()

                driver.get(job['url'])
                time.sleep(1)  # Várunk az átirányításra

                # 💡 Ha az aloldal widgetje is iframe-ben él, átváltunk abba is!
                try:
                    iframe = driver.find_element(By.CSS_SELECTOR, 'iframe')
                    driver.switch_to.frame(iframe)
                except:
                    pass

                # Várunk, amíg a végleges állásoldal (a .content div) betölt
                for _ in range(15):
                    if driver.execute_script("return document.querySelectorAll('.content, .content-body').length > 0"):
                        break
                    time.sleep(0.5)

                job_soup = BeautifulSoup(driver.page_source, 'html.parser')
                clean_desc = "Leírás nem található."

                desc_el = job_soup.find('div', class_='content')
                if not desc_el:
                    desc_el = job_soup.find('div', class_='content-body')

                if desc_el:
                    for trash in desc_el.find_all('div', class_='content__intro'):
                        trash.decompose()

                    raw_html = str(desc_el)
                    clean_desc = md(raw_html, heading_style="ATX", bullets="-",
                                    strip=['img', 'script', 'style', 'a', 'button', 'svg'])

                clean_desc = clean_desc.replace(
                    '•', '-').replace('·', '-').replace('\xa0', ' ').strip()
                clean_desc = re.sub(r'\n{3,}', '\n\n', clean_desc).strip()

                print(
                    f"\n      -> Cím: {job['title']} | Hely: {job['city']}, {job['country']}")

                conn.execute('''INSERT OR REPLACE INTO jobs (id, url, title, company, location_raw, city, country, description, category, date_found)
                                VALUES (
                                    (SELECT id FROM jobs WHERE url = ?), 
                                    ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP
                                )''',
                             (job['url'], job['url'], job['title'], COMPANY_NAME, job['location_raw'], job['city'], job['country'], clean_desc, job['category']))
                conn.commit()
                saved_count += 1

            except Exception as e:
                print(f"\n      ⚠️ Hiba a(z) {idx}. állásnál: {e}")

        conn.close()
        print(
            f"\n✨ SIKER! {saved_count} db {COMPANY_NAME} állás letöltve a Magyar mappába.")

    except Exception as e:
        print(f"\n❌ Váratlan hiba történt a futás során: {e}")
    finally:
        try:
            # Visszaváltunk a fő dokumentumra zárás előtt, hogy elkerüljük az OS hibát
            driver.switch_to.default_content()
            driver.quit()
        except:
            pass


if __name__ == "__main__":
    run_scraper()
