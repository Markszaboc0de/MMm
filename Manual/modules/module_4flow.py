import sys as _sys
import os as _os
_sys.path.append(_os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))
from driver_setup import get_chrome_driver
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
COMPANY_NAME = "4flow"
BASE_SEARCH_URL = "https://careers.4flow.com/search?utm_source=homepage&utm_medium=banner&utm_campaign=jobshop&location=Budapest&location=Berlin&location=Dresden&location=D%C3%BCsseldorf&location=Hamburg&location=Heidelberg&location=Bad%20Nauheim&location=Munich&location=Paris&location=R%C3%BCsselsheim%20am%20Main&location=Stuttgart"

# Mentés a Manual mappába!
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Manual\data"
DB_PATH = os.path.join(DATA_FOLDER, "4flow_jobs.db")

# Csak a gyakornoki és pályakezdő kulcsszavak
ENTRY_LEVEL_KEYWORDS = ['intern', 'internship', 'trainee', 'student',
                        'working student', 'werkstudent', 'praktikant', 'praktikum', 'graduate', 'gyakornok', 'pályakezdő', 'diákmunka', 'duális', 'diplomás', 'abschlussarbeit', 'bachelor', 'master', 'junior']

# Ország hozzárendelés a megadott városok alapján
COUNTRY_MAPPING = {
    'Budapest': 'Hungary',
    'Berlin': 'Germany',
    'Dresden': 'Germany',
    'Düsseldorf': 'Germany',
    'Hamburg': 'Germany',
    'Heidelberg': 'Germany',
    'Bad Nauheim': 'Germany',
    'Munich': 'Germany',
    'Paris': 'France',
    'Rüsselsheim am Main': 'Germany',
    'Stuttgart': 'Germany',
    'Frankfurt': 'Germany'
}


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
    print(f"🚀 {COMPANY_NAME} Scraper indítása (Intelligens Widget-kereső mód)...")

    driver = create_driver()
    job_links = []
    unique_urls = set()

    try:
        # --- 1. FÁZIS: LINKEK ÉS VÁROSOK GYŰJTÉSE ---
        print("📂 Karrieroldal betöltése és várás a widgetre...")
        driver.get(BASE_SEARCH_URL)

        # 💡 INTELLIGENS WIDGET KERESŐ
        widget_found = False
        for _ in range(15):
            driver.switch_to.default_content()

            # 1. Próba: Benne van-e a fő DOM-ban?
            if driver.execute_script("return document.querySelectorAll('a.result-item').length > 0"):
                widget_found = True
                break

            # 2. Próba: Iframe-ek átfésülése
            iframes = driver.find_elements(By.TAG_NAME, "iframe")
            for iframe in iframes:
                try:
                    driver.switch_to.frame(iframe)
                    if driver.execute_script("return document.querySelectorAll('a.result-item').length > 0"):
                        widget_found = True
                        break
                    driver.switch_to.default_content()
                except:
                    driver.switch_to.default_content()

            if widget_found:
                break
            time.sleep(1)

        if not widget_found:
            print("❌ Nem találtuk meg az állásokat tartalmazó blokkot az oldalon!")
            return

        # 💡 Végtelen görgetés a widgeten belül
        print("   🔄 Görgetés a lista végéig...")
        last_count = 0
        scroll_attempts = 0
        while True:
            # Görgetünk a legutolsó kártyához
            driver.execute_script("""
                let items = document.querySelectorAll('a.result-item');
                if(items.length > 0) {
                    items[items.length - 1].scrollIntoView({block: 'center'});
                }
            """)
            time.sleep(2)

            current_count = driver.execute_script(
                "return document.querySelectorAll('a.result-item').length")
            if current_count == last_count:
                scroll_attempts += 1
                if scroll_attempts >= 3:
                    break
            else:
                scroll_attempts = 0
                last_count = current_count

        # 💡 ADATOK KINYERÉSE JS-SEL
        jobs_on_page = driver.execute_script("""
            let results = [];
            document.querySelectorAll('a.result-item').forEach(card => {
                let url = card.href;
                let titleEl = card.querySelector('h3.title');
                let cityEl = card.querySelector('.location');
                
                if(url && titleEl) {
                    results.push({
                        url: url, 
                        title: titleEl.getAttribute('title') || titleEl.innerText.trim(),
                        city: cityEl ? cityEl.innerText.trim() : 'N/A'
                    });
                }
            });
            return results;
        """)

        for job in jobs_on_page:
            # 💡 SZŰRÉS: Csak a gyakornoki / pályakezdő állások kellenek a Manual mappába!
            title_lower = job['title'].lower()
            if not any(kw in title_lower for kw in ENTRY_LEVEL_KEYWORDS):
                continue

            clean_job_url = job['url'].split('#')[0].rstrip('/')

            if clean_job_url not in unique_urls:
                unique_urls.add(clean_job_url)

                # Ország meghatározása a szótár alapján
                city = job['city']
                country = "Germany"  # Alapértelmezett ország
                for key, val in COUNTRY_MAPPING.items():
                    if key.lower() in city.lower():
                        city = key
                        country = val
                        break

                clean_location_raw = f"{city}, {country}" if city != "N/A" else country

                job_links.append({
                    "url": clean_job_url,
                    "title": job['title'],
                    "location_raw": clean_location_raw,
                    "city": city,
                    "country": country,
                    "category": "Supply Chain / Consulting"
                })

        print(
            f"\n✅ Összesen {len(job_links)} db gyakornoki állás azonosítva! Kezdődik a mélyfúrás...")

        if not job_links:
            return

        # --- 2. FÁZIS: LEÍRÁSOK KINYERÉSE (SELENIUMMAL) ---
        print("📄 Részletek letöltése...")
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

                driver.switch_to.default_content()
                driver.get(job['url'])
                time.sleep(2)

                # 💡 INTELLIGENS IFRAME KERESŐ AZ ALOLDALON IS
                content_found = False
                for _ in range(10):
                    driver.switch_to.default_content()
                    if driver.execute_script("return document.querySelectorAll('.content-body, .content').length > 0"):
                        content_found = True
                        break

                    iframes = driver.find_elements(By.TAG_NAME, "iframe")
                    for iframe in iframes:
                        try:
                            driver.switch_to.frame(iframe)
                            if driver.execute_script("return document.querySelectorAll('.content-body, .content').length > 0"):
                                content_found = True
                                break
                            driver.switch_to.default_content()
                        except:
                            driver.switch_to.default_content()

                    if content_found:
                        break
                    time.sleep(1)

                job_soup = BeautifulSoup(driver.page_source, 'html.parser')
                clean_desc = "Leírás nem található."

                desc_el = job_soup.find(
                    'div', class_=re.compile(r'\bcontent-body\b'))
                if not desc_el:
                    desc_el = job_soup.find('div', class_='content')

                if desc_el:
                    for trash in desc_el.find_all('div', class_='content__intro'):
                        trash.decompose()

                    raw_html = str(desc_el)
                    clean_desc = md(raw_html, heading_style="ATX", bullets="-",
                                    strip=['img', 'script', 'style', 'a', 'button', 'svg'])

                clean_desc = clean_desc.replace(
                    '•', '-').replace('·', '-').replace('\xa0', ' ').strip()
                clean_desc = re.sub(r'\n{3,}', '\n\n', clean_desc).strip()

                # 4flow Boilerplate levágása
                truncation_markers = [
                    "**Bereit für 4flow?**",
                    "Bereit für 4flow?",
                    "Ready for 4flow?"
                ]
                for marker in truncation_markers:
                    if marker in clean_desc:
                        clean_desc = clean_desc.split(marker)[0].strip()

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
            f"\n✨ SIKER! {saved_count} db {COMPANY_NAME} állás letöltve a Manual mappába.")

    except Exception as e:
        print(f"\n❌ Váratlan hiba történt a futás során: {e}")
    finally:
        try:
            driver.switch_to.default_content()
            driver.quit()
        except:
            pass


if __name__ == "__main__":
    run_scraper()
