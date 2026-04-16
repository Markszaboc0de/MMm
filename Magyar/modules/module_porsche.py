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
COMPANY_NAME = "Porsche Hungaria"
BASE_SEARCH_URL = "https://karrier.porschehungaria.hu/DataCenter/Registration/JobAdvertisements/allasok"

# Mentés a Magyar mappába!
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "porsche_jobs.db")


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
    # Képek letiltása a villámgyors navigációért (A Base64 képekre sincs szükségünk)
    return get_chrome_driver()


def run_scraper():
    init_db()
    print(
        f"🚀 {COMPANY_NAME} Scraper indítása (Angular SPA Mód + E2E Lokációkereső)...")

    driver = create_driver()
    job_links = []
    unique_urls = set()

    try:
        # --- 1. FÁZIS: LINKEK GYŰJTÉSE A FŐOLDALRÓL ---
        print("📂 Karrieroldal betöltése és lista görgetése...")
        driver.get(BASE_SEARCH_URL)

        wait = WebDriverWait(driver, 10, poll_frequency=0.2)
        try:
            wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, '.positionList__card')))
        except:
            print("❌ Nem sikerült betölteni az állásokat.")
            return

        # Végiggörgetjük az oldalt (lazy load kezelése)
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

        # 💡 LINKEK KINYERÉSE JS-SEL
        jobs_on_page = driver.execute_script("""
            let results = [];
            document.querySelectorAll('.positionList__card').forEach(card => {
                let url = card.getAttribute('data-position-url');
                let title = card.getAttribute('data-position-name');
                if(url && title) {
                    results.push({url: url, title: title.trim()});
                }
            });
            return results;
        """)

        for job in jobs_on_page:
            if job['url'] not in unique_urls:
                unique_urls.add(job['url'])
                job_links.append(job)

        print(
            f"✅ Összesen {len(job_links)} db állás azonosítva! Kezdődik a mélyfúrás...")

        if not job_links:
            return

        # --- 2. FÁZIS: LEÍRÁSOK KINYERÉSE (Selenium Smart Wait) ---
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

                driver.get(job['url'])

                # 💡 GARANCIA: Várunk a leírás vagy a lokáció betöltésére
                try:
                    wait.until(EC.presence_of_element_located(
                        (By.CSS_SELECTOR, '.jobAdvertisement__content')))
                except:
                    pass

                job_soup = BeautifulSoup(driver.page_source, 'html.parser')

                # 💡 ÚJ LOKÁCIÓ KINYERÉSE A DATA-E2E-TESTING ATTRIBÚTUMMAL
                location_raw = "N/A"
                loc_el = job_soup.find(
                    attrs={'data-e2e-testing': 'Recruitment.Registration.Position.LocationOfWork'})

                if loc_el:
                    location_raw = loc_el.get_text(strip=True)
                else:
                    # Fallback (ha véletlenül máshol lenne a szöveg)
                    location_labels = job_soup.find_all(
                        'div', class_='positionCardBody__row__label__text')
                    for label in location_labels:
                        text = label.get_text(strip=True)
                        if 'Magyarország' in text or 'Hungary' in text:
                            location_raw = text
                            break

                # Darabolás: "Szeged, Magyarország" -> Város: Szeged, Ország: Hungary
                city = "N/A"
                country = "Hungary"  # Fixen beállítva

                if location_raw != "N/A":
                    parts = [p.strip() for p in location_raw.split(',')]
                    city = parts[0]

                clean_location_raw = f"{city}, {country}" if city != "N/A" else country

                # 💡 LEÍRÁS KINYERÉSE ÉS BASE64 TISZTÍTÁS
                desc_el = job_soup.find(
                    'div', class_='jobAdvertisement__content')
                clean_desc = "Leírás nem található."

                if desc_el:
                    raw_html = str(desc_el)
                    # A strip=['img'] kidobja a Base64 kódokat a szövegből!
                    clean_desc = md(raw_html, heading_style="ATX", bullets="-",
                                    strip=['img', 'script', 'style', 'a', 'button', 'svg'])

                    clean_desc = clean_desc.replace(
                        '•', '-').replace('·', '-').replace('\xa0', ' ').replace('\u202f', ' ')
                    clean_desc = re.sub(r'\n{3,}', '\n\n', clean_desc).strip()

                print(
                    f"\n      -> Cím: {job['title']} | Hely: {city}, {country}")

                category = "Automotive / Sales"

                conn.execute('''INSERT INTO jobs (url, title, company, location_raw, city, country, description, category) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                             (job['url'], job['title'], COMPANY_NAME, clean_location_raw, city, country, clean_desc, category))
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
            driver.quit()
        except:
            pass


if __name__ == "__main__":
    run_scraper()
