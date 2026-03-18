from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

import sys as _sys
import os as _os
_sys.path.append(_os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))
from driver_setup import get_chrome_driver
from bs4 import BeautifulSoup
import sqlite3
import os
import sys
import time
from urllib.parse import urljoin

# Windows terminál UTF-8 kódolásának kikényszerítése
sys.stdout.reconfigure(encoding='utf-8')

# ==========================================
# ⚙️ CONFIGURATION
# ==========================================
COMPANY_NAME = "Motherson"
# Az új, egyszerűsített URL
BASE_URL = "https://careers.motherson.com/en/jobs"

DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "motherson_jobs.db")


def init_db():
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        url TEXT UNIQUE NOT NULL,
        title TEXT NOT NULL,
        company TEXT NOT NULL,
        location_raw TEXT,
        city TEXT,
        country TEXT,
        description TEXT
    )
    ''')
    conn.commit()
    conn.close()


def run_scraper():
    print(f"   🏢 Scraper indítása: {COMPANY_NAME} (Egyszerű SPA mód)...")
    init_db()

    driver = get_chrome_driver()
    try:
        driver.get(BASE_URL)
        print("   ⏳ Várakozás az oldal betöltésére (6 mp)...")
        time.sleep(2)  # Hagyunk időt a Reactnek az adatok letöltésére

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        links = soup.find_all('a', href=True)
        job_targets = []

        for link in links:
            href = link.get('href')
            if '/job/' in href:
                title = link.get_text(strip=True)
                if not title:
                    continue

                job_url = urljoin("https://careers.motherson.com", href)

                city = "Ismeretlen"
                country = "Ismeretlen"
                location_raw = "Ismeretlen"

                try:
                    # Visszamegyünk a fő kártya konténerig a lokációért
                    card_container = link.find_parent(
                        'div').find_parent('div').find_parent('div')
                    if card_container:
                        for div in card_container.find_all('div'):
                            if div.find('br'):
                                location_raw = div.get_text(
                                    separator=", ", strip=True)
                                parts = location_raw.split(", ")
                                if len(parts) >= 2:
                                    country = parts[0]
                                    city = parts[1]
                                else:
                                    city = location_raw
                                break
                except Exception:
                    pass

                job_targets.append({
                    "url": job_url,
                    "title": title,
                    "city": city,
                    "country": country,
                    "location_raw": location_raw
                })

        # Duplikációk szűrése
        job_targets = [dict(t)
                       for t in {tuple(d.items()) for d in job_targets}]

        if not job_targets:
            print(
                "   ⚠️ Nem találtunk állásokat. Kérlek, ellenőrizd, hogy az oldal betöltött-e!")
            return

        print(
            f"   🔍 {len(job_targets)} egyedi állás hivatkozás megtalálva. Részletek kinyerése...")

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        new_jobs_added = 0

        # Végigmegyünk az aloldalakon a leírásért
        for job in job_targets:
            job_url = job["url"]
            title = job["title"]
            city = job["city"]
            country = job["country"]
            location_raw = job["location_raw"]

            try:
                driver.get(job_url)
                time.sleep(3)  # Betöltési idő a leírásnak

                job_soup = BeautifulSoup(driver.page_source, 'html.parser')
                description_lines = []

                # Általános tartalomkereső a belső oldalra
                main_content = job_soup.find('main') or job_soup.find('article') or job_soup.find(
                    'div', id='job-description') or job_soup.find('body')

                if main_content:
                    for tag in main_content.find_all(['p', 'h2', 'h3', 'h4', 'ul', 'ol']):
                        classes = str(tag.get('class', '')).lower()
                        if any(nav in classes for nav in ['nav', 'menu', 'footer', 'header', 'cookie']):
                            continue

                        if tag.name in ['ul', 'ol']:
                            for li in tag.find_all('li'):
                                li_text = li.get_text(strip=True)
                                if li_text:
                                    description_lines.append(f"- {li_text}")
                            description_lines.append("")
                        else:
                            text = tag.get_text(strip=True)
                            if text and text not in description_lines and text != title:
                                description_lines.append(text)

                description = "\n".join(description_lines).strip()
                if not description:
                    description = "A leírás kinyerése nem sikerült. Kérlek, ellenőrizd az oldalt."

                cursor.execute('''
                    INSERT OR IGNORE INTO jobs (url, title, company, location_raw, city, country, description)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (job_url, title, COMPANY_NAME, location_raw, city, country, description))

                if cursor.rowcount > 0:
                    new_jobs_added += 1

            except Exception as e:
                print(
                    f"   ❌ Hiba az állás feldolgozása közben ({job_url}): {e}")

        conn.commit()
        conn.close()

        print(
            f"   ✅ {COMPANY_NAME} kész! {new_jobs_added} új állás lementve az adatbázisba.")

    finally:
        try:
            if 'driver' in locals():
                driver.quit()
        except OSError:
            pass  # WinError 6 némítása
        except Exception:
            pass


if __name__ == "__main__":
    run_scraper()
