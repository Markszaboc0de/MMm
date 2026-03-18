from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

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
from urllib.parse import urljoin

# Windows terminál UTF-8 kódolásának kikényszerítése
sys.stdout.reconfigure(encoding='utf-8')

# ==========================================
# ⚙️ CONFIGURATION
# ==========================================
COMPANY_NAME = "Mercedes-Benz"
BASE_URL = "https://gyar.mercedes-benz.hu/karrier/gyakornoki-program"

DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "mercedes_jobs.db")


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
    print(f"   🏢 Scraper indítása: {COMPANY_NAME} (Kétlépcsős JS mód)...")
    init_db()

    driver = get_chrome_driver()
    try:
        # 1. Listaoldal betöltése a magyar szerverről
        driver.get(BASE_URL)
        print("   ⏳ Várakozás a lista betöltésére (5 mp)...")
        time.sleep(5)

        soup = BeautifulSoup(driver.page_source, 'html.parser')

        # Álláskártyák keresése a "job_item" osztály alapján
        job_cards = soup.find_all('div', class_='job_item')
        job_targets = []

        for card in job_cards:
            title_tag = card.find('h3')
            a_tag = card.find('a', class_='btn-primary')

            if title_tag and a_tag and a_tag.get('href'):
                job_title = title_tag.get_text(strip=True)
                job_url = a_tag.get('href')

                # Biztosítjuk az abszolút URL-t
                if not job_url.startswith('http'):
                    job_url = urljoin(
                        "https://jobs.mercedes-benz.com", job_url)

                job_targets.append({
                    "url": job_url,
                    "title": job_title
                })

        # Duplikációk szűrése
        job_targets = [dict(t)
                       for t in {tuple(d.items()) for d in job_targets}]

        if not job_targets:
            print(
                f"   ⚠️ Nem találtunk állásokat a(z) {COMPANY_NAME} oldalon.")
            return

        print(
            f"   🔍 {len(job_targets)} állás megtalálva. Átirányítás a globális portálra...")

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        new_jobs_added = 0

        # 2. Végigmegyünk a globális aloldalakon
        for job in job_targets:
            job_url = job["url"]
            title = job["title"]

            try:
                driver.get(job_url)
                # A globális nagyvállalati portálok lassan tölthetnek be (PhenomPeople / SAP)
                time.sleep(4)

                job_soup = BeautifulSoup(driver.page_source, 'html.parser')
                description_lines = []

                # Keresés a leírás tartályára
                main_content = job_soup.find('div', class_='job-description') or \
                    job_soup.find('section', class_='job-description') or \
                    job_soup.find('div', class_='desc') or \
                    job_soup.find('div', class_='job-content') or \
                    job_soup.find('main')

                if main_content:
                    for tag in main_content.find_all(['p', 'h2', 'h3', 'h4', 'ul', 'ol']):
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
                    description = "A leírás kinyerése nem sikerült. Az oldal felépítése eltérhet a várttól."

                location_raw = "Magyarország, Kecskemét"
                city = "Kecskemét"
                country = "Hungary"

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
            pass  # A szokásos WinError 6 némítása
        except Exception:
            pass


if __name__ == "__main__":
    run_scraper()
