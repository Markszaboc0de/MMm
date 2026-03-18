from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import sqlite3
import os
import sys
import time

# Windows terminál UTF-8 kódolásának kikényszerítése
sys.stdout.reconfigure(encoding='utf-8')

# ==========================================
# ⚙️ CONFIGURATION
# ==========================================
COMPANY_NAME = "BGE"
BASE_URL = "https://allasok.uni-bge.hu/DataCenter/Registration/JobAdvertisements/allasok"

DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "bge_jobs.db")


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
    print(f"   🏢 Scraper indítása: {COMPANY_NAME} (JavaScript mód)...")
    init_db()

    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.binary_location = "/usr/bin/chromium-browser"
    _service = Service(executable_path="/usr/bin/chromedriver")
    driver = webdriver.Chrome(service=_service, options=options)
    try:
        # 1. Oldal betöltése a böngészővel
        driver.get(BASE_URL)

        # 2. Várakozás, amíg az Angular betölti a listát
        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located(
                    (By.CLASS_NAME, "positionList__positionRow"))
            )
        except:
            print(
                f"   ⚠️ Nem töltődtek be az állások időben, vagy nincs nyitott pozíció.")
            driver.quit()
            return

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        job_rows = soup.find_all('div', class_='positionList__positionRow')

        job_targets = []

        for row in job_rows:
            job_url = row.get('data-position-url')

            title_div = row.find('div', attrs={
                                 "data-e2e-testing": "Recruitment.Registration.PositionRepeater.Name"})
            if title_div and title_div.find('div'):
                job_title = title_div.find('div').get_text(strip=True)
            else:
                job_title = "Ismeretlen pozíció"

            if job_url:
                job_targets.append({
                    "url": job_url,
                    "title": job_title
                })

        print(
            f"   🔍 {len(job_targets)} állás megtalálva. Részletek kinyerése...")

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        new_jobs_added = 0

        # 3. Végigmegyünk az aloldalakon
        for job in job_targets:
            job_url = job["url"]
            title = job["title"]

            try:
                driver.get(job_url)
                time.sleep(3)  # Várunk, hogy az Angular felépítse a domot

                job_soup = BeautifulSoup(driver.page_source, 'html.parser')

                # 🧠 THE FIX: A pontos szövegtároló megkeresése a beküldött e2e attribútum alapján!
                main_content = job_soup.find('div', attrs={
                                             "data-e2e-testing": "Recruitment.Registration.Position.JobAdContent"})

                description_lines = []

                if main_content:
                    # Végigmegyünk az összes lényeges szöveges elemen (bekezdések, alcímek, listák)
                    for tag in main_content.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'ul', 'ol']):

                        # Ha egy bekezdés véletlenül egy listán belül van, azt átugorjuk, mert a lista feldolgozó megoldja
                        if tag.name not in ['ul', 'ol'] and tag.find_parent(['ul', 'ol']):
                            continue

                        # Lista elemek gyönyörű formázása
                        if tag.name in ['ul', 'ol']:
                            for li in tag.find_all('li'):
                                li_text = li.get_text(strip=True)
                                if li_text:
                                    description_lines.append(f"- {li_text}")
                            # Üres sor a lista végére a jobb olvashatóságért
                            description_lines.append("")

                        # Sima bekezdések és alcímek
                        else:
                            text = tag.get_text(strip=True)
                            if text:
                                description_lines.append(text)
                else:
                    description_lines.append(
                        "A leírás nem található a várt formátumban.")

                description = "\n".join(description_lines).strip()

                location_raw = "Magyarország"
                city = "Budapest"
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
        driver.quit()


if __name__ == "__main__":
    run_scraper()
