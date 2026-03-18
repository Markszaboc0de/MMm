import requests
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
COMPANY_NAME = "Process Solutions (PS BPO)"
BASE_URL = "https://career.ps-bpo.com/csatlakozz-hozzank/"

DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "psbpo_jobs.db")


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
    print(f"   🏢 Scraper indítása: {COMPANY_NAME}...")
    init_db()

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    try:
        response = requests.get(BASE_URL, headers=headers, timeout=15)
        response.encoding = 'utf-8'
        soup = BeautifulSoup(response.text, 'html.parser')

        job_targets = []

        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href')
            text = a_tag.get_text(strip=True)

            href_lower = href.lower()
            if ('allas' in href_lower or 'pozicio' in href_lower or 'job' in href_lower or 'karrier' in href_lower):
                if text and len(text) > 5 and not href.endswith('#nyitott-pozicioink'):
                    job_url = urljoin(BASE_URL, href)
                    job_targets.append({
                        "url": job_url,
                        "title": text
                    })

        job_targets = [dict(t)
                       for t in {tuple(d.items()) for d in job_targets}]

        if not job_targets:
            print(
                f"   ⚠️ Nem találtunk állásokat a(z) {COMPANY_NAME} oldalon.")
            return

        print(
            f"   🔍 {len(job_targets)} állás hivatkozás megtalálva. Részletek kinyerése...")

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        new_jobs_added = 0

        for job in job_targets:
            job_url = job["url"]
            title = job["title"]

            try:
                job_res = requests.get(job_url, headers=headers, timeout=10)
                job_res.encoding = 'utf-8'
                job_soup = BeautifulSoup(job_res.text, 'html.parser')

                description_lines = []

                # 🧠 THE FIX: A többoszlopos c2_container célzása
                main_content = job_soup.find(
                    'div', class_='c2_container') or job_soup.find('div', class_='content')
                stop_parsing = False

                if main_content:
                    # Végigmegyünk az összes article tagen a bal és jobb oszlopban is
                    for article in main_content.find_all('article'):
                        if stop_parsing:
                            break

                        for tag in article.find_all(['p', 'h2', 'h3', 'h4', 'ul', 'ol']):
                            text = tag.get_text(strip=True)
                            if not text:
                                continue

                            # 🛑 STOP FELTÉTEL: Ha elérjük a jelentkezési instrukciókat, álljunk meg!
                            if "hogyan jelentkezz" in text.lower() or "hr-hu@ps-bpo.com" in text.lower() or "több információra van szükséged" in text.lower():
                                stop_parsing = True
                                break

                            # Listák feldolgozása
                            if tag.name in ['ul', 'ol']:
                                for li in tag.find_all('li'):
                                    li_text = li.get_text(strip=True)
                                    if li_text:
                                        description_lines.append(
                                            f"- {li_text}")
                                description_lines.append("")
                            # Normál bekezdések és alcímek
                            else:
                                if text not in description_lines and text != title:
                                    description_lines.append(text)

                description = "\n".join(description_lines).strip()
                if not description:
                    description = "A leírás kinyerése nem sikerült. Kérlek, ellenőrizd a HTML struktúrát."

                location_raw = "Magyarország"
                city = "Budapest"
                country = "Hungary"

                cursor.execute('''
                    INSERT OR IGNORE INTO jobs (url, title, company, location_raw, city, country, description)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (job_url, title, COMPANY_NAME, location_raw, city, country, description))

                if cursor.rowcount > 0:
                    new_jobs_added += 1

                time.sleep(1)

            except Exception as e:
                print(
                    f"   ❌ Hiba az állás feldolgozása közben ({job_url}): {e}")

        conn.commit()
        conn.close()

        print(
            f"   ✅ {COMPANY_NAME} kész! {new_jobs_added} új állás lementve az adatbázisba.")

    except Exception as e:
        print(
            f"   ❌ Kritikus hiba a(z) {COMPANY_NAME} oldal futtatása közben: {e}")


if __name__ == "__main__":
    run_scraper()
