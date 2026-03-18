import requests
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
COMPANY_NAME = "Grünwald"
BASE_URL = "https://grunwald.co.hu/grunwald-karrier/"

DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "grunwald_jobs.db")


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
    print(
        f"   🏢 Scraper indítása: {COMPANY_NAME} (Villámgyors WordPress mód)...")
    init_db()

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    try:
        response = requests.get(BASE_URL, headers=headers, timeout=15)
        response.encoding = 'utf-8'
        soup = BeautifulSoup(response.text, 'html.parser')

        # A Divi builder 'blurb' moduljainak keresése
        job_cards = soup.find_all('div', class_='et_pb_blurb_content')
        job_targets = []

        for card in job_cards:
            title_tag = card.find('h4', class_='et_pb_module_header')
            if title_tag and title_tag.find('a'):
                a_tag = title_tag.find('a')
                job_url = a_tag['href']
                job_title = a_tag.get_text(strip=True)

                # Város okos tippelése a címből vagy az URL-ből
                city = "Ismeretlen"
                country = "Hungary"
                location_raw = "Magyarország"

                url_lower = job_url.lower()
                title_lower = job_title.lower()

                if 'pecs' in url_lower or 'pécs' in title_lower:
                    city = "Pécs"
                elif 'budapest' in url_lower or 'budapest' in title_lower:
                    city = "Budapest"

                location_raw = f"{city}, {country}"

                job_targets.append({
                    "url": job_url,
                    "title": job_title,
                    "city": city,
                    "country": country,
                    "location_raw": location_raw
                })

        # Duplikációk szűrése
        job_targets = [dict(t)
                       for t in {tuple(d.items()) for d in job_targets}]

        if not job_targets:
            print(
                f"   ⚠️ Nem találtunk állásokat a(z) {COMPANY_NAME} oldalon.")
            return

        print(
            f"   🔍 {len(job_targets)} állás megtalálva. Részletek kinyerése...")

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        new_jobs_added = 0

        # Végigmegyünk az aloldalakon
        for job in job_targets:
            job_url = job["url"]
            title = job["title"]
            city = job["city"]
            country = job["country"]
            location_raw = job["location_raw"]

            try:
                job_res = requests.get(job_url, headers=headers, timeout=10)
                job_res.encoding = 'utf-8'
                job_soup = BeautifulSoup(job_res.text, 'html.parser')

                description_lines = []

                # A Divi általában a szövegeket az 'et_pb_text_inner' osztályba rakja
                text_modules = job_soup.find_all(
                    'div', class_='et_pb_text_inner')

                if text_modules:
                    for module in text_modules:
                        for tag in module.find_all(['p', 'h2', 'h3', 'h4', 'ul', 'ol']):
                            if tag.name in ['ul', 'ol']:
                                for li in tag.find_all('li'):
                                    li_text = li.get_text(strip=True)
                                    if li_text:
                                        description_lines.append(
                                            f"- {li_text}")
                                description_lines.append("")
                            else:
                                text = tag.get_text(strip=True)
                                if text and text != title and text not in description_lines:
                                    description_lines.append(text)
                else:
                    # Fallback ha nincs et_pb_text_inner
                    main_content = job_soup.find(
                        'div', class_='entry-content') or job_soup.find('main')
                    if main_content:
                        for tag in main_content.find_all(['p', 'ul', 'ol']):
                            text = tag.get_text(strip=True)
                            if text and text not in description_lines:
                                description_lines.append(text)

                description = "\n".join(description_lines).strip()
                if not description:
                    description = "A leírás kinyerése nem sikerült. Kérlek, ellenőrizd az oldalt."

                # Mentés az adatbázisba
                cursor.execute('''
                    INSERT OR IGNORE INTO jobs (url, title, company, location_raw, city, country, description)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (job_url, title, COMPANY_NAME, location_raw, city, country, description))

                if cursor.rowcount > 0:
                    new_jobs_added += 1

                time.sleep(0.5)  # Kicsi pihenő, hogy kíméljük a szervert

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
