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
COMPANY_NAME = "Audi Hungaria"
BASE_URL = "https://audi.hu/gyakorlat/nyitott-poziciok"

# 🎯 Az adatmappa pontos elérési útja
DATA_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")

# 🗄️ SAJÁT ADATBÁZIS AZ AUDINAK
DB_PATH = os.path.join(DATA_FOLDER, "audi_jobs.db")


def init_db():
    """Adatbázis és tábla ellenőrzése, létrehozása."""
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
        # 1. A fő karrieroldal letöltése
        response = requests.get(BASE_URL, headers=headers, timeout=15)
        response.encoding = 'utf-8'
        soup = BeautifulSoup(response.text, 'html.parser')

        # 2. Megkeressük az összes állás kártyát
        job_cards = soup.find_all('div', class_='job_card')

        job_targets = []

        for card in job_cards:
            title_div = card.find('div', class_='job_name')
            if not title_div:
                continue

            job_title = title_div.get_text(strip=True)

            # A "Részletek" gomb linkjének kinyerése
            job_url = None
            buttons = card.find_all('a', class_='button')
            for btn in buttons:
                if 'részletek' in btn.get_text(strip=True).lower() or 'details' in btn.get_text(strip=True).lower():
                    href = btn.get('href')
                    if href:
                        if href.startswith('/'):
                            job_url = f"https://audi.hu{href}"
                        else:
                            job_url = href
                    break

            if job_url:
                job_targets.append({
                    "url": job_url,
                    "title": job_title
                })

        if not job_targets:
            print(
                f"   ⚠️ Nem találtunk állásokat a(z) {COMPANY_NAME} oldalon.")
            return

        print(
            f"   🔍 {len(job_targets)} állás hivatkozás megtalálva. Részletek kinyerése...")

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        new_jobs_added = 0

        # 3. Végigmegyünk az aloldalakon a pontos leírásért
        for job in job_targets:
            job_url = job["url"]
            title = job["title"]

            try:
                job_res = requests.get(job_url, headers=headers, timeout=10)
                job_res.encoding = 'utf-8'
                job_soup = BeautifulSoup(job_res.text, 'html.parser')

                description_lines = []

                # 🧠 THE FIX: A pontos HTML konténer célzása az Audi oldalán
                main_content = job_soup.find('div', class_='job_cards details')

                if main_content:
                    for tag in main_content.find_all(['div', 'p', 'ul']):
                        classes = tag.get('class', [])

                        # Alcímek (pl. Feladatok, Elvárások)
                        if tag.name == 'div' and 'job_name' in classes:
                            text = tag.get_text(strip=True)
                            # Kiszűrjük a legfelső főcímet, hogy ne duplikáljuk
                            if text and text != title:
                                if description_lines and description_lines[-1] != "":
                                    # Üres sor az alcím előtt
                                    description_lines.append("")
                                # Csupa nagybetűvel kiemeljük az alcímet
                                description_lines.append(text.upper())

                        # Rövid leírás bekezdése
                        elif tag.name == 'p' and 'job_desc' in classes:
                            text = tag.get_text(strip=True)
                            if text:
                                description_lines.append(text)

                        # Felsorolások (feladatok, elvárások)
                        elif tag.name == 'ul':
                            for li in tag.find_all('li'):
                                li_text = li.get_text(strip=True)
                                if li_text:
                                    description_lines.append(f"- {li_text}")
                            # Üres sor a lista után
                            description_lines.append("")

                        # Határidő és Bérezés kinyerése
                        elif tag.name == 'div' and 'job_place' in classes:
                            place_label = tag.get_text(strip=True)
                            # A mellette lévő job_city tartalmazza az értéket
                            city_tag = tag.find_next_sibling(
                                'div', class_='job_city')
                            if city_tag:
                                value_text = city_tag.get_text(strip=True)
                                description_lines.append(
                                    f"{place_label}: {value_text}")

                description = "\n".join(description_lines).strip()
                if not description:
                    description = "A leírás nem található a várt formátumban."

                # Címadatok (Ahogy megbeszéltük: fixen Győr)
                location_raw = "Magyarország"
                city = "Győr"
                country = "Hungary"

                # 4. Mentés az adatbázisba
                cursor.execute('''
                    INSERT OR IGNORE INTO jobs (url, title, company, location_raw, city, country, description)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (job_url, title, COMPANY_NAME, location_raw, city, country, description))

                if cursor.rowcount > 0:
                    new_jobs_added += 1

                time.sleep(1)  # Ne terheljük túl az Audi szervereit

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
