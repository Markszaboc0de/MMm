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
COMPANY_NAME = "Budapest Főpolgármesteri Hivatal"
BASE_URL = "https://budapest.hu/varoshaza/fopolgarmesteri-hivatal/karrier/nyitott-poziciok"

# 🎯 Az adatmappa pontos elérési útja
DATA_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")

# 🗄️ SAJÁT ADATBÁZIS
DB_PATH = os.path.join(DATA_FOLDER, "budapest_jobs.db")


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

        # 2. Megkeressük az összes állás kártyát a stabil "role='listitem'" alapján
        job_cards = soup.find_all('a', role='listitem')

        job_targets = []

        for card in job_cards:
            # A cím egy h3 tagben van
            title_tag = card.find('h3')
            if not title_tag:
                continue

            job_title = title_tag.get_text(strip=True)
            href = card.get('href')

            if href:
                # Abszolút URL generálása, ha szükséges
                if href.startswith('/'):
                    job_url = f"https://budapest.hu{href}"
                else:
                    job_url = href

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

                # Általános keresés: a cikkek általában egy 'article', 'main' tagben, vagy egy konténer div-ben vannak
                main_content = job_soup.find('article') or job_soup.find(
                    'main') or job_soup.find('body')

                if main_content:
                    for tag in main_content.find_all(['p', 'h2', 'h3', 'h4', 'ul', 'ol', 'strong']):
                        # Kiszűrjük a formázó tageket, amik más bekezdésen belül vannak
                        if tag.name == 'strong' and tag.find_parent(['p', 'li', 'h2', 'h3', 'h4']):
                            continue

                        # Szűrjük a menüket, lábléceket
                        if tag.get('class') and any(nav in str(tag.get('class')).lower() for nav in ['nav', 'menu', 'footer', 'header']):
                            continue

                        # Listák kezelése
                        if tag.name in ['ul', 'ol']:
                            for li in tag.find_all('li'):
                                li_text = li.get_text(strip=True)
                                if li_text:
                                    description_lines.append(f"- {li_text}")
                            description_lines.append("")
                        # Normál szövegek
                        else:
                            text = tag.get_text(strip=True)
                            # Ha a szöveg nem üres, nem ismétli a címet
                            if text and text != title and text not in description_lines:
                                description_lines.append(text)

                description = "\n".join(description_lines).strip()
                if not description:
                    description = "A leírás kinyerése nem sikerült. Kérlek, ellenőrizd a HTML struktúrát."

                # Címadatok - Természetesen Budapest
                location_raw = "Magyarország"
                city = "Budapest"
                country = "Hungary"

                # 4. Mentés az adatbázisba
                cursor.execute('''
                    INSERT OR IGNORE INTO jobs (url, title, company, location_raw, city, country, description)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (job_url, title, COMPANY_NAME, location_raw, city, country, description))

                if cursor.rowcount > 0:
                    new_jobs_added += 1

                time.sleep(1)  # Kicsit várunk a kérések között

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
