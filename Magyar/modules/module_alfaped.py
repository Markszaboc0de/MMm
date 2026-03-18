import requests
from bs4 import BeautifulSoup
import sqlite3
import os
import sys

# Windows terminál UTF-8 kódolásának kikényszerítése
sys.stdout.reconfigure(encoding='utf-8')

# ==========================================
# ⚙️ CONFIGURATION
# ==========================================
COMPANY_NAME = "Alfaped"
BASE_URL = "https://alfaped.hu/karrier/"

# 🎯 Az adatmappa pontos elérési útja
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "alfaped_jobs.db")


def init_db():
    """Adatbázis és tábla ellenőrzése, létrehozása."""
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)
        print(f"   📁 Adatmappa létrehozva: {DATA_FOLDER}")

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
        response.encoding = 'utf-8'  # Magyar ékezetek biztosítása
        soup = BeautifulSoup(response.text, 'html.parser')

        # 2. Minden állás egy "wp-block-stackable-column" osztályú div-ben van
        job_containers = soup.find_all(
            'div', class_='wp-block-stackable-column')

        if not job_containers:
            print(
                f"   ⚠️ Nem találtunk állásokat a(z) {COMPANY_NAME} oldalon.")
            return

        print(
            f"   🔍 {len(job_containers)} potenciális állásblokk megtalálva. Adatok kinyerése...")

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        new_jobs_added = 0

        # 3. Végigmegyünk minden egyes állás blokkon
        for container in job_containers:
            try:
                # Cím kinyerése
                title_tag = container.find(
                    'h3', class_='stk-block-heading__text')
                if not title_tag:
                    continue  # Ha nincs benne h3 cím, akkor ez nem egy álláshirdetés blokkja

                title = title_tag.get_text(strip=True)

                # Egyedi URL generálása az ID alapján (pl. #junior-energetikus)
                # Az id a h3 tag szülő div-jében van
                title_wrapper = title_tag.find_parent('div')
                job_id = title_wrapper.get('id') if title_wrapper else None

                if job_id:
                    job_url = f"{BASE_URL}#{job_id}"
                else:
                    # Ha véletlen nincs ID, csinálunk egyet a címből, hogy ne legyen duplikáció hibánk
                    slug = title.lower().replace(' ', '-').replace('/', '-')
                    job_url = f"{BASE_URL}#{slug}"

                # Leírás kinyerése a blokk tartalmából ("stk-inner-blocks")
                inner_blocks = container.find('div', class_='stk-inner-blocks')
                description_lines = []

                if inner_blocks:
                    # Végigiterálunk a blokk összes közvetlen gyermek elemén
                    for child in inner_blocks.find_all(recursive=False):

                        # A címet (h3) átugorjuk, hogy ne legyen benne duplán a leírásban
                        if child.find('h3', class_='stk-block-heading__text'):
                            continue

                        # Bekezdések
                        if child.name == 'p':
                            text = child.get_text(strip=True)
                            if text:
                                description_lines.append(text)

                        # Felsorolások (ul, ol)
                        elif child.name in ['ul', 'ol']:
                            for li in child.find_all('li'):
                                li_text = li.get_text(strip=True)
                                if li_text:
                                    description_lines.append(f"- {li_text}")
                            # Üres sor a lista után
                            description_lines.append("")

                        # Alcímek (pl. a h4 tagok div-be vannak csomagolva ennél a WordPress témánál)
                        elif child.name == 'div':
                            h_tag = child.find(['h2', 'h3', 'h4', 'h5', 'h6'])
                            if h_tag:
                                h_text = h_tag.get_text(strip=True)
                                if h_text:
                                    description_lines.append(h_text)

                description = "\n".join(description_lines).strip()

                # Nincs szűrés, mindent lementünk!
                # Fix lokáció adatok Budaörsre
                location_raw = "Magyarország"
                city = "Budaörs"
                country = "Hungary"

                # 4. Mentés az adatbázisba
                cursor.execute('''
                    INSERT OR IGNORE INTO jobs (url, title, company, location_raw, city, country, description)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (job_url, title, COMPANY_NAME, location_raw, city, country, description))

                if cursor.rowcount > 0:
                    new_jobs_added += 1

            except Exception as e:
                print(f"   ❌ Hiba az állás feldolgozása közben: {e}")

        conn.commit()
        conn.close()

        print(
            f"   ✅ {COMPANY_NAME} kész! {new_jobs_added} új állás lementve az adatbázisba.")

    except Exception as e:
        print(
            f"   ❌ Kritikus hiba a(z) {COMPANY_NAME} oldal futtatása közben: {e}")


if __name__ == "__main__":
    run_scraper()
