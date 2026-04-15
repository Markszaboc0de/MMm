import requests
from bs4 import BeautifulSoup
import sqlite3
import os
import sys
import time
import re
from markdownify import markdownify as md

sys.stdout.reconfigure(encoding='utf-8')

# --- KONFIGURÁCIÓ ---
COMPANY_NAME = "Euroleasing"
BASE_CAREER_URL = "https://www.euroleasing.hu/magunkrol/karrier/"

# Mentés a Magyar mappába!
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "euroleasing_jobs.db")

# Entry-level kulcsszavak (Biztos, ami biztos alapon)
ENTRY_LEVEL_KEYWORDS = ['intern', 'trainee', 'student',
                        'graduate', 'gyakornok', 'pályakezdő', 'junior', 'diák']


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


def run_scraper():
    init_db()
    print(f"🚀 {COMPANY_NAME} Scraper indítása (Villámgyors WordPress mód)...")

    job_links = []
    unique_urls = set()

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    try:
        # --- 1. FÁZIS: LINKEK GYŰJTÉSE A FŐOLDALRÓL ---
        print("📂 Karrieroldal letöltése és linkek kinyerése...")

        try:
            response = requests.get(
                BASE_CAREER_URL, headers=headers, timeout=15)
            response.raise_for_status()
        except Exception as e:
            print(f"\n❌ Hiba a főoldal lekérésekor: {e}")
            return

        soup = BeautifulSoup(response.text, 'html.parser')

        # A WordPress HTML struktúrában keressük az állásokat tartalmazó blokkot
        # A "Aktuális állásajánlatok" fül tartalma egy 'allasajanlatok' ID-jű div-ben lesz, vagy utána
        jobs_container = soup.find('div', id='allasajanlatok')

        # Ha a div nincs meg közvetlenül ID alapján, megkeressük az összes '/karrier/' linket
        if jobs_container:
            a_tags = jobs_container.find_all('a', href=True)
        else:
            a_tags = soup.find_all('a', href=True)

        for a_tag in a_tags:
            href = a_tag['href']

            # Csak azokat a linkeket tartjuk meg, amik a '/karrier/...' útvonalra mutatnak
            # Kizárjuk a főoldalakat és a paginációt, hogy csak az aloldalak maradjanak
            if '/karrier/' in href and href != BASE_CAREER_URL and href != "https://www.euroleasing.hu/karrier/":
                # Tisztítjuk a linket
                # Visszateszünk egy perjelet, a WP szereti
                clean_job_url = href.split('#')[0].rstrip('/') + '/'

                title = a_tag.get_text(strip=True)
                if not title:
                    continue

                # 💡 PYTHON OLDALI SZŰRÉS: Entry Level a cím alapján?
                title_lower = title.lower()
                if not any(kw in title_lower for kw in ENTRY_LEVEL_KEYWORDS):
                    continue

                if clean_job_url not in unique_urls:
                    unique_urls.add(clean_job_url)
                    job_links.append({
                        "url": clean_job_url,
                        "title": title
                    })

        print(
            f"✅ Összesen {len(job_links)} db gyakornoki állás azonosítva! Kezdődik a mélyfúrás...")

        if not job_links:
            return

        # --- 2. FÁZIS: LEÍRÁSOK KINYERÉSE (Villámgyors Requests) ---
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

                res = requests.get(job['url'], headers=headers, timeout=10)
                job_soup = BeautifulSoup(res.text, 'html.parser')

                # 💡 PONTOS CÍM KINYERÉSE
                title_el = job_soup.find('h2', class_='-entry-title')
                if title_el:
                    job['title'] = title_el.get_text(strip=True)

                # 💡 LOKÁCIÓ BEÁLLÍTÁSA (Kérésed alapján mindig fix)
                city = "Budapest"
                country = "Hungary"
                location_raw = f"{city}, {country}"

                # 💡 LEÍRÁS KINYERÉSE
                desc_el = job_soup.find('div', class_='entry-content')
                clean_desc = "Leírás nem található."

                if desc_el:
                    raw_html = str(desc_el)
                    clean_desc = md(raw_html, heading_style="ATX", bullets="-",
                                    strip=['img', 'script', 'style', 'button', 'svg'])

                    # Tisztítás
                    clean_desc = clean_desc.replace(
                        '•', '-').replace('·', '-').replace('\xa0', ' ').strip()
                    clean_desc = re.sub(r'\n{3,}', '\n\n', clean_desc).strip()

                    # 💡 EUROLEASING BOILERPLATE LEVÁGÁSA A VÉGÉRŐL
                    truncation_markers = [
                        "Amennyiben felkeltettük az érdeklődésedet, kérjük olvasd el",
                        "Álláshirdetésre jelentkezők részére szóló adatkezelési tájékoztatónkat",
                        "allas@euroleasing.hu"
                    ]
                    for marker in truncation_markers:
                        if marker in clean_desc:
                            clean_desc = clean_desc.split(marker)[0].strip()

                print(f"\n      -> Hely: {city}, {country}")

                conn.execute('''INSERT INTO jobs (url, title, company, location_raw, city, country, description, category) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                             (job['url'], job['title'], COMPANY_NAME, location_raw, city, country, clean_desc, "Finance / Corporate"))
                conn.commit()
                saved_count += 1

                time.sleep(0.1)

            except Exception as e:
                print(f"\n      ⚠️ Hiba a(z) {idx}. állásnál: {e}")

        conn.close()
        print(
            f"\n✨ SIKER! {saved_count} db {COMPANY_NAME} állás letöltve villámgyorsan a Magyar mappába.")

    except Exception as e:
        print(f"\n❌ Váratlan hiba történt a futás során: {e}")


if __name__ == "__main__":
    run_scraper()
