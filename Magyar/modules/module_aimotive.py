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
COMPANY_NAME = "aiMotive"
BASE_URL = "https://aimotive.com/career"
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "aimotive_jobs.db")


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
    print(f"🚀 {COMPANY_NAME} Scraper indítása (Villámgyors Requests + BS4 mód)...")

    job_links = []
    unique_urls = set()

    # Fejlécek, hogy a szerver normál böngészőnek higgyen minket
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,hu;q=0.8"
    }

    try:
        # --- 1. FÁZIS: LINKEK GYŰJTÉSE ---
        print("📂 aiMotive karrieroldal lekérése...")
        response = requests.get(BASE_URL, headers=headers, timeout=10)

        if response.status_code != 200:
            print(
                f"❌ Hiba az oldal lekérésekor! HTTP Kód: {response.status_code}")
            return

        soup = BeautifulSoup(response.text, 'html.parser')

        # 💡 A MÁGIA: A képed alapján a gombok szövege "Details & apply". Ezt keressük!
        links = soup.find_all('a', href=True)
        for a in links:
            text = a.get_text(strip=True).lower()
            if "details" in text and "apply" in text:
                url = a['href']

                # Ha relatív URL-t kapunk, kiegészítjük a domainnel
                if url.startswith('/'):
                    url = "https://aimotive.com" + url

                if url not in unique_urls:
                    unique_urls.add(url)
                    job_links.append({
                        "url": url,
                        "category": "Automotive / AI / Tech"  # Alapértelmezett kategória
                    })

        print(
            f"\n✅ Összesen {len(job_links)} db állás azonosítva! Kezdődik a mélyfúrás...")

        if not job_links:
            return

        # --- 2. FÁZIS: LEÍRÁSOK KINYERÉSE ---
        conn = sqlite3.connect(DB_PATH)
        saved_count = 0

        for idx, job in enumerate(job_links, 1):
            try:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT id FROM jobs WHERE url = ?", (job['url'],))
                if cursor.fetchone():
                    print(
                        f"   [{idx}/{len(job_links)}] Ugrás (Már az adatbázisban van)")
                    continue

                sys.stdout.write(
                    f"\r   [{idx}/{len(job_links)}] Állás lekérése: {job['url']} ...")
                sys.stdout.flush()

                # HTML lekérése a háttérben
                res = requests.get(job['url'], headers=headers, timeout=10)
                job_soup = BeautifulSoup(res.text, 'html.parser')

                # 1. Cím kinyerése (Keresünk h3-at, h1-et, vagy egyértelmű title taget)
                # A mintád alapján <h3>-ban van a cím
                title_el = job_soup.find('h3')
                if not title_el:
                    title_el = job_soup.find('h1')

                title = "N/A"
                if title_el:
                    title = title_el.get_text(strip=True).replace(
                        '\xa0', ' ').replace('\u202f', ' ')

                # 2. Lokáció kinyerése (Liferay attribútum alapján)
                loc_el = job_soup.find(
                    attrs={"data-lfr-editable-id": "post-location"})
                location_raw = loc_el.get_text(strip=True) if loc_el else "N/A"

                city = "N/A"
                country = "N/A"

                if location_raw != 'N/A':
                    parts = [p.strip() for p in location_raw.split(',')]
                    city = parts[0]
                    country = parts[-1] if len(parts) > 1 else parts[0]

                print(f"\n      -> Cím: {title} | Hely: {city}, {country}")

                # 3. Leírás kinyerése (Liferay attribútum alapján)
                desc_el = job_soup.find(
                    attrs={"data-lfr-editable-id": "post-content"})

                # --- FORMAT DESCRIPTION (Markdownify) ---
                if desc_el:
                    raw_html = str(desc_el)
                    clean_desc = md(raw_html, heading_style="ATX", bullets="-",
                                    strip=['img', 'script', 'style', 'a', 'button', 'svg'])

                    # HTML entitások, láthatatlan karakterek és extra sortörések tisztítása
                    clean_desc = clean_desc.replace(
                        '•', '-').replace('·', '-').replace('\xa0', ' ').replace('\u202f', ' ')
                    clean_desc = re.sub(r'\n{3,}', '\n\n', clean_desc).strip()
                else:
                    clean_desc = "Leírás nem található."

                conn.execute('''INSERT INTO jobs (url, title, company, location_raw, city, country, description, category) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                             (job['url'], title, COMPANY_NAME, location_raw, city, country, clean_desc, job['category']))
                conn.commit()
                saved_count += 1

                # Piciny várakozás, hogy ne terheljük túl a szervert
                time.sleep(0.2)

            except Exception as e:
                print(f"\n      ⚠️ Hiba a(z) {idx}. állásnál: {e}")

        conn.close()
        print(
            f"\n✨ SIKER! {saved_count} db {COMPANY_NAME} állás letöltve villámgyorsan az adatbázisba.")

    except Exception as e:
        print(f"\n❌ Váratlan hiba történt a futás során: {e}")


if __name__ == "__main__":
    run_scraper()
