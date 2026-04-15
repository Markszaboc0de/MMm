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
COMPANY_NAME = "V-Híd"
BASE_CAREER_URL = "https://vhid.hu/hu/karrier/"

# Mentés a Magyar mappába!
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "vhid_jobs.db")


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
    print(f"🚀 {COMPANY_NAME} Scraper indítása (Villámgyors Requests mód)...")

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

        # Megkeressük az összes linket
        a_tags = soup.find_all('a', href=True)

        for a_tag in a_tags:
            href = a_tag['href']

            # Csak azokat a linkeket tartjuk meg, amik a '/hu/karrier/' alatti konkrét állásokra mutatnak
            if href.startswith(BASE_CAREER_URL) and href != BASE_CAREER_URL:
                # Záró perjel levágása a duplikációk elkerülésére
                # Visszarakjuk a perjelet, ha a szerver úgy szereti
                clean_job_url = href.split('#')[0].rstrip('/') + '/'

                title = a_tag.get_text(strip=True)
                if not title:
                    continue

                if clean_job_url not in unique_urls:
                    unique_urls.add(clean_job_url)
                    job_links.append({
                        "url": clean_job_url,
                        "title": title
                    })

        print(
            f"✅ Összesen {len(job_links)} db V-Híd állás azonosítva! Kezdődik a mélyfúrás...")

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

                # 💡 LOKÁCIÓ BEÁLLÍTÁSA (Kérésed alapján fixen Budapest, Hungary)
                city = "Budapest"
                country = "Hungary"
                location_raw = f"{city}, {country}"

                # 💡 LEÍRÁS KINYERÉSE
                # A megadott HTML minta alapján a tartalom a col-md-12 osztályú div-ben van az aloldalon
                desc_el = job_soup.find('div', class_='col-md-12')
                clean_desc = "Leírás nem található."

                if desc_el:
                    raw_html = str(desc_el)
                    clean_desc = md(raw_html, heading_style="ATX", bullets="-",
                                    strip=['img', 'script', 'style', 'a', 'button', 'svg'])

                    clean_desc = clean_desc.replace(
                        '•', '-').replace('·', '-').replace('\xa0', ' ').strip()
                    clean_desc = re.sub(r'\n{3,}', '\n\n', clean_desc).strip()

                    # 💡 V-HÍD BOILERPLATE LEVÁGÁSA A VÉGÉRŐL (Jelentkezés szöveg)
                    truncation_markers = [
                        "**Jelentkezz hozzánk MOST!**",
                        "Jelentkezz hozzánk MOST!",
                        "Jelentkezéseket a toborzás@vhid.hu",
                        "toborzas@vhid.hu"
                    ]

                    first_marker_index = len(clean_desc)
                    for marker in truncation_markers:
                        index = clean_desc.find(marker)
                        if index != -1 and index < first_marker_index:
                            first_marker_index = index

                    if first_marker_index < len(clean_desc):
                        clean_desc = clean_desc[:first_marker_index].strip()

                print(f"\n      -> Hely: {city}, {country}")

                conn.execute('''INSERT INTO jobs (url, title, company, location_raw, city, country, description, category) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                             (job['url'], job['title'], COMPANY_NAME, location_raw, city, country, clean_desc, "Construction / Engineering"))
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
