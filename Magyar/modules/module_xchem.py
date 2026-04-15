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
COMPANY_NAME = "X-Chem"
# Keresési URL (Alapból a magyarországi /HU/ állásokra szűrve)
BASE_SEARCH_URL = "https://careers.xchemrx.com/search/?location=HU&sortColumn=referencedate&sortDirection=desc&startrow="

# Mentés a Manual mappába!
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "xchem_jobs.db")

# Entry-level kulcsszavak a memóriaszűréshez (kiegészítve a 'talent pool' kifejezéssel)
ENTRY_LEVEL_KEYWORDS = ['intern', 'trainee', 'student', 'graduate',
                        'vocational', 'gyakornok', 'pályakezdő', 'junior', 'talent pool']


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
    print(
        f"🚀 {COMPANY_NAME} Scraper indítása (Villámgyors API Lapozás + Python Szűrés)...")

    job_links = []
    unique_urls = set()

    # Valódi böngészőnek álcázzuk magunkat
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    try:
        # --- 1. FÁZIS: AZ ÖSSZES RELEVÁNS ÁLLÁS BEGYŰJTÉSE ---
        print("📂 A magyarországi álláslista letöltése (25-ösével)...")
        offset = 0
        consecutive_empty_pages = 0

        while True:
            sys.stdout.write(
                f"\r   🔄 Lapozás: {offset} - {offset + 25}. találatok feldolgozása... (Eddig talált gyakornoki: {len(job_links)})")
            sys.stdout.flush()

            url = f"{BASE_SEARCH_URL}{offset}"

            try:
                response = requests.get(url, headers=headers, timeout=15)
                if response.status_code != 200:
                    print(
                        f"\n   ❌ HTTP Hiba: {response.status_code}. Megállítjuk a lapozást.")
                    break
            except Exception as e:
                print(f"\n   ❌ Hálózati hiba: {e}")
                break

            soup = BeautifulSoup(response.text, 'html.parser')

            # Megkeressük az állásokat tartalmazó linkeket
            job_elements = soup.find_all('a', class_='jobTitle-link')

            if not job_elements:
                consecutive_empty_pages += 1
                if consecutive_empty_pages >= 2:
                    print("\n🏁 Elértük a lista végét.")
                    break
            else:
                consecutive_empty_pages = 0

            # 💡 PYTHON OLDALI SZŰRÉS
            for a_tag in job_elements:
                try:
                    job_url = "https://careers.xchemrx.com" + a_tag['href']
                    title = a_tag.get_text(strip=True)

                    # SZŰRÉS: Entry Level a cím alapján?
                    title_lower = title.lower()
                    if not any(kw in title_lower for kw in ENTRY_LEVEL_KEYWORDS):
                        continue

                    # 💡 FIX LOKÁCIÓ (Kérésed alapján)
                    city = "Budapest"
                    country = "Hungary"
                    location_raw = "Budapest, Hungary"

                    if job_url not in unique_urls:
                        unique_urls.add(job_url)

                        job_links.append({
                            "url": job_url,
                            "title": title,
                            "location_raw": location_raw,
                            "city": city,
                            "country": country,
                            "category": "Science / Research"
                        })
                except Exception:
                    pass

            offset += 25  # Az SAP RMK motor 25-ösével adja az eredményeket
            time.sleep(0.2)

        print(
            f"\n\n✅ Összesen {len(job_links)} db gyakornoki/pályakezdő állás sikeresen kiszűrve!")

        if not job_links:
            return

        # --- 2. FÁZIS: LEÍRÁSOK KINYERÉSE (Villámgyors Requests) ---
        print("📄 Leírások letöltése...")
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
                    f"\r   [{idx}/{len(job_links)}] Fetching: {job['title'][:30]}...")
                sys.stdout.flush()

                # HTML lekérése a háttérben
                res = requests.get(job['url'], headers=headers, timeout=10)
                job_soup = BeautifulSoup(res.text, 'html.parser')

                # Leírás kinyerése a megadott class és itemprop alapján
                desc_el = job_soup.find('span', class_='jobdescription')
                clean_desc = "Leírás nem található."

                if desc_el:
                    raw_html = str(desc_el)
                    clean_desc = md(raw_html, heading_style="ATX", bullets="-",
                                    strip=['img', 'script', 'style', 'a', 'button', 'svg'])

                    clean_desc = clean_desc.replace(
                        '•', '-').replace('·', '-').replace('\xa0', ' ').replace('\u202f', ' ')
                    clean_desc = re.sub(r'\n{3,}', '\n\n', clean_desc).strip()

                    # 💡 X-CHEM BOILERPLATE LEVÁGÁSA A VÉGÉRŐL (Pl: "Amennyiben felkeltettük az érdeklődésed...")
                    truncation_markers = [
                        "Amennyiben felkeltettük az érdeklődésed jelentkezz hozzánk!",
                        "Apply now",
                        "Jelentkezz"
                    ]
                    for marker in truncation_markers:
                        if marker in clean_desc:
                            clean_desc = clean_desc.split(marker)[0].strip()

                print(f"\n      -> Hely: {job['city']}, {job['country']}")

                conn.execute('''INSERT INTO jobs (url, title, company, location_raw, city, country, description, category) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                             (job['url'], job['title'], COMPANY_NAME, job['location_raw'], job['city'], job['country'], clean_desc, job['category']))
                conn.commit()
                saved_count += 1

                time.sleep(0.1)  # Kíméletes várakozás a szerver felé

            except Exception as e:
                print(f"\n      ⚠️ Hiba a(z) {idx}. állásnál: {e}")

        conn.close()
        print(
            f"\n✨ SIKER! {saved_count} db {COMPANY_NAME} állás letöltve villámgyorsan a Manual mappába.")

    except Exception as e:
        print(f"\n❌ Váratlan hiba történt a futás során: {e}")


if __name__ == "__main__":
    run_scraper()
