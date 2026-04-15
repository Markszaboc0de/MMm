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
COMPANY_NAME = "Veolia"
BASE_LIST_URL = "https://karrier.veolia.hu/allasok/"
BASE_DOMAIN = "https://karrier.veolia.hu"

# Mentés a Magyar mappába!
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "veolia_jobs.db")


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
    print(f"🚀 {COMPANY_NAME} Scraper indítása (Duplikáció-szűrővel javítva)...")

    job_links = []
    unique_urls = set()

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    try:
        # --- 1. FÁZIS: LINKEK GYŰJTÉSE A LAPOZÓVAL ---
        print("📂 Álláslista letöltése (Maximum 10 oldal)...")

        for page in range(1, 11):
            sys.stdout.write(
                f"\r   🔄 Lapozás: {page}. oldal lekérése... (Eddig begyűjtve: {len(job_links)})")
            sys.stdout.flush()

            url = f"{BASE_LIST_URL}{page}"

            try:
                response = requests.get(url, headers=headers, timeout=10)
                if response.status_code != 200:
                    break
            except Exception as e:
                print(f"\n   ❌ Hálózati hiba: {e}")
                break

            soup = BeautifulSoup(response.text, 'html.parser')

            links = soup.find_all('a', href=True)
            found_jobs_on_page = 0

            for a_tag in links:
                href = a_tag['href']

                if '/allasok/megtekintes/' in href:
                    if href.startswith('/'):
                        job_url = BASE_DOMAIN + href
                    else:
                        job_url = href

                    # 💡 A MÁGIA ITT VAN: Levágjuk a horgonyt ÉS a záró perjelet is!
                    clean_job_url = job_url.split('#')[0].rstrip('/')

                    if clean_job_url not in unique_urls:
                        unique_urls.add(clean_job_url)
                        job_links.append(clean_job_url)
                        found_jobs_on_page += 1

            if found_jobs_on_page == 0:
                print(f"\n🏁 Elértük a lista végét a(z) {page}. oldalon.")
                break

            time.sleep(0.2)

        print(
            f"\n\n✅ Összesen {len(job_links)} db egyedi Veolia állás link begyűjtve! Kezdődik a mélyfúrás...")

        if not job_links:
            return

        # --- 2. FÁZIS: LEÍRÁSOK ÉS LOKÁCIÓK KINYERÉSE ---
        print("📄 Részletek letöltése...")
        conn = sqlite3.connect(DB_PATH)
        saved_count = 0

        for idx, job_url in enumerate(job_links, 1):
            try:
                cursor = conn.cursor()
                cursor.execute("SELECT id FROM jobs WHERE url = ?", (job_url,))
                if cursor.fetchone():
                    sys.stdout.write(
                        f"\r   [{idx}/{len(job_links)}] Ugrás (Már az adatbázisban van)")
                    sys.stdout.flush()
                    continue

                sys.stdout.write(
                    f"\r   [{idx}/{len(job_links)}] Feldolgozás folyamatban...")
                sys.stdout.flush()

                res = requests.get(job_url, headers=headers, timeout=10)
                job_soup = BeautifulSoup(res.text, 'html.parser')

                # CÍM KINYERÉSE
                title_el = job_soup.find(
                    'h1', class_=re.compile(r'heading--3'))
                title = title_el.get_text(
                    strip=True) if title_el else "Ismeretlen pozíció"

                # LOKÁCIÓ KINYERÉSE
                loc_el = job_soup.find('h1', class_=re.compile(
                    r'heading--4.*align--sm--right'))
                city = loc_el.get_text(strip=True) if loc_el else "Ismeretlen"
                country = "Hungary"
                clean_location_raw = f"{city}, {country}" if city != "Ismeretlen" else "Hungary"

                # LEÍRÁS KINYERÉSE
                desc_container = job_soup.find(
                    'div', class_=re.compile(r'grid__item.*grid__item--sm--3/4'))
                clean_desc = "Leírás nem található."

                if desc_container:
                    raw_html = str(desc_container)
                    clean_desc = md(raw_html, heading_style="ATX", bullets="-",
                                    strip=['img', 'script', 'style', 'a', 'button', 'svg'])
                    clean_desc = clean_desc.replace(
                        '•', '-').replace('·', '-').replace('\xa0', ' ').strip()
                    clean_desc = re.sub(r'\n{3,}', '\n\n', clean_desc).strip()

                print(
                    f"\n      -> Cím: {title[:40]}... | Hely: {city}, {country}")

                conn.execute('''INSERT INTO jobs (url, title, company, location_raw, city, country, description, category) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                             (job_url, title, COMPANY_NAME, clean_location_raw, city, country, clean_desc, "Engineering / Utilities"))
                conn.commit()
                saved_count += 1

                time.sleep(0.1)

            except Exception as e:
                print(f"\n      ⚠️ Hiba a(z) {idx}. állásnál: {e}")

        conn.close()
        print(
            f"\n✨ SIKER! {saved_count} db {COMPANY_NAME} állás letöltve a Magyar mappába.")

    except Exception as e:
        print(f"\n❌ Váratlan hiba történt a futás során: {e}")


if __name__ == "__main__":
    run_scraper()
