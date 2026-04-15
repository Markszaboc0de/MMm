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
COMPANY_NAME = "AGCO"

# RMK Kategória oldalaknál az offset a perjelek közé kerül!
BASE_CATEGORY_URL = "https://careers.agcocorp.com/go/Entry-Level/2575700/"
QUERY_PARAMS = "?q=&sortColumn=referencedate&sortDirection=desc"
BASE_DOMAIN = "https://careers.agcocorp.com"

# Mentés a Manual mappába!
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Manual\data"
DB_PATH = os.path.join(DATA_FOLDER, "agco_jobs.db")

# Országkód feloldó szótár az SAP RMK rendszerekhez
COUNTRY_CODES = {
    'US': 'United States',
    'DE': 'Germany',
    'HU': 'Hungary',
    'GB': 'United Kingdom',
    'UK': 'United Kingdom',
    'FR': 'France',
    'IT': 'Italy',
    'ES': 'Spain',
    'AT': 'Austria',
    'CH': 'Switzerland',
    'BR': 'Brazil',
    'CN': 'China',
    'IN': 'India'
}


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
    print(f"🚀 {COMPANY_NAME} Scraper indítása (Szűretlen, RMK SEO Lapozó Mód)...")

    job_links = []
    unique_urls = set()
    all_seen_raw_urls = set()

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    try:
        # --- 1. FÁZIS: LINKEK ÉS LOKÁCIÓK GYŰJTÉSE ---
        print("📂 Álláslista letöltése (25-ösével lapozva)...")
        offset = 0

        while True:
            sys.stdout.write(
                f"\r   🔄 Lapozás: {offset} - {offset + 25}. találatok vizsgálata... (Eddig begyűjtve: {len(job_links)})")
            sys.stdout.flush()

            # 💡 A MÁGIA: Az SAP RMK megfelelő URL felépítése
            if offset == 0:
                url = f"{BASE_CATEGORY_URL}{QUERY_PARAMS}"
            else:
                url = f"{BASE_CATEGORY_URL}{offset}/{QUERY_PARAMS}"

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

            job_elements = soup.find_all('a', class_='jobTitle-link')

            if not job_elements:
                print("\n🏁 Elértük a lista végét (Nincs több állás az oldalon).")
                break

            new_jobs_this_page = 0

            for a_tag in job_elements:
                try:
                    job_url = a_tag['href']
                    if job_url.startswith('/'):
                        job_url = BASE_DOMAIN + job_url
                    clean_job_url = job_url.split('#')[0].rstrip('/')

                    if clean_job_url not in all_seen_raw_urls:
                        all_seen_raw_urls.add(clean_job_url)
                        new_jobs_this_page += 1

                        title = a_tag.get_text(strip=True)

                        city = "N/A"
                        country = "N/A"
                        loc_text = "N/A"

                        parent_row = a_tag.find_parent('tr')
                        if not parent_row:
                            parent_row = a_tag.find_parent(
                                'li') or a_tag.find_parent('div')

                        if parent_row:
                            loc_span = parent_row.find(
                                class_=re.compile(r'jobLocation', re.IGNORECASE))
                            if loc_span:
                                loc_text = loc_span.get_text(strip=True)
                                parts = [p.strip()
                                         for p in loc_text.split(',')]

                                if len(parts) >= 1:
                                    city = parts[0]
                                if len(parts) >= 3:
                                    country_code = parts[-1]
                                    country = COUNTRY_CODES.get(
                                        country_code, country_code)
                                elif len(parts) == 2:
                                    country_code = parts[1]
                                    country = COUNTRY_CODES.get(
                                        country_code, country_code)

                        clean_location_raw = f"{city}, {country}" if city != "N/A" and country != "N/A" else loc_text

                        if clean_job_url not in unique_urls:
                            unique_urls.add(clean_job_url)
                            job_links.append({
                                "url": clean_job_url,
                                "title": title,
                                "location_raw": clean_location_raw,
                                "city": city,
                                "country": country,
                                "category": "Manufacturing / Engineering"
                            })
                except Exception:
                    pass

            if new_jobs_this_page == 0:
                print("\n🏁 Elértük a lista végét (A szerver ismétli a találatokat).")
                break

            offset += 25
            time.sleep(0.2)

        print(
            f"\n\n✅ Összesen {len(job_links)} db AGCO állás azonosítva! Kezdődik a leírások letöltése...")

        if not job_links:
            return

        # --- 2. FÁZIS: LEÍRÁSOK KINYERÉSE ---
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

                desc_el = job_soup.find('span', class_='jobdescription')
                if not desc_el:
                    desc_el = job_soup.find(attrs={"itemprop": "description"})

                clean_desc = "Leírás nem található."

                if desc_el:
                    raw_html = str(desc_el)
                    clean_desc = md(raw_html, heading_style="ATX", bullets="-",
                                    strip=['img', 'script', 'style', 'a', 'button', 'svg'])

                    clean_desc = clean_desc.replace(
                        '•', '-').replace('·', '-').replace('\xa0', ' ').replace('\u202f', ' ')
                    clean_desc = re.sub(r'\n{3,}', '\n\n', clean_desc).strip()

                    truncation_markers = [
                        "**Join us as we bring agriculture into the future and apply now!**",
                        "Join us as we bring agriculture into the future",
                        "Nearest Major Market:"
                    ]
                    for marker in truncation_markers:
                        if marker in clean_desc:
                            clean_desc = clean_desc.split(marker)[0].strip()

                print(
                    f"\n      -> Cím: {job['title']} | Hely: {job['city']}, {job['country']}")

                conn.execute('''INSERT OR REPLACE INTO jobs (id, url, title, company, location_raw, city, country, description, category, date_found)
                                VALUES (
                                    (SELECT id FROM jobs WHERE url = ?), 
                                    ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP
                                )''',
                             (job['url'], job['url'], job['title'], COMPANY_NAME, job['location_raw'], job['city'], job['country'], clean_desc, job['category']))
                conn.commit()
                saved_count += 1

                time.sleep(0.1)

            except Exception as e:
                print(f"\n      ⚠️ Hiba a(z) {idx}. állásnál: {e}")

        conn.close()
        print(
            f"\n✨ SIKER! {saved_count} db {COMPANY_NAME} állás letöltve a Manual mappába.")

    except Exception as e:
        print(f"\n❌ Váratlan hiba történt a futás során: {e}")


if __name__ == "__main__":
    run_scraper()
