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
COMPANY_NAME = "JYSK"
BASE_SEARCH_URL = "https://allas.jysk.hu/nyitott-poziciok-hu"
BASE_DOMAIN = "https://allas.jysk.hu"

# Mentés a Magyar mappába!
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "jysk_jobs.db")


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
    print(f"🚀 {COMPANY_NAME} Scraper indítása (Villámgyors Drupal mód)...")

    job_links = []
    unique_urls = set()

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    try:
        # --- 1. FÁZIS: LINKEK ÉS LOKÁCIÓK GYŰJTÉSE ---
        print("📂 Álláslista letöltése és lapozása...")
        page = 0

        while True:
            sys.stdout.write(
                f"\r   🔄 Lapozás: {page + 1}. oldal lekérése... (Eddig begyűjtve: {len(job_links)})")
            sys.stdout.flush()

            url = f"{BASE_SEARCH_URL}?page={page}"

            try:
                response = requests.get(url, headers=headers, timeout=15)
                if response.status_code != 200:
                    break
            except Exception as e:
                print(f"\n   ❌ Hálózati hiba: {e}")
                break

            soup = BeautifulSoup(response.text, 'html.parser')

            # A Drupal Views a tbody-n belül tr tag-eket használ a sorokhoz
            job_rows = soup.find_all('tr')
            found_jobs_on_page = 0

            for row in job_rows:
                try:
                    # Cím és link kinyerése
                    title_td = row.find(
                        'td', class_=re.compile(r'views-field-title'))
                    if not title_td:
                        continue

                    a_tag = title_td.find('a')
                    if not a_tag:
                        continue

                    href = a_tag['href']
                    if href.startswith('/'):
                        job_url = BASE_DOMAIN + href
                    else:
                        job_url = href

                    title = a_tag.get_text(strip=True)

                    # 💡 VÁROS KINYERÉSE (A táblázat oszlopából)
                    city = "N/A"
                    city_td = row.find('td', class_=re.compile(
                        r'views-field-field-city'))
                    if city_td:
                        city = city_td.get_text(strip=True)

                    # Fix Magyarország
                    country = "Hungary"
                    clean_location_raw = f"{city}, {country}" if city != "N/A" else country

                    # 💡 KATEGÓRIA KINYERÉSE (Ha van ilyen oszlop)
                    category_raw = "Retail / Sales"  # Alapértelmezett
                    cat_div = row.find('div', class_='field-work-area')
                    if cat_div:
                        cat_text = cat_div.get_text(strip=True).lower()
                        if "központi iroda" in cat_text:
                            category_raw = "Corporate / Management"
                        elif "elosztóközpont" in cat_text or "logisztika" in cat_text:
                            category_raw = "Logistics / Supply Chain"

                    # Duplikációk szűrése
                    clean_job_url = job_url.split('#')[0].rstrip('/')

                    if clean_job_url not in unique_urls:
                        unique_urls.add(clean_job_url)

                        job_links.append({
                            "url": clean_job_url,
                            "title": title,
                            "location_raw": clean_location_raw,
                            "city": city,
                            "country": country,
                            "category": category_raw
                        })
                        found_jobs_on_page += 1
                except Exception:
                    pass

            if found_jobs_on_page == 0:
                print("\n🏁 Elértük a lista végét.")
                break

            page += 1
            time.sleep(0.2)

        print(
            f"\n\n✅ Összesen {len(job_links)} db JYSK állás sikeresen begyűjtve! Kezdődik a mélyfúrás...")

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

                # 💡 LEÍRÁS KINYERÉSE
                desc_el = job_soup.find('div', class_='jp-job-description')
                clean_desc = "Leírás nem található."

                if desc_el:
                    raw_html = str(desc_el)
                    clean_desc = md(raw_html, heading_style="ATX", bullets="-",
                                    strip=['img', 'script', 'style', 'a', 'button', 'svg'])

                    clean_desc = clean_desc.replace(
                        '•', '-').replace('·', '-').replace('\xa0', ' ').strip()
                    clean_desc = re.sub(r'\n{3,}', '\n\n', clean_desc).strip()

                    # 💡 JYSK BOILERPLATE LEVÁGÁSA A VÉGÉRŐL
                    truncation_markers = [
                        "**EZ A TE KÖVETKEZŐ LEHETŐSÉGED?",
                        "EZ A TE KÖVETKEZŐ LEHETŐSÉGED?",
                        "**Jelentkezz ma!**",
                        "**Kiválasztási folyamatunk:**",
                        "Kiválasztási folyamatunk:",
                        "Ha bármilyen tanácsra vagy támogatásra lenne szükséged"
                    ]

                    # Megkeressük a legelső előforduló markert, és annál vágjuk el a szöveget
                    first_marker_index = len(clean_desc)
                    for marker in truncation_markers:
                        index = clean_desc.find(marker)
                        if index != -1 and index < first_marker_index:
                            first_marker_index = index

                    if first_marker_index < len(clean_desc):
                        clean_desc = clean_desc[:first_marker_index].strip()

                print(f"\n      -> Hely: {job['city']}, {job['country']}")

                conn.execute('''INSERT INTO jobs (url, title, company, location_raw, city, country, description, category) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                             (job['url'], job['title'], COMPANY_NAME, job['location_raw'], job['city'], job['country'], clean_desc, job['category']))
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
