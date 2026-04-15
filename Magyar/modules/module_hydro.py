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
COMPANY_NAME = "Hydro"
# SAP RMK lapozós alap URL (50-esével ugrik)
BASE_SEARCH_URL = "https://jobs.hydro.com/search/?q=&sortColumn=referencedate&sortDirection=desc&startrow="

# Mentés a Manual mappába!
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "hydro_jobs.db")


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
    print(f"🚀 {COMPANY_NAME} Scraper indítása (Golyóálló Magyar Szűréssel)...")

    job_links = []
    unique_urls = set()

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    try:
        # --- 1. FÁZIS: AZ ÖSSZES ÁLLÁS BEGYŰJTÉSE ÉS MAGYAR SZŰRÉSE ---
        print("📂 A globális álláslista letöltése és szűrése (50-esével)...")
        offset = 0
        consecutive_empty_pages = 0

        while True:
            sys.stdout.write(
                f"\r   🔄 Lapozás: {offset} - {offset + 50}. találatok feldolgozása... (Eddig talált magyar: {len(job_links)})")
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
            job_rows = soup.find_all('tr', class_='data-row')

            if not job_rows:
                consecutive_empty_pages += 1
                if consecutive_empty_pages >= 2:
                    print("\n🏁 Elértük a lista végét.")
                    break
            else:
                consecutive_empty_pages = 0

            # 💡 PYTHON OLDALI SZŰRÉS (Golyóálló logika)
            for row in job_rows:
                try:
                    title_elem = row.find('a', class_='jobTitle-link')
                    if not title_elem:
                        continue

                    job_url = "https://jobs.hydro.com" + title_elem['href']
                    title = title_elem.get_text(strip=True)

                    # 💡 LOKÁCIÓ KINYERÉSE (Ha a span nincs meg, kivesszük a 2. oszlopból)
                    loc_elem = row.find('span', class_=re.compile(
                        r'job.*Location|jobFacility', re.IGNORECASE))
                    if not loc_elem:
                        tds = row.find_all('td')
                        if len(tds) > 1:
                            loc_elem = tds[1]  # A táblázat második oszlopa

                    location_raw = loc_elem.get_text(
                        separator=', ', strip=True) if loc_elem else "N/A"

                    # Tisztítjuk a whitespace-eket (felesleges sortöréseket)
                    location_raw = re.sub(r'\s+', ' ', location_raw)

                    # 💡 MAGYARORSZÁG SZŰRÉSE ("Székesfehérvár, HU, 8000")
                    city = "N/A"
                    country = "N/A"
                    is_hungary = False

                    if location_raw != 'N/A':
                        parts = [p.strip() for p in location_raw.split(',')]
                        city = parts[0]

                        # Bárhol is van a 'HU' vagy 'Hungary' a szövegben, elkapjuk!
                        if 'HU' in parts or 'Hungary' in parts:
                            country = 'Hungary'
                            is_hungary = True

                    if is_hungary and job_url not in unique_urls:
                        unique_urls.add(job_url)
                        clean_location_raw = f"{city}, {country}"

                        job_links.append({
                            "url": job_url,
                            "title": title,
                            "location_raw": clean_location_raw,
                            "city": city,
                            "country": country,
                            "category": "Manufacturing / Corporate"
                        })
                except Exception:
                    pass

            offset += 50
            time.sleep(0.2)

        print(
            f"\n\n✅ Összesen {len(job_links)} db magyarországi állás sikeresen kiszűrve!")

        if not job_links:
            return

        # --- 2. FÁZIS: LEÍRÁSOK KINYERÉSE ---
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

                res = requests.get(job['url'], headers=headers, timeout=10)
                job_soup = BeautifulSoup(res.text, 'html.parser')

                # RMK motoroknál a leírás általában a jobdescription class-ban vagy itemprop-ban van
                desc_el = job_soup.find(attrs={"class": "jobdescription"})
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
            f"\n✨ SIKER! {saved_count} db {COMPANY_NAME} állás letöltve a Manual mappába.")

    except Exception as e:
        print(f"\n❌ Váratlan hiba történt a futás során: {e}")


if __name__ == "__main__":
    run_scraper()
