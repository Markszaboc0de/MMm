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
COMPANY_NAME = "ALTEO"
BASE_SEARCH_URL = "https://karrier.alteo.hu/go/%C3%81ll%C3%A1saink/9274655/?startrow="

# Mentés a Magyar mappába!
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "alteo_jobs.db")


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
    print(f"🚀 {COMPANY_NAME} Scraper indítása (Végtelen hurok védelemmel)...")

    job_links = []
    unique_urls = set()

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    try:
        # --- 1. FÁZIS: LINKEK ÉS LOKÁCIÓK GYŰJTÉSE A HÁTTÉRBEN ---
        print("📂 Álláslista letöltése (25-ösével lapozva)...")
        offset = 0

        while True:
            sys.stdout.write(
                f"\r   🔄 Lapozás: {offset} - {offset + 25}. találatok feldolgozása... (Eddig begyűjtve: {len(job_links)})")
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
            job_elements = soup.find_all('a', class_='jobTitle-link')

            if not job_elements:
                print("\n🏁 Elértük a lista végét (üres oldal).")
                break

            new_jobs_in_this_batch = 0  # 💡 A MEGOLDÁS: Új állások számlálója az adott oldalon

            for a_tag in job_elements:
                try:
                    job_url = "https://karrier.alteo.hu" + a_tag['href']
                    title = a_tag.get_text(strip=True)

                    job_id_match = re.search(r'/(\d+)/?$', a_tag['href'])
                    job_id = job_id_match.group(1) if job_id_match else None

                    city = "N/A"
                    category = "Energy / Utilities"

                    if job_id:
                        city_el = soup.find(
                            'div', id=f"job-{job_id}-tablet-section-city-value")
                        if not city_el:
                            city_el = soup.find(
                                'div', id=f"job-{job_id}-desktop-section-city-value")
                        if city_el:
                            city = city_el.get_text(strip=True)

                        cat_el = soup.find(
                            'div', id=f"job-{job_id}-desktop-section-customfield1-value")
                        if cat_el:
                            cat_text = cat_el.get_text(strip=True).lower()
                            if "fizikai" in cat_text:
                                category = "Manufacturing / Operations"
                            elif "mérnök" in cat_text or "műszaki" in cat_text:
                                category = "Engineering"

                    country = "Hungary"
                    clean_location_raw = f"{city}, {country}" if city != "N/A" else country

                    # Záró perjel levágása a biztonság kedvéért
                    clean_job_url = job_url.split('#')[0].rstrip('/')

                    if clean_job_url not in unique_urls:
                        unique_urls.add(clean_job_url)

                        job_links.append({
                            "url": clean_job_url,
                            "title": title,
                            "location_raw": clean_location_raw,
                            "city": city,
                            "country": country,
                            "category": category
                        })
                        new_jobs_in_this_batch += 1  # Találtunk egy új állást!
                except Exception:
                    pass

            # 💡 Ha feldolgoztunk egy oldalt, de EGYETLEN új állást sem találtunk, a szerver ismétel!
            if new_jobs_in_this_batch == 0:
                print(
                    "\n🏁 Elértük a lista végét (A szerver elkezdte ismételni az állásokat).")
                break

            offset += 25
            time.sleep(0.2)

        print(
            f"\n\n✅ Összesen {len(job_links)} db egyedi ALTEO állás sikeresen begyűjtve! Kezdődik a leírások letöltése...")

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
                    print(
                        f"   [{idx}/{len(job_links)}] Ugrás (Már az adatbázisban van)")
                    continue

                sys.stdout.write(
                    f"\r   [{idx}/{len(job_links)}] Fetching: {job['title'][:30]}...")
                sys.stdout.flush()

                res = requests.get(job['url'], headers=headers, timeout=10)
                job_soup = BeautifulSoup(res.text, 'html.parser')

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

                    truncation_markers = [
                        "**AMENNYIBEN HIRDETÉSÜNK FELKELTETTE ÉRDEKLŐDÉSED:**",
                        "AMENNYIBEN HIRDETÉSÜNK FELKELTETTE ÉRDEKLŐDÉSED:",
                        "A Társaság ezúton jelzi"
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
