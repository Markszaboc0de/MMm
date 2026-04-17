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
COMPANY_NAME = "SAP"
BASE_URL = "https://jobs.sap.com/search/"
DATA_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
DB_PATH = os.path.join(DATA_FOLDER, "sap_jobs.db")

# A célzott karrier szintek (URL paraméterként küldjük be)
TARGET_STATUSES = ["Graduate", "Student", "Vocational"]

# Kétbetűs ISO országkódok -> Teljes angol országnevek (Kizárólag EU)
EU_COUNTRY_MAPPING = {
    'AT': 'Austria', 'BE': 'Belgium', 'BG': 'Bulgaria', 'HR': 'Croatia',
    'CY': 'Cyprus', 'CZ': 'Czechia', 'DK': 'Denmark', 'EE': 'Estonia',
    'FI': 'Finland', 'FR': 'France', 'DE': 'Germany', 'GR': 'Greece',
    'HU': 'Hungary', 'IE': 'Ireland', 'IT': 'Italy', 'LV': 'Latvia',
    'LT': 'Lithuania', 'LU': 'Luxembourg', 'MT': 'Malta', 'NL': 'Netherlands',
    'PL': 'Poland', 'PT': 'Portugal', 'RO': 'Romania', 'SK': 'Slovakia',
    'SI': 'Slovenia', 'ES': 'Spain', 'SE': 'Sweden'
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
    print(
        f"🚀 {COMPANY_NAME} Scraper indítása (Villámgyors Requests + URL Pre-Filter mód)...")

    job_links = []
    unique_urls = set()

    # Valódi böngészőnek álcázzuk magunkat
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    try:
        # --- 1. FÁZIS: LINKEK GYŰJTÉSE (Kategóriánként) ---
        for status in TARGET_STATUSES:
            print(f"\n📂 '{status}' státuszú állások keresése a szerveren...")
            offset = 0

            while True:
                # 💡 A MÁGIA: Közvetlenül az URL-ben mondjuk meg a szervernek a szűrőket és a lapozást!
                url = f"{BASE_URL}?optionsFacetsDD_customfield3={status}&startrow={offset}"

                response = requests.get(url, headers=headers, timeout=10)
                if response.status_code != 200:
                    print(
                        f"   ❌ Hiba az oldal lekérésekor! HTTP Kód: {response.status_code}")
                    break

                soup = BeautifulSoup(response.text, 'html.parser')

                # Állás sorok megkeresése a táblázatban
                job_rows = soup.select('tr.data-row')

                if not job_rows:
                    # Ha nincs több sor, végeztünk ezzel a státusszal
                    break

                sys.stdout.write(
                    f"\r   🔄 {status} lekérdezés: {offset} - {offset + 25}. találatok...")
                sys.stdout.flush()

                for row in job_rows:
                    title_elem = row.select_one('.jobTitle-link')
                    location_elem = row.select_one(
                        '.jobLocation')  # Vagy a második <td>

                    if not title_elem:
                        continue

                    job_url = "https://jobs.sap.com" + title_elem['href']
                    title = title_elem.get_text(strip=True)

                    if not location_elem:
                        # Fallback, ha nem találja a jobLocation classt
                        tds = row.find_all('td')
                        location_raw = tds[1].get_text(
                            strip=True) if len(tds) > 1 else "N/A"
                    else:
                        location_raw = location_elem.get_text(strip=True)

                    # 💡 LOKÁCIÓ ÉS EU SZŰRÉS (Pl: "Walldorf, DE, 69190")
                    # Szétszedjük a vesszők mentén, és megnézzük, van-e benne EU-s kód
                    city = "N/A"
                    country = "N/A"
                    is_eu = False

                    if location_raw != 'N/A':
                        parts = [p.strip() for p in location_raw.split(',')]
                        city = parts[0]

                        # Keresünk a részek között olyan kódot, ami szerepel a listánkban
                        for part in parts:
                            if part in EU_COUNTRY_MAPPING:
                                country = EU_COUNTRY_MAPPING[part]
                                is_eu = True
                                break

                    if is_eu and job_url not in unique_urls:
                        unique_urls.add(job_url)
                        job_links.append({
                            "url": job_url,
                            "title": title,
                            "location_raw": location_raw,
                            "city": city,
                            "country": country,
                            "category": "Tech / IT / Corporate"
                        })

                offset += 25  # Az SAP oldal 25-ösével lapoz

        print(
            f"\n\n✅ Összesen {len(job_links)} db EU-s, {', '.join(TARGET_STATUSES)} állás azonosítva! Kezdődik a mélyfúrás...")

        if not job_links:
            return

        # --- 2. FÁZIS: LEÍRÁSOK KINYERÉSE ÉS TISZTÍTÁSA ---
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
                    f"\r   [{idx}/{len(job_links)}] Állás lekérése: {job['title'][:30]}...")
                sys.stdout.flush()

                # HTML lekérése a háttérben
                res = requests.get(job['url'], headers=headers, timeout=10)
                job_soup = BeautifulSoup(res.text, 'html.parser')

                # Leírás kinyerése
                desc_el = job_soup.find('span', class_='jobdescription')

                if desc_el:
                    raw_html = str(desc_el)
                    clean_desc = md(raw_html, heading_style="ATX", bullets="-",
                                    strip=['img', 'script', 'style', 'a', 'button', 'svg'])

                    # HTML entitások tisztítása
                    clean_desc = clean_desc.replace(
                        '•', '-').replace('·', '-').replace('\xa0', ' ').replace('\u202f', ' ')
                    clean_desc = re.sub(r'\n{3,}', '\n\n', clean_desc).strip()

                    # 💡 SAP MARKETING BOILERPLATE LEVÁGÁSA
                    # Sokszor ezzel a mondattal kezdődik:
                    if "We help the world run better" in clean_desc:
                        # Keresünk egy "What you'll do" vagy "EXPECTATIONS" fázist,
                        # vagy csak simán kidobjuk a legelső bekezdést
                        parts = re.split(r'\*\*What you\'ll do\*\*|\*\*EXPECTATIONS\*\*|What you\'ll do|EXPECTATIONS',
                                         clean_desc, maxsplit=1, flags=re.IGNORECASE)
                        if len(parts) > 1:
                            clean_desc = "**What you'll do**\n\n" + parts[1]

                    # A szöveg aljáról levágjuk a sallangokat
                    truncation_markers = [
                        "Bring out your best",
                        "We win with inclusion",
                        "AI Usage in the Recruitment Process",
                        "Job Segment:"
                    ]
                    for marker in truncation_markers:
                        # Markdown bolddal is megnézzük
                        for variant in [marker, f"**{marker}**"]:
                            if variant in clean_desc:
                                clean_desc = clean_desc.split(variant)[
                                    0].strip()

                else:
                    clean_desc = "Leírás nem található."

                print(f"\n      -> Hely: {job['city']}, {job['country']}")

                conn.execute('''INSERT INTO jobs (url, title, company, location_raw, city, country, description, category) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                             (job['url'], job['title'], COMPANY_NAME, job['location_raw'], job['city'], job['country'], clean_desc, job['category']))
                conn.commit()
                saved_count += 1

            except Exception as e:
                print(f"\n      ⚠️ Hiba a(z) {idx}. állásnál: {e}")

        conn.close()
        print(
            f"\n✨ SIKER! {saved_count} db {COMPANY_NAME} állás letöltve villámgyorsan az adatbázisba.")

    except Exception as e:
        print(f"\n❌ Váratlan hiba történt a futás során: {e}")


if __name__ == "__main__":
    run_scraper()
