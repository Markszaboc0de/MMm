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
COMPANY_NAME = "Ferrero"
BASE_SEARCH_URL = "https://www.ferrerocareers.com/int/en/jobs?career_stage%5BInternship%5D=Internship&page="

# Mentés a Manual mappába!
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Manual\data"
DB_PATH = os.path.join(DATA_FOLDER, "ferrero_jobs.db")

# Globális országkód térkép az átnevezéshez (EU és EU-n kívüli országok)
GLOBAL_COUNTRY_MAPPING = {
    'AR': 'Argentina', 'AU': 'Australia', 'AT': 'Austria', 'BE': 'Belgium', 'BR': 'Brazil',
    'BG': 'Bulgaria', 'CA': 'Canada', 'CL': 'Chile', 'CN': 'China', 'CO': 'Colombia',
    'HR': 'Croatia', 'CY': 'Cyprus', 'CZ': 'Czechia', 'DK': 'Denmark', 'EC': 'Colombia',
    'EE': 'Estonia', 'FI': 'Finland', 'FR': 'France', 'DE': 'Germany', 'GR': 'Greece',
    'HK': 'Hong Kong', 'HU': 'Hungary', 'IN': 'India', 'ID': 'Indonesia', 'IE': 'Ireland',
    'IL': 'Israel', 'IT': 'Italy', 'JP': 'Japan', 'LU': 'Luxembourg', 'MY': 'Malaysia',
    'MX': 'Mexico', 'NL': 'Netherlands', 'NZ': 'New Zealand', 'NO': 'Norway', 'PE': 'Peru',
    'PH': 'Philippines', 'PL': 'Poland', 'PT': 'Portugal', 'PR': 'Puerto Rico', 'QA': 'Qatar',
    'RO': 'Romania', 'RU': 'Russia', 'SA': 'Saudi Arabia', 'SG': 'Singapore', 'SK': 'Slovakia',
    'SI': 'Slovenia', 'ZA': 'South Africa', 'KR': 'South Korea', 'ES': 'Spain', 'SE': 'Sweden',
    'CH': 'Switzerland', 'TW': 'Taiwan', 'TH': 'Thailand', 'TR': 'Turkey', 'AE': 'United Arab Emirates',
    'GB': 'United Kingdom', 'UK': 'United Kingdom', 'US': 'United States', 'VN': 'Vietnam'
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
    print(f"🚀 {COMPANY_NAME} Scraper indítása (Globális keresés + Teljes Országnév Konverzió)...")

    job_links = []
    unique_urls = set()

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    try:
        # --- 1. FÁZIS: LINKEK GYŰJTÉSE A LAPOZÓVAL ---
        print("📂 Gyakornoki lista letöltése és lapozása...")
        page = 0

        while True:
            sys.stdout.write(
                f"\r   🔄 Lapozás: {page + 1}. oldal lekérése... (Eddig begyűjtve: {len(job_links)})")
            sys.stdout.flush()

            url = f"{BASE_SEARCH_URL}{page}"

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
            links = soup.find_all('a', href=True)
            found_jobs_on_page = 0

            for a_tag in links:
                href = a_tag['href']

                if '/jobs/' in href and '/apply' not in href and '?' not in href:
                    if href.startswith('/'):
                        job_url = "https://www.ferrerocareers.com" + href
                    else:
                        job_url = href

                    if job_url not in unique_urls:
                        unique_urls.add(job_url)
                        job_links.append(job_url)
                        found_jobs_on_page += 1

            if found_jobs_on_page == 0:
                print("\n🏁 Elértük a lista végét.")
                break

            page += 1
            time.sleep(0.2)

        print(
            f"\n\n✅ Összesen {len(job_links)} db Ferrero állás link begyűjtve! Kezdődik a mélyfúrás...")

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

                # 💡 LOKÁCIÓ KINYERÉSE, MEGYE ELDOBÁSA ÉS TELJES ORSZÁGNÉV
                loc_el = job_soup.find('div', class_='location')
                location_raw = loc_el.get_text(strip=True) if loc_el else "N/A"

                city = "N/A"
                country = "N/A"
                clean_location_raw = "N/A"

                if location_raw != "N/A":
                    parts = [p.strip() for p in location_raw.split(',')]

                    if len(parts) >= 3:
                        city = parts[0]
                        country_part = parts[-1]
                    elif len(parts) == 2:
                        city = parts[0]
                        country_part = parts[1]
                    else:
                        city = parts[0]
                        country_part = ""

                    if country_part:
                        country_match = re.search(
                            r'\b([A-Z]{2})\b', country_part)
                        if country_match:
                            country_code = country_match.group(1)
                            # 💡 ITT TÖRTÉNIK A VARÁZSLAT: Kód -> Teljes név
                            country = GLOBAL_COUNTRY_MAPPING.get(
                                country_code, country_code)
                        else:
                            country = re.sub(
                                r'\(.*?\)', '', country_part).strip()

                    clean_location_raw = f"{city}, {country}" if country and country != "N/A" else city

                # 💡 CÍM KINYERÉSE
                title_el = job_soup.find('span', class_='field--name-title')
                if not title_el:
                    title_el = job_soup.find('h1') or job_soup.find('h3')

                title = title_el.get_text(
                    strip=True) if title_el else "Ismeretlen pozíció"
                formatted_title = f"{title} Internship" if "internship" not in title.lower(
                ) and "trainee" not in title.lower() else title

                # 💡 LEÍRÁS KINYERÉSE (Accordion Logika)
                desc_container = job_soup.find(
                    'div', class_='job-posting__body')
                clean_desc = ""

                if desc_container:
                    accordions = desc_container.find_all('div', class_='ac')
                    for ac in accordions:
                        trigger = ac.find('button', class_='ac-trigger')
                        panel = ac.find('div', class_='ac-panel')

                        if trigger and panel:
                            header_text = trigger.get_text(strip=True)

                            if any(bp in header_text for bp in ["Our Benefits & Perks", "About Ferrero", "DE&I at Ferrero", "Diversity"]):
                                continue

                            panel_md = md(str(panel), heading_style="ATX", bullets="-",
                                          strip=['img', 'script', 'style', 'a', 'button', 'svg'])
                            clean_desc += f"**{header_text}**\n\n{panel_md}\n\n"

                    clean_desc = clean_desc.replace(
                        '•', '-').replace('·', '-').replace('\xa0', ' ').strip()
                    clean_desc = re.sub(r'\n{3,}', '\n\n', clean_desc).strip()
                else:
                    clean_desc = "Leírás nem található."

                print(
                    f"\n      -> Cím: {formatted_title[:40]}... | Hely: {city}, {country}")

                conn.execute('''INSERT INTO jobs (url, title, company, location_raw, city, country, description, category) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                             (job_url, formatted_title, COMPANY_NAME, clean_location_raw, city, country, clean_desc, "FMCG / Corporate"))
                conn.commit()
                saved_count += 1

                time.sleep(0.1)

            except Exception as e:
                print(f"\n      ⚠️ Hiba a(z) {idx}. állásnál: {e}")

        conn.close()
        print(
            f"\n✨ SIKER! {saved_count} db {COMPANY_NAME} globális gyakornoki állás letöltve a Manual mappába.")

    except Exception as e:
        print(f"\n❌ Váratlan hiba történt a futás során: {e}")


if __name__ == "__main__":
    run_scraper()
