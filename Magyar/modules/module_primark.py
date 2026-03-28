import requests
from bs4 import BeautifulSoup
import sqlite3
import os
import sys
import time
from urllib.parse import urljoin

# Windows terminál UTF-8 kódolásának kikényszerítése
sys.stdout.reconfigure(encoding='utf-8')

# ==========================================
# ⚙️ CONFIGURATION
# ==========================================
COMPANY_NAME = "Primark"
BASE_URL = "https://careers.primark.com/en/search_jobs"

DATA_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
DB_PATH = os.path.join(DATA_FOLDER, "primark_jobs.db")


def init_db():
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        url TEXT UNIQUE NOT NULL,
        title TEXT NOT NULL,
        company TEXT NOT NULL,
        location_raw TEXT,
        city TEXT,
        country TEXT,
        description TEXT
    )
    ''')
    conn.commit()
    conn.close()


def run_scraper():
    print(
        f"   🏢 Scraper indítása: {COMPANY_NAME} (Session + Mély-olvasó mód)...")
    init_db()

    # 🧠 THE FIX: Session használata! Ez megtartja a sütiket (cookie-kat),
    # így nem tűnünk spamelő botnak a szerver szemében.
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5"
    })

    try:
        current_page_url = BASE_URL
        page_num = 1
        total_jobs_added = 0
        seen_page_urls = set()

        while current_page_url:
            if current_page_url in seen_page_urls:
                print(f"   ⚠️ Végtelen ciklust észleltünk a lapozásban (ismétlődő URL: {current_page_url}). Kilépés.")
                break
            seen_page_urls.add(current_page_url)
            
            print(f"\n   📄 {page_num}. oldal letapogatása: {current_page_url}")

            # Próbáljuk meg letölteni az oldalt, ha beszakad a hálózat, újrapróbáljuk!
            try:
                response = session.get(current_page_url, timeout=5)
                # Ha 403-as (Tiltott) vagy 500-as hibát kapunk, ez jelezni fog!
                response.raise_for_status()
            except Exception as e:
                print(f"   ⚠️ Hiba az oldal betöltésekor: {e}")
                print("   ⏳ Várakozás 5 másodpercet, majd újrapróbálkozás...")
                time.sleep(5)
                continue  # Újra nekifut a while ciklusnak ugyanazzal az URL-lel

            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text, 'html.parser')

            job_cards = soup.find_all('a', class_='job-list-anchor')
            job_targets = []

            for card in job_cards:
                href = card.get('href')
                title_tag = card.find('h3', class_='job-list-title')

                if href and title_tag:
                    job_title = title_tag.get_text(strip=True)
                    job_url = urljoin("https://careers.primark.com", href)

                    city = "Ismeretlen"
                    country = "Ismeretlen"
                    location_raw = "Ismeretlen"

                    loc_tag = card.find(
                        'span', class_='job-list-info--location')
                    if loc_tag:
                        location_raw = loc_tag.get_text(strip=True)
                        parts = [p.strip() for p in location_raw.split(',')]
                        if len(parts) >= 2:
                            city = parts[0]
                            country = parts[-1]
                        else:
                            city = location_raw

                    job_targets.append({
                        "url": job_url,
                        "title": job_title,
                        "city": city,
                        "country": country,
                        "location_raw": location_raw
                    })

            job_targets = [dict(t)
                           for t in {tuple(d.items()) for d in job_targets}]

            if not job_targets:
                print(
                    "   ⚠️ Nem találtunk állásokat ezen az oldalon (Lehet, hogy elfogytak vagy blokkoltak). Kilépés.")
                break

            print(
                f"   🔍 {len(job_targets)} állás megtalálva. Részletek kinyerése és mentése...")

            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()

            for job in job_targets:
                job_url = job["url"]
                title = job["title"]
                city = job["city"]
                country = job["country"]
                location_raw = job["location_raw"]

                try:
                    # Itt is a Session-t használjuk az aloldalak letöltésére!
                    job_res = session.get(job_url, timeout=5)
                    job_res.encoding = 'utf-8'
                    job_soup = BeautifulSoup(job_res.text, 'html.parser')

                    description_lines = []

                    main_content = job_soup.find(
                        'div', class_='job-description') or job_soup.find('article') or job_soup.find('main')

                    if main_content:
                        for tag in main_content.find_all(['p', 'h2', 'h3', 'h4', 'ul', 'ol']):
                            classes = str(tag.get('class', '')).lower()
                            if any(nav in classes for nav in ['nav', 'menu', 'footer', 'header', 'cookie']):
                                continue

                            if tag.name in ['ul', 'ol']:
                                for li in tag.find_all('li'):
                                    li_text = li.get_text(strip=True)
                                    if li_text:
                                        description_lines.append(
                                            f"- {li_text}")
                                description_lines.append("")
                            else:
                                text = tag.get_text(strip=True)
                                if text and text not in description_lines and text != title:
                                    description_lines.append(text)

                    description = "\n".join(description_lines).strip()
                    if not description:
                        description = "A leírás kinyerése nem sikerült."

                    cursor.execute('''
                        INSERT OR IGNORE INTO jobs (url, title, company, location_raw, city, country, description)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (job_url, title, COMPANY_NAME, location_raw, city, country, description))

                    if cursor.rowcount > 0:
                        total_jobs_added += 1

                except Exception as e:
                    print(
                        f"   ❌ Hiba az állás feldolgozása közben ({job_url}): {e}")

            conn.commit()
            conn.close()

            # Lapozás a következő oldalra
            next_btn = soup.find('a', class_='next')
            if next_btn and next_btn.get('href'):
                href = next_btn.get('href')

                if '&p=' in href and '?' not in href:
                    href = href.replace('&p=', '?p=')

                if href.startswith('/'):
                    if not href.startswith('/en/'):
                        href = '/en' + href
                    current_page_url = f"https://careers.primark.com{href}"
                else:
                    current_page_url = f"https://careers.primark.com/en/{href}"

                page_num += 1
                # Kicsit hosszabb pihenő, hogy a Primark szervere ne kapjon szívrohamot
                time.sleep(1.5)
            else:
                print("\n   ⏹️ Elértük az utolsó oldalt (Nincs több Next gomb).")
                current_page_url = None

        print(
            f"   ✅ {COMPANY_NAME} teljesen kész! Összesen {total_jobs_added} új állás lementve az adatbázisba.")

    except Exception as e:
        print(
            f"   ❌ Kritikus hiba a(z) {COMPANY_NAME} oldal futtatása közben: {e}")


if __name__ == "__main__":
    run_scraper()
