from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
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
COMPANY_NAME = "PwC"
BASE_URL = "https://jobs-cee.pwc.com/hu/hu/search-results"

DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "pwc_jobs.db")


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
    print(f"   🏢 Scraper indítása: {COMPANY_NAME} (Kétfázisú Gyűjtő mód)...")
    init_db()

    options = Options()
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.binary_location = "/usr/bin/chromium-browser"
    _service = Service(executable_path="/usr/bin/chromedriver")
    driver = webdriver.Chrome(service=_service, options=options)
    try:
        # FÁZIS 1: ÖSSZES LINK KIGYŰJTÉSE ODA-VISSZA UGRÁLÁS NÉLKÜL
        print("   📥 FÁZIS 1: Álláslinkek összegyűjtése...")
        driver.get(BASE_URL)

        all_job_targets = []
        page_num = 1

        while True:
            print(f"      📄 {page_num}. oldal letapogatása...")

            # Várjuk a kártyák betöltését
            try:
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located(
                        (By.CLASS_NAME, "jobs-list-item"))
                )
                time.sleep(2)  # Kicsi plusz idő a biztonság kedvéért
            except:
                print(
                    "      ⚠️ Nem találtunk állásokat vagy az oldal lassan töltött be.")
                break

            soup = BeautifulSoup(driver.page_source, 'html.parser')
            job_cards = soup.find_all('li', class_='jobs-list-item')

            for card in job_cards:
                a_tag = card.find('a', {'data-ph-at-id': 'job-link'})

                if a_tag and a_tag.get('href'):
                    job_url = a_tag.get('href')
                    if not job_url.startswith('http'):
                        job_url = urljoin("https://jobs-cee.pwc.com", job_url)

                    job_title = a_tag.get(
                        'data-ph-at-job-title-text', '').strip()
                    if not job_title:
                        title_div = card.find('div', class_='job-title')
                        if title_div:
                            job_title = title_div.get_text(strip=True)

                    location_raw = a_tag.get(
                        'data-ph-at-job-location-text', '').strip()

                    city = "Ismeretlen"
                    country = "Ismeretlen"

                    if location_raw:
                        parts = [p.strip() for p in location_raw.split(',')]
                        if len(parts) >= 2:
                            city = parts[0]
                            country = parts[-1]
                        else:
                            city = location_raw

                    all_job_targets.append({
                        "url": job_url,
                        "title": job_title,
                        "city": city,
                        "country": country,
                        "location_raw": location_raw
                    })

            # Lapozás gomb megkeresése
            next_btn = soup.find(
                'a', {'data-ph-at-id': 'pagination-next-link'})

            # Ha van gomb, és nincs rajta az 'aurelia-hide' (ami elrejti, ha a végére értünk)
            if next_btn and 'aurelia-hide' not in next_btn.get('class', []):
                # Egyenesen a böngészővel kattintatjuk meg, mintha egy ember csinálná!
                clicked = driver.execute_script("""
                    let btn = document.querySelector('a[data-ph-at-id="pagination-next-link"]');
                    if(btn && !btn.classList.contains('aurelia-hide')) {
                        btn.click();
                        return true;
                    }
                    return false;
                """)

                if clicked:
                    page_num += 1
                    # Várunk, amíg az SPA betölti a következő oldalt
                    time.sleep(3)
                else:
                    break
            else:
                break

        # Duplikációk kiszűrése (ha véletlenül kétszer olvasnánk ugyanazt az oldalt)
        all_job_targets = [dict(t) for t in {tuple(
            d.items()) for d in all_job_targets}]
        print(
            f"\n   ✅ FÁZIS 1 Kész! Összesen {len(all_job_targets)} egyedi álláslinket gyűjtöttünk össze.")

        if not all_job_targets:
            return

        # FÁZIS 2: RÉSZLETEK KINYERÉSE
        print("\n   🔍 FÁZIS 2: Állások részleteinek kinyerése és mentése...")
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        new_jobs_added = 0

        for index, job in enumerate(all_job_targets, 1):
            job_url = job["url"]
            title = job["title"]
            city = job["city"]
            country = job["country"]
            location_raw = job["location_raw"]

            try:
                # Most már nyugodtan ugrálhatunk a linkek között!
                driver.get(job_url)

                try:
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located(
                            (By.CLASS_NAME, "job-description"))
                    )
                except:
                    time.sleep(2)

                job_soup = BeautifulSoup(driver.page_source, 'html.parser')
                description_lines = []

                main_content = job_soup.find('section', class_='job-description') or \
                    job_soup.find('div', class_='job-description') or \
                    job_soup.find('div', class_='job-content')

                if main_content:
                    for tag in main_content.find_all(['p', 'h2', 'h3', 'h4', 'ul', 'ol']):
                        classes = str(tag.get('class', '')).lower()
                        if any(nav in classes for nav in ['nav', 'menu', 'footer', 'header', 'cookie']):
                            continue

                        if tag.name in ['ul', 'ol']:
                            for li in tag.find_all('li'):
                                li_text = li.get_text(strip=True)
                                if li_text:
                                    description_lines.append(f"- {li_text}")
                            description_lines.append("")
                        else:
                            text = tag.get_text(strip=True)
                            if text and text not in description_lines and text != title:
                                description_lines.append(text)

                description = "\n".join(description_lines).strip()
                if not description:
                    description = "A leírás kinyerése nem sikerült. A felépítés eltérhet."

                cursor.execute('''
                    INSERT OR IGNORE INTO jobs (url, title, company, location_raw, city, country, description)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (job_url, title, COMPANY_NAME, location_raw, city, country, description))

                if cursor.rowcount > 0:
                    new_jobs_added += 1

                # Egy kis progress bar jellegű visszajelzés
                sys.stdout.write(
                    f"      [{index}/{len(all_job_targets)}] Feldolgozva...\r")
                sys.stdout.flush()

            except Exception as e:
                print(
                    f"   ❌ Hiba az állás feldolgozása közben ({job_url}): {e}")

        conn.commit()
        conn.close()

        print(
            f"\n   ✅ {COMPANY_NAME} teljesen kész! {new_jobs_added} új állás lementve az adatbázisba.")

    finally:
        try:
            if 'driver' in locals():
                driver.quit()
        except OSError:
            pass  # WinError 6 némítása
        except Exception:
            pass


if __name__ == "__main__":
    run_scraper()
