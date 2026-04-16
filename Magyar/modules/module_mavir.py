import sys as _sys
import os as _os
_sys.path.append(_os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))
from driver_setup import get_chrome_driver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import sqlite3
import os
import sys
import time
import re
from markdownify import markdownify as md

sys.stdout.reconfigure(encoding='utf-8')

# --- KONFIGURÁCIÓ ---
COMPANY_NAME = "MAVIR"
BASE_SEARCH_URL = "https://karrier.mavir.hu/jobs"

# Mentés a Magyar mappába!
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "mavir_jobs.db")


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


def create_driver():
    return get_chrome_driver()


def run_scraper():
    init_db()
    print(
        f"🚀 {COMPANY_NAME} Scraper indítása (Full Selenium + Map Marker Város Szűrő)...")

    driver = create_driver()
    job_links = []
    unique_urls = set()

    try:
        # --- 1. FÁZIS: LINKEK ÉS VÁROSOK GYŰJTÉSE ---
        print("📂 Karrieroldal betöltése és lista görgetése a legaljáig...")
        driver.get(BASE_SEARCH_URL)

        wait = WebDriverWait(driver, 10, poll_frequency=0.2)
        try:
            wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, '.job-post-item')))
        except:
            print("❌ Nem sikerült betölteni az állásokat.")
            return

        last_height = driver.execute_script(
            "return document.body.scrollHeight")
        scroll_attempts = 0
        while True:
            sys.stdout.write(
                f"\r   🔄 Görgetés folyamatban... ({scroll_attempts + 1}. fázis)")
            sys.stdout.flush()

            driver.execute_script(
                "window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)

            new_height = driver.execute_script(
                "return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height
            scroll_attempts += 1

        # A PONTOS CÍM ÉS VÁROS KINYERÉSE (BS4-gyel a Selenium forráskódjából)
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        cards = soup.find_all('div', class_='job-post-item')

        for card in cards:
            title_h2 = card.find('h2', class_=re.compile(r'index-job-title'))
            if not title_h2:
                continue

            a_tag = title_h2.find('a')
            if not a_tag or not a_tag.get('href'):
                continue

            job_url = a_tag['href']
            title = a_tag.get_text(strip=True)

            # 💡 VÁROS KINYERÉSE (Csak azt a dobozt nézzük, ahol ott a térkép ikon!)
            city = "N/A"
            loc_divs = card.find_all('div', class_='float-sm-left')
            for div in loc_divs:
                if div.find('span', class_='icon-map-marker'):
                    city = div.get_text(strip=True)
                    break

            clean_job_url = job_url.split('#')[0].rstrip('/')

            if clean_job_url not in unique_urls:
                unique_urls.add(clean_job_url)

                country = "Hungary"
                clean_location_raw = f"{city}, {country}" if city != "N/A" else country

                job_links.append({
                    "url": clean_job_url,
                    "title": title,
                    "location_raw": clean_location_raw,
                    "city": city,
                    "country": country,
                    "category": "Energy / Utilities"
                })

        print(
            f"\n✅ Összesen {len(job_links)} db állás azonosítva! Kezdődik a mélyfúrás...")

        if not job_links:
            return

        # --- 2. FÁZIS: LEÍRÁSOK KINYERÉSE (SELENIUMMAL!) ---
        print("📄 Részletek letöltése (API várakozással)...")
        conn = sqlite3.connect(DB_PATH)
        saved_count = 0

        # A 2. fázishoz szigorúbb Wait-et használunk
        content_wait = WebDriverWait(driver, 15, poll_frequency=0.5)

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

                # 💡 ITT A LÉNYEG: A Selenium nyitja meg az aloldalt, hogy a MAVIR API-ja lefusson!
                driver.get(job['url'])

                # Várunk, amíg a JS legenerálja a leírást tartalmazó blokkokat
                try:
                    content_wait.until(EC.presence_of_element_located(
                        (By.CSS_SELECTOR, '.singleJobBlockHolder, h4.pb-2')))
                except:
                    pass

                time.sleep(1.5)  # Kis extra idő az animációknak

                job_soup = BeautifulSoup(driver.page_source, 'html.parser')
                clean_desc = ""

                # 1. Kigyűjtjük az "Elvárt képességek" címkéket
                tags_container = job_soup.find(
                    'div', class_=re.compile(r'col-md-6 mb-3'))
                if tags_container:
                    tags = tags_container.find_all(
                        'span', class_=re.compile(r'tagcloud|tag-cloud-link'))
                    if tags:
                        clean_desc += "**Kiemelt elvárások:**\n"
                        for tag in tags:
                            clean_desc += f"- {tag.get_text(strip=True)}\n"
                        clean_desc += "\n"

                # 2. Kigyűjtjük a dinamikusan felépülő blokkokat
                job_content_area = job_soup.find(
                    'div', class_=re.compile(r'col-md-8'))

                if job_content_area:
                    headings = job_content_area.find_all(
                        'h2', class_='singleJobH2')
                    blocks = job_content_area.find_all(
                        'div', class_='singleJobBlockHolder')

                    if headings and blocks:
                        for i in range(min(len(headings), len(blocks))):
                            h2_text = headings[i].get_text(strip=True)
                            block_html = str(blocks[i])

                            block_md = md(block_html, heading_style="ATX", bullets="-",
                                          strip=['img', 'script', 'style', 'a', 'button', 'svg'])
                            clean_desc += f"### {h2_text}\n\n{block_md.strip()}\n\n"
                    else:
                        # FALLBACK
                        for trash in job_content_area.find_all(['ul', 'div'], class_=re.compile(r'ftco-footer-social|row pb-5|hidden-lg')):
                            trash.decompose()
                        clean_desc += md(str(job_content_area), heading_style="ATX", bullets="-", strip=[
                                         'img', 'script', 'style', 'a', 'button', 'svg'])

                clean_desc = clean_desc.replace(
                    '•', '-').replace('·', '-').replace('\xa0', ' ').strip()
                clean_desc = re.sub(r'\n{3,}', '\n\n', clean_desc).strip()

                if not clean_desc:
                    clean_desc = "Leírás nem található (az API nem küldött adatot)."

                print(
                    f"\n      -> Cím: {job['title']} | Hely: {job['city']}, {job['country']}")

                # Ha a korábbi hibás futásokból benne maradt volna az üres adat, ezt felülírhatjuk (ha egyezik az URL)
                conn.execute('''INSERT OR REPLACE INTO jobs (id, url, title, company, location_raw, city, country, description, category, date_found)
                                VALUES (
                                    (SELECT id FROM jobs WHERE url = ?), 
                                    ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP
                                )''',
                             (job['url'], job['url'], job['title'], COMPANY_NAME, job['location_raw'], job['city'], job['country'], clean_desc, job['category']))
                conn.commit()
                saved_count += 1

            except Exception as e:
                print(f"\n      ⚠️ Hiba a(z) {idx}. állásnál: {e}")

        conn.close()
        print(
            f"\n✨ SIKER! {saved_count} db {COMPANY_NAME} állás letöltve a Magyar mappába.")

    except Exception as e:
        print(f"\n❌ Váratlan hiba történt a futás során: {e}")
    finally:
        try:
            driver.quit()
        except:
            pass


if __name__ == "__main__":
    run_scraper()
