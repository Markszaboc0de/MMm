import undetected_chromedriver as uc
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
COMPANY_NAME = "Hilti"
# Az általad megadott előszűrt URL, kiegészítve a dinamikus oldalszám paraméterrel
BASE_SEARCH_URL = "https://careers.hilti.group/en-us/jobs/?search=&country=20000072&country=20000084&country=20000934&country=20000144&country=20000171&country=20000177&country=20000197&country=20001093&country=20000567&country=20000290&country=20000414&country=20000231&country=20000212&country=20000302&country=20000441&country=20000493&country=20000511&country=20000466&country=20001108&experience=20000034&experience=20000035&experience=20000036&pagesize=20&page="

# Mentés a Manual mappába!
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Manual\data"
DB_PATH = os.path.join(DATA_FOLDER, "hilti_jobs.db")


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
    options = uc.ChromeOptions()
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    # Képek letiltása a villámgyors navigációért
    options.add_argument('--blink-settings=imagesEnabled=false')
    return uc.Chrome(options=options, version_main=145)


def run_scraper():
    init_db()
    print(f"🚀 {COMPANY_NAME} Scraper indítása (Selenium Anti-Bot Mód)...")

    driver = create_driver()
    job_links = []
    unique_urls = set()

    try:
        # --- 1. FÁZIS: LINKEK GYŰJTÉSE A LAPOZÓVAL ---
        print("📂 Előszűrt álláslista letöltése (Böngésző emulációval)...")
        page = 1

        while True:
            sys.stdout.write(
                f"\r   🔄 Lapozás: {page}. oldal lekérése... (Eddig begyűjtve: {len(job_links)})")
            sys.stdout.flush()

            url = f"{BASE_SEARCH_URL}{page}"
            driver.get(url)

            # 💡 Várunk egy kicsit, hogy a Hilti betöltse az oldal tartalmát
            time.sleep(2.5)

            # Átadjuk az oldal forráskódját a BeautifulSoup-nak a gyors parsolásért
            soup = BeautifulSoup(driver.page_source, 'html.parser')

            # Megkeressük az állásokat tartalmazó linkeket
            job_elements = soup.find_all('a', class_='js-view-job')

            # Ha nincs több elem, vagy az oldal üres, végeztünk a lapozással
            if not job_elements:
                print("\n🏁 Elértük a lista végét (nincs több állás a köv. oldalon).")
                break

            for a_tag in job_elements:
                try:
                    href = a_tag.get('href')
                    if not href:
                        continue

                    # Relatív URL-ek kiegészítése
                    if href.startswith('/'):
                        job_url = "https://careers.hilti.group" + href
                    else:
                        job_url = href

                    title = a_tag.get_text(strip=True)

                    if job_url not in unique_urls:
                        unique_urls.add(job_url)
                        job_links.append({
                            "url": job_url,
                            "title": title,
                            "category": "Engineering / Business"
                        })
                except Exception:
                    pass

            page += 1

        print(
            f"\n\n✅ Összesen {len(job_links)} db Hilti állás sikeresen begyűjtve! Kezdődik a mélyfúrás...")

        if not job_links:
            return

        # --- 2. FÁZIS: LEÍRÁSOK ÉS LOKÁCIÓK KINYERÉSE ---
        print("📄 Részletek letöltése (Smart Wait módszerrel)...")
        conn = sqlite3.connect(DB_PATH)
        saved_count = 0

        # Gyorsított várakozás a DOM elemekre
        wait = WebDriverWait(driver, 10, poll_frequency=0.1)

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

                # Tényleges oldal megnyitása a Seleniummal
                driver.get(job['url'])

                # 💡 GARANCIA: Várunk a cikk konténerre
                try:
                    wait.until(EC.presence_of_element_located(
                        (By.CSS_SELECTOR, 'article.cms-content')))
                except:
                    pass

                job_soup = BeautifulSoup(driver.page_source, 'html.parser')

                # 💡 LOKÁCIÓ KINYERÉSE ÉS DARABOLÁSA
                location_raw = "N/A"
                city = "N/A"
                country = "N/A"

                list_items = job_soup.find_all('li', class_='list-inline-item')
                for item in list_items:
                    item_text = item.get_text(strip=True)
                    if ',' in item_text:
                        location_raw = item_text
                        break

                if location_raw != "N/A":
                    parts = [p.strip() for p in location_raw.split(',')]
                    if len(parts) >= 2:
                        city = parts[0]
                        country = parts[-1]  # Az utolsó elem az ország
                    else:
                        city = parts[0]

                # 💡 LEÍRÁS KINYERÉSE
                desc_el = job_soup.find('article', class_='cms-content')
                clean_desc = "Leírás nem található."

                if desc_el:
                    raw_html = str(desc_el)
                    clean_desc = md(raw_html, heading_style="ATX", bullets="-",
                                    strip=['img', 'script', 'style', 'a', 'button', 'svg'])

                    clean_desc = clean_desc.replace(
                        '•', '-').replace('·', '-').replace('\xa0', ' ').replace('\u202f', ' ')
                    clean_desc = re.sub(r'\n{3,}', '\n\n', clean_desc).strip()

                    # 💡 HILTI BOILERPLATE LEVÁGÁSA A VÉGÉRŐL
                    truncation_markers = [
                        "**Why Hilti**",
                        "Why Hilti",
                        "**Commitment to Inclusion**"
                    ]
                    for marker in truncation_markers:
                        if marker in clean_desc:
                            clean_desc = clean_desc.split(marker)[0].strip()

                # Csak akkor írjuk ki a konzolra, ha találtunk releváns helyet
                if city != "N/A":
                    print(f"\n      -> Hely: {city}, {country}")

                conn.execute('''INSERT INTO jobs (url, title, company, location_raw, city, country, description, category) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                             (job['url'], job['title'], COMPANY_NAME, location_raw, city, country, clean_desc, job['category']))
                conn.commit()
                saved_count += 1

            except Exception as e:
                print(f"\n      ⚠️ Hiba a(z) {idx}. állásnál: {e}")

        conn.close()
        print(
            f"\n✨ SIKER! {saved_count} db {COMPANY_NAME} állás letöltve a Manual mappába.")

    except Exception as e:
        print(f"\n❌ Váratlan hiba történt a futás során: {e}")
    finally:
        try:
            driver.quit()
        except:
            pass


if __name__ == "__main__":
    run_scraper()
