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
COMPANY_NAME = "Graphisoft"
BASE_URL = "https://careers.graphisoft.com/"
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "graphisoft_jobs.db")

# Kibővített országkód térkép az átnevezéshez (HU -> Hungary)
COUNTRY_MAPPING = {
    'AT': 'Austria', 'BE': 'Belgium', 'BG': 'Bulgaria', 'HR': 'Croatia',
    'CY': 'Cyprus', 'CZ': 'Czechia', 'DK': 'Denmark', 'EE': 'Estonia',
    'FI': 'Finland', 'FR': 'France', 'DE': 'Germany', 'GR': 'Greece',
    'HU': 'Hungary', 'IE': 'Ireland', 'IT': 'Italy', 'LV': 'Latvia',
    'LT': 'Lithuania', 'LU': 'Luxembourg', 'MT': 'Malta', 'NL': 'Netherlands',
    'PL': 'Poland', 'PT': 'Portugal', 'RO': 'Romania', 'SK': 'Slovakia',
    'SI': 'Slovenia', 'ES': 'Spain', 'SE': 'Sweden',
    'US': 'United States', 'GB': 'United Kingdom', 'UK': 'United Kingdom',
    'JP': 'Japan', 'SG': 'Singapore', 'BR': 'Brazil', 'CA': 'Canada'
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


def create_driver():
    options = uc.ChromeOptions()
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    # Képek letiltása a villámgyors navigációért
    options.add_argument('--blink-settings=imagesEnabled=false')
    return uc.Chrome(options=options, version_main=145)


def run_scraper():
    init_db()
    print(f"🚀 {COMPANY_NAME} Scraper indítása (Selenium + BS4 Parsolás)...")

    driver = create_driver()
    job_links = []
    unique_urls = set()

    try:
        # --- 1. FÁZIS: LINKEK GYŰJTÉSE ---
        print("📂 Graphisoft karrieroldal megnyitása...")
        driver.get(BASE_URL)

        # Várjuk meg, amíg a kártyák elkezdenek betölteni
        time.sleep(5)

        print("🔄 Oldal görgetése az összes állás betöltéséhez...")
        last_height = driver.execute_script(
            "return document.body.scrollHeight")
        while True:
            driver.execute_script(
                "window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1.5)
            new_height = driver.execute_script(
                "return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

        # Kinyerjük az összes linket a DOM-ból
        jobs_on_page = driver.execute_script("""
            let results = [];
            let positionDivs = document.querySelectorAll('.position__text');
            positionDivs.forEach(div => {
                let aTag = div.querySelector('a');
                if(aTag && aTag.href) {
                    results.push({
                        title: aTag.innerText.trim(),
                        url: aTag.href
                    });
                }
            });
            return results;
        """)

        for job in jobs_on_page:
            if job['url'] not in unique_urls:
                unique_urls.add(job['url'])
                job_links.append(job)

        print(
            f"✅ Összesen {len(job_links)} db állás linkje begyűjtve! Kezdődik a mélyfúrás...")

        if not job_links:
            return

        # --- 2. FÁZIS: LEÍRÁSOK KINYERÉSE (Selenium + BeautifulSoup) ---
        print("📄 Állás részletek és lokációk letöltése...")
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

                # Tényleges URL megnyitása a Seleniummal
                driver.get(job['url'])

                # 💡 GARANCIA: Várunk, amíg az Angular felépíti az oldalt és beleteszi a leírást!
                try:
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located(
                            (By.CSS_SELECTOR, '[itemprop="description"]'))
                    )
                    # Kis puffer az Angular adat-kötésének befejezésére
                    time.sleep(0.5)
                except:
                    pass

                # 💡 A MÁGIA: Átadjuk az oldal betöltött forráskódját a BeautifulSoupnak
                soup = BeautifulSoup(driver.page_source, 'html.parser')

                # 1. Cím kinyerése
                title_el = soup.find(attrs={"itemprop": "title"})
                title = title_el.get_text(
                    strip=True) if title_el else job['title']

                # 2. Lokáció kinyerése és darabolása (Pl: "Budapest, HU, 1031")
                loc_el = soup.find(attrs={"itemprop": "address"})
                location_raw = loc_el.get_text(strip=True) if loc_el else "N/A"

                city = "N/A"
                country = "N/A"

                if location_raw != 'N/A':
                    parts = [p.strip() for p in location_raw.split(',')]
                    if len(parts) >= 2:
                        city = parts[0]
                        country_code = parts[1]

                        # Átkonvertáljuk a rövidítést teljes országra (HU -> Hungary)
                        country = COUNTRY_MAPPING.get(
                            country_code, country_code)
                        # Újraépítjük a location_raw-t az irányítószám NÉLKÜL
                        location_raw = f"{city}, {country}"
                    else:
                        city = parts[0]
                        location_raw = city

                print(f"\n      -> Cím: {title} | Hely: {city}, {country}")

                # 3. Leírás formázása
                desc_el = soup.find(attrs={"itemprop": "description"})
                clean_desc = "Leírás nem található."

                if desc_el:
                    raw_html = str(desc_el)
                    clean_desc = md(raw_html, heading_style="ATX", bullets="-",
                                    strip=['img', 'script', 'style', 'a', 'button', 'svg'])
                    clean_desc = clean_desc.replace(
                        '•', '-').replace('·', '-').replace('\xa0', ' ')
                    clean_desc = re.sub(r'\n{3,}', '\n\n', clean_desc).strip()

                    # 💡 BOILERPLATE LEVÁGÁSA (Levágjuk a Graphisoft céges bemutatkozót az elejéről)
                    if "About Graphisoft" in clean_desc:
                        # Ha találunk releváns alcímet, onnantól tartjuk meg a szöveget
                        parts = re.split(r'\*\*About the role\*\*|\*\*Responsibilities:\*\*|\*\*Key responsibilities\*\*|\*\*Feladatok:\*\*|Responsibilities:|Feladatok:',
                                         clean_desc, maxsplit=1, flags=re.IGNORECASE)
                        if len(parts) > 1:
                            # Visszatesszük a levágott alcímet, hogy szép maradjon
                            if "About the role" in clean_desc:
                                clean_desc = "**About the role**\n\n" + \
                                    parts[1]
                            else:
                                clean_desc = "**Responsibilities:**\n\n" + \
                                    parts[1]

                    # Sallangok a végéről
                    clean_desc = clean_desc.split("#Graphisoft")[0].strip()

                conn.execute('''INSERT INTO jobs (url, title, company, location_raw, city, country, description, category) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                             (job['url'], title, COMPANY_NAME, location_raw, city, country, clean_desc, "Technology / Software"))
                conn.commit()
                saved_count += 1

            except Exception as e:
                print(f"\n      ⚠️ Hiba a(z) {idx}. állásnál: {e}")

        conn.close()
        print(
            f"\n✨ SIKER! {saved_count} db {COMPANY_NAME} állás letöltve az adatbázisba.")

    except Exception as e:
        print(f"\n❌ Váratlan hiba történt a futás során: {e}")
    finally:
        try:
            driver.quit()
        except:
            pass


if __name__ == "__main__":
    run_scraper()
