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
COMPANY_NAME = "BCG"

# EU Országok listája a Phenom URL szűrőhöz
EU_COUNTRIES = [
    'Austria', 'Belgium', 'Bulgaria', 'Croatia', 'Cyprus', 'Czechia', 'Denmark',
    'Estonia', 'Finland', 'France', 'Germany', 'Greece', 'Hungary', 'Ireland',
    'Italy', 'Latvia', 'Lithuania', 'Luxembourg', 'Malta', 'Netherlands', 'Poland',
    'Portugal', 'Romania', 'Slovakia', 'Slovenia', 'Spain', 'Sweden'
]

# 💡 A MÁGIA: Felépítjük az előszűrt URL-t az Employment Type (jobType) és az EU országok paramétereivel
COUNTRY_PARAMS = "&".join(
    [f"country={c.replace(' ', '%20')}" for c in EU_COUNTRIES])
BASE_SEARCH_URL = f"https://careers.bcg.com/global/en/search-results?jobType=Internship&{COUNTRY_PARAMS}"

# Mentés a Manual mappába!
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Manual\data"
DB_PATH = os.path.join(DATA_FOLDER, "bcg_jobs.db")

# Globális országkód/név térkép a tisztításhoz
GLOBAL_COUNTRY_MAPPING = {
    'AT': 'Austria', 'BE': 'Belgium', 'BG': 'Bulgaria', 'HR': 'Croatia', 'CY': 'Cyprus',
    'CZ': 'Czechia', 'DK': 'Denmark', 'EE': 'Estonia', 'FI': 'Finland', 'FR': 'France',
    'DE': 'Germany', 'GR': 'Greece', 'HU': 'Hungary', 'IE': 'Ireland', 'IT': 'Italy',
    'LV': 'Latvia', 'LT': 'Lithuania', 'LU': 'Luxembourg', 'MT': 'Malta', 'NL': 'Netherlands',
    'PL': 'Poland', 'PT': 'Portugal', 'RO': 'Romania', 'SK': 'Slovakia', 'SI': 'Slovenia',
    'ES': 'Spain', 'SE': 'Sweden', 'UK': 'United Kingdom', 'GB': 'United Kingdom', 'US': 'United States'
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
    print(f"🚀 {COMPANY_NAME} Scraper indítása (Előszűrt URL: EU + Internship)...")

    driver = create_driver()
    job_links = []
    unique_urls = set()

    try:
        # --- 1. FÁZIS: LINKEK GYŰJTÉSE A LAPOZÓVAL ---
        print("📂 Szűrt álláslista letöltése (10-esével lapozva)...")
        offset = 0
        consecutive_empty_pages = 0
        wait = WebDriverWait(driver, 10, poll_frequency=0.2)

        while True:
            sys.stdout.write(
                f"\r   🔄 Lapozás: {offset} - {offset + 10}. találatok feldolgozása... (Eddig talált: {len(job_links)})")
            sys.stdout.flush()

            # Hozzáadjuk a lapozást az előszűrt "Super-URL"-hez
            url = f"{BASE_SEARCH_URL}&from={offset}&s=1"
            driver.get(url)

            try:
                wait.until(EC.presence_of_element_located(
                    (By.CSS_SELECTOR, '.job-title, .jobs-list-item, .no-jobs-msg')))
            except:
                pass

            time.sleep(1)  # Extra JS render várakozás a Phenomnak

            # Kinyerjük a linkeket JS segítségével
            jobs_on_page = driver.execute_script("""
                let results = [];
                let items = document.querySelectorAll('li[data-ph-at-id="jobs-list-item"], .jobs-list-item');
                
                if (items.length > 0) {
                    items.forEach(li => {
                        let aTag = li.querySelector('a');
                        let titleEl = li.querySelector('.job-title');
                        let locEl = li.querySelector('.job-location');
                        if (aTag && aTag.href && aTag.href.includes('/job/')) {
                            // Phenom néha beleteszi a 'Location' szót vagy fura ikon karaktereket a szövegbe
                            let locText = locEl ? locEl.innerText.replace(/Location/gi, '').replace('location_on', '').trim() : 'N/A';
                            results.push({
                                url: aTag.href,
                                title: titleEl ? titleEl.innerText.trim() : aTag.innerText.trim(),
                                location_raw: locText
                            });
                        }
                    });
                }
                return results;
            """)

            if not jobs_on_page:
                consecutive_empty_pages += 1
                if consecutive_empty_pages >= 2:
                    print("\n🏁 Elértük a szűrt lista végét.")
                    break
            else:
                consecutive_empty_pages = 0

            # 💡 MEMÓRIASZŰRÉS ÉS LOKÁCIÓTISZTÍTÁS (Dupla ellenőrzés)
            for job in jobs_on_page:
                title = job['title']
                location_raw = job['location_raw']
                job_url = job['url']

                city = "N/A"
                country = "N/A"
                clean_location_raw = "N/A"

                if location_raw != 'N/A':
                    parts = [p.strip() for p in location_raw.split(',')]

                    if len(parts) >= 2:
                        city = parts[0]
                        country_raw = parts[-1]
                    else:
                        city = parts[0]
                        country_raw = parts[0]

                    country = GLOBAL_COUNTRY_MAPPING.get(
                        country_raw, country_raw)
                    clean_location_raw = f"{city}, {country}"

                if job_url not in unique_urls:
                    unique_urls.add(job_url)
                    job_links.append({
                        "url": job_url,
                        "title": title,
                        "location_raw": clean_location_raw,
                        "city": city,
                        "country": country,
                        "category": "Consulting / Business"
                    })

            offset += 10

        print(
            f"\n\n✅ Összesen {len(job_links)} db EU-s gyakornoki állás sikeresen kiszűrve! Kezdődik a mélyfúrás...")

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

                driver.get(job['url'])

                # 💡 GARANCIA: Várunk a leírás betöltésére
                try:
                    wait.until(EC.presence_of_element_located(
                        (By.CSS_SELECTOR, '.job-description, .jd-info')))
                except:
                    pass

                job_soup = BeautifulSoup(driver.page_source, 'html.parser')

                # Ha a lokáció N/A volt a listában, utólag pontosítjuk az aloldalról
                if job['country'] == "N/A":
                    loc_el = job_soup.find('span', class_='job-location')
                    if loc_el:
                        loc_clean = re.sub(r'(?i)Location', '', loc_el.get_text(
                            strip=True)).replace('location_on', '').strip()
                        parts = [p.strip() for p in loc_clean.split(',')]
                        if len(parts) >= 2:
                            job['city'] = parts[0]
                            country_raw = parts[-1]
                            job['country'] = GLOBAL_COUNTRY_MAPPING.get(
                                country_raw, country_raw)
                        else:
                            job['city'] = parts[0]
                            job['country'] = GLOBAL_COUNTRY_MAPPING.get(
                                parts[0], parts[0])
                        job['location_raw'] = f"{job['city']}, {job['country']}"

                # 💡 LEÍRÁS KINYERÉSE
                desc_el = job_soup.find('div', class_='jd-info')
                if not desc_el:
                    desc_el = job_soup.find(
                        'section', class_='job-description')

                clean_desc = "Leírás nem található."

                if desc_el:
                    raw_html = str(desc_el)
                    clean_desc = md(raw_html, heading_style="ATX", bullets="-",
                                    strip=['img', 'script', 'style', 'a', 'button', 'svg'])
                    clean_desc = clean_desc.replace(
                        '•', '-').replace('·', '-').replace('\xa0', ' ').replace('\u202f', ' ')
                    clean_desc = re.sub(r'\n{3,}', '\n\n', clean_desc).strip()

                    # 💡 BCG BOILERPLATE LEVÁGÁSA A VÉGÉRŐL (EEO / E-Verify)
                    truncation_markers = [
                        "Boston Consulting Group is an Equal Opportunity Employer",
                        "**Boston Consulting Group is an Equal Opportunity Employer",
                        "BCG is an E - Verify Employer",
                        "Equal Opportunity Employer"
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
