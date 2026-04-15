import undetected_chromedriver as uc
import requests
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import sqlite3
import os
import sys
import time
import re
import json
from markdownify import markdownify as md

sys.stdout.reconfigure(encoding='utf-8')

# --- KONFIGURÁCIÓ ---
COMPANY_NAME = "BCG"
BASE_SEARCH_URL = "https://careers.bcg.com/global/en/search-results"

# EU Országok listája a kattintáshoz
EU_COUNTRIES = [
    'Austria', 'Belgium', 'Bulgaria', 'Croatia', 'Cyprus', 'Czechia', 'Denmark',
    'Estonia', 'Finland', 'France', 'Germany', 'Greece', 'Hungary', 'Ireland',
    'Italy', 'Latvia', 'Lithuania', 'Luxembourg', 'Malta', 'Netherlands', 'Poland',
    'Portugal', 'Romania', 'Slovakia', 'Slovenia', 'Spain', 'Sweden'
]

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

    # A verziószám törlésével automatikusan felismeri a legújabb Chrome-ot:
    return uc.Chrome(options=options)


def run_scraper():
    init_db()
    print(
        f"🚀 {COMPANY_NAME} Scraper indítása (UI Filter + JSON-LD Mód, Szűrés nélkül)...")

    driver = create_driver()
    job_links = []
    unique_urls = set()

    try:
        # --- 1. FÁZIS: LINKEK GYŰJTÉSE A KLIENS OLDALI SZŰRÉS UTÁN ---
        print("📂 Karrieroldal betöltése és szűrők beállítása...")
        driver.get(BASE_SEARCH_URL)
        wait = WebDriverWait(driver, 15, poll_frequency=0.5)

        # Várunk, amíg a szűrő panelek betöltődnek
        try:
            wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, 'input[data-ph-at-facetkey]')))
        except:
            print("❌ Nem sikerült betölteni a szűrőket az oldalon.")
            return

        print("   🖱️ Job Type (Internship) kiválasztása...")
        driver.execute_script("""
            let cb = document.querySelector('input[data-ph-at-facetkey="facet-jobType"][data-ph-at-text="Internship"]');
            if(cb && !cb.checked) {
                cb.click();
            }
        """)
        time.sleep(3)  # Várunk az első AJAX frissítésre

        print("   🖱️ EU Országok kiválasztása...")
        driver.execute_script("""
            let eu_countries = arguments[0];
            let checkboxes = document.querySelectorAll('input[data-ph-at-facetkey="facet-country"]');
            checkboxes.forEach(cb => {
                let text = cb.getAttribute('data-ph-at-text');
                if (eu_countries.includes(text) && !cb.checked) {
                    cb.click();
                }
            });
        """, EU_COUNTRIES)

        print("   ⏳ Várakozás az álláslista frissülésére...")
        # Hosszabb várakozás, hogy a szerver lehozza a teljes listát
        time.sleep(5)

        page_num = 1
        all_seen_raw_urls = set()

        while True:
            sys.stdout.write(
                f"\r   🔄 {page_num}. oldal feldolgozása... (Eddig talált: {len(job_links)})")
            sys.stdout.flush()

            # Kinyerjük a linkeket JS segítségével, KIZÁRÓLAG a fő találati listából
            jobs_on_page = driver.execute_script("""
                let results = [];
                // Csak a fő találati listában keressünk (kizárjuk az "Ajánlott állások" szekciót a lap alján)
                let container = document.querySelector('[data-ph-at-id="search-results-list"]') || document;
                let items = container.querySelectorAll('li[data-ph-at-id="jobs-list-item"]');
                
                if (items.length > 0) {
                    items.forEach(li => {
                        let aTag = li.querySelector('a');
                        let titleEl = li.querySelector('.job-title');
                        if (aTag && aTag.href && aTag.href.includes('/job/')) {
                            results.push({
                                url: aTag.href,
                                title: titleEl ? titleEl.innerText.trim() : aTag.innerText.trim()
                            });
                        }
                    });
                }
                return results;
            """)

            new_jobs_this_page = 0

            if jobs_on_page:
                for job in jobs_on_page:
                    title = job['title']
                    job_url = job['url'].split('?')[0].split('#')[0]

                    if job_url not in all_seen_raw_urls:
                        all_seen_raw_urls.add(job_url)
                        new_jobs_this_page += 1

                        if job_url not in unique_urls:
                            unique_urls.add(job_url)
                            job_links.append({
                                "url": job_url,
                                "title": title,
                                "category": "Consulting / Business"
                            })

            # Ha ezen az oldalon nem volt egyetlen új állás sem (pl. végtelen lapozási hurok), kilépünk
            if new_jobs_this_page == 0 and page_num > 1:
                print("\n🏁 Nincsenek új állások, elértük a lista végét.")
                break

            # 💡 LAPOZÁS: Következő oldal gomb keresése és kattintása a UI-ban
            has_next = driver.execute_script("""
                let nextBtn = document.querySelector('[data-ph-at-id="pagination-next-link"]');
                // Megnézzük, hogy létezik-e és nincs-e letiltva
                if (nextBtn && nextBtn.getAttribute('aria-disabled') !== 'true' && !nextBtn.classList.contains('disabled') && !nextBtn.parentElement.classList.contains('disabled')) {
                    nextBtn.click();
                    return true;
                }
                return false;
            """)

            if not has_next:
                print("\n🏁 Elértük az utolsó oldalt.")
                break

            page_num += 1
            time.sleep(2.5)  # Várunk a következő oldal AJAX betöltésére

        print(
            f"\n\n✅ Összesen {len(job_links)} db állás azonosítva! Kezdődik a leírások letöltése...")

        if not job_links:
            return

        # Bezárjuk a Seleniumot, innen a Requests is elég (Több 10x sebességnövekedés)
        driver.quit()

        # --- 2. FÁZIS: LEÍRÁSOK KINYERÉSE (Villámgyors Requests + JSON-LD) ---
        print("📄 Részletek letöltése...")
        conn = sqlite3.connect(DB_PATH)
        saved_count = 0

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }

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

                res = requests.get(job['url'], headers=headers, timeout=10)
                job_soup = BeautifulSoup(res.text, 'html.parser')

                clean_desc = "Leírás nem található."
                city = "N/A"
                country = "N/A"

                # 💡 SEO JSON-LD blokk kinyerése
                ld_json_tag = job_soup.find(
                    'script', type='application/ld+json')

                if ld_json_tag:
                    try:
                        job_data = json.loads(ld_json_tag.string)
                        if isinstance(job_data, list):
                            job_data = job_data[0]

                        raw_desc_html = job_data.get('description', '')
                        if raw_desc_html:
                            clean_desc = md(raw_desc_html, heading_style="ATX", bullets="-", strip=[
                                            'img', 'script', 'style', 'a', 'button', 'svg'])

                        job_loc = job_data.get('jobLocation', {})
                        if isinstance(job_loc, list) and len(job_loc) > 0:
                            job_loc = job_loc[0]

                        address = job_loc.get('address', {})
                        city = address.get('addressLocality', city)
                        country_raw = address.get('addressCountry', country)

                        if isinstance(country_raw, str):
                            country = GLOBAL_COUNTRY_MAPPING.get(
                                country_raw, country_raw)

                    except json.JSONDecodeError:
                        pass

                # Fallback HTML alapú leírás
                if clean_desc == "Leírás nem található.":
                    desc_el = job_soup.find(attrs={
                                            "data-ph-at-id": "jobdescription-text"}) or job_soup.find('div', class_='job-description')
                    if desc_el:
                        clean_desc = md(str(desc_el), heading_style="ATX", bullets="-",
                                        strip=['img', 'script', 'style', 'a', 'button', 'svg'])

                clean_desc = clean_desc.replace(
                    '•', '-').replace('·', '-').replace('\xa0', ' ').replace('\u202f', ' ')
                clean_desc = re.sub(r'\n{3,}', '\n\n', clean_desc).strip()

                # BCG BOILERPLATE LEVÁGÁSA
                truncation_markers = [
                    "Boston Consulting Group is an Equal Opportunity Employer",
                    "**Boston Consulting Group is an Equal Opportunity Employer",
                    "BCG is an E - Verify Employer",
                    "Equal Opportunity Employer"
                ]
                for marker in truncation_markers:
                    if marker in clean_desc:
                        clean_desc = clean_desc.split(marker)[0].strip()

                location_raw = f"{city}, {country}" if city != "N/A" else country

                # Írjuk ki a UI-ra a várost
                print(f"\n      -> Hely: {city}, {country}")

                conn.execute('''INSERT OR REPLACE INTO jobs (id, url, title, company, location_raw, city, country, description, category, date_found) 
                                VALUES (
                                    (SELECT id FROM jobs WHERE url = ?),
                                    ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP
                                )''',
                             (job['url'], job['url'], job['title'], COMPANY_NAME, location_raw, city, country, clean_desc, job['category']))
                conn.commit()
                saved_count += 1

                time.sleep(0.1)

            except Exception as e:
                print(f"\n      ⚠️ Hiba a(z) {idx}. állásnál: {e}")

        conn.close()
        print(
            f"\n✨ SIKER! {saved_count} db {COMPANY_NAME} állás letöltve villámgyorsan a Manual mappába.")

    except Exception as e:
        print(f"\n❌ Váratlan hiba történt a futás során: {e}")
    finally:
        try:
            driver.quit()
        except:
            pass


if __name__ == "__main__":
    run_scraper()
