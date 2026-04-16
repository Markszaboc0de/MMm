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

# --- CONFIGURATION ---
COMPANY_NAME = "Roche"
BASE_URL = "https://careers.roche.com/global/en/search-results"
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Manual\data"
DB_PATH = os.path.join(DATA_FOLDER, "roche_jobs.db")

# EU Országok listája
EU_COUNTRIES = [
    'Austria', 'Belgium', 'Bulgaria', 'Croatia', 'Cyprus', 'Czechia', 'Czech Republic',
    'Denmark', 'Estonia', 'Finland', 'France', 'Germany', 'Greece', 'Hungary', 'Ireland',
    'Italy', 'Latvia', 'Lithuania', 'Luxembourg', 'Malta', 'Netherlands', 'Poland',
    'Portugal', 'Romania', 'Slovakia', 'Slovenia', 'Spain', 'Sweden'
]


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
    # 💡 SEBESSÉGNÖVELÉS: Képek kikapcsolása, hogy a leírás oldal azonnal betöltsön!
    return get_chrome_driver()


def run_scraper():
    init_db()
    print(f"🚀 {COMPANY_NAME} Scraper indítása (Fast API + Python Filter Mód)...")

    driver = create_driver()
    job_links = []
    unique_urls = set()

    try:
        # --- PHASE 1: CLOUDFLARE BYPASS & API FETCH ---
        print("📂 Roche megnyitása a Tokenek megszerzéséhez...")
        driver.get(BASE_URL)

        try:
            WebDriverWait(driver, 10).until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, 'li.jobs-list-item')))
        except:
            pass

        print("🎛️ API lekérdezés: MINDEN Entry Level állás letöltése...")

        offset = 0
        limit = 100

        while True:
            sys.stdout.write(
                f"\r   🔄 API lekérdezés: {offset} - {offset + limit}. találatok...")
            sys.stdout.flush()

            # Hajszálpontos payload a cURL logod alapján, KIZÁRÓLAG az Entry Level szűrővel
            api_response = driver.execute_async_script("""
                var callback = arguments[arguments.length - 1];
                var offset = arguments[0];
                var limit = arguments[1];
                
                var csrfToken = (window.phApp && window.phApp.csrfToken) ? window.phApp.csrfToken : '';
                
                var payload = {
                    "lang": "en_global",
                    "deviceType": "desktop",
                    "country": "global",
                    "pageName": "search-results",
                    "ddoKey": "refineSearch",
                    "sortBy": "",
                    "subsearch": "",
                    "from": offset,
                    "irs": false,
                    "jobs": true,
                    "counts": true,
                    "all_fields": [
                        "category", "subCategory", "country", "state", "city", 
                        "type", "jobLevel", "jobType", "campaignHashtag"
                    ],
                    "size": limit,
                    "clearAll": false,
                    "jdsource": "facets",
                    "isSliderEnable": false,
                    "pageId": "page11-ds",
                    "siteType": "external",
                    "keywords": "",
                    "global": true,
                    "selected_fields": {
                        "jobLevel": ["Entry Level"]
                    },
                    "locationData": {}
                };

                fetch(window.location.origin + '/widgets', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'x-csrf-token': csrfToken
                    },
                    body: JSON.stringify(payload)
                })
                .then(res => res.json())
                .then(data => callback(data))
                .catch(err => callback({"error": err.toString()}));
            """, offset, limit)

            if "error" in api_response:
                print(
                    f"\n❌ Hiba a belső API hívásban: {api_response['error']}")
                break

            jobs = api_response.get("refineSearch", {}).get(
                "data", {}).get("jobs", [])

            if not jobs:
                print("\n🏁 Elértük a lista végét az API-ban.")
                break

            for j in jobs:
                job_id = j.get('jobId') or j.get('reqId') or j.get('jobSeqNo')
                if not job_id:
                    continue

                url = f"https://careers.roche.com/global/en/job/{job_id}"

                if url not in unique_urls:
                    country = j.get('country', 'N/A')

                    # 💡 PYTHON OLDALI EU SZŰRÉS: Villámgyors és hibabiztos!
                    if country not in EU_COUNTRIES:
                        continue

                    unique_urls.add(url)
                    city = j.get('city', 'N/A')
                    location_raw = j.get('location', f"{city}, {country}")

                    job_links.append({
                        "url": url,
                        "title": j.get('title', "N/A"),
                        "location_raw": location_raw,
                        "city": city,
                        "country": country,
                        "category": j.get('category', "Healthcare / Pharma")
                    })

            offset += limit
            time.sleep(0.3)

        print(
            f"\n✅ Összesen {len(job_links)} db EU-s, Entry Level állás kinyerve!")

        if not job_links:
            return

        # --- PHASE 2: FAST RENDER DESCRIPTIONS ---
        print("📄 Kezdődik a leírások kinyerése...")
        conn = sqlite3.connect(DB_PATH)
        saved_count = 0
        # 💡 Ultra-gyors ellenőrzési gyakoriság!
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
                    f"\r   [{idx}/{len(job_links)}] Fetching: {job['title'][:30]}... | Hely: {job['city']}, {job['country']}")
                sys.stdout.flush()

                # Tényleges oldal betöltése
                driver.get(job['url'])

                # 💡 A GARANCIA: Nem időre várunk, hanem arra, hogy az AJAX befecskendezze a szöveget a dobozba!
                try:
                    wait.until(
                        lambda d: len(d.execute_script(
                            "return (document.querySelector('.jd-info') || {}).innerText || '';").strip()) > 30
                    )
                except:
                    pass  # Ha nem sikerült, megpróbáljuk kiolvasni amink van

                # A teljes HTML blokk kinyerése
                details = driver.execute_script("""
                    let el = document.querySelector('.jd-info') || document.querySelector('[data-ph-at-id="jobdescription-text"]');
                    return el ? el.innerHTML : '';
                """)

                clean_desc = "Leírás nem található."

                if details:
                    clean_desc = md(details, heading_style="ATX", bullets="-",
                                    strip=['img', 'script', 'style', 'a', 'button', 'svg'])
                    clean_desc = clean_desc.replace(
                        '•', '-').replace('·', '-').replace('\xa0', ' ').replace('&nbsp;', ' ')
                    clean_desc = re.sub(r'\n{3,}', '\n\n', clean_desc).strip()

                    # 💡 CÉLZOTT LEVÁGÁS: Eltávolítjuk a sallangokat a leírás aljáról
                    truncation_markers = [
                        "### Who we are", "Roche is an Equal Opportunity Employer", "Apply Now", "Jelentkezés"]
                    for marker in truncation_markers:
                        if marker in clean_desc:
                            clean_desc = clean_desc.split(marker)[0].strip()

                conn.execute('''INSERT INTO jobs (url, title, company, location_raw, city, country, description, category) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                             (job['url'], job['title'], COMPANY_NAME, job['location_raw'], job['city'], job['country'], clean_desc, job['category']))
                conn.commit()
                saved_count += 1

            except Exception as e:
                print(f"\n      ⚠️ Hiba a(z) {idx}. állásnál: {e}")

        conn.close()
        print(
            f"\n✨ SIKER! {saved_count} db EU-s, Entry Level {COMPANY_NAME} állás biztonságosan mentve.")

    finally:
        try:
            driver.quit()
        except:
            pass


if __name__ == "__main__":
    run_scraper()
