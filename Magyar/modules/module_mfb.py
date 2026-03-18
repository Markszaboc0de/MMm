import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import sqlite3
import os
import sys
import time
import re
from urllib.parse import urljoin

sys.stdout.reconfigure(encoding='utf-8')

# --- KONFIGURÁCIÓ ---
COMPANY_NAME = "MFB Bank"
BASE_URL = "https://karrier.mfb.hu/"
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "mfb_jobs.db")


def init_db():
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)
    conn = sqlite3.connect(DB_PATH)
    conn.execute('''CREATE TABLE IF NOT EXISTS jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT, url TEXT UNIQUE, title TEXT, 
        company TEXT, city TEXT, description TEXT, category TEXT,
        date_found TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    conn.close()


def run_scraper():
    init_db()
    print(f"🚀 {COMPANY_NAME} Scraper indítása (Accordion & Sniper mód)...")

    options = uc.ChromeOptions()
    options.add_argument("--window-size=1920,1080")
    driver = uc.Chrome(options=options, version_main=145)

    job_links = []

    try:
        driver.get(BASE_URL)
        time.sleep(6)

        # Cookie-k elfogadása, ha van
        try:
            driver.find_element(
                By.CSS_SELECTOR, ".cookie-consent-accept").click()
            time.sleep(1)
        except:
            pass

        # --- 1. FÁZIS: KATEGÓRIÁK KINYITÁSA ---
        headers = driver.find_elements(
            By.CSS_SELECTOR, ".positionList__posGroupHeader")
        for header in headers:
            if header.get_attribute("data-is-opened") == "false":
                driver.execute_script("arguments[0].click();", header)
                time.sleep(0.5)

        # --- 2. FÁZIS: LINKEK BÖNGÉSZÉSE ---
        groups = driver.find_elements(
            By.CSS_SELECTOR, "[data-e2e-testing='Recruitment.Registration.PositionGroupRepeater']")

        for group in groups:
            try:
                cat_name = group.find_element(
                    By.CSS_SELECTOR, "[data-e2e-testing*='Name']").text.strip()
                rows = group.find_elements(
                    By.CSS_SELECTOR, ".positionList__positionRow--selectable")

                for row in rows:
                    url = row.get_attribute("data-position-url")
                    title = row.find_element(
                        By.CSS_SELECTOR, "[data-e2e-testing='Recruitment.Registration.PositionRepeater.Name']").text.strip()
                    # A helyszín kinyerése és tisztítása (Nádor utca levágása)
                    location_raw = row.find_element(
                        By.CSS_SELECTOR, "[data-e2e-testing*='LocationOfWork']").text.strip()
                    city = location_raw.split(
                        '-')[0].strip() if '-' in location_raw else location_raw

                    if url:
                        job_links.append({
                            "url": url, "title": title, "city": city, "category": cat_name
                        })
            except:
                continue

        print(f"✅ {len(job_links)} állás azonosítva. Kezdődik a mélyfúrás...")

        # --- 3. FÁZIS: LEÍRÁSOK SCRAPELÉSE ---
        conn = sqlite3.connect(DB_PATH)
        wait = WebDriverWait(driver, 10)

        for idx, job in enumerate(job_links, 1):
            try:
                conn.execute("DELETE FROM jobs WHERE url = ?", (job['url'],))

                print(
                    f"   [{idx}/{len(job_links)}] {job['title']} ({job['category']})")
                driver.get(job['url'])

                # Megvárjuk, amíg a tényleges hirdetés tartalma betölt (hogy ne kapjunk üres stringet)
                try:
                    wait.until(EC.presence_of_element_located(
                        (By.CSS_SELECTOR, ".jobAdvertisement__content")))
                    time.sleep(1)
                except:
                    time.sleep(2)

                # Mesterlövész adatkinyerés: Csak és kizárólag a 2 specifikus konténer tartalmát kérjük le
                description = driver.execute_script("""
                    let text = "";
                    
                    // 1. Rész: Fejléc adatok (Cím, Pozíciócsoport, Munkavégzés helye)
                    let header = document.querySelector('.positionList__section--firstSection');
                    if (header) {
                        text += header.innerText + "\\n\\n--- RÉSZLETEK ---\\n\\n";
                    }
                    
                    // 2. Rész: A hirdetés tényleges szövege
                    let content = document.querySelector('.jobAdvertisement__content');
                    if (content) {
                        text += content.innerText;
                    }
                    
                    return text;
                """)

                # Tisztítás: Többszörös üres sorok eltávolítása, hogy szép legyen az adatbázisban
                clean_desc = re.sub(r'\n{3,}', '\n\n', description).strip()

                conn.execute('''INSERT INTO jobs (url, title, company, city, description, category) 
                                VALUES (?, ?, ?, ?, ?, ?)''',
                             (job['url'], job['title'], COMPANY_NAME, job['city'], clean_desc, job['category']))
                conn.commit()

            except Exception as e:
                print(f"      ⚠️ Hiba: {e}")

        conn.close()
        print(f"\n✨ SIKER! Az MFB összes nyitott pozíciója tiszta leírással elmentve.")

    finally:
        driver.quit()


if __name__ == "__main__":
    run_scraper()
