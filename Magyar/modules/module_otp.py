from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
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
COMPANY_NAME = "OTP_Bank"
BASE_URL = "https://karrier.otpbank.hu/go/Minden-allasajanlat/1167001/"
DOMAIN_URL = "https://karrier.otpbank.hu"
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "otp_jobs.db")


def init_db():
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)
    conn = sqlite3.connect(DB_PATH)
    conn.execute('''CREATE TABLE IF NOT EXISTS jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT, url TEXT UNIQUE, title TEXT, 
        company TEXT, city TEXT, field TEXT, description TEXT, 
        date_found TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    conn.close()


def run_scraper():
    init_db()
    print(f"🚀 {COMPANY_NAME} Scraper indítása (Load-More mód)...")

    options = Options()
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.binary_location = "/usr/bin/chromium-browser"
    _service = Service(executable_path="/usr/bin/chromedriver")
    driver = webdriver.Chrome(service=_service, options=options)
    wait = WebDriverWait(driver, 15)

    job_links = []

    try:
        driver.get(BASE_URL)
        time.sleep(5)

        # --- 1. FÁZIS: AZ ÖSSZES ÁLLÁS BETÖLTÉSE ---
        while True:
            try:
                # Megkeressük a "További találatok" gombot
                load_more_btn = driver.find_elements(
                    By.ID, "tile-more-results")

                if not load_more_btn or not load_more_btn[0].is_displayed():
                    print(
                        "🏁 Minden állás betöltve (nincs több 'További találatok' gomb).")
                    break

                # Mentjük a jelenlegi állásszámot ellenőrzéshez
                current_count = len(driver.find_elements(
                    By.CLASS_NAME, "job-tile-cell"))
                print(
                    f"   📥 Jelenleg betöltve: {current_count} állás. Kattintás a folytatáshoz...")

                # Kattintás JS-sel a stabilitás miatt
                driver.execute_script(
                    "arguments[0].scrollIntoView({block: 'center'});", load_more_btn[0])
                driver.execute_script(
                    "arguments[0].click();", load_more_btn[0])

                # Várunk, amíg több állás lesz a DOM-ban
                wait.until(lambda d: len(d.find_elements(
                    By.CLASS_NAME, "job-tile-cell")) > current_count)
                time.sleep(2)

            except Exception as e:
                print(f"   ℹ️ Megállt a betöltés: {e}")
                break

        # --- 2. FÁZIS: LINKEK ÉS METAADATOK KIGYŰJTÉSE ---
        tiles = driver.find_elements(By.CLASS_NAME, "job-tile-cell")
        print(
            f"✅ Összesen {len(tiles)} állás azonosítva. Adatok kigyűjtése...")

        for tile in tiles:
            try:
                link_el = tile.find_element(By.CLASS_NAME, "jobTitle-link")
                url = urljoin(DOMAIN_URL, link_el.get_attribute("href"))
                title = link_el.text.strip()

                # Város és Szakterület kinyerése a beküldött ID-k alapján (desktop verzió)
                city = tile.find_element(
                    By.CSS_SELECTOR, "[id*='-desktop-section-city-value']").text.strip()
                field = tile.find_element(
                    By.CSS_SELECTOR, "[id*='-desktop-section-customfield2-value']").text.strip()
                company_val = tile.find_element(
                    By.CSS_SELECTOR, "[id*='-desktop-section-businessunit-value']").text.strip()

                job_links.append({
                    "url": url, "title": title, "city": city, "field": field, "company": company_val
                })
            except:
                continue

        # --- 3. FÁZIS: MÉLYFÚRÁS (LEÍRÁSOK) ---
        conn = sqlite3.connect(DB_PATH)
        for idx, job in enumerate(job_links, 1):
            if conn.execute("SELECT 1 FROM jobs WHERE url = ?", (job['url'],)).fetchone():
                continue

            print(f"   [{idx}/{len(job_links)}] {job['title']} ({job['city']})")
            driver.get(job['url'])
            time.sleep(3.5)

            # OTP specifikus leírás kinyerő (a 'jobdescription' div-et keressük)
            description = driver.execute_script("""
                function walk(el) {
                    let text = "";
                    if (!el) return "";
                    if (el.nodeType === 3) text += el.nodeValue.trim() + " ";
                    else if (el.nodeType === 1) {
                        if (['SCRIPT','STYLE','NAV','FOOTER'].includes(el.tagName)) return "";
                        for (let child of el.childNodes) text += walk(child);
                        if (['P','DIV','BR','LI','H1','H2','H3'].includes(el.tagName)) text += "\\n";
                    }
                    return text;
                }
                let container = document.querySelector('.jobdescription') || document.querySelector('.job-description') || document.body;
                return walk(container);
            """)

            clean_desc = re.sub(r'\n\s*\n', '\n\n',
                                re.sub(r'[ \t]+', ' ', description)).strip()

            conn.execute('''INSERT INTO jobs (url, title, company, city, field, description) 
                            VALUES (?, ?, ?, ?, ?, ?)''',
                         (job['url'], job['title'], job['company'], job['city'], job['field'], clean_desc))
            conn.commit()

        conn.close()
        print(f"\n✨ SIKER! Az OTP Bank összes állása elmentve.")

    finally:
        driver.quit()


if __name__ == "__main__":
    run_scraper()
