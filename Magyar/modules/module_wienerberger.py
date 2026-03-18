from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

import sys as _sys
import os as _os
_sys.path.append(_os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))
from driver_setup import get_chrome_driver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import sqlite3
import os
import sys
import time
import re
from urllib.parse import urljoin

# Force UTF-8 encoding for Windows terminals
sys.stdout.reconfigure(encoding='utf-8')

# ==========================================
# ⚙️ CONFIGURATION
# ==========================================
COMPANY_NAME = "Wienerberger"
BASE_URL = "https://wienerberger.hrmaster.hu/DataCenter/Registration/JobAdvertisements/wienerberger"
DOMAIN_URL = "https://wienerberger.hrmaster.hu"
DATA_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
DB_PATH = os.path.join(DATA_FOLDER, "wienerberger_jobs.db")
CHROME_VERSION = 145


def init_db():
    """Ensure the data directory and database exist before trying to insert."""
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)
        print(f"   📁 Created new data directory: {DATA_FOLDER}")

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
        description TEXT,
        date_found TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    conn.commit()
    conn.close()


def _clean_city(raw_loc):
    """Extracts the exact city from the HRMaster location string."""
    if not raw_loc or raw_loc == "Unknown":
        return "Unknown"

    if " - " in raw_loc:
        parts = raw_loc.split(" - ")
        potential_city = parts[-1].strip()
        if not any(char.isdigit() for char in potential_city):
            return potential_city

    cleaned = re.sub(r'\d+', '', raw_loc)
    parts = cleaned.split(',')
    city = parts[0].strip()
    return city if city else "Unknown"


def run_scraper():
    init_db()
    print(f"🚀 Indul a {COMPANY_NAME} HRMaster scraper...")

    options.add_argument("--disable-popup-blocking")
    driver = get_chrome_driver()
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        # --- 1. FÁZIS: Linkek gyűjtése ---
        print("📂 Karrieroldal megnyitása...")
        driver.get(BASE_URL)
        time.sleep(5)  # Várjuk meg az Angular renderelést

        # Görgetés a lap aljára, hogy minden betöltsön
        for _ in range(3):
            driver.execute_script("window.scrollBy(0, 800);")
            time.sleep(1)

        print("🔍 Állásajánlatok keresése az oldalon...")

        # DOM-walker JavaScript a linkek kinyeréséhez (ugyanaz a logika, ami az Omni-Catcherben bevált)
        jobs_on_page = driver.execute_script("""
            let results = [];
            let processedPaths = new Set();
            
            // Keresünk linkeket vagy Angular ng-click eseményeket
            let elements = document.querySelectorAll('a[href*="/JobAdvertisement/"], [ng-click*="/JobAdvertisement/"], div[data-position-url*="/JobAdvertisement/"]');
            
            elements.forEach(el => {
                let urlPath = "";
                
                if (el.hasAttribute('data-position-url')) {
                    urlPath = el.getAttribute('data-position-url');
                } else if (el.hasAttribute('href')) {
                    urlPath = el.getAttribute('href');
                } else if (el.hasAttribute('ng-click')) {
                    let match = el.getAttribute('ng-click').match(/,\\s*["']([^"']+)["']/);
                    if (match && match[1]) urlPath = match[1];
                }
                
                if (urlPath && !processedPaths.has(urlPath)) {
                    processedPaths.add(urlPath);
                    
                    // Alapvető cím kinyerése a listából (a pontosabbat a 2. fázisban vesszük)
                    let title = (el.innerText || "").split('\\n')[0].trim();
                    if (!title || ['részletek', 'jelentkezem'].includes(title.toLowerCase())) {
                        title = "Wienerberger Pozíció"; 
                    }
                    
                    results.push({
                        title: title,
                        url_path: urlPath
                    });
                }
            });
            return results;
        """)

        if not jobs_on_page:
            print(
                f"⚠️ Nem találtam egyetlen állást sem a(z) {COMPANY_NAME} oldalán.")
            return

        print(
            f"✅ {len(jobs_on_page)} db állás linket találtam. Részletek kinyerése...")

        # --- 2. FÁZIS: Állások részleteinek kinyerése ---
        for idx, job in enumerate(jobs_on_page, 1):
            full_job_url = urljoin(DOMAIN_URL, job['url_path'])

            # Ellenőrizzük, hogy benne van-e már az adatbázisban
            cursor.execute("SELECT id FROM jobs WHERE url = ?",
                           (full_job_url,))
            if cursor.fetchone():
                print(
                    f"   [{idx}/{len(jobs_on_page)}] ⏩ Már létezik: {job['title']}")
                continue

            print(
                f"   [{idx}/{len(jobs_on_page)}] ⬇️ Feldolgozás: {job['title']}")
            driver.get(full_job_url)

            try:
                # Várunk, amíg a fr-view class vagy a body tartalom betölt
                WebDriverWait(driver, 8).until(
                    lambda d: len(d.find_element(
                        By.TAG_NAME, "body").text) > 200
                )
            except:
                pass  # Timeout esetén is megpróbáljuk kinyerni

            # JS a tökéletes cím, lokáció és a "heaviest container" leírás kinyeréséhez
            page_data = driver.execute_script("""
                // Pontos lokáció kinyerése az e2e tag alapján
                let locElement = document.querySelector('[data-e2e-testing="Recruitment.Registration.Position.LocationOfWork"]');
                let exactLoc = locElement ? locElement.innerText.trim() : "Unknown";
                
                // Pontos cím kinyerése a részletes oldalról az e2e tag alapján
                let titleElement = document.querySelector('[data-e2e-testing="Recruitment.Registration.Position.PositionHeader"]');
                let exactTitle = titleElement ? titleElement.innerText.trim() : "";
                
                function walk(el) {
                    let text = "";
                    if (!el) return "";
                    
                    if (el.nodeType === 3) {
                        let val = el.nodeValue.replace(/\\s+/g, ' '); 
                        if (val !== ' ') text += val;
                    } else if (el.nodeType === 1) {
                        let tag = el.tagName.toUpperCase();
                        let cls = (el.className && typeof el.className === 'string') ? el.className.toLowerCase() : "";
                        
                        if (['SCRIPT','STYLE','NAV','FOOTER','HEADER','BUTTON','SVG'].includes(tag)) return "";
                        if (cls.includes('share') || cls.includes('apply')) return "";
                        if (tag === 'LI') text += "- ";
                        
                        for (let child of el.childNodes) text += walk(child);
                        
                        if (['H1','H2','H3','H4'].includes(tag)) text += "\\n\\n";
                        else if (['P','DIV','BR'].includes(tag)) text += "\\n";
                        else if (tag === 'LI') text += "\\n";
                    }
                    return text;
                }

                let bestText = '';
                // A Wienerberger specifikus content class keresése
                let mainContainer = document.querySelector('.jobAdvertisement__content, .fr-view, [data-e2e-testing="Recruitment.Registration.Position.JobAdContent"]');
                
                if (mainContainer) {
                    bestText = walk(mainContainer).trim();
                } else {
                    let allDivs = document.querySelectorAll('div');
                    for (let c of allDivs) {
                        let currentText = walk(c).trim();
                        if (currentText.length > bestText.length && currentText.length < 8000) {
                            bestText = currentText;
                        }
                    }
                }
                
                return { 
                    location: exactLoc, 
                    description: bestText || 'Description could not be loaded.',
                    exact_title: exactTitle
                };
            """)

            # Tisztítás
            clean_desc = re.sub(r' +', ' ', page_data['description'])
            clean_desc = re.sub(r'\n[ \t]+\n', '\n\n', clean_desc)
            clean_desc = re.sub(r'\n{3,}', '\n\n', clean_desc).strip()
            clean_desc = clean_desc.replace('\xa0', ' ')

            city = _clean_city(page_data['location'])

            # Ha megtalálta az e2e tag alapján a pontos címet, azt használjuk
            final_title = page_data.get('exact_title') or job['title']

            try:
                cursor.execute('''
                    INSERT OR IGNORE INTO jobs (url, title, company, location_raw, city, country, description)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (full_job_url, final_title, COMPANY_NAME, page_data['location'], city, "Hungary", clean_desc))
                conn.commit()
            except Exception as e:
                print(f"      ⚠️ Adatbázis mentési hiba: {e}")

    except Exception as e:
        print(
            f"❌ Végzetes hiba történt a(z) {COMPANY_NAME} scraper futása közben: {e}")

    finally:
        driver.quit()
        conn.close()
        print(f"\n🏁 {COMPANY_NAME} scraper befejezte a futást.")


if __name__ == "__main__":
    run_scraper()
