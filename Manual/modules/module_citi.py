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
COMPANY_NAME = "Citi"
BASE_URL = "https://jobs.citi.com/search-jobs"
DOMAIN_URL = "https://jobs.citi.com"
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Manual\data"
DB_PATH = os.path.join(DATA_FOLDER, "citi_jobs.db")


def init_db():
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)
    conn = sqlite3.connect(DB_PATH)

    conn.execute('''CREATE TABLE IF NOT EXISTS jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT, url TEXT UNIQUE, title TEXT, 
        company TEXT, location_raw TEXT, city TEXT, country TEXT, description TEXT, category TEXT,
        date_found TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')

    conn.commit()
    conn.close()


def run_scraper():
    init_db()
    print(f"🚀 {COMPANY_NAME} Scraper indítása (Precíz Lokáció Mód)...")

    options = Options()
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    driver = webdriver.Chrome(options=options)

    job_links = []
    unique_urls = set()

    try:
        # --- 1. FÁZIS: LINKEK GYŰJTÉSE ---
        print("📂 Citi karrieroldal megnyitása...")
        driver.get(BASE_URL)
        time.sleep(5)

        # Cookie ablak bezárása (ha van)
        try:
            cookie_btn = driver.find_element(
                By.XPATH, "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'accept') or contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'elfogad') or contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'ok')]")
            driver.execute_script("arguments[0].click();", cookie_btn)
            time.sleep(1)
        except:
            pass

        page_num = 1

        while True:

            print(f"📄 {page_num}. oldal adatainak begyűjtése...")

            try:
                WebDriverWait(driver, 10).until(EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "a.sr-job-item__link")))
            except:
                print("⚠️ Nem találtam kártyákat, lehet, hogy üres a lista.")
                break

            jobs_on_page = driver.execute_script("""
                let results = [];
                let links = document.querySelectorAll('a.sr-job-item__link');
                
                links.forEach(a => {
                    if (a.href) {
                        let loc = "N/A";
                        let parent = a.closest('li, .job-item, .sr-job-item');
                        if (parent) {
                            let locEl = parent.querySelector('.job-location, .location');
                            if (locEl) loc = locEl.innerText.trim();
                        }
                        
                        results.push({
                            url: a.href,
                            title: a.innerText.trim(),
                            location_raw: loc,
                            category: 'Pénzügy / Bank'
                        });
                    }
                });
                return results;
            """)

            if not jobs_on_page:
                break

            for job in jobs_on_page:
                if job['url'] not in unique_urls:
                    unique_urls.add(job['url'])
                    job_links.append(job)

            # LAPOZÁS: Következő URL kinyerése a Next gombból
            try:
                next_button = driver.find_element(By.CSS_SELECTOR, "a.next")
                if "disabled" in next_button.get_attribute("class"):
                    print("🏁 Nincs több oldal, lista vége.")
                    break

                next_url = next_button.get_attribute("href")
                if next_url:
                    driver.get(next_url)
                    page_num += 1
                    time.sleep(3)
                else:
                    break
            except:
                print("🏁 Nincs következő gomb, lista vége.")
                break

        print(f"✅ {len(job_links)} egyedi állás azonosítva. Kezdődik a mélyfúrás...")

        if not job_links:
            return

        # --- 2. FÁZIS: LEÍRÁSOK ÉS VÁROSOK SCRAPELÉSE ---
        conn = sqlite3.connect(DB_PATH)
        wait = WebDriverWait(driver, 10)

        for idx, job in enumerate(job_links, 1):
            try:
                conn.execute("DELETE FROM jobs WHERE url = ?", (job['url'],))

                print(f"   [{idx}/{len(job_links)}] {job['title']}")
                driver.get(job['url'])

                try:
                    wait.until(EC.presence_of_element_located(
                        (By.CSS_SELECTOR, ".ats-description")))
                    time.sleep(1)
                except:
                    time.sleep(2)

                # DOM Walker bejáró + PRECIZIÓS LOKÁCIÓ KERESŐ
                details = driver.execute_script("""
                    // 1. Lokáció kinyerése a specifikus .job-location konténerből
                    let exactLoc = "";
                    let locContainer = document.querySelector('.job-location .job-description__desc-detail');
                    if (locContainer) {
                        exactLoc = locContainer.innerText.trim();
                    }

                    // 2. DOM Walker a leíráshoz
                    function walk(el) {
                        let text = "";
                        if (!el) return "";
                        if (el.nodeType === 3) {
                            let val = el.nodeValue.trim();
                            if (val) text += val + " ";
                        } else if (el.nodeType === 1) {
                            let tag = el.tagName.toUpperCase();
                            let cls = (el.className && typeof el.className === 'string') ? el.className.toLowerCase() : "";

                            if (['SCRIPT','STYLE','NAV','FOOTER','HEADER','BUTTON','SVG'].includes(tag)) return "";
                            if (cls.includes('share') || cls.includes('apply') || cls.includes('back')) return "";
                            
                            if (cls.includes('jd-evergreen')) return "";

                            if (tag === 'LI') text += "• ";
                            for (let child of el.childNodes) text += walk(child); 
                            
                            if (['H1','H2','H3','H4'].includes(tag)) text += "\\n\\n";
                            else if (['B','STRONG'].includes(tag) && text.length > 10) text += "\\n\\n";
                            else if (['P','DIV','BR','LI'].includes(tag)) text += "\\n";
                        }
                        return text;
                    }

                    let mainContent = document.querySelector('.ats-description');
                    let descText = mainContent ? walk(mainContent) : walk(document.body);
                    
                    return { description: descText, location: exactLoc };
                """)

                clean_desc = re.sub(r'\n{3,}', '\n\n',
                                    details['description']).strip()

                truncation_markers = [
                    "------------------------------------------------------",
                    "Job Family Group:",
                    "Job Family:",
                    "Automated Processing and AI",
                    "By joining Citi"
                ]
                for marker in truncation_markers:
                    if marker in clean_desc:
                        clean_desc = clean_desc.split(marker)[0].strip()

                # --- LOKÁCIÓ, VÁROS ÉS ORSZÁG PRECIZIÓS FELDOLGOZÁSA ---
                location_raw = details['location'] if details['location'] else job.get(
                    'location_raw', 'N/A')
                city = "N/A"
                country = "N/A"

                if location_raw and location_raw != 'N/A':
                    parts = [p.strip() for p in location_raw.split(',')]
                    city = parts[0]  # Város (pl. "Kuala Lumpur" vagy "Dublin")
                    # Ország (pl. "Malaysia" vagy "Ireland")
                    country = parts[-1]
                else:
                    match = re.search(r'/job/([^/]+)/', job['url'])
                    if match:
                        city = match.group(1).replace('-', ' ').title()
                        location_raw = city
                        country = "N/A"

                conn.execute('''INSERT INTO jobs (url, title, company, location_raw, city, country, description, category) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                             (job['url'], job['title'], COMPANY_NAME, location_raw, city, country, clean_desc, job['category']))
                conn.commit()

            except Exception as e:
                print(f"      ⚠️ Hiba a(z) {idx}. állásnál: {e}")

        conn.close()
        print(
            f"\n✨ SIKER! A {COMPANY_NAME} pozíciók mentve, a precíziós ország beolvasással együtt.")

    finally:
        try:
            driver.quit()
        except:
            pass


if __name__ == "__main__":
    run_scraper()
