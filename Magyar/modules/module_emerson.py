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

sys.stdout.reconfigure(encoding='utf-8')

# --- KONFIGURÁCIÓ ---
COMPANY_NAME = "Emerson"
# Az általad küldött URL, ami már szűrve van Magyarországra és Gyakornoki/Pályakezdő pozíciókra
BASE_URL = "https://hdjq.fa.us2.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1/jobs?lastSelectedFacet=AttributeChar2&location=Hungary&selectedFlexFieldsFacets=%22AttributeChar2%7CEntry-level%3BStudent+Internships%2FCo-ops%22"
DATA_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
DB_PATH = os.path.join(DATA_FOLDER, "emerson_jobs.db")


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
    print(f"🚀 {COMPANY_NAME} Scraper indítása (Oracle Cloud HCM mód)...")

    # Verzió kényszerítés a stabilitásért
    driver = get_chrome_driver()

    job_links = []
    unique_urls = set()

    try:
        # --- 1. FÁZIS: LINKEK GYŰJTÉSE ---
        print("📂 Oracle Cloud keresőoldal megnyitása...")
        driver.get(BASE_URL)

        # Az Oracle HCM lassan inicializálja a JS keretrendszert
        time.sleep(8)

        # Sütik elfogadása (Oracle alapértelmezett cookie banner kezelése)
        try:
            cookie_js = """
                let btns = Array.from(document.querySelectorAll('button'));
                let acceptBtn = btns.find(b => b.innerText.toLowerCase().includes('accept') || b.innerText.toLowerCase().includes('elfogad'));
                if (acceptBtn) acceptBtn.click();
            """
            driver.execute_script(cookie_js)
            time.sleep(1)
        except:
            pass

        print("📄 Linkek begyűjtése az oldalról...")

        # Várunk az álláskártyákra
        try:
            WebDriverWait(driver, 15).until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, ".job-list-item__link")))
        except:
            print("⚠️ Nem találtam kártyákat, lehet, hogy üres a lista. Ellenőrzés...")

        # Mivel az Oracle "Load More" (Mutass többet) gombot használhat,
        # ha kevés az állás (gyakornoki szűrésnél), általában egy oldalon elfér.
        jobs_on_page = driver.execute_script("""
            let results = [];
            let links = document.querySelectorAll('.job-list-item__link');
            
            links.forEach(l => {
                let href = l.href;
                // Az Oracle kártyák belsejében gyakran van egy span a címmel, 
                // vagy maga a link tartalmazza.
                let title = l.innerText.trim();
                
                if (href && href.includes('/job/')) {
                    results.push({ url: href, title: title });
                }
            });
            return results;
        """)

        for job in jobs_on_page:
            if job['url'] not in unique_urls:
                unique_urls.add(job['url'])
                job_links.append(job)

        print(f"✅ {len(job_links)} egyedi állás azonosítva. Kezdődik a mélyfúrás...")

        if not job_links:
            print("⚠️ A lista üres. (Vagy nincs nyitott pozíció, vagy a Selenium nem töltötte be az Oracle modult).")
            return

        # --- 2. FÁZIS: LEÍRÁSOK ÉS CÍM SCRAPELÉSE ---
        conn = sqlite3.connect(DB_PATH)
        wait = WebDriverWait(driver, 15)

        for idx, job in enumerate(job_links, 1):
            try:
                conn.execute("DELETE FROM jobs WHERE url = ?", (job['url'],))

                # Tisztítjuk a címet, ha esetleg egy azonosító ragadt volna bele
                display_title = job['title'] if len(
                    job['title']) > 3 else "Emerson Pozíció"
                print(f"   [{idx}/{len(job_links)}] {display_title}")

                driver.get(job['url'])

                # Várakozás a leírás betöltődésére
                try:
                    wait.until(EC.presence_of_element_located(
                        (By.CSS_SELECTOR, ".job-details__description-content")))
                    time.sleep(1)
                except:
                    time.sleep(3)

                details = driver.execute_script("""
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

                            if (tag === 'LI') text += "• ";
                            for (let child of el.childNodes) text += walk(child); 
                            
                            // Ha H1-H4, dupla sortörés
                            if (['H1','H2','H3','H4'].includes(tag)) text += "\\n\\n";
                            // Az Oracle sokszor <p><strong>Címsor</strong></p> kombinációt használ
                            else if (['B','STRONG'].includes(tag) && text.length > 10) text += "\\n\\n";
                            else if (['P','DIV','BR','LI'].includes(tag)) text += "\\n";
                        }
                        return text;
                    }

                    let mainContent = document.querySelector('.job-details__description-content');
                    let descText = mainContent ? walk(mainContent) : walk(document.body);
                    
                    // Az Oracle HCM-ben a cím (pl. Junior Mérnök) általában egy H1-ben van az oldal tetején
                    let realTitleEl = document.querySelector('h1');
                    let realTitle = realTitleEl ? realTitleEl.innerText.trim() : "";
                    
                    // A lokáció lekérése (általában egy ikon melletti span-ben van, .job-info-location vagy hasonló)
                    let locEls = Array.from(document.querySelectorAll('span, div')).filter(el => el.innerText.includes('Hungary'));
                    let locText = locEls.length > 0 ? locEls[locEls.length - 1].innerText.trim() : 'Magyarország';
                    
                    return { description: descText, title: realTitle, location: locText };
                """)

                # Tisztítás
                clean_desc = re.sub(r'\n{3,}', '\n\n',
                                    details['description']).strip()
                final_title = details['title'] if details['title'] else display_title
                location_raw = details['location']

                # Ha a lokációban benne van a város (pl. "Székesfehérvár, Hungary")
                city = "Székesfehérvár" if "fehérvár" in location_raw.lower() else "Magyarország"
                if "," in location_raw:
                    city = location_raw.split(",")[0].strip()

                conn.execute('''INSERT INTO jobs (url, title, company, location_raw, city, country, description, category) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                             (job['url'], final_title, COMPANY_NAME, location_raw, city, "Magyarország", clean_desc, "Gyakornok / Pályakezdő"))
                conn.commit()

            except Exception as e:
                print(f"      ⚠️ Hiba a(z) {idx}. állásnál: {e}")

        conn.close()
        print(f"\n✨ SIKER! Az Oracle HCM (Emerson) pozíciók mentve.")

    finally:
        try:
            driver.quit()
        except:
            pass


if __name__ == "__main__":
    run_scraper()
