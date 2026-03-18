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
COMPANY_NAME = "MNB"
BASE_URL = "https://toborzas.mnb.hu/allasok"
DATA_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
DB_PATH = os.path.join(DATA_FOLDER, "mnb_jobs.db")


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
        conn.execute("ALTER TABLE jobs ADD COLUMN category TEXT")
    except:
        pass

    conn.commit()
    conn.close()


def run_scraper():
    init_db()
    print(f"🚀 {COMPANY_NAME} Scraper indítása (Nexum Páncélos Linkvadász mód)...")

    driver = get_chrome_driver()

    job_links = []
    unique_urls = set()

    try:
        # --- 1. FÁZIS: LINKEK GYŰJTÉSE ---
        print("📂 MNB karrieroldal megnyitása...")
        driver.get(BASE_URL)
        time.sleep(5)

        # Cookie ablak bezárása (ha van)
        try:
            cookie_btn = driver.find_element(
                By.XPATH, "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'elfogad')] | //button[contains(@class, 'cookie')]")
            driver.execute_script("arguments[0].click();", cookie_btn)
            time.sleep(1)
        except:
            pass

        print("⏳ Állások betöltése (görgetés a lap aljára)...")
        last_count = 0
        retries = 0
        scroll_cycles = 0

        # Végtelen görgetés a betöltéshez (ha lazy load lenne az oldalon)
        while True:
            if scroll_cycles > 10:
                break

            count = driver.execute_script(
                "return document.querySelectorAll('a[href*=\"/allas/\"]').length;")
            print(f"   📥 Jelenleg betöltve: {count} álláslink...")

            driver.execute_script(
                "window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)

            new_count = driver.execute_script(
                "return document.querySelectorAll('a[href*=\"/allas/\"]').length;")

            if new_count == count:
                retries += 1
                if retries >= 2:
                    print("🏁 Lista vége, nincs több betölthető állás.")
                    break
            else:
                retries = 0
                last_count = new_count

            scroll_cycles += 1

        print("📄 Adatok kinyerése a linkekből...")

        # Páncélos JavaScript extraktor a Részletek gombokhoz
        jobs_on_page = driver.execute_script("""
            let results = [];
            let processedUrls = new Set();
            let links = document.querySelectorAll('a[href*="/allas/"]');

            links.forEach(a => {
                let url = a.href;
                if (url && !processedUrls.has(url) && url.length > 25) {
                    processedUrls.add(url);
                    
                    let title = "MNB Pozíció";
                    let text = a.innerText.trim();
                    
                    // Ha a link a 'Részletek' gomb, kiolvassuk a mellette lévő szöveget a sorból
                    if (text && !text.toLowerCase().includes('részletek')) {
                        title = text;
                    } else {
                        let row = a.closest('div, li, tr');
                        if (row) {
                            let rowText = row.innerText.replace('Részletek', '').trim();
                            if (rowText.length > 3) title = rowText.split('\\n')[0].trim();
                        }
                    }

                    results.push({
                        url: url,
                        title: title,
                        category: 'Bank / Pénzügy'
                    });
                }
            });
            return results;
        """)

        for job in jobs_on_page:
            if job['url'] not in unique_urls and job['title'] != 'MNB Pozíció':
                unique_urls.add(job['url'])
                job_links.append(job)

        print(f"✅ {len(job_links)} egyedi állás azonosítva. Kezdődik a mélyfúrás...")

        if not job_links:
            print("⚠️ Üres lista. Valami blokkolja az oldalt.")
            return

        # --- 2. FÁZIS: LEÍRÁSOK SCRAPELÉSE ---
        conn = sqlite3.connect(DB_PATH)
        wait = WebDriverWait(driver, 10)

        for idx, job in enumerate(job_links, 1):
            try:
                conn.execute("DELETE FROM jobs WHERE url = ?", (job['url'],))

                print(f"   [{idx}/{len(job_links)}] {job['title']}")
                driver.get(job['url'])

                try:
                    # Várunk a fő tartalomra
                    wait.until(EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "#pg_body_col_1, .pg_body_col")))
                    time.sleep(1)
                except:
                    time.sleep(2)

                details = driver.execute_script("""
                    // 1. Város kinyerése a táblázatból
                    let city = "Budapest";
                    let ths = document.querySelectorAll('table.job_description th');
                    ths.forEach(th => {
                        if(th.innerText.includes('Munkavégzés helye')) {
                            let td = th.nextElementSibling;
                            if(td) city = td.innerText.trim();
                        }
                    });

                    // 2. DOM Walker a szöveg kinyeréséhez
                    function walk(el) {
                        let text = "";
                        if (!el) return "";
                        if (el.nodeType === 3) {
                            let val = el.nodeValue.trim();
                            if (val) text += val + " ";
                        } else if (el.nodeType === 1) {
                            let tag = el.tagName.toUpperCase();
                            let id = el.id || "";
                            let cls = (el.className && typeof el.className === 'string') ? el.className.toLowerCase() : "";

                            if (['SCRIPT','STYLE','NAV','FOOTER','HEADER','BUTTON','SVG'].includes(tag)) return "";
                            if (cls.includes('share') || cls.includes('job_submit') || cls.includes('apply')) return "";
                            
                            // MNB specifikus szűrő: Közösségi média ikonok és megosztás gombok kitiltása
                            if (id.includes('addtoany')) return "";

                            if (tag === 'LI') text += "• ";
                            for (let child of el.childNodes) text += walk(child); 
                            
                            if (['H1','H2','H3','H4'].includes(tag)) text += "\\n\\n";
                            else if (['B','STRONG'].includes(tag) && text.length > 10) text += "\\n\\n";
                            else if (['P','DIV','BR','LI','TR'].includes(tag)) text += "\\n";
                        }
                        return text;
                    }

                    let mainContent = document.querySelector('#pg_body_col_1') || document.querySelector('.pg_body_col');
                    let descText = mainContent ? walk(mainContent) : walk(document.body);
                    
                    return { description: descText, city: city };
                """)

                clean_desc = re.sub(r'\n{3,}', '\n\n',
                                    details['description']).strip()
                city = details['city']

                conn.execute('''INSERT INTO jobs (url, title, company, location_raw, city, country, description, category) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                             (job['url'], job['title'], COMPANY_NAME, city, city, "Magyarország", clean_desc, job['category']))
                conn.commit()

            except Exception as e:
                print(f"      ⚠️ Hiba a(z) {idx}. állásnál: {e}")

        conn.close()
        print(f"\n✨ SIKER! Az {COMPANY_NAME} pozíciók mentve.")

    finally:
        try:
            driver.quit()
        except:
            pass


if __name__ == "__main__":
    run_scraper()
