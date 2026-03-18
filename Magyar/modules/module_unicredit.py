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
COMPANY_NAME = "UniCredit_Bank"
BASE_URL = "https://careers.unicredit.eu/hu_HU/jobsuche/SearchJobs/?1286=%5B1888%5D&1286_format=1068&&listFilterMode=1&jobRecordsPerPage=15&"
DATA_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
DB_PATH = os.path.join(DATA_FOLDER, "unicredit_jobs.db")


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


def run_scraper():
    init_db()
    print(f"🚀 {COMPANY_NAME} Scraper indítása (Smart Link Filter mód)...")

    driver = get_chrome_driver()
    wait = WebDriverWait(driver, 10)

    job_links = []
    unique_urls = set()

    try:
        # --- 1. FÁZIS: LINKEK ÉS PONTOS CÍMEK GYŰJTÉSE ---
        print("📂 UniCredit karrieroldal megnyitása...")
        driver.get(BASE_URL)
        time.sleep(5)

        try:
            cookie_btn = driver.find_element(
                By.XPATH, "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'elfogad')] | //button[contains(@class, 'cookie')]")
            driver.execute_script("arguments[0].click();", cookie_btn)
            time.sleep(1)
        except:
            pass

        current_page = 1
        while True:

            print(f"📄 {current_page}. oldal adatainak begyűjtése...")
            time.sleep(2)

            # A beküldött HTML alapján a TÖKÉLETES cím és link kinyerő
            jobs_on_page = driver.execute_script("""
                let results = [];
                let allLinks = document.querySelectorAll('a');
                
                allLinks.forEach(aTag => {
                    if (aTag.href && aTag.href.includes('/JobDetail/')) {
                        let title = aTag.innerText.trim();
                        
                        // Csak azokat tartjuk meg, amik tényleg címek (nem a Részletek/Jelentkezés gombok)
                        if (title && title !== 'Részletek' && title !== 'Details' && title !== 'Jelentkezés' && title !== 'Megosztás') {
                            results.push({
                                url: aTag.href,
                                title: title
                            });
                        }
                    }
                });
                
                return results;
            """)

            for job in jobs_on_page:
                if job['url'] not in unique_urls:
                    unique_urls.add(job['url'])
                    job_links.append(job)

            # LAPOZÁS
            try:
                next_btn = driver.find_element(
                    By.CSS_SELECTOR, "a.paginationNextLink")
                next_url = next_btn.get_attribute("href")

                if next_url:
                    print(f"🔄 Lapozás a következő oldalra...")
                    driver.get(next_url)
                    current_page += 1
                    time.sleep(3.5)
                else:
                    break
            except:
                print("🏁 Nincs több lapozógomb, lista vége.")
                break

        print(f"✅ {len(job_links)} egyedi állás azonosítva. Kezdődik a mélyfúrás...")

        # --- 2. FÁZIS: LEÍRÁSOK ÉS MEZŐK SCRAPELÉSE ---
        conn = sqlite3.connect(DB_PATH)
        for idx, job in enumerate(job_links, 1):
            try:
                conn.execute("DELETE FROM jobs WHERE url = ?", (job['url'],))

                # Itt már a gyönyörű, tiszta címet fogja kiírni!
                print(f"   [{idx}/{len(job_links)}] {job['title']}")
                driver.get(job['url'])

                try:
                    wait.until(EC.presence_of_element_located(
                        (By.CSS_SELECTOR, ".section__content")))
                    time.sleep(1.5)
                except:
                    time.sleep(3)

                # NATIVE INNERTEXT ÉS MEZŐ BÁNYÁSZAT
                data = driver.execute_script("""
                    let result = {
                        city: "Budapest",
                        country: "Magyarország",
                        category: "Pénzügy / Bank",
                        desc: ""
                    };

                    let fields = document.querySelectorAll('.article__content__view__field');
                    fields.forEach(f => {
                        let labelEl = f.querySelector('.article__content__view__field__label');
                        let valEl = f.querySelector('.article__content__view__field__value');
                        if (labelEl && valEl) {
                            let lbl = labelEl.innerText.trim().toLowerCase();
                            let val = valEl.innerText.trim();
                            
                            if (lbl.includes('város')) result.city = val;
                            if (lbl.includes('ország')) result.country = val;
                            if (lbl.includes('munkaterület')) result.category = val;
                        }
                    });

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
                            
                            for (let child of el.childNodes) { 
                                text += walk(child); 
                            }
                            
                            if (['H1','H2','H3','H4'].includes(tag)) text += "\\n\\n";
                            else if (['P','DIV','BR','LI'].includes(tag)) text += "\\n";
                        }
                        return text;
                    }

                    let mainContent = document.querySelector('.section__content') || document.body;
                    result.desc = walk(mainContent);
                    
                    return result;
                """)

                clean_desc = re.sub(r'\n{3,}', '\n\n', data['desc']).strip()

                if "Jelentkezem" in clean_desc:
                    clean_desc = clean_desc.split("Jelentkezem")[0].strip()

                location_raw = data['city']
                country = "Magyarország" if data['country'].lower(
                ) in ["hungary", "magyarország"] else data['country']

                conn.execute('''INSERT INTO jobs (url, title, company, location_raw, city, country, description, category) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                             (job['url'], job['title'], "UniCredit Bank", location_raw, data['city'], country, clean_desc, data['category']))
                conn.commit()

            except Exception as e:
                print(f"      ⚠️ Hiba a(z) {idx}. állásnál: {e}")

        conn.close()
        print(f"\n✨ SIKER! Az UniCredit Bank pozíciók tökéletesítve mentve.")

    finally:
        driver.quit()


if __name__ == "__main__":
    run_scraper()
