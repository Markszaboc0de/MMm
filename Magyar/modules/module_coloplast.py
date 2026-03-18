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
COMPANY_NAME = "Coloplast"
# Keresési alap URL (A Magyarországra szűrt lista)
BASE_URL = "https://careers.coloplast.com/search/?createNewAlert=false&q=&locationsearch=Hungary"
DATA_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
DB_PATH = os.path.join(DATA_FOLDER, "coloplast_jobs.db")


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
    print(f"🚀 {COMPANY_NAME} Scraper indítása (SAP SuccessFactors mód)...")

    driver = get_chrome_driver()

    job_links = []
    unique_urls = set()

    try:
        # --- 1. FÁZIS: LINKEK GYŰJTÉSE URL-ALAPÚ LAPOZÁSSAL ---
        print("📂 Coloplast karrieroldal megnyitása...")

        driver.get(BASE_URL)
        time.sleep(4)

        # Süti ablak lekezelése
        try:
            cookie_js = """
                let btns = Array.from(document.querySelectorAll('button, a'));
                let acceptBtn = btns.find(b => b.innerText.toLowerCase().includes('elfogad') || b.innerText.toLowerCase().includes('accept'));
                if (acceptBtn) acceptBtn.click();
            """
            driver.execute_script(cookie_js)
            time.sleep(1)
        except:
            pass

        # Az SAP RMK a Coloplastnál alapból 25-ösével lapoz (a képen 22 találat van, így valószínűleg egy oldal lesz, de a logika biztosítja a végtelent)
        startrow = 0
        page_num = 1

        while True:

            print(
                f"📄 {page_num}. oldal adatainak begyűjtése (startrow={startrow})...")

            page_url = f"{BASE_URL}&startrow={startrow}"
            driver.get(page_url)

            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "a.jobTitle-link")))
                time.sleep(2)
            except:
                print("🏁 Nem találtam több álláslinket, lista vége.")
                break

            # Adatkinyerés a táblázat soraiból (tr)
            jobs_on_page = driver.execute_script("""
                let results = [];
                let rows = document.querySelectorAll('tbody tr');
                
                rows.forEach(row => {
                    let titleLink = row.querySelector('a.jobTitle-link');
                    
                    if (titleLink && titleLink.href) {
                        let cells = row.querySelectorAll('td');
                        
                        // Kép alapján: Title (0), Job Family (1), Location (2), Date (3)
                        let cat = cells.length > 1 ? cells[1].innerText.trim() : 'N/A';
                        let loc = cells.length > 2 ? cells[2].innerText.trim() : 'N/A';
                        
                        results.push({
                            url: titleLink.href,
                            title: titleLink.innerText.trim(),
                            location_raw: loc,
                            category: cat
                        });
                    }
                });
                return results;
            """)

            if not jobs_on_page:
                print("⚠️ Nem találtam több állást ezen az oldalon.")
                break

            added_new = False
            for job in jobs_on_page:
                if job['url'] not in unique_urls:
                    unique_urls.add(job['url'])
                    job_links.append(job)
                    added_new = True

            # Ha egyetlen új linket sem találtunk, a végére értünk
            if not added_new:
                print("🏁 Nincs több új állás, lista vége.")
                break

            startrow += 25
            page_num += 1

        print(f"✅ {len(job_links)} egyedi állás azonosítva. Kezdődik a mélyfúrás...")

        if not job_links:
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
                    wait.until(EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "span.jobdescription, .jobdescription")))
                    time.sleep(1)
                except:
                    time.sleep(3)

                # DOM Walker bejáró
                description = driver.execute_script("""
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
                            
                            if (['H1','H2','H3','H4'].includes(tag)) text += "\\n\\n";
                            else if (['B','STRONG'].includes(tag) && text.length > 10) text += "\\n\\n";
                            else if (['P','DIV','BR','LI'].includes(tag)) text += "\\n";
                        }
                        return text;
                    }

                    let mainContent = document.querySelector('span.jobdescription') || document.querySelector('.jobdescription');
                    return mainContent ? walk(mainContent) : walk(document.body);
                """)

                clean_desc = re.sub(r'\n{3,}', '\n\n', description).strip()

                # COLOPLAST specifikus zajszűrés: céges bemutatkozó levágása a végéről
                if "Pursuing an ambitious growth agenda" in clean_desc:
                    clean_desc = clean_desc.split(
                        "Pursuing an ambitious growth agenda")[0].strip()
                elif "Coloplast is committed to being an inclusive organization" in clean_desc:
                    clean_desc = clean_desc.split(
                        "Coloplast is committed")[0].strip()

                # VÁROS KISZŰRÉSE: "Nyírbátor, HU" -> "Nyírbátor"
                location_raw = job['location_raw']
                city = location_raw
                if city != 'N/A' and "," in city:
                    city = city.split(",")[0].strip()

                conn.execute('''INSERT INTO jobs (url, title, company, location_raw, city, country, description, category) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                             (job['url'], job['title'], COMPANY_NAME, location_raw, city, "Magyarország", clean_desc, job['category']))
                conn.commit()

            except Exception as e:
                print(f"      ⚠️ Hiba a(z) {idx}. állásnál: {e}")

        conn.close()
        print(f"\n✨ SIKER! A {COMPANY_NAME} pozíciók mentve.")

    finally:
        try:
            driver.quit()
        except:
            pass


if __name__ == "__main__":
    run_scraper()
