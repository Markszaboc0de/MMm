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
COMPANY_NAME = "Duna Group"
BASE_URL = "https://www.duna.group/aktualis-allasajanlatok/"
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "duna_jobs.db")


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
    print(f"🚀 {COMPANY_NAME} Scraper indítása (Precíz fókusz mód)...")

    driver = get_chrome_driver()

    job_links = []
    unique_urls = set()

    try:
        # --- 1. FÁZIS: LINKEK GYŰJTÉSE ---
        print("📂 Duna Group karrieroldal megnyitása...")
        driver.get(BASE_URL)
        time.sleep(5)

        try:
            cookie_btn = driver.find_element(
                By.XPATH, "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'elfogad')] | //button[contains(@class, 'cookie')]")
            driver.execute_script("arguments[0].click();", cookie_btn)
            time.sleep(1)
        except:
            pass

        print("📄 'Részletek' gombok és linkek begyűjtése...")

        jobs_on_page = driver.execute_script("""
            let results = [];
            let buttons = document.querySelectorAll('a.ct-link');
            
            buttons.forEach(btn => {
                let text = btn.innerText.trim().toLowerCase();
                let href = btn.href;
                
                if (href && text.includes('részletek')) {
                    let category = "Építőipar / Műszaki";
                    let accordion = btn.closest('.oxy-pro-accordion');
                    if (accordion) {
                        let titleEl = accordion.querySelector('.oxy-pro-accordion_title');
                        if (titleEl) category = titleEl.innerText.trim();
                    }
                    results.push({ url: href, category: category });
                }
            });
            return results;
        """)

        for job in jobs_on_page:
            if job['url'] not in unique_urls:
                unique_urls.add(job['url'])
                job_links.append(job)

        print(
            f"✅ {len(job_links)} egyedi álláslink azonosítva. Kezdődik a mélyfúrás...")

        if not job_links:
            return

        # --- 2. FÁZIS: AZ ÚJ, VALÓDI ALOLDALAK SCRAPELÉSE ---
        conn = sqlite3.connect(DB_PATH)
        wait = WebDriverWait(driver, 10)

        for idx, job in enumerate(job_links, 1):
            try:
                conn.execute("DELETE FROM jobs WHERE url = ?", (job['url'],))
                driver.get(job['url'])

                try:
                    wait.until(EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "h1.ct-headline")))
                    time.sleep(1)
                except:
                    time.sleep(3)

                # JS kinyerő logika fókuszált konténer-kereséssel
                details = driver.execute_script("""
                    // 1. Cím
                    let titleEl = document.querySelector('h1.ct-headline');
                    let title = titleEl ? titleEl.innerText.trim() : "Duna Group Pozíció";

                    // 2. Lokáció és Szint
                    let loc = "N/A";
                    let level = "N/A";
                    
                    let blocks = document.querySelectorAll('.ct-text-block');
                    blocks.forEach(block => {
                        let text = block.innerText.trim().toLowerCase();
                        if (text.includes('munkavégzés helyszíne')) {
                            let nextBlock = block.nextElementSibling;
                            if (nextBlock) loc = nextBlock.innerText.trim();
                        }
                        if (text.includes('karrierszint')) {
                            let nextBlock = block.nextElementSibling;
                            if (nextBlock) level = nextBlock.innerText.trim();
                        }
                    });

                    // 3. Fő konténer megkeresése (hogy ne szedjük le a fejlécet/láblécet)
                    let mainContainer = titleEl;
                    if (mainContainer) {
                        // Felfelé lépkedünk a DOM-ban, amíg meg nem találjuk azt a dobozt, 
                        // ami már tartalmazza a H4-es alcímeket is.
                        while (mainContainer && mainContainer.tagName !== 'BODY') {
                            if (mainContainer.querySelectorAll('h4.ct-headline').length > 0) {
                                break;
                            }
                            mainContainer = mainContainer.parentElement;
                        }
                    }

                    // 4. Leírás (Jelentkezem gombok kihagyásával)
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
                            // Kihagyjuk a Jelentkezem gombokat a Duna-Gomb-Felirat alapján
                            if (cls.includes('duna-gomb-felirat') || cls.includes('oxel_icon_button')) return "";

                            if (tag === 'LI') text += "• ";
                            for (let child of el.childNodes) text += walk(child); 
                            
                            if (['H1','H2','H3','H4'].includes(tag)) text += "\\n\\n";
                            else if (['B','STRONG'].includes(tag) && text.length > 10) text += "\\n\\n";
                            else if (['P','DIV','BR','LI'].includes(tag)) text += "\\n";
                        }
                        return text;
                    }
                    
                    // Csak a célzott konténert járjuk be!
                    let descText = mainContainer ? walk(mainContainer) : walk(document.body);
                    
                    return { title: title, location: loc, level: level, description: descText };
                """)

                print(
                    f"   [{idx}/{len(job_links)}] {details['title']} ({details['location']})")

                # Tisztítás
                clean_desc = re.sub(r'\n{3,}', '\n\n',
                                    details['description']).strip()
                if "Jelentkezem!" in clean_desc:
                    clean_desc = clean_desc.split("Jelentkezem!")[0].strip()

                location_raw = details['location']
                city = location_raw

                # Helyszín finomhangolása ("Országos" és "Központtal" kezelése)
                if "KÖZPONTTAL" in city.upper():
                    try:
                        city = city.split(
                            ",")[-1].replace("központtal", "").replace("KÖZPONTTAL", "").strip()
                    except:
                        pass
                elif "ORSZÁGOS" in city.upper():
                    city = "Országos"

                final_category = f"{job['category']} - {details['level']}" if details['level'] != "N/A" else job['category']

                conn.execute('''INSERT INTO jobs (url, title, company, location_raw, city, country, description, category) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                             (job['url'], details['title'], COMPANY_NAME, location_raw, city, "Magyarország", clean_desc, final_category))
                conn.commit()

            except Exception as e:
                print(f"      ⚠️ Hiba a(z) {idx}. állásnál: {e}")

        conn.close()
        print(
            f"\n✨ SIKER! A {COMPANY_NAME} pozíciók mentve az adatbázisba (felesleges szövegek nélkül).")

    finally:
        try:
            driver.quit()
        except:
            pass


if __name__ == "__main__":
    run_scraper()
