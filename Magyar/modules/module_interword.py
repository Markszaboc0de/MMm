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
COMPANY_NAME = "Interword"
BASE_URL = "https://www.interword.hu/hu/karrier"
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "interword_jobs.db")


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
    print(f"🚀 {COMPANY_NAME} Scraper indítása (Targeted .show-description mód)...")

    driver = get_chrome_driver()
    wait = WebDriverWait(driver, 10)

    job_links = []
    unique_urls = set()

    try:
        # --- 1. FÁZIS: LINKEK GYŰJTÉSE ---
        print("📂 Interword karrieroldal megnyitása...")
        driver.get(BASE_URL)
        time.sleep(4)

        # Cookie ablak bezárása
        try:
            cookie_btn = driver.find_element(
                By.XPATH, "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'elfogad')] | //a[contains(text(), 'Elfogadom')]")
            driver.execute_script("arguments[0].click();", cookie_btn)
            time.sleep(1)
        except:
            pass

        print("🔍 Álláskártyák beolvasása...")

        jobs_on_page = driver.execute_script("""
            let results = [];
            let cards = document.querySelectorAll('.holder-box');
            
            cards.forEach(card => {
                let linkEl = card.querySelector('a.txt-box');
                
                if (linkEl && linkEl.href) {
                    let titleEl = card.querySelector('.name');
                    results.push({
                        url: linkEl.href,
                        title: titleEl ? titleEl.innerText.trim() : 'Ismeretlen pozíció'
                    });
                }
            });
            return results;
        """)

        for job in jobs_on_page:
            if job['url'] not in unique_urls:
                unique_urls.add(job['url'])
                job_links.append(job)

        print(f"✅ {len(job_links)} állás azonosítva. Kezdődik a mélyfúrás...")

        # --- 2. FÁZIS: LEÍRÁSOK SCRAPELÉSE ---
        conn = sqlite3.connect(DB_PATH)
        for idx, job in enumerate(job_links, 1):
            try:
                conn.execute("DELETE FROM jobs WHERE url = ?", (job['url'],))

                print(f"   [{idx}/{len(job_links)}] {job['title']}")
                driver.get(job['url'])

                try:
                    # Megvárjuk az egyedi osztályokat
                    wait.until(EC.presence_of_element_located(
                        (By.CSS_SELECTOR, ".show-description, .show-intro")))
                    time.sleep(1.5)
                except:
                    time.sleep(3)

                # NATIVE INNERTEXT A PONTOS DOBOZOKBÓL
                description = driver.execute_script("""
                    let intro = document.querySelector('.show-intro');
                    let desc = document.querySelector('.show-description');
                    
                    let text = "";
                    
                    // Ha megvannak a specifikus dobozok, csak azokat olvassuk be!
                    if (intro || desc) {
                        if (intro) text += intro.innerText.trim() + "\\n\\n";
                        if (desc) text += desc.innerText.trim();
                    } else {
                        // Tartalék, ha nagyon máshogy épülne fel az oldal
                        let mainContent = document.querySelector('article') || 
                                          document.querySelector('.content') || 
                                          document.querySelector('main') || 
                                          document.querySelector('.container');
                        if (mainContent) {
                            text = mainContent.innerText;
                        } else {
                            text = document.body.innerText;
                        }
                    }
                    
                    return text;
                """)

                # Felesleges tripla sortörések eltávolítása
                clean_desc = re.sub(r'\n\s*\n', '\n\n', description).strip()

                # Interword alapértelmezett adatok (Ahogy a hirdetés is írja: Munkavégzés helye: Veszprém)
                location_raw = "Veszprém"
                city = "Veszprém"
                country = "Magyarország"
                category = "Marketing / Média / IT"

                conn.execute('''INSERT INTO jobs (url, title, company, location_raw, city, country, description, category) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                             (job['url'], job['title'], "Interword", location_raw, city, country, clean_desc, category))
                conn.commit()

            except Exception as e:
                print(f"      ⚠️ Hiba a(z) {idx}. állásnál: {e}")

        conn.close()
        print(f"\n✨ SIKER! Az Interword pozíciók (tökéletesített formázással) elmentve.")

    finally:
        driver.quit()


if __name__ == "__main__":
    run_scraper()
