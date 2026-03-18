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
COMPANY_NAME = "MVM"
# Több URL együttes bejárása
BASE_URLS = [
    "https://mvm.karrierportal.hu/frissdiplomas-program",
    "https://mvm.karrierportal.hu/gyakornoki-program",
    "https://mvm.karrierportal.hu/kotelezo-gyakorlat"
]
DATA_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
DB_PATH = os.path.join(DATA_FOLDER, "mvm_jobs.db")


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
    print(f"🚀 {COMPANY_NAME} Scraper indítása (Multi-URL & Nexum Engine mód)...")

    driver = get_chrome_driver()
    wait = WebDriverWait(driver, 10)

    job_links = []
    unique_urls = set()

    try:
        # --- 1. FÁZIS: LINKEK GYŰJTÉSE MIND A HÁROM OLDALRÓL ---
        for url_idx, current_url in enumerate(BASE_URLS, 1):
            print(
                f"\n📂 [{url_idx}/{len(BASE_URLS)}] Kategória megnyitása: {current_url.split('/')[-1]}")
            driver.get(current_url)
            time.sleep(5)

            # Cookie ablak bezárása (általában csak az első URL-nél ugrik fel)
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

                try:
                    wait.until(EC.presence_of_element_located(
                        (By.CSS_SELECTOR, ".job_item_inner")))
                except:
                    print("⚠️ Nincsenek állások ezen az oldalon.")
                    break

                # Álláskártyák beolvasása a beküldött HTML alapján
                jobs_on_page = driver.execute_script("""
                    let results = [];
                    let cards = document.querySelectorAll('.job_item_inner');
                    
                    cards.forEach(card => {
                        let linkEl = card.querySelector('a.job_list_title');
                        
                        if (linkEl && linkEl.href) {
                            let locEl = card.querySelector('.job_list_place');
                            
                            results.push({
                                url: linkEl.href,
                                title: linkEl.innerText.trim(),
                                location_raw: locEl ? locEl.innerText.trim() : 'Ismeretlen'
                            });
                        }
                    });
                    return results;
                """)

                for job in jobs_on_page:
                    if job['url'] not in unique_urls:
                        unique_urls.add(job['url'])
                        job_links.append(job)

                # LAPOZÁS: Keresünk egy aktív "következő" gombot
                try:
                    next_btn = driver.find_element(
                        By.CSS_SELECTOR, "span.pager-element_next")

                    # A Nexum motornál ha vége a listának, gyakran eltűnik vagy inaktívvá válik a gomb
                    if not next_btn.is_displayed() or "disabled" in next_btn.get_attribute("class"):
                        print("🏁 Nincs több lapozógomb, kategória vége.")
                        break

                    print(f"🔄 Lapozás a következő oldalra...")
                    driver.execute_script(
                        "arguments[0].scrollIntoView({block: 'center'});", next_btn)
                    time.sleep(1)
                    driver.execute_script("arguments[0].click();", next_btn)

                    current_page += 1
                    time.sleep(3.5)  # AJAX várakozás
                except:
                    print("🏁 Nincs (több) lapozógomb, kategória vége.")
                    break

        print(
            f"\n✅ Összesen {len(job_links)} egyedi állás azonosítva a 3 kategóriából. Kezdődik a mélyfúrás...")

        # --- 2. FÁZIS: LEÍRÁSOK SCRAPELÉSE ---
        conn = sqlite3.connect(DB_PATH)
        for idx, job in enumerate(job_links, 1):
            try:
                conn.execute("DELETE FROM jobs WHERE url = ?", (job['url'],))

                print(f"   [{idx}/{len(job_links)}] {job['title']}")
                driver.get(job['url'])

                try:
                    # Mivel Nexum motor, ugyanazokat a konténereket várjuk, mint a Raiffeisennél vagy Ersténél
                    wait.until(EC.presence_of_element_located(
                        (By.CSS_SELECTOR, ".job-description, .jobDetails, .jobEnd__tasks, article")))
                    time.sleep(1.5)
                except:
                    time.sleep(3)

                # NATIVE INNERTEXT A PONTOS DOBOZOKBÓL (Mély bejáró)
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
                            if (cls.includes('share') || cls.includes('apply') || cls.includes('back') || cls.includes('cartbuttons')) return "";

                            if (tag === 'LI') text += "• ";
                            
                            for (let child of el.childNodes) { 
                                text += walk(child); 
                            }
                            
                            if (['H1','H2','H3','H4'].includes(tag)) text += "\\n\\n";
                            else if (['P','DIV','BR','LI'].includes(tag)) text += "\\n";
                        }
                        return text;
                    }

                    // A Nexum alapú oldalak tipikus szövegtartó div-jei
                    let mainContent = document.querySelector('.job-description') || 
                                      document.querySelector('.jobDetails') || 
                                      document.querySelector('.jobEnd__tasks') ||
                                      document.querySelector('article');
                                      
                    if (mainContent) {
                        return walk(mainContent);
                    } else {
                        let fallback = document.querySelector('main');
                        return fallback ? walk(fallback) : walk(document.body);
                    }
                """)

                clean_desc = re.sub(r'\n{3,}', '\n\n', description).strip()

                if "Jelentkezem" in clean_desc:
                    clean_desc = clean_desc.split("Jelentkezem")[0].strip()
                elif "Jelentkezés" in clean_desc:
                    clean_desc = clean_desc.split("Jelentkezés")[0].strip()

                # --- 3. FÁZIS: VÁROS ÉS ORSZÁG BONTÁSA + TISZTÍTÁS ---
                location_raw = job['location_raw']
                parts = [p.strip() for p in location_raw.split(',')]

                # "III.Budapest" vagy "Budapest" kinyerése
                city_raw = parts[0] if parts else "Budapest"

                # Regex tisztító: Eltünteti a római számokat a városnév elejéről (pl. "III.Budapest" -> "Budapest")
                city = re.sub(r'^[IVXLCDM]+\.\s*', '',
                              city_raw, flags=re.IGNORECASE).strip()

                country = "Magyarország"

                # Kategória besorolás az URL alapján (Mivel három URL-ünk van)
                if "frissdiplomas" in job['url']:
                    category = "Energia / Frissdiplomás"
                elif "gyakornoki" in job['url'] or "kotelezo" in job['url']:
                    category = "Energia / Gyakornok"
                else:
                    category = "Energia / Közmű"

                conn.execute('''INSERT INTO jobs (url, title, company, location_raw, city, country, description, category) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                             (job['url'], job['title'], "MVM", location_raw, city, country, clean_desc, category))
                conn.commit()

            except Exception as e:
                print(f"      ⚠️ Hiba a(z) {idx}. állásnál: {e}")

        conn.close()
        print(f"\n✨ SIKER! Az MVM pozíciók mind a 3 kategóriából elmentve.")

    finally:
        driver.quit()


if __name__ == "__main__":
    run_scraper()
