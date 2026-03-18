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

sys.stdout.reconfigure(encoding='utf-8')

# --- KONFIGURÁCIÓ ---
COMPANY_NAME = "Fovarosi_Vizmuvek"
BASE_URL = "https://vizmuvek.hu/hu/karrier/gyakornoki-dualis-kepzes"
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "vizmuvek_jobs.db")


def init_db():
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)
    conn = sqlite3.connect(DB_PATH)

    # Adatbázis inicializálása a legfrissebb sémával
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
    print(f"🚀 {COMPANY_NAME} Scraper indítása (Single-Page mód)...")

    options = Options()
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    driver = webdriver.Chrome(options=options)

    try:
        print(f"📂 Vízművek karrieroldal megnyitása: {BASE_URL}")
        driver.get(BASE_URL)
        time.sleep(4)  # Várakozás a teljes oldal betöltésére

        # Cookie ablak bezárása (ha van)
        try:
            cookie_btn = driver.find_element(
                By.XPATH, "//button[contains(text(), 'Elfogadom') or contains(text(), 'Rendben')]")
            driver.execute_script("arguments[0].click();", cookie_btn)
            time.sleep(1)
        except:
            pass

        print("🔍 Leírás kinyerése folyamatban...")

        # --- ADATKINYERÉS JAVASCRIPTTEL ---
        # Kifejezetten a tartalom div-et célozzuk a kapott HTML alapján
        job_data = driver.execute_script("""
            let result = { title: "Gyakornoki program", description: "" };
            
            // Cím dinamikus kinyerése (biztonság kedvéért)
            let titleEl = document.querySelector('h1');
            if (titleEl) {
                result.title = titleEl.innerText.trim();
            }

            function walk(el) {
                if (!el) return;
                if (el.nodeType === 3) {
                    let val = el.nodeValue.trim();
                    if (val) result.description += val + " ";
                } else if (el.nodeType === 1) {
                    let tag = el.tagName.toUpperCase();
                    let cls = (el.className && typeof el.className === 'string') ? el.className.toLowerCase() : "";

                    // Kizárjuk a szkripteket és az oldalmenüt
                    if (['SCRIPT','STYLE','NAV','FOOTER','BUTTON'].includes(tag)) return;
                    if (cls.includes('gray-box') || cls.includes('quicknav')) return;

                    // Listaformázás csinosítása
                    if (tag === 'LI') result.description += "• ";
                    for (let child of el.childNodes) { walk(child); }
                    if (['P','DIV','BR','LI','H1','H2','H3','H4'].includes(tag)) result.description += "\\n";
                }
            }

            // Kifejezetten a te általad küldött osztályt célozzuk meg
            let mainContent = document.querySelector('.col-12.col-xl-9.mr-0') || document.querySelector('#main-content');
            if (mainContent) {
                walk(mainContent);
            } else {
                result.description = "Hiba: A tartalom konténere nem található.";
            }

            return result;
        """)

        # Tisztítás és formázás
        description = job_data['description']
        clean_desc = re.sub(r'\n\s*\n', '\n\n',
                            re.sub(r'[ \t]+', ' ', description)).strip()
        title = job_data['title']

        # Fix helyszín, mivel ez a Fővárosi Vízművek
        location_raw = "Budapest"
        city = "Budapest"
        country = "Magyarország"
        category = "Gyakornok"

        # --- ADATBÁZISBA MENTÉS ---
        conn = sqlite3.connect(DB_PATH)

        # Töröljük a korábbit, hogy a legfrissebb állapottal legyen felülírva
        conn.execute("DELETE FROM jobs WHERE url = ?", (BASE_URL,))

        conn.execute('''INSERT INTO jobs (url, title, company, location_raw, city, country, description, category) 
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                     (BASE_URL, title, "Fővárosi Vízművek", location_raw, city, country, clean_desc, category))
        conn.commit()
        conn.close()

        print(f"   ✅ '{title}' sikeresen elmentve!")
        print(f"\n✨ SIKER! A Vízművek állás learatva.")

    except Exception as e:
        print(f"⚠️ Váratlan hiba történt: {e}")

    finally:
        driver.quit()


if __name__ == "__main__":
    run_scraper()
