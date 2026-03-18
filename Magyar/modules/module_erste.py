import undetected_chromedriver as uc
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
COMPANY_NAME = "Erste Bank"
# A szűrt URL (1-3 év tapasztalat, Gyakornok/Pályakezdő)
BASE_URL = "https://karrier.erstebank.hu/allasok?q=ZXhwZXJpZW5jZXMlNUIlNUQlM0QxLTMlMjAlQzMlQTl2JTI2ZXhwZXJpZW5jZXMlNUIlNUQlM0RHeWFrb3Jub2slMjAlMkYlMjBwJUMzJUExbHlha2V6ZCVDNSU5MSUyNguuzzuuzz=#!"
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "erste_jobs.db")


def init_db():
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)
    conn = sqlite3.connect(DB_PATH)

    # Frissített séma, hogy passzoljon a többi modulhoz
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
    print(f"🚀 {COMPANY_NAME} Scraper indítása (Infinite Scroll & Nexum mód)...")

    options = uc.ChromeOptions()
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--headless=new")
    driver = uc.Chrome(options=options)

    job_links = []
    unique_urls = set()

    try:
        # --- 1. FÁZIS: LINKEK GYŰJTÉSE VÉGTELEN GÖRGETÉSSEL ---
        print("📂 Erste Bank karrieroldal megnyitása...")
        driver.get(BASE_URL)
        time.sleep(6)  # Várjuk meg a Nexum motor indulását

        # Cookie ablak bezárása
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

        while True:

            # Jelenlegi kártyák számának lekérése
            count = driver.execute_script(
                "return document.querySelectorAll('.jobList__item').length;")
            print(f"   📥 Jelenleg betöltve: {count} állás...")

            # Görgetés a lap legeslegalkjára
            driver.execute_script(
                "window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(3)  # Várunk, hogy az AJAX betöltse az új elemeket

            new_count = driver.execute_script(
                "return document.querySelectorAll('.jobList__item').length;")

            if new_count == count:
                retries += 1
                if retries >= 3:
                    print("🏁 Lista vége, nincs több betölthető állás.")
                    break
                # Trükk: Picit feljebb görgetünk, majd újra le, hátha beragadt a lazy-load
                driver.execute_script("window.scrollBy(0, -500);")
                time.sleep(1)
                driver.execute_script(
                    "window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
            else:
                retries = 0  # Ha nőtt a szám, nullázzuk a próbálkozásokat
                last_count = new_count

            scroll_cycles += 1

        print("📄 Linkek és adatok kinyerése a betöltött kártyákról...")
        jobs_on_page = driver.execute_script("""
            let results = [];
            let cards = document.querySelectorAll('.jobList__item');
            
            cards.forEach(card => {
                let titleEl = card.querySelector('.jobList__item__title');
                let locEl = card.querySelector('.job_list_city');
                let catEl = card.querySelector('.job_list_specialities');
                let expEl = card.querySelector('.job_list_experiences');
                
                if (titleEl && titleEl.href) {
                    let category = catEl ? catEl.innerText.trim() : 'Pénzügy / Bank';
                    if (expEl) { category += " - " + expEl.innerText.trim(); }
                    
                    results.push({
                        url: titleEl.href, // A JS .href automatikusan abszolút URL-t ad!
                        title: titleEl.innerText.trim(),
                        location_raw: locEl ? locEl.innerText.trim() : 'N/A',
                        category: category
                    });
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
                    # Várunk a megadott .jobEnd__tasks konténerre
                    wait.until(EC.presence_of_element_located(
                        (By.CSS_SELECTOR, ".jobEnd__tasks")))
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

                    // Fókuszálunk a te HTML-edben látott konténerre
                    let mainContent = document.querySelector('.jobEnd__tasks') || document.querySelector('.jobEnd__content');
                    return mainContent ? walk(mainContent) : walk(document.body);
                """)

                clean_desc = re.sub(r'\n{3,}', '\n\n', description).strip()
                if "Jelentkezem" in clean_desc:
                    clean_desc = clean_desc.split("Jelentkezem")[0].strip()

                # VÁROS TISZTÍTÁSA (Az Erste általában tiszta városneveket használ, de biztos ami biztos)
                location_raw = job['location_raw']
                city = location_raw
                if "," in city:
                    city = city.split(",")[0].strip()

                conn.execute('''INSERT INTO jobs (url, title, company, location_raw, city, country, description, category) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                             (job['url'], job['title'], COMPANY_NAME, location_raw, city, "Magyarország", clean_desc, job['category']))
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
