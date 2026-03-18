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
COMPANY_NAME = "K&H Bank"
# A megadott szűrt URL (szakmai gyakorlat / pályakezdő)
BASE_URL = "https://karrier.kh.hu/allasok?q=ZXhwZXJpZW5jZXMlNUIlNUQlM0RwJUMzJUExbHlha2V6ZCVDNSU5MSUyNmV4cGVyaWVuY2VzJTVCJTVEJTNEcCVDMyVBMWx5YWtlemQlQzUlOTElMkMlMjBzemFrbWFpJTIwZ3lha29ybGF0dGFsJTI2ZXhwZXJpZW5jZXMlNUIlNUQlM0RzemFrbWFpJTIwZ3lha29ybGF0JTI2#!"
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "kh_jobs.db")


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
    print(f"🚀 {COMPANY_NAME} Scraper indítása (Nexum Engine & DOM Walker mód)...")

    options = uc.ChromeOptions()
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--headless=new")
    driver = uc.Chrome(options=options)

    job_links = []
    unique_urls = set()

    try:
        # --- 1. FÁZIS: LINKEK GYŰJTÉSE ---
        print("📂 K&H karrieroldal megnyitása...")
        driver.get(BASE_URL)
        time.sleep(5)  # Várjuk meg a Nexum JS motor indulását

        # Ha a hashbang URL miatt esetleg üres lenne (mint a Suzukinál), egy frissítés segít
        card_count = driver.execute_script(
            "return document.querySelectorAll('.jobList__item').length;")
        if card_count == 0:
            print("🔄 Üres az oldal, kényszerített frissítés a szűrők aktiválásához...")
            driver.refresh()
            time.sleep(5)

        # Cookie ablak bezárása (K&H specifikus gombok keresése)
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

            # Dinamikus várakozó a kártyákra
            for _ in range(10):
                if driver.execute_script("return document.querySelectorAll('.jobList__item').length;") > 0:
                    break
                time.sleep(1)

            # Linkek és metaadatok JS alapú kinyerése a DOM-ból
            jobs_on_page = driver.execute_script("""
                let results = [];
                let cards = document.querySelectorAll('.jobList__item');
                
                cards.forEach(card => {
                    let titleEl = card.querySelector('a[data-cy="job_title"]');
                    let locEl = card.querySelector('[data-cy="address"] span');
                    let catEl = card.querySelector('[data-cy="area"]');
                    
                    if (titleEl && titleEl.href) {
                        results.push({
                            url: titleEl.href,
                            title: titleEl.innerText.trim(),
                            location_raw: locEl ? locEl.innerText.trim() : 'N/A',
                            category: catEl ? catEl.innerText.trim() : 'Pénzügy / Bank'
                        });
                    }
                });
                return results;
            """)

            if not jobs_on_page:
                print("⚠️ Nem találtam több állást ezen az oldalon.")
                break

            for job in jobs_on_page:
                if job['url'] not in unique_urls:
                    unique_urls.add(job['url'])
                    job_links.append(job)

            # LAPOZÁS: Következő számozott gomb keresése
            try:
                next_page_num = current_page + 1
                next_btn = driver.execute_script(f"""
                    let btns = Array.from(document.querySelectorAll('button.pager-element'));
                    let target = btns.find(b => b.innerText.trim() === '{next_page_num}');
                    return target;
                """)

                if next_btn:
                    print(
                        f"🔄 Lapozás a következő oldalra ({next_page_num})...")
                    driver.execute_script(
                        "arguments[0].scrollIntoView({block: 'center'});", next_btn)
                    time.sleep(1)
                    driver.execute_script("arguments[0].click();", next_btn)
                    current_page += 1
                    time.sleep(4)  # Várjuk meg a JS alapú újratöltést
                else:
                    print("🏁 Nincs több lapozógomb, lista vége.")
                    break
            except:
                print("🏁 Lapozás befejezve, lista vége.")
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
                        (By.CSS_SELECTOR, ".jobEnd__tasks")))
                    time.sleep(1)
                except:
                    time.sleep(3)

                # DOM Walker a tisztított leírásért
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

                    let mainContent = document.querySelector('.jobEnd__tasks') || document.querySelector('.jobEnd__content');
                    return mainContent ? walk(mainContent) : walk(document.body);
                """)

                clean_desc = re.sub(r'\n{3,}', '\n\n', description).strip()
                if "Jelentkezem" in clean_desc:
                    clean_desc = clean_desc.split("Jelentkezem")[0].strip()

                # VÁROS KISZŰRÉSE: "2700 Cegléd, Szabadság tér 1." -> "Cegléd"
                location_raw = job['location_raw']
                city = location_raw
                if city != 'N/A':
                    if "," in city:
                        city = city.split(",")[0].strip()
                    # Levágjuk a 4 jegyű irányítószámot (K&H specifikusan 1000-9999 tartomány)
                    city = re.sub(r'^\d{4}\s*', '', city).strip()

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
