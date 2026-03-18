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
COMPANY_NAME = "Bonafarm"
# A megadott Pályakezdő URL
BASE_URL = "https://karrier.bonafarmcsoport.hu/go/Pályakezdők/9558801/"
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "bonafarm_jobs.db")


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

    options = uc.ChromeOptions()
    options.add_argument("--window-size=1920,1080")
    driver = uc.Chrome(options=options, version_main=145)

    job_links = []
    unique_urls = set()

    try:
        # --- 1. FÁZIS: LINKEK GYŰJTÉSE ---
        print("📂 Bonafarm karrieroldal megnyitása...")
        driver.get(BASE_URL)
        time.sleep(4)

        # Süti ablak lekezelése (ha felugrik)
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

        page_num = 1

        while True:

            print(f"📄 {page_num}. oldal adatainak begyűjtése...")

            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "a.jobTitle-link")))
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
                        
                        // Kép alapján: Cím (0), Kategória/Érdeklődési terület (1), Hely (2)
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

            for job in jobs_on_page:
                if job['url'] not in unique_urls:
                    unique_urls.add(job['url'])
                    job_links.append(job)

            # LAPOZÁS: Következő oldal gombjának megkeresése és kattintása
            next_page_num = page_num + 1
            has_next = driver.execute_script(f"""
                let nextBtn = document.querySelector('a[title="{next_page_num}. oldal"]');
                if (nextBtn) {{
                    nextBtn.click();
                    return true;
                }}
                return false;
            """)

            if has_next:
                page_num += 1
                time.sleep(4)  # Várjuk meg, amíg az új oldal betöltődik
            else:
                print("🏁 Nincs több lapozógomb, lista vége.")
                break

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
                        (By.CSS_SELECTOR, ".jobdescription")))
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

                            if (['SCRIPT','STYLE','NAV','FOOTER','HEADER','BUTTON','SVG','IMG'].includes(tag)) return "";
                            if (cls.includes('share') || cls.includes('apply') || cls.includes('back')) return "";

                            if (tag === 'LI') text += "• ";
                            for (let child of el.childNodes) text += walk(child); 
                            
                            if (['H1','H2','H3','H4'].includes(tag)) text += "\\n\\n";
                            else if (['B','STRONG'].includes(tag) && text.length > 10) text += "\\n\\n";
                            else if (['P','DIV','BR','LI'].includes(tag)) text += "\\n";
                        }
                        return text;
                    }

                    let mainContent = document.querySelector('.jobdescription');
                    return mainContent ? walk(mainContent) : walk(document.body);
                """)

                clean_desc = re.sub(r'\n{3,}', '\n\n', description).strip()

                # BONAFARM specifikus zajszűrés: céges bemutatkozó levágása a végéről
                if "A Bonafarm Zrt. a Bonafarm Csoport menedzsment cége" in clean_desc:
                    clean_desc = clean_desc.split(
                        "A Bonafarm Zrt. a Bonafarm Csoport menedzsment cége")[0].strip()
                elif "A Bonafarm Zrt. a Bonafarm Csoport tagja" in clean_desc:
                    clean_desc = clean_desc.split(
                        "A Bonafarm Zrt. a Bonafarm Csoport tagja")[0].strip()

                # VÁROS KISZŰRÉSE: "Szeged, Csongrád-Csanád, HU, 6720 - még 1..." -> "Szeged"
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
