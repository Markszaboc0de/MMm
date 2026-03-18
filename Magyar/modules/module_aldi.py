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
COMPANY_NAME = "ALDI"
BASE_URL = "https://karrier.aldi.hu/allaskereso?query=&location=&careerLevels[]=26&sortBy=SORT_BY_TITLE_ASCENDING"
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "aldi_jobs.db")


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
    print(f"🚀 {COMPANY_NAME} Scraper indítása (Cím-felismerő mód)...")

    options = uc.ChromeOptions()
    options.add_argument("--window-size=1920,1080")
    driver = uc.Chrome(options=options, version_main=145)

    job_links = []
    unique_urls = set()

    try:
        # --- 1. FÁZIS: LINKEK GYŰJTÉSE ---
        print("📂 ALDI keresőoldal megnyitása...")
        driver.get(BASE_URL)
        time.sleep(6)

        try:
            cookie_btn = driver.find_element(
                By.XPATH, "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'elfogad')] | //button[contains(@class, 'cookie')]")
            driver.execute_script("arguments[0].click();", cookie_btn)
            time.sleep(1)
        except:
            pass

        print("📄 Linkek begyűjtése az oldalról...")

        jobs_on_page = driver.execute_script("""
            let results = [];
            let links = document.querySelectorAll('a');
            
            links.forEach(l => {
                let href = l.href;
                let text = l.innerText.trim();
                
                // Keresünk mindent, ami álláshivatkozásnak tűnik
                if (href && (href.includes('/job/') || href.includes('/allas/')) && text.length > 5 && !text.includes('\\n')) {
                    results.push({ url: href, title: text });
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
            print("⚠️ Nem találtam egyetlen álláslinket sem!")
            return

        # --- 2. FÁZIS: LEÍRÁSOK ÉS CÍM SCRAPELÉSE ---
        conn = sqlite3.connect(DB_PATH)
        wait = WebDriverWait(driver, 10)

        for idx, job in enumerate(job_links, 1):
            try:
                conn.execute("DELETE FROM jobs WHERE url = ?", (job['url'],))

                print(f"   [{idx}/{len(job_links)}] {job['title']}")
                driver.get(job['url'])

                try:
                    wait.until(EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "#jobdetails .description")))
                    time.sleep(1)
                except:
                    time.sleep(3)

                # JS kinyeri a leírást és az általad megadott .job-address mezőt is!
                details = driver.execute_script("""
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

                    let mainContent = document.querySelector('#jobdetails .description') || document.querySelector('#jobdetails');
                    let descText = mainContent ? walk(mainContent) : walk(document.body);
                    
                    // A te általad küldött .job-address megkeresése
                    let addrEl = document.querySelector('.job-address');
                    let addrText = addrEl ? addrEl.innerText.trim() : 'N/A';
                    
                    return { description: descText, address: addrText };
                """)

                # Leírás tisztítása
                clean_desc = re.sub(r'\n{3,}', '\n\n',
                                    details['description']).strip()
                if "Online application:" in clean_desc:
                    clean_desc = clean_desc.split(
                        "Online application:")[0].strip()
                elif "Jelentkezés:" in clean_desc:
                    clean_desc = clean_desc.split("Jelentkezés:")[0].strip()

                # CÍM TISZTÍTÁSA (Város kinyerése)
                location_raw = details['address']
                city = location_raw

                if city != 'N/A':
                    # 1. Ha van vessző, levágjuk a végét (utca, házszám)
                    if "," in city:
                        city = city.split(",")[0].strip()
                    # 2. Levágjuk a 4 jegyű irányítószámot az elejéről (pl. "1112 Budapest" -> "Budapest")
                    city = re.sub(r'^\d{4}\s*', '', city).strip()

                conn.execute('''INSERT INTO jobs (url, title, company, location_raw, city, country, description, category) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                             (job['url'], job['title'], "ALDI", location_raw, city, "Magyarország", clean_desc, "Gyakornok / Diákmunka"))
                conn.commit()

            except Exception as e:
                print(f"      ⚠️ Hiba a(z) {idx}. állásnál: {e}")

        conn.close()
        print(f"\n✨ SIKER! Az ALDI pozíciók (és a városok) elmentve.")

    finally:
        try:
            driver.quit()
        except:
            pass


if __name__ == "__main__":
    run_scraper()
