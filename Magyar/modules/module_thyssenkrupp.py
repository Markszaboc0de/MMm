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
COMPANY_NAME = "thyssenkrupp"
BASE_URL = "https://jobs.thyssenkrupp.com/hu?location=Magyarorsz%C3%A1g&lat=47.1817585&lng=19.5060937&placeId=512e36525b8f8133405990a2cedc43974740f00101f9015753000000000000c0020b92030748756e67617279&radius=0&entryLevel=Kezd%C5%91+szint+(0-2+%C3%A9v),Di%C3%A1kmunka,Gyakorlat+(hallgat%C3%B3k),Diplomamunka,Diplom%C3%A1s+gyakornoki+program,Du%C3%A1lis+k%C3%A9pz%C3%A9s"
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "thyssenkrupp_jobs.db")


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
    print(f"🚀 {COMPANY_NAME} Scraper indítása (Smart List & Dynamic Location mód)...")

    options = uc.ChromeOptions()
    options.add_argument("--window-size=1920,1080")
    driver = uc.Chrome(options=options, version_main=145)
    wait = WebDriverWait(driver, 10)

    job_links = []
    unique_urls = set()

    try:
        # --- 1. FÁZIS: LINKEK GYŰJTÉSE ---
        print("📂 thyssenkrupp karrieroldal megnyitása...")
        driver.get(BASE_URL)
        time.sleep(6)

        try:
            cookie_btn = driver.find_element(
                By.XPATH, "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'elfogad')] | //button[contains(@class, 'cookie') and contains(@class, 'accept')]")
            driver.execute_script("arguments[0].click();", cookie_btn)
            time.sleep(1)
        except:
            pass

        print("🔄 Lista betöltése és görgetés...")
        scrolls = 0
        last_count = 0

        while True:

            try:
                load_more = driver.find_element(
                    By.XPATH, "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'további') or contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'load more')]")
                if load_more.is_displayed():
                    driver.execute_script(
                        "arguments[0].scrollIntoView({block: 'center'});", load_more)
                    time.sleep(1)
                    driver.execute_script("arguments[0].click();", load_more)
                    time.sleep(3)
                    scrolls += 1
                    continue
            except:
                pass

            driver.execute_script(
                "window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)

            items = driver.execute_script(
                "return document.querySelectorAll('a[href*=\"/job/\"]').length;")
            if items == last_count or items == 0:
                break

            last_count = items
            scrolls += 1

        print("🔍 Álláskártyák beolvasása az oldalról...")

        jobs_on_page = driver.execute_script("""
            let results = [];
            let links = document.querySelectorAll('a');
            
            links.forEach(aTag => {
                let href = aTag.href;
                if (href && (href.includes('/job/') || href.includes('/jobs/'))) {
                    let titleEl = aTag.querySelector('h2, h3, strong, span') || aTag;
                    let title = titleEl.innerText.trim();
                    
                    if (title && title.length > 3) {
                        results.push({
                            url: href,
                            title: title,
                            category: 'Mérnöki / IT / Gyakornok'
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

        print(f"✅ {len(job_links)} egyedi állás azonosítva. Kezdődik a mélyfúrás...")

        # --- 2. FÁZIS: LEÍRÁSOK ÉS HELYSZÍN SCRAPELÉSE ---
        conn = sqlite3.connect(DB_PATH)
        for idx, job in enumerate(job_links, 1):
            try:
                conn.execute("DELETE FROM jobs WHERE url = ?", (job['url'],))

                print(f"   [{idx}/{len(job_links)}] {job['title']}")
                driver.get(job['url'])

                try:
                    wait.until(EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "article.inner-container")))
                    time.sleep(1.5)
                except:
                    time.sleep(3)

                # --- 2/A. HELYSZÍN KINYERÉSE A BEKÜLDÖTT HTML ALAPJÁN ---
                location_raw = driver.execute_script("""
                    // A megadott p.text-lg taget keressük
                    let locEl = document.querySelector('p.text-lg.font-medium');
                    if (locEl) {
                        return locEl.innerText.trim();
                    }
                    return 'Budapest, Hungary'; // Tartalék
                """)

                # Nyers pl: "Budapest, Hungary — thyssenkrupp Components Technology Hungary Kft"
                # Első körben levágjuk a gondolatjel utáni céges részt
                loc_split = location_raw.split(
                    '—') if '—' in location_raw else location_raw.split('-')
                loc_clean = loc_split[0].strip()  # "Budapest, Hungary"

                parts = [p.strip() for p in loc_clean.split(',')]

                if len(parts) >= 2:
                    city = parts[0]
                    # Normalizáljuk a Hungary-t Magyarországra az adatbázisban
                    country = "Magyarország" if parts[1].lower(
                    ) in ["hungary", "magyarország"] else parts[1]
                else:
                    city = loc_clean if loc_clean else "Budapest"
                    country = "Magyarország"

                # --- 2/B. NATIVE INNERTEXT A PONTOS DOBOZOKBÓL ---
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
                            
                            for (let child of el.childNodes) { 
                                text += walk(child); 
                            }
                            
                            if (['H1','H2','H3','H4'].includes(tag)) text += "\\n\\n";
                            else if (['P','DIV','BR','LI'].includes(tag)) text += "\\n";
                        }
                        return text;
                    }

                    let mainContent = document.querySelector('article.inner-container');
                                      
                    if (mainContent) {
                        return walk(mainContent);
                    } else {
                        let fallback = document.querySelector('article') || document.querySelector('main');
                        return fallback ? walk(fallback) : walk(document.body);
                    }
                """)

                clean_desc = re.sub(r'\n{3,}', '\n\n', description).strip()

                if "Apply now" in clean_desc:
                    clean_desc = clean_desc.split("Apply now")[0].strip()
                elif "Jelentkezem" in clean_desc:
                    clean_desc = clean_desc.split("Jelentkezem")[0].strip()

                conn.execute('''INSERT INTO jobs (url, title, company, location_raw, city, country, description, category) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                             (job['url'], job['title'], "thyssenkrupp", location_raw, city, country, clean_desc, job['category']))
                conn.commit()

            except Exception as e:
                print(f"      ⚠️ Hiba a(z) {idx}. állásnál: {e}")

        conn.close()
        print(f"\n✨ SIKER! A thyssenkrupp pozíciók elmentve.")

    finally:
        driver.quit()


if __name__ == "__main__":
    run_scraper()
