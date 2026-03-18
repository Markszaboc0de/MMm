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
COMPANY_NAME = "Magyar Suzuki"
# Az új, stabil alap URL, ami nem akad ki a böngésző megnyitásakor
BASE_URL = "https://karrier.suzuki.hu/nyitott-poziciok"
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "suzuki_jobs.db")


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
        conn.execute("ALTER TABLE jobs ADD COLUMN category TEXT")
    except:
        pass

    conn.commit()
    conn.close()


def run_scraper():
    init_db()
    print(
        f"🚀 {COMPANY_NAME} Scraper indítása (Stabil URL & Páncélos Linkvadász mód)...")

    options = uc.ChromeOptions()
    options.add_argument("--window-size=1920,1080")
    driver = uc.Chrome(options=options, version_main=145)

    job_links = []
    unique_urls = set()

    try:
        # --- 1. FÁZIS: LINKEK GYŰJTÉSE VÉGTELEN GÖRGETÉSSEL ---
        print("📂 Suzuki karrieroldal megnyitása...")
        driver.get(BASE_URL)
        time.sleep(6)  # Várjuk meg az oldal és az elemek betöltését

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
            # Biztonsági fék (maximum 15 görgetés)
            if scroll_cycles > 15:
                break

            # A legbiztosabb pont: Hány darab /allas/ link van a DOM-ban?
            count = driver.execute_script(
                "return document.querySelectorAll('a[href*=\"/allas/\"]').length;")
            print(f"   📥 Jelenleg betöltve: {count} álláslink...")

            driver.execute_script(
                "window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(3)

            new_count = driver.execute_script(
                "return document.querySelectorAll('a[href*=\"/allas/\"]').length;")

            if new_count == count:
                retries += 1
                if retries >= 3:
                    print("🏁 Lista vége, nincs több betölthető állás.")
                    break
                # Ha beragadt, picit fel-le görgetünk
                driver.execute_script("window.scrollBy(0, -500);")
                time.sleep(1)
                driver.execute_script(
                    "window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
            else:
                retries = 0
                last_count = new_count

            scroll_cycles += 1

        print("📄 Adatok kinyerése a linkekből...")

        # Páncélos JavaScript extraktor
        jobs_on_page = driver.execute_script("""
            let results = [];
            let processedUrls = new Set();
            let links = document.querySelectorAll('a[href*="/allas/"]');

            links.forEach(a => {
                let url = a.href;
                // Kiszűrjük a hibás vagy üres linkeket
                if (url && !processedUrls.has(url) && url.length > 25) {
                    processedUrls.add(url);
                    
                    let title = "Suzuki Pozíció";
                    let text = a.innerText.trim();
                    
                    // Ha a link maga a cím (nem az 'Érdekel' vagy 'Tovább' gomb)
                    if (text && !text.toLowerCase().includes('érdekel') && !text.toLowerCase().includes('részletek')) {
                        title = text;
                    } else {
                        // Ha az 'Érdekel' gombra futottunk rá, visszalépünk a szülő kártyához, és megkeressük a Címsort
                        let card = a.closest('div');
                        for(let i=0; i<6; i++) {
                            if(card) {
                                let h = card.querySelector('h2, h3, h4, .title, [class*="title"]');
                                if(h && h.innerText.trim().length > 3) {
                                    title = h.innerText.trim();
                                    break;
                                }
                                card = card.parentElement;
                            }
                        }
                    }

                    results.push({
                        url: url,
                        title: title,
                        location_raw: 'Esztergom',
                        category: 'Járműipar / Gyártás'
                    });
                }
            });
            return results;
        """)

        for job in jobs_on_page:
            if job['url'] not in unique_urls and job['title'] != 'Suzuki Pozíció':
                unique_urls.add(job['url'])
                job_links.append(job)

        print(f"✅ {len(job_links)} egyedi állás azonosítva. Kezdődik a mélyfúrás...")

        if not job_links:
            print("⚠️ Üres lista. Valami nagyon blokkolja az oldalt.")
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
                        (By.CSS_SELECTOR, ".jobEnd__tasks")))
                    time.sleep(1)
                except:
                    time.sleep(2)

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
                if "Érdekel" in clean_desc:
                    clean_desc = clean_desc.split("Érdekel")[0].strip()

                conn.execute('''INSERT INTO jobs (url, title, company, location_raw, city, country, description, category) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                             (job['url'], job['title'], COMPANY_NAME, job['location_raw'], job['location_raw'], "Magyarország", clean_desc, job['category']))
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
