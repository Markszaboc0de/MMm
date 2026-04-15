import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import sqlite3
import os
import sys
import time
import re
from markdownify import markdownify as md

sys.stdout.reconfigure(encoding='utf-8')

# --- KONFIGURÁCIÓ ---
COMPANY_NAME = "INNIO"
# Az URL már tartalmazza az "Internship" (gyakornok) szűrőt
BASE_URL = "https://jobs.jobvite.com/innio/search?r=&l=&c=&t=Internship&q="
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "innio_jobs.db")


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


def create_driver():
    options = uc.ChromeOptions()
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    return uc.Chrome(options=options, version_main=145)


def run_scraper():
    init_db()
    print(f"🚀 {COMPANY_NAME} Scraper indítása (Jobvite Engine mód)...")

    driver = create_driver()
    job_links = []
    unique_urls = set()

    try:
        # --- 1. FÁZIS: LINKEK GYŰJTÉSE ---
        print("📂 INNIO állásoldal megnyitása (Internship szűrővel)...")
        driver.get(BASE_URL)
        time.sleep(6)

        # Cookie ablak bezárása (ha van)
        try:
            cookie_btn = driver.find_element(
                By.XPATH, "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'accept')] | //button[@id='onetrust-accept-btn-handler']")
            driver.execute_script("arguments[0].click();", cookie_btn)
            time.sleep(1)
        except:
            pass

        print("🔍 Álláskártyák keresése...")

        # A Jobvite általában egy listában tölti be az összes találatot, de a biztonság kedvéért legörgetünk
        driver.execute_script(
            "window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)

        # JavaScript alapú kinyerő a Jobvite struktúrára hangolva
        jobs_on_page = driver.execute_script("""
            let results = [];
            
            // Megkeressük az összes címet tartalmazó konténert
            let nameDivs = document.querySelectorAll('.jv-job-list-name');
            
            nameDivs.forEach(nameDiv => {
                // A Jobvite gyakran magát a sort (tr vagy div) vagy a szöveget csomagolja egy <a> tagbe
                let aTag = nameDiv.closest('a') || nameDiv.querySelector('a') || nameDiv.parentElement.querySelector('a');
                
                if (aTag && aTag.href) {
                    let title = nameDiv.innerText.trim();
                    let loc = "N/A";
                    
                    // Megkeressük a hozzá tartozó lokáció dobozt
                    let parentRow = nameDiv.closest('tr, .jv-job-list, .jv-job-item') || nameDiv.parentElement;
                    if (parentRow) {
                        let locEl = parentRow.querySelector('.jv-job-list-location');
                        if (locEl) {
                            // A HTML tele van sortöréssel és szóközökkel, ezt egyetlen szép stringgé alakítjuk (pl. "Budapest, Hungary")
                            loc = locEl.innerText.replace(/\\s+/g, ' ').trim();
                        }
                    }
                    
                    results.push({
                        url: aTag.href,
                        title: title,
                        location_raw: loc,
                        category: 'Energy / Engineering' // INNIO profilja alapján
                    });
                }
            });
            return results;
        """)

        for job in jobs_on_page:
            if "/job/" in job['url'] and job['url'] not in unique_urls:
                unique_urls.add(job['url'])
                job_links.append(job)

        print(
            f"✅ Összesen {len(job_links)} egyedi állás azonosítva. Böngésző újraindítása a mélyfúráshoz...")

        if not job_links:
            return

        # --- PRE-PHASE 2: BROWSER REBOOT ---
        try:
            driver.quit()
        except:
            pass
        time.sleep(2)

        driver = create_driver()
        wait = WebDriverWait(driver, 10)

        # --- 2. FÁZIS: LEÍRÁSOK ÉS ADATOK TISZTÍTÁSA ---
        conn = sqlite3.connect(DB_PATH)
        saved_count = 0

        for idx, job in enumerate(job_links, 1):
            try:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT id FROM jobs WHERE url = ?", (job['url'],))
                if cursor.fetchone():
                    print(
                        f"   [{idx}/{len(job_links)}] {job['title']} (Már az adatbázisban, ugrás...)")
                    continue

                # AUTO-RECOVERY BLOCK
                try:
                    driver.get(job['url'])
                except:
                    print(
                        "   🔄 Kapcsolat megszakadt. Böngésző automatikus újraindítása...")
                    try:
                        driver.quit()
                    except:
                        pass
                    time.sleep(2)
                    driver = create_driver()
                    wait = WebDriverWait(driver, 10)
                    driver.get(job['url'])

                try:
                    # Megvárjuk az elküldött HTML alapján a leírás dobozt
                    wait.until(EC.presence_of_element_located(
                        (By.CSS_SELECTOR, ".jv-job-detail-description, article")))
                    time.sleep(1)
                except:
                    time.sleep(2)

                details = driver.execute_script("""
                    let result = { description: "" };
                    
                    let descEl = document.querySelector('.jv-job-detail-description') || 
                                 document.querySelector('.job-description') ||
                                 document.querySelector('article') ||
                                 document.querySelector('main');
                                 
                    if (descEl) {
                        result.description = descEl.innerHTML;
                    } else {
                        result.description = document.body.innerHTML;
                    }
                    
                    return result;
                """)

                # --- 3. FÁZIS: LOKÁCIÓ ÉS ADATOK FORMÁZÁSA ---
                location_raw = job['location_raw']
                city = "N/A"
                country = "N/A"

                if location_raw and location_raw != 'N/A':
                    # Szétszedjük a "Budapest, Hungary" formátumot
                    parts = [p.strip() for p in location_raw.split(',')]
                    city = parts[0]
                    country = parts[-1] if len(parts) > 1 else parts[0]

                print(
                    f"   [{idx}/{len(job_links)}] Formázás: {job['title']} | Hely: {city}, {country}")

                # --- FORMAT DESCRIPTION (Markdownify) ---
                if details['description']:
                    clean_desc = md(details['description'], heading_style="ATX", bullets="-", strip=[
                                    'img', 'script', 'style', 'a', 'button', 'svg'])

                    # HTML entitások és felesleges térközök javítása
                    clean_desc = clean_desc.replace(
                        '•', '-').replace('·', '-').replace('\xa0', ' ').replace('&nbsp;', ' ')
                    clean_desc = re.sub(r'\n{3,}', '\n\n', clean_desc).strip()
                else:
                    clean_desc = "Leírás nem található."

                # Felesleges jelentkezési szövegek levágása a végéről (ha a fallback aktiválódott volna)
                truncation_markers = [
                    "Apply", "Jelentkezés", "Share this job", "Megosztás"]
                for marker in truncation_markers:
                    if marker in clean_desc:
                        clean_desc = clean_desc.split(marker)[0].strip()

                conn.execute('''INSERT INTO jobs (url, title, company, location_raw, city, country, description, category) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                             (job['url'], job['title'], COMPANY_NAME, location_raw, city, country, clean_desc, job['category']))
                conn.commit()
                saved_count += 1

            except Exception as e:
                print(f"      ⚠️ Hiba a(z) {idx}. állásnál: {e}")

        conn.close()
        print(
            f"\n✨ SIKER! {saved_count} {COMPANY_NAME} állás biztonságosan mentve.")

    finally:
        try:
            driver.quit()
        except:
            pass


if __name__ == "__main__":
    run_scraper()
