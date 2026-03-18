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

# --- CONFIGURATION ---
COMPANY_NAME = "Schaeffler"
SESSION_URL = "https://jobs.schaeffler.com/?locale=en_US"

# The massive URL with pageSize=300 to load everything on Page 1
BASE_URL = "https://jobs.schaeffler.com/?locale=en_US&currentPage=1&pageSize=300&addresses%2FcountryCity=Bulgaria%3ASofia&addresses%2FcountryCity=Czech+Republic%3AFrenstat+p%2FR&addresses%2FcountryCity=Czech+Republic%3ALan%C5%A1kroun&addresses%2FcountryCity=Czech+Republic%3AOstrava+-+Hru%C5%A1ov&addresses%2FcountryCity=Czech+Republic%3APraha&addresses%2FcountryCity=Czech+Republic%3ASvitavy&addresses%2FcountryCity=Czech+Republic%3ATrutnov&addresses%2FcountryCity=France%3ABoussens&addresses%2FcountryCity=France%3AChambery&addresses%2FcountryCity=France%3AFoix&addresses%2FcountryCity=France%3AParis&addresses%2FcountryCity=France%3AToulouse&addresses%2FcountryCity=Germany%3ABebra&addresses%2FcountryCity=Germany%3AB%C3%BChl&addresses%2FcountryCity=Germany%3AChemnitz&addresses%2FcountryCity=Germany%3ADortmund&addresses%2FcountryCity=Germany%3AErlangen&addresses%2FcountryCity=Germany%3AFrankfurt+am+Main&addresses%2FcountryCity=Germany%3AGunzenhausen&addresses%2FcountryCity=Germany%3AHalle&addresses%2FcountryCity=Germany%3AHamburg&addresses%2FcountryCity=Germany%3AHerzogenaurach&addresses%2FcountryCity=Germany%3AHirschaid&addresses%2FcountryCity=Germany%3AH%C3%B6chstadt&addresses%2FcountryCity=Germany%3AHomburg&addresses%2FcountryCity=Germany%3AKappelrodeck&addresses%2FcountryCity=Germany%3AKarlsruhe&addresses%2FcountryCity=Germany%3AKitzingen&addresses%2FcountryCity=Germany%3ALahr&addresses%2FcountryCity=Germany%3ALimbach-Oberfrohna&addresses%2FcountryCity=Germany%3AN%C3%BCrnberg&addresses%2FcountryCity=Germany%3ARegensburg&addresses%2FcountryCity=Germany%3ASchwalbach&addresses%2FcountryCity=Germany%3ASchweinfurt&addresses%2FcountryCity=Hungary%3ADebrecen&addresses%2FcountryCity=Hungary%3ASzombathely&addresses%2FcountryCity=Italy%3AMilan&addresses%2FcountryCity=Italy%3AMomo&addresses%2FcountryCity=Luxembourg%3ALivange&addresses%2FcountryCity=Netherlands%3ABarneveld&addresses%2FcountryCity=Netherlands%3AVaassen&addresses%2FcountryCity=Poland%3ASieroniowice&addresses%2FcountryCity=Poland%3AWroclaw&addresses%2FcountryCity=Portugal%3ACaldas+da+Rainha&addresses%2FcountryCity=Romania%3ABrasov&addresses%2FcountryCity=Romania%3AGhimbav&addresses%2FcountryCity=Romania%3AIasi&addresses%2FcountryCity=Romania%3ASibiu&addresses%2FcountryCity=Romania%3ATimisoara&addresses%2FcountryCity=Slovakia%3AKysuck%C3%A9+Nov%C3%A9+Mesto&addresses%2FcountryCity=Slovakia%3ASkalica&addresses%2FcountryCity=Slovakia%3AZilina&addresses%2FcountryCity=Spain%3AElgoibar&addresses%2FcountryCity=Sweden%3AArlandastad&addresses%2FcountryCity=Sweden%3AG%C3%B6teborg&jobTypeId=RC_jobTypeParent_StudentsTrainees%3ADS&jobTypeId=RC_jobTypeParent_StudentsTrainees%3AIN&jobTypeId=RC_jobTypeParent_StudentsTrainees%3ATH&jobTypeId=RC_jobTypeParent_StudentsTrainees%3ATR&jobTypeId=RC_jobTypeParent_StudentsTrainees%3AWS"

DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Manual\data"
DB_PATH = os.path.join(DATA_FOLDER, "schaeffler_jobs.db")

# We define the JS utility as a prefix so it's perfectly injected in every single execution
JS_QSA_DEEP = """
    function qsaDeep(selector, root = document) {
        let results = Array.from(root.querySelectorAll(selector));
        let els = root.querySelectorAll('*');
        for (let el of els) {
            if (el.shadowRoot) {
                results = results.concat(qsaDeep(selector, el.shadowRoot));
            }
        }
        return results;
    }
"""

CITY_TO_COUNTRY = {
    "Sofia": "Bulgaria", "Frenstat p/R": "Czech Republic", "Lanškroun": "Czech Republic", "Ostrava - Hrušov": "Czech Republic",
    "Praha": "Czech Republic", "Svitavy": "Czech Republic", "Trutnov": "Czech Republic", "Boussens": "France", "Chambery": "France",
    "Foix": "France", "Paris": "France", "Toulouse": "France", "Bebra": "Germany", "Bühl": "Germany",
    "Chemnitz": "Germany", "Dortmund": "Germany", "Erlangen": "Germany", "Frankfurt am Main": "Germany",
    "Gunzenhausen": "Germany", "Halle": "Germany", "Hamburg": "Germany", "Herzogenaurach": "Germany",
    "Hirschaid": "Germany", "Höchstadt": "Germany", "Homburg": "Germany", "Kappelrodeck": "Germany",
    "Karlsruhe": "Germany", "Kitzingen": "Germany", "Lahr": "Germany", "Limbach-Oberfrohna": "Germany",
    "Nürnberg": "Germany", "Regensburg": "Germany", "Schwalbach": "Germany", "Schweinfurt": "Germany",
    "Milan": "Italy", "Momo": "Italy", "Livange": "Luxembourg", "Debrecen": "Hungary", "Szombathely": "Hungary",
    "Barneveld": "Netherlands", "Vaassen": "Netherlands", "Sieroniowice": "Poland", "Wroclaw": "Poland",
    "Caldas da Rainha": "Portugal", "Brasov": "Romania", "Ghimbav": "Romania", "Iasi": "Romania",
    "Sibiu": "Romania", "Timisoara": "Romania", "Kysucké Nové Mesto": "Slovakia", "Skalica": "Slovakia",
    "Zilina": "Slovakia", "Elgoibar": "Spain", "Arlandastad": "Sweden", "Göteborg": "Sweden"
}


def init_db():
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)
    conn = sqlite3.connect(DB_PATH)

    conn.execute('''CREATE TABLE IF NOT EXISTS jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT, url TEXT UNIQUE, title TEXT, 
        company TEXT, location_raw TEXT, city TEXT, country TEXT, description TEXT, category TEXT,
        date_found TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')

    conn.commit()
    conn.close()


def run_scraper():
    init_db()
    print(
        f"🚀 Starting {COMPANY_NAME} Scraper (Ultimate Content Extractor Mode)...")

    options = Options()
    options.add_argument("--window-size=1920,1080")
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    driver = webdriver.Chrome(options=options)

    job_links = []
    unique_urls = set()

    try:
        # --- PHASE 1.A: ESTABLISH SESSION ---
        print("📂 Visiting base domain to establish session & cookies (WAF Bypass)...")
        driver.get(SESSION_URL)
        time.sleep(6)

        try:
            cookie_btn = driver.find_element(
                By.XPATH, "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'accept') or contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'agree')]")
            driver.execute_script("arguments[0].click();", cookie_btn)
            time.sleep(2)
        except:
            pass

        # --- PHASE 1.B: LOAD MASSIVE 300-ITEM URL ---
        print("🎛️ Injecting the pageSize=300 URL to load all jobs at once...")
        driver.get(BASE_URL)
        time.sleep(8)

        try:
            WebDriverWait(driver, 15).until(lambda d: d.execute_script(
                JS_QSA_DEEP + "return qsaDeep('.job-title').length > 0;"))
        except:
            print("🔄 Initial load failed. Forcing a hard refresh...")
            driver.refresh()
            time.sleep(12)

            try:
                WebDriverWait(driver, 15).until(lambda d: d.execute_script(
                    JS_QSA_DEEP + "return qsaDeep('.job-title').length > 0;"))
            except:
                print("⚠️ Still no job cards loaded. Please verify the URL.")
                return

        # --- PHASE 1.C: SMOOTH SCROLL TO RENDER ALL CARDS ---
        print("⏳ Scrolling down smoothly to trigger lazy-loaded HTML...")
        driver.execute_script("""
            let totalHeight = 0;
            let distance = 800;
            let timer = setInterval(() => {
                window.scrollBy(0, distance);
                totalHeight += distance;
                if(totalHeight >= document.body.scrollHeight + 3000){
                    clearInterval(timer);
                }
            }, 400);
        """)
        time.sleep(10)

        total_cards_rendered = driver.execute_script(
            JS_QSA_DEEP + "return qsaDeep('.job-title').length;")
        print(
            f"📥 Master Load Complete: Found {total_cards_rendered} job cards on the page!")

        print("📄 Extracting data from all loaded cards...")

        jobs_on_page = driver.execute_script(JS_QSA_DEEP + """
            let results = [];
            let processed = new Set();
            let titles = qsaDeep('.job-title');
            
            titles.forEach(header => {
                let el = header;
                let aTag = null;
                
                while (el && el !== document) {
                    if (el.tagName && el.tagName.toUpperCase() === 'A') { aTag = el; break; }
                    if (el instanceof DocumentFragment) { el = el.host; } 
                    else { el = el.parentNode; } 
                }
                
                if (!aTag) {
                    let host = header.getRootNode().host;
                    if (host) {
                        aTag = host.querySelector('a');
                        if (!aTag && host.hasAttribute('href')) {
                            let hrefAttr = host.getAttribute('href');
                            aTag = { href: hrefAttr.startsWith('http') ? hrefAttr : window.location.origin + (hrefAttr.startsWith('/') ? '' : '/') + hrefAttr };
                        }
                    }
                }
                
                if (aTag && aTag.href && aTag.href.includes('http')) {
                    let url = aTag.href;
                    if(!processed.has(url)) {
                        processed.add(url);
                        
                        let title = (header.textContent || "").trim();
                        let loc = "N/A";
                        let cat = "Students / Trainees";
                        
                        let card = header;
                        while(card && card !== document && (!card.classList || !card.classList.contains('content'))) {
                            if (card instanceof DocumentFragment) card = card.host;
                            else card = card.parentNode;
                        }
                        
                        if (card) {
                            let pills = Array.from(card.querySelectorAll('.pill'));
                            if (pills.length === 0 && card.shadowRoot) {
                                pills = Array.from(card.shadowRoot.querySelectorAll('.pill'));
                            }
                            if (pills.length > 0) {
                                loc = (pills[pills.length - 1].textContent || "").replace(/\\n/g, '').trim();
                            }
                        }
                        
                        results.push({ url: url, title: title, location_raw: loc, category: cat });
                    }
                }
            });
            return results;
        """)

        for job in jobs_on_page:
            if job['url'] not in unique_urls:
                unique_urls.add(job['url'])
                job_links.append(job)

        print(
            f"✅ Identified {len(job_links)} unique jobs total! Starting deep scrape...")

        if not job_links:
            return

        # --- PHASE 2: DEEP SCRAPING DESCRIPTIONS ---
        conn = sqlite3.connect(DB_PATH)
        wait = WebDriverWait(driver, 10)

        for idx, job in enumerate(job_links, 1):
            try:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT id FROM jobs WHERE url = ?", (job['url'],))
                if cursor.fetchone():
                    print(
                        f"   [{idx}/{len(job_links)}] {job['title']} (Already in DB, skipping...)")
                    continue

                print(f"   [{idx}/{len(job_links)}] {job['title']}")
                driver.get(job['url'])

                try:
                    # 💥 CRITICAL FIX: Wait until the container actually has >100 characters of text!
                    wait.until(lambda d: d.execute_script(JS_QSA_DEEP + """
                        let els = qsaDeep('section.text, .desc, .job-description');
                        for(let e of els) {
                            if((e.textContent || "").trim().length > 100) return true;
                        }
                        return false;
                    """))
                    time.sleep(1)  # Give formatting an extra second to settle
                except:
                    time.sleep(2)  # Fallback wait

                description = driver.execute_script(JS_QSA_DEEP + """
                    let containers = qsaDeep('section.text, .desc, .job-description');
                    let bestText = "";
                    
                    for (let c of containers) {
                        // Priority 1: innerText. It automatically formats bullet points and paragraphs perfectly!
                        let text = c.innerText || "";
                        
                        // Priority 2 (Fallback): If innerText is empty due to Shadow DOM visibility rules, parse innerHTML manually
                        if (text.trim().length < 50) {
                            let html = c.innerHTML || "";
                            text = html.replace(/<br\\s*[\\/]?>/gi, "\\n")
                                       .replace(/<\\/p>/gi, "\\n\\n")
                                       .replace(/<\\/div>/gi, "\\n")
                                       .replace(/<\\/h[1-6]>/gi, "\\n\\n")
                                       .replace(/<li>/gi, "\\n- ")
                                       .replace(/<[^>]+>/g, " ");
                                       
                            // Decode basic HTML entities safely
                            let doc = new DOMParser().parseFromString(text, "text/html");
                            text = doc.documentElement.textContent || text;
                        }
                        
                        // Pick the container with the most content that IS NOT the dummy template
                        if (!text.includes('Lorem ipsum')) {
                            if (text.length > bestText.length) {
                                bestText = text;
                            }
                        }
                    }
                    return bestText.trim();
                """)

                # --- Python-side Formatting & Cleanup ---
                # Fix encoding errors for bullet points (converts •, ·, to standard dashes)
                clean_desc = description.replace(
                    '•', '-').replace('·', '-').replace('\xa0', ' ')

                # Clean up excess spaces and normalize line breaks
                clean_desc = re.sub(r' +', ' ', clean_desc)
                clean_desc = re.sub(r'\n[ \t]+\n', '\n\n', clean_desc)
                clean_desc = re.sub(r'\n{3,}', '\n\n', clean_desc).strip()

                # Safely remove the "Search by Keyword" header if it got caught in the crossfire
                if "Search by Keyword" in clean_desc and "Clear" in clean_desc:
                    clear_idx = clean_desc.find("Clear")
                    if clear_idx < 1500:  # Ensure we don't accidentally delete real descriptions
                        clean_desc = clean_desc[clear_idx +
                                                len("Clear"):].strip()

                # Remove generic ATS footer strings
                markers_to_remove = ["Find similar jobs:",
                                     "Job Segment:", "Keywords:"]
                for marker in markers_to_remove:
                    if marker in clean_desc:
                        clean_desc = clean_desc.split(marker)[0].strip()

                location_raw = job.get('location_raw', 'N/A')
                city = location_raw
                country = "N/A"

                for mapped_city, mapped_country in CITY_TO_COUNTRY.items():
                    if mapped_city.lower() in location_raw.lower():
                        city = mapped_city
                        country = mapped_country
                        break

                conn.execute('''INSERT INTO jobs (url, title, company, location_raw, city, country, description, category) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                             (job['url'], job['title'], COMPANY_NAME, location_raw, city, country, clean_desc, job['category']))
                conn.commit()

            except Exception as e:
                print(f"      ⚠️ Error processing job {idx}: {e}")

        conn.close()
        print(f"\n✨ SUCCESS! {COMPANY_NAME} jobs saved successfully.")

    finally:
        try:
            driver.quit()
        except:
            pass


if __name__ == "__main__":
    run_scraper()
