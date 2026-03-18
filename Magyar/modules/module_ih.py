import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import sqlite3
import os
import sys
import time
import re
from urllib.parse import urljoin

sys.stdout.reconfigure(encoding='utf-8')

# --- KONFIGURÁCIÓ ---
COMPANY_NAME = "Informacios_Hivatal"
BASE_URL = "https://karrier.ih.gov.hu/index.php/nyitott-poziciok/"
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "ih_jobs.db")
CHROME_VERSION = 145


def init_db():
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)
    conn = sqlite3.connect(DB_PATH)
    conn.execute('''CREATE TABLE IF NOT EXISTS jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT, url TEXT UNIQUE, title TEXT, 
        company TEXT, location_raw TEXT, city TEXT, description TEXT, 
        date_found TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    conn.close()


def run_scraper():
    init_db()
    print(f"🚀 {COMPANY_NAME} Scraper indítása (Deep Link Extraction)...")

    options = uc.ChromeOptions()
    options.add_argument("--window-size=1920,1080")
    driver = uc.Chrome(options=options, version_main=CHROME_VERSION)

    job_links = []

    try:
        driver.get(BASE_URL)
        time.sleep(5)

        # --- 1. FÁZIS: KATEGÓRIÁK NYITÁSA ---
        categories = driver.find_elements(
            By.CSS_SELECTOR, ".accordion-item > .accordion-header > button")

        for i in range(len(categories)):
            categories = driver.find_elements(
                By.CSS_SELECTOR, ".accordion-item > .accordion-header > button")
            cat_btn = categories[i]

            if "collapsed" in cat_btn.get_attribute("class"):
                driver.execute_script("arguments[0].click();", cat_btn)
                time.sleep(1.5)

            # --- 2. FÁZIS: POZÍCIÓK NYITÁSA ---
            target_id = cat_btn.get_attribute(
                "data-bs-target").replace("#", "")
            parent_panel = driver.find_element(By.ID, target_id)
            job_btns = parent_panel.find_elements(
                By.CSS_SELECTOR, ".accordion-button")

            for j in range(len(job_btns)):
                try:
                    job_btns = parent_panel.find_elements(
                        By.CSS_SELECTOR, ".accordion-button")
                    job_btn = job_btns[j]
                    job_title = job_btn.text.strip()

                    if "collapsed" in job_btn.get_attribute("class"):
                        driver.execute_script("arguments[0].click();", job_btn)
                        time.sleep(1.5)

                    # --- 3. FÁZIS: A "INFORMÁCIÓK A POZÍCIÓRÓL" GOMB KERESÉSE ---
                    # A gomb a lenyílt panelben van
                    job_panel_id = job_btn.get_attribute(
                        "data-bs-target").replace("#", "")
                    job_panel = driver.find_element(By.ID, job_panel_id)

                    info_link_el = job_panel.find_element(
                        By.CSS_SELECTOR, "a.jelentkezes-gomb")
                    final_url = info_link_el.get_attribute("href")

                    if final_url:
                        job_links.append(
                            {"url": final_url, "title": job_title})
                        print(f"   🔗 Talált link: {job_title} -> {final_url}")

                except Exception as e:
                    continue

        print(
            f"✅ Összesen {len(job_links)} mélylink begyűjtve. Kezdődik a tartalom letöltése...")

        # --- 4. FÁZIS: ADATOK KINYERÉSE AZ ALOLDALAKRÓL ---
        conn = sqlite3.connect(DB_PATH)
        for idx, job in enumerate(job_links, 1):
            try:
                print(f"   [{idx}/{len(job_links)}] {job['title']}")
                driver.get(job['url'])
                time.sleep(4)

                # Speciális tartalom-kinyerő: az IH aloldalakon a szöveg gyakran section-ökben van
                description = driver.execute_script("""
                    function walk(el) {
                        let text = "";
                        if (el.nodeType === 3) text += el.nodeValue.trim() + " ";
                        else if (el.nodeType === 1) {
                            if (['SCRIPT','STYLE','NAV','FOOTER'].includes(el.tagName)) return "";
                            for (let child of el.childNodes) text += walk(child);
                            if (['P','DIV','BR','LI','H1','H2','H3'].includes(el.tagName)) text += "\\n";
                        }
                        return text;
                    }
                    // Megpróbáljuk a fő cikktörzset megcélozni
                    let container = document.querySelector('article') || document.querySelector('.entry-content') || document.body;
                    return walk(container);
                """)

                clean_desc = re.sub(
                    r'\n\s*\n', '\n\n', re.sub(r'[ \t]+', ' ', description)).strip()

                conn.execute('''INSERT OR IGNORE INTO jobs (url, title, company, location_raw, city, description) 
                                VALUES (?, ?, ?, ?, ?, ?)''',
                             (job['url'], job['title'], "Információs Hivatal", "Budapest", "Budapest", clean_desc))
                conn.commit()

            except Exception as e:
                print(f"      ⚠️ Hiba: {e}")

        conn.close()
        print(f"\n✨ KÉSZ! Az összes IH leírás kimentve.")

    finally:
        driver.quit()


if __name__ == "__main__":
    run_scraper()
