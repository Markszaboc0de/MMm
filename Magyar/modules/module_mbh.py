from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

import sys as _sys
import os as _os
_sys.path.append(_os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))
from driver_setup import get_chrome_driver
from selenium.webdriver.common.by import By
import sqlite3
import os
import sys
import time
import re
from urllib.parse import urljoin

sys.stdout.reconfigure(encoding='utf-8')

# --- KONFIGURÁCIÓ ---
COMPANY_NAME = "MBH_Bank_All"
MAIN_URL = "https://karrier.mbhbank.hu/Datacenter/Registration/JobAdvertisements/allasok"
DATA_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
DB_PATH = os.path.join(DATA_FOLDER, "mbh_jobs.db")


def init_db():
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)
    conn = sqlite3.connect(DB_PATH)
    conn.execute('''CREATE TABLE IF NOT EXISTS jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT, url TEXT UNIQUE, title TEXT, 
        company TEXT, city TEXT, description TEXT, category TEXT,
        date_found TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    conn.close()


def run_scraper():
    init_db()
    print(f"🚀 {COMPANY_NAME} Scraper indítása (Multi-Category mód)...")

    driver = get_chrome_driver()

    category_links = []

    try:
        # --- 1. LÉPÉS: KATEGÓRIÁK ÖSSZEGYŰJTÉSE ---
        print("📂 Kategória linkek keresése...")
        driver.get(MAIN_URL)
        time.sleep(5)

        # A beküldött HTML alapján keressük az 'a' tageket a tartalom divben
        cat_elements = driver.find_elements(
            By.CSS_SELECTOR, ".positionList__description__content a")
        for el in cat_elements:
            href = el.get_attribute("href")
            if href and "/JobAdvertisements/" in href and href != MAIN_URL:
                category_links.append(href)

        # Tisztítás (duplikációk kiszűrése)
        category_links = list(set(category_links))
        print(
            f"✅ Talált kategóriák ({len(category_links)} db): {category_links}")

        all_job_links = []

        # --- 2. LÉPÉS: ÁLLÁSOK GYŰJTÉSE KATEGÓRIÁNKÉNT ---
        for cat_url in category_links:
            cat_name = cat_url.split('/')[-1]
            print(f"⏳ Állások gyűjtése: {cat_name}...")
            driver.get(cat_url)
            time.sleep(4)

            rows = driver.find_elements(
                By.CSS_SELECTOR, ".positionList__positionRow--selectable")
            for row in rows:
                try:
                    url = row.get_attribute("data-position-url")
                    title = row.find_element(
                        By.CSS_SELECTOR, "[data-e2e-testing*='Name']").text.strip()
                    city = row.find_element(
                        By.CSS_SELECTOR, "[data-e2e-testing*='LocationOfWork']").text.strip()

                    if url:
                        all_job_links.append({
                            "url": url, "title": title, "city": city, "category": cat_name
                        })
                except:
                    continue
            print(f"   📥 Eddig összesen: {len(all_job_links)} állás")

        # --- 3. LÉPÉS: EGYÉNI OLDALAK SCRAPELÉSE ---
        conn = sqlite3.connect(DB_PATH)
        for idx, job in enumerate(all_job_links, 1):
            try:
                # MÓDOSÍTÁS: Töröljük a már bent lévő rossz hirdetéseket, hogy felülírhassuk a jóval!
                conn.execute("DELETE FROM jobs WHERE url = ?", (job['url'],))

                print(
                    f"   [{idx}/{len(all_job_links)}] {job['title']} ({job['category']})")
                driver.get(job['url'])
                # Kicsit több időt hagyunk a JavaScript renderelésre
                time.sleep(4)

                # JAVÍTOTT LEÍRÁS KINYERŐ (A képernyőkép és a JS Path alapján)
                description = driver.execute_script("""
                    function walk(el) {
                        let text = "";
                        if (!el) return "";
                        if (el.nodeType === 3) text += el.nodeValue.trim() + " ";
                        else if (el.nodeType === 1) {
                            let tag = el.tagName.toUpperCase();
                            if (['SCRIPT','STYLE','NAV','FOOTER'].includes(tag)) return "";
                            if (tag === 'LI') text += "• "; // Lista golyó hozzáadása
                            for (let child of el.childNodes) text += walk(child);
                            if (['P','DIV','BR','LI','H1','H2','H3'].includes(tag)) text += "\\n";
                        }
                        return text;
                    }
                    // Pontosan a bekeretezett részt célozzuk meg
                    let container = document.querySelector('#jobadvertisementList > div > div:nth-child(2)') || 
                                    document.querySelector('.positionList__section[style*="word-wrap"]');
                    return walk(container);
                """)

                clean_desc = re.sub(
                    r'\n\s*\n', '\n\n', re.sub(r'[ \t]+', ' ', description)).strip()

                conn.execute('''INSERT INTO jobs (url, title, company, city, description, category) 
                                VALUES (?, ?, ?, ?, ?, ?)''',
                             (job['url'], job['title'], "MBH Bank", job['city'], clean_desc, job['category']))
                conn.commit()

            except Exception as e:
                print(f"      ⚠️ Hiba: {e}")

        conn.close()
        print(f"\n✨ KÉSZ! Az összes MBH kategória feldolgozva.")

    finally:
        driver.quit()


if __name__ == "__main__":
    run_scraper()
