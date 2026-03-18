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
COMPANY_NAME = "NIX_Tech"
BASE_URL = "https://nixstech.com/hu/allasok/"
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "nix_jobs.db")


def init_db():
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)
    conn = sqlite3.connect(DB_PATH)
    conn.execute('''CREATE TABLE IF NOT EXISTS jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT, url TEXT UNIQUE, title TEXT, 
        company TEXT, tags TEXT, description TEXT, 
        date_found TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    conn.close()


def run_scraper():
    init_db()
    print(f"🚀 {COMPANY_NAME} Scraper indítása...")

    options = uc.ChromeOptions()
    options.add_argument("--window-size=1920,1080")
    # Verzió fix a SessionNotCreatedException ellen
    driver = uc.Chrome(options=options, version_main=145)
    wait = WebDriverWait(driver, 15)

    try:
        driver.get(BASE_URL)
        time.sleep(5)

        # --- 1. FÁZIS: ÖSSZES ÁLLÁS KIBONTÁSA ---
        while True:
            try:
                # Megkeressük a gombot
                btn_elements = driver.find_elements(
                    By.CLASS_NAME, "pagination-btn")
                if not btn_elements or not btn_elements[0].is_displayed():
                    break

                # Ellenőrizzük az oldalszámot a HTML attribútumból
                current = btn_elements[0].get_attribute("data-current-page")
                max_pg = btn_elements[0].get_attribute("data-max-page")
                print(f"   ⏳ Oldalak betöltése: {current} / {max_pg}")

                if current == max_pg:
                    break

                # Görgetés és kattintás
                driver.execute_script(
                    "arguments[0].scrollIntoView({block: 'center'});", btn_elements[0])
                time.sleep(1)
                driver.execute_script("arguments[0].click();", btn_elements[0])

                # Várunk, amíg a "current-page" érték megnő
                wait.until(lambda d: d.find_element(
                    By.CLASS_NAME, "pagination-btn").get_attribute("data-current-page") != current)
                time.sleep(2)
            except:
                break

        # --- 2. FÁZIS: ADATOK GYŰJTÉSE ---
        items = driver.find_elements(By.CLASS_NAME, "post-item")
        job_data = []
        for item in items:
            try:
                url = item.get_attribute("data-href")
                title = item.find_element(
                    By.CLASS_NAME, "vacancies-card__title").text.strip()
                # Tegeket (Python, 2+ év stb.) összefűzzük
                tags_elements = item.find_elements(
                    By.CLASS_NAME, "tag-list-container-list__tag")
                tags_str = ", ".join([t.text.strip() for t in tags_elements])

                job_data.append({"url": url, "title": title, "tags": tags_str})
            except:
                continue

        print(f"✅ {len(job_data)} állás azonosítva. Részletek letöltése...")

        # --- 3. FÁZIS: MÉLYFÚRÁS ---
        conn = sqlite3.connect(DB_PATH)
        for idx, job in enumerate(job_data, 1):
            if conn.execute("SELECT 1 FROM jobs WHERE url = ?", (job['url'],)).fetchone():
                continue

            print(f"   [{idx}/{len(job_data)}] {job['title']}")
            driver.get(job['url'])
            time.sleep(3)

            # Leírás kinyerése
            desc = driver.execute_script(
                "return document.querySelector('.career-post__content') ? document.querySelector('.career-post__content').innerText : document.body.innerText;")

            conn.execute('INSERT INTO jobs (url, title, company, tags, description) VALUES (?, ?, ?, ?, ?)',
                         (job['url'], job['title'], "NIX Tech", job['tags'], desc.strip()))
            conn.commit()

        conn.close()
        print(f"✨ KÉSZ! NIX Tech adatok elmentve.")

    finally:
        driver.quit()


if __name__ == "__main__":
    run_scraper()
