from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

import sys as _sys
import os as _os
_sys.path.append(_os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))
from driver_setup import get_chrome_driver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import sqlite3
import os
import time
import sys

# --- Beállítások ---
URL = "https://granitbank.hu/karrier#allashirdetesek"
DATA_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
DB_PATH = os.path.join(DATA_FOLDER, "granit_jobs.db")
CHROME_VERSION = 145


def init_db():
    """Létrehozza a mappát és az adatbázist a megfelelő oszlopokkal."""
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    # Most már van city, country és location_raw is!
    cursor.execute('''CREATE TABLE IF NOT EXISTS jobs 
                      (id INTEGER PRIMARY KEY AUTOINCREMENT, 
                       url TEXT,
                       title TEXT, 
                       company TEXT,
                       location_raw TEXT,
                       city TEXT,
                       country TEXT,
                       description TEXT, 
                       date_found TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    conn.commit()
    conn.close()


def run_scraper():
    print(f"🚀 Gránit Bank Scraper indítása (DATA_FOLDER: {DATA_FOLDER})...")
    init_db()

    driver = get_chrome_driver()
    try:
        driver.get(URL)
        wait = WebDriverWait(driver, 20)
        wait.until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, 'button[data-radix-collection-item]')))
        time.sleep(3)

        buttons = driver.find_elements(
            By.CSS_SELECTOR, 'button[data-radix-collection-item]')
        print(f"🔍 Talált pozíciók: {len(buttons)}")

        results = []

        for i in range(len(buttons)):
            try:
                current_btns = driver.find_elements(
                    By.CSS_SELECTOR, 'button[data-radix-collection-item]')
                btn = current_btns[i]

                job_title = btn.find_element(By.TAG_NAME, 'h3').text.strip()
                content_id = btn.get_attribute("aria-controls")

                print(f"   [{i+1}/{len(buttons)}] Feldolgozás: {job_title}")

                driver.execute_script(
                    "arguments[0].scrollIntoView({block: 'center'});", btn)
                if btn.get_attribute("data-state") == "closed":
                    driver.execute_script("arguments[0].click();", btn)

                wait.until(EC.visibility_of_element_located(
                    (By.ID, content_id)))
                time.sleep(1)

                content_div = driver.find_element(By.ID, content_id)
                inner_html = content_div.get_attribute('innerHTML')
                soup = BeautifulSoup(inner_html, 'html.parser')

                # Takarítás (gombok, képek kiszedése)
                for tag in soup.find_all(['button', 'svg', 'img']):
                    tag.decompose()
                description = soup.get_text(separator="\n", strip=True)

                results.append({
                    'title': job_title,
                    'url': URL,  # A gránitnál nincs külön aloldal URL, marad a főoldal
                    'description': description,
                    'city': 'Budapest',
                    'location_raw': 'Budapest, Magyarország'
                })

                driver.execute_script("arguments[0].click();", btn)

            except Exception as e:
                print(f"   ⚠️ Hiba a(z) {i}. elemnél: {e}")

        # Mentés az új struktúra szerint
        if results:
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            for item in results:
                cursor.execute("""INSERT INTO jobs (url, title, company, location_raw, city, country, description) 
                                  VALUES (?, ?, ?, ?, ?, ?, ?)""",
                               (item['url'], item['title'], "Granit Bank", item['location_raw'],
                                item['city'], "Magyarország", item['description']))
            conn.commit()
            conn.close()
            print(f"✅ Mentve {len(results)} állás.")

    finally:
        driver.quit()


if __name__ == "__main__":
    run_scraper()
