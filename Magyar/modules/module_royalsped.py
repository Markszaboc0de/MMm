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

sys.stdout.reconfigure(encoding='utf-8')

# --- KONFIGURÁCIÓ ---
COMPANY_NAME = "Royal_Sped"
BASE_URL = "https://royalsped.eu/karrier/allasajanlataink/"
DATA_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
DB_PATH = os.path.join(DATA_FOLDER, "royalsped_jobs.db")


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
    print(f"🚀 {COMPANY_NAME} Scraper indítása (Direct Card-Extraction mód)...")

    # Mivel nem kell aloldalakat nyitogatni, szupergyors lesz
    driver = get_chrome_driver()

    try:
        driver.get(BASE_URL)
        time.sleep(5)  # Várakozás az oldal betöltésére

        # Kártyák megkeresése
        job_cards = driver.find_elements(By.CSS_SELECTOR, "a.banner-item")
        print(f"✅ {len(job_cards)} állás kártya azonosítva az oldalon.")

        conn = sqlite3.connect(DB_PATH)

        for idx, card in enumerate(job_cards, 1):
            try:
                # Alapadatok kinyerése a kártyáról
                url = card.get_attribute("href")
                raw_title = card.find_element(
                    By.CSS_SELECTOR, ".banner-title").text.strip()

                # Leírás kinyerése a <p> tagből
                try:
                    short_desc = card.find_element(
                        By.TAG_NAME, "p").text.strip()
                except:
                    short_desc = "Nincs rövid leírás megadva."

                # Helyszín (Város) és tiszta Cím szétválasztása
                # Pl: "Vámügyintéző – Röszke" -> title: "Vámügyintéző", city: "Röszke"
                city = "Budapest"  # Alapértelmezett (Központ)
                title = raw_title

                # Regex a különféle kötőjelekhez (sima -, nagykötőjel –)
                split_match = re.split(r'\s*[-–]\s*', raw_title)
                if len(split_match) > 1:
                    city = split_match[-1].strip()
                    title = split_match[0].strip()

                # Leírás formázása: Rövid szöveg + PDF hivatkozás
                description = f"{short_desc}\n\n[A teljes leírás ezen a linken érhető el: {url}]"

                # Mentés (Ha már van ilyen, felülírjuk a friss adatokkal)
                conn.execute('''INSERT OR REPLACE INTO jobs (url, title, company, city, description, category) 
                                VALUES (?, ?, ?, ?, ?, ?)''',
                             (url, title, "Royal Sped", city, description, "Logisztika / Vám"))
                conn.commit()
                print(f"   [{idx}/{len(job_cards)}] {title} ({city}) ✅")

            except Exception as e:
                print(f"      ⚠️ Hiba a(z) {idx}. kártyánál: {e}")

        conn.close()
        print(f"\n✨ SIKER! A Royal Sped állások elmentve a kártyák alapján.")

    finally:
        driver.quit()


if __name__ == "__main__":
    run_scraper()
