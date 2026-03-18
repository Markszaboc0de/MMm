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

COMPANY_NAME = "MOL_Group"
BASE_URL = "https://molgroup.taleo.net/careersection/external/jobsearch.ftl?"
DOMAIN_URL = "https://molgroup.taleo.net"
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "mol_jobs.db")


def init_db():
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)
    conn = sqlite3.connect(DB_PATH)
    conn.execute('''CREATE TABLE IF NOT EXISTS jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT, url TEXT UNIQUE, title TEXT, 
        company TEXT, city TEXT, description TEXT, date_posted TEXT,
        date_found TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    conn.close()


def run_scraper():
    init_db()
    print(f"🚀 {COMPANY_NAME} Scraper indítása (Robust Pagination mód)...")

    options = uc.ChromeOptions()
    options.add_argument("--window-size=1920,1080")
    # A Taleo érzékeny a botokra, maradunk az undetected-nél
    options.add_argument("--headless=new")
    driver = uc.Chrome(options=options)

    job_links = []

    try:
        driver.get(BASE_URL)
        time.sleep(8)  # Hagyjunk időt a Taleo belső scriptjeinek

        page_num = 1
        while True:
            # 1. Várjuk meg, amíg a lista tényleg betöltődik
            try:
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "tr[id^='job']"))
                )
            except:
                print("❌ Nem találtam állásokat az oldalon. Megállás.")
                break

            current_rows = driver.find_elements(
                By.CSS_SELECTOR, "tr[id^='job']")
            print(f"📄 {page_num}. oldal: {len(current_rows)} állás észleléve.")

            # Mentjük az aktuális első állás szövegét, hogy tudjuk, mikor váltott az oldal
            first_job_title_before = current_rows[0].text

            # Adatok begyűjtése az aktuális oldalról
            for row in current_rows:
                try:
                    link_el = row.find_element(
                        By.CSS_SELECTOR, "th[scope='row'] a")
                    url = urljoin(DOMAIN_URL, link_el.get_attribute("href"))
                    title = link_el.text.strip()
                    cells = row.find_elements(By.TAG_NAME, "td")
                    location = cells[1].text.strip() if len(
                        cells) > 1 else "N/A"
                    job_links.append(
                        {"url": url, "title": title, "city": location})
                except:
                    continue

            # 2. Lapozás megkísérlése
            try:
                next_btn = driver.find_element(By.ID, "next")
                is_disabled = next_btn.get_attribute("aria-disabled")

                if is_disabled == "true" or "disabled" in next_btn.get_attribute("class"):
                    print("🏁 Ez volt az utolsó oldal.")
                    break

                # JS alapú kattintás, mert a Selenium .click() néha elnyelődik a Taleo-ban
                driver.execute_script("arguments[0].click();", next_btn)
                print("   🖱️ Lapozás folyamatban...")

                # Trükkös várakozás: addig várunk, amíg az első sor szövege meg nem változik
                success = False
                for _ in range(15):  # Max 15 másodperc várakozás
                    time.sleep(1)
                    new_rows = driver.find_elements(
                        By.CSS_SELECTOR, "tr[id^='job']")
                    if new_rows and new_rows[0].text != first_job_title_before:
                        success = True
                        break

                if not success:
                    print(
                        "⚠️ Az oldal nem frissült 15 mp után sem. Megpróbáljuk folytatni...")
                    # Ha beragadt, megpróbálunk egy kényszerített scroll-t, hátha az segít az AJAX-nak
                    driver.execute_script("window.scrollTo(0, 0);")

                page_num += 1
                time.sleep(2)

            except Exception as e:
                print(f"ℹ️ Megállt a lapozás: {e}")
                break

        # 3. Mélyfúrás (Adatbázisba mentés)
        print(f"✅ Begyűjtve: {len(job_links)} link. Adatok letöltése...")
        conn = sqlite3.connect(DB_PATH)
        for idx, job in enumerate(job_links, 1):
            if conn.execute("SELECT 1 FROM jobs WHERE url = ?", (job['url'],)).fetchone():
                continue

            print(f"   [{idx}/{len(job_links)}] {job['title']}")
            driver.get(job['url'])
            time.sleep(4)

            # A Taleo leírás kinyerése
            desc = driver.execute_script(
                "return document.querySelector('.editablesection') ? document.querySelector('.editablesection').innerText : document.body.innerText;")

            conn.execute('INSERT INTO jobs (url, title, company, city, description) VALUES (?, ?, ?, ?, ?)',
                         (job['url'], job['title'], "MOL Group", job['city'], desc.strip()))
            conn.commit()
        conn.close()

    finally:
        driver.quit()


if __name__ == "__main__":
    run_scraper()
