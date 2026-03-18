import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import sqlite3
import time
import sys

# --- Beállítások ---
URL = "https://hungexpo.hu/karrier/"
DB_PATH = "hungexpo_jobs.db"
CHROME_VERSION = 145  # A hibaüzeneted alapján ezt hagyd így


def save_to_db(job_data):
    """Egy állás mentése az adatbázisba."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS jobs 
                          (id INTEGER PRIMARY KEY AUTOINCREMENT, 
                           title TEXT, 
                           location TEXT,
                           description TEXT, 
                           url TEXT,
                           company TEXT,
                           date_found TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')

        cursor.execute("""INSERT INTO jobs (title, location, description, url, company) 
                          VALUES (?, ?, ?, ?, ?)""",
                       (job_data['title'], job_data['location'],
                        job_data['description'], job_data['url'], "Hungexpo"))

        conn.commit()
        conn.close()
    except Exception as e:
        print(f"❌ Adatbázis hiba: {e}")


def run_scraper():
    options = uc.ChromeOptions()
        options.add_argument("--window-size=1920,1080")

    print(f"🚀 Driver indítása (v{CHROME_VERSION})...")
    try:
        options.add_argument("--headless=new")
    driver = uc.Chrome(options=options)
    except Exception as e:
        print(f"❌ Hiba: {e}")
        return

    try:
        print(f"🔗 Főoldal betöltése: {URL}")
        driver.get(URL)
        wait = WebDriverWait(driver, 15)

        # Megvárjuk, amíg az állásblokkok betöltődnek
        wait.until(EC.presence_of_element_located(
            (By.CLASS_NAME, "block-list__item")))

        # 1. Linkek és alap adatok összegyűjtése a főoldalról
        job_links = []
        items = driver.find_elements(By.CLASS_NAME, "block-list__item")

        for item in items:
            try:
                link_el = item.find_element(By.TAG_NAME, "a")
                title = link_el.text.strip()
                href = link_el.get_attribute("href")

                # A helyszín a harmadik oszlopban van (col-lg-3)
                cols = item.find_elements(By.CLASS_NAME, "col-lg-3")
                location = cols[1].text.strip() if len(
                    cols) > 1 else "Budapest"

                job_links.append({
                    "title": title,
                    "url": href,
                    "location": location
                })
            except:
                continue

        print(f"🔍 Talált állások száma: {len(job_links)}")

        # 2. Minden link meglátogatása a leírásért
        for i, job in enumerate(job_links):
            print(f"   [{i+1}/{len(job_links)}] Navigálás: {job['title']}")

            driver.get(job['url'])
            time.sleep(2)  # Várunk a tartalomra

            try:
                # Megkeressük a fő tartalmi részt (Hungexpo-nál általában 'article' vagy specifikus div)
                # A leírás kinyerése BeautifulSoup-al a tisztább szövegért
                page_source = driver.page_source
                soup = BeautifulSoup(page_source, 'html.parser')

                # Megpróbáljuk megkeresni a hirdetés törzsét
                # A Hungexpo-nál ez gyakran az 'entry-content' vagy hasonló div-ben van
                content_div = soup.find('div', class_='entry-content') or \
                    soup.find('article') or \
                    soup.find('main')

                if content_div:
                    description = content_div.get_text(
                        separator="\n", strip=True)
                else:
                    description = "Nem sikerült kinyerni a leírást."

                job['description'] = description

                # Mentés azonnal (így ha megszakad, megvannak az eddigiek)
                save_to_db(job)

            except Exception as e:
                print(f"   ⚠️ Hiba a leírás kinyerésekor: {e}")

        print("\n✅ Kész! Minden állás elmentve az adatbázisba.")

    except Exception as e:
        print(f"💥 Kritikus hiba: {e}")
    finally:
        driver.quit()


if __name__ == "__main__":
    run_scraper()
