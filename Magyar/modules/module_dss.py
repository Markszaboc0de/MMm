import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import sqlite3
import os
import sys
import time

# Windows terminál UTF-8 kódolásának kikényszerítése
sys.stdout.reconfigure(encoding='utf-8')

# ==========================================
# ⚙️ CONFIGURATION
# ==========================================
COMPANY_NAME = "DSS Consulting"
BASE_URL = "https://dss.hu/karrier/"

DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "dss_jobs.db")


def init_db():
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        url TEXT UNIQUE NOT NULL,
        title TEXT NOT NULL,
        company TEXT NOT NULL,
        location_raw TEXT,
        city TEXT,
        country TEXT,
        description TEXT
    )
    ''')
    conn.commit()
    conn.close()


def run_scraper():
    print(f"   🏢 Scraper indítása: {COMPANY_NAME} (Iframe áttörő mód)...")
    init_db()

    options = uc.ChromeOptions()
    # Rejtett mód (vedd ki a hashtaget, ha látni akarod)
    options.add_argument('--headless=new')
    options.add_argument("--window-size=1920,1080")

    try:
        driver = uc.Chrome(options=options, version_main=145)
    except Exception as e:
        print(f"   ❌ Hiba a Chrome elindításakor: {e}")
        return

    try:
        driver.get(BASE_URL)
        print("   ⏳ Várakozás a Zoho Recruit keret betöltésére...")
        time.sleep(5)  # Hagyunk időt az iframe betöltésének

        # 1. 🧠 THE FIX: Keresünk egy iframe-et, és belépünk (Switch) a belsejébe!
        iframes = driver.find_elements(By.TAG_NAME, "iframe")
        iframe_found = False

        for iframe in iframes:
            src = iframe.get_attribute('src')
            if src and 'recruit' in src.lower():
                driver.switch_to.frame(iframe)
                iframe_found = True
                print("   🚪 Sikeresen beléptünk az iframe-be!")
                break

        if not iframe_found:
            # Ha nincs iframe, megpróbáljuk a főoldalon (hátha proxyzzák)
            pass

        # 2. Várjuk meg a táblázat sorait
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "tr.jobDetailRow"))
            )
        except:
            print(
                f"   ⚠️ Nem találtuk meg a 'jobDetailRow' osztályú sorokat a(z) {COMPANY_NAME} oldalon.")
            return

        # 3. Kinyerjük a sorokat a Selenium segítségével
        rows = driver.find_elements(By.CSS_SELECTOR, "tr.jobDetailRow")
        job_targets = []

        for row in rows:
            try:
                a_tag = row.find_element(By.CSS_SELECTOR, "a.jobdetail")
                job_title = a_tag.text.strip()

                # A Selenium hatalmas előnye: a get_attribute("href") megoldja a relatív URL-eket a háttérben!
                job_url = a_tag.get_attribute("href")

                tds = row.find_elements(By.TAG_NAME, "td")
                city = "Budapest"  # Alapértelmezett

                # A HTML-ed alapján a 2. oszlop (index 1) a város
                if len(tds) >= 2:
                    city_text = tds[1].text.strip()
                    if city_text:
                        city = city_text

                job_targets.append({
                    "url": job_url,
                    "title": job_title,
                    "city": city,
                    "country": "Hungary"
                })
            except Exception as e:
                pass  # Ha egy sor hibás lenne, átlépjük

        # Duplikációk szűrése
        job_targets = [dict(t)
                       for t in {tuple(d.items()) for d in job_targets}]

        if not job_targets:
            print("   ⚠️ Nem találtunk állásokat. Kilépés.")
            return

        print(
            f"   🔍 {len(job_targets)} állás megtalálva az iframe-ben. Részletek kinyerése...")

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        new_jobs_added = 0

        # 4. Végigmegyünk az állás URL-eken (Itt már kilépünk az iframe-ből az új oldalakra)
        for job in job_targets:
            job_url = job["url"]
            title = job["title"]
            city = job["city"]
            country = job["country"]

            try:
                driver.get(job_url)
                time.sleep(3)  # A Zoho belső oldalai is tölthetnek egy picit

                job_soup = BeautifulSoup(driver.page_source, 'html.parser')
                description_lines = []

                # Zoho Recruit általános konténerei
                main_content = job_soup.find('div', class_='jobDesc') or job_soup.find(
                    'div', class_='job-description') or job_soup.find('main') or job_soup.find('form') or job_soup.find('body')

                if main_content:
                    for tag in main_content.find_all(['p', 'h2', 'h3', 'h4', 'ul', 'ol', 'div']):

                        classes = str(tag.get('class', '')).lower()
                        if any(nav in classes for nav in ['nav', 'menu', 'footer', 'header', 'apply']):
                            continue

                        # Szűrés a div-ekre, hogy ne ismétlődjenek, ha p vagy ul van bennük
                        if tag.name == 'div' and tag.find(['p', 'ul', 'ol']):
                            continue

                        if tag.name in ['ul', 'ol']:
                            for li in tag.find_all('li'):
                                li_text = li.get_text(strip=True)
                                if li_text:
                                    description_lines.append(f"- {li_text}")
                            description_lines.append("")
                        else:
                            text = tag.get_text(strip=True)
                            if text and text not in description_lines and text != title:
                                description_lines.append(text)

                description = "\n".join(description_lines).strip()
                if not description:
                    description = "A leírás kinyerése nem sikerült."

                location_raw = f"{city}, {country}"

                cursor.execute('''
                    INSERT OR IGNORE INTO jobs (url, title, company, location_raw, city, country, description)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (job_url, title, COMPANY_NAME, location_raw, city, country, description))

                if cursor.rowcount > 0:
                    new_jobs_added += 1

            except Exception as e:
                print(
                    f"   ❌ Hiba az állás feldolgozása közben ({job_url}): {e}")

        conn.commit()
        conn.close()

        print(
            f"   ✅ {COMPANY_NAME} kész! {new_jobs_added} új állás lementve az adatbázisba.")

    except Exception as e:
        print(
            f"   ❌ Kritikus hiba a(z) {COMPANY_NAME} oldal futtatása közben: {e}")
    finally:
        try:
            if 'driver' in locals():
                driver.quit()
        except OSError:
            pass  # WinError 6 némítása
        except Exception:
            pass


if __name__ == "__main__":
    run_scraper()
