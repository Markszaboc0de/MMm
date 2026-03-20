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
import sqlite3
import os
import sys
import time
import re
from markdownify import markdownify as md

sys.stdout.reconfigure(encoding='utf-8')

# --- KONFIGURÁCIÓ ---
COMPANY_NAME = "E.ON"

# Az URL a te előre beállított szűrőiddel
BASE_URL = "https://jobs.eon.com/en?locale=hu_HU&filter=entryLevel_multi%3AApprentice%2CentryLevel_multi%3ADual+Student%2CentryLevel_multi%3AInternship%2CentryLevel_multi%3AWorking+Student%2CentryLevel_multi%3ATrainee%2CentryLevel_multi%3AFinal+Thesis"

DATA_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
DB_PATH = os.path.join(DATA_FOLDER, "eon_jobs.db")


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


# remove create_driver as it was broken and we use get_chrome_driver now

def run_scraper():
    init_db()
    print(f"🚀 {COMPANY_NAME} Scraper indítása (GE Aerospace várakozási logika, Tiszta DOM mód)...")

    driver = get_chrome_driver()
    job_links = []
    unique_urls = set()

    try:
        # --- 1. FÁZIS: LINKEK GYŰJTÉSE A "LOAD MORE" GOMBBAL ---
        print("📂 E.ON karrieroldal megnyitása...")
        driver.get(BASE_URL)
        time.sleep(6)

        print("🍪 Sütik kezelése...")
        try:
            # Usercentrics Shadow DOM kezelése (E.ON gyakran ezt használja)
            driver.execute_script("""
                let uc = document.getElementById('usercentrics-root');
                if (uc && uc.shadowRoot) {
                    let btn = uc.shadowRoot.querySelector('button[data-testid="uc-accept-all-button"]');
                    if (btn) btn.click();
                }
                let buttons = document.querySelectorAll('button');
                for (let btn of buttons) {
                    let text = (btn.innerText || "").toLowerCase();
                    if (text.includes('accept') || text.includes('allow') || text.includes('akzeptieren') || text.includes('elfogad')) {
                        btn.click();
                    }
                }
            """)
            time.sleep(3)
        except:
            pass

        # GE AEROSPACE VÁRAKOZÁSI LOGIKA
        print("⏳ Várakozás az első álláskártyák betöltődésére...")
        cards_found = False
        for _ in range(20):
            # A data-source="job" attribútumra támaszkodunk, mert az garantáltan jelen van
            count = driver.execute_script(
                "return document.querySelectorAll('span[data-source=\"job\"]').length;")
            if count > 0:
                cards_found = True
                break
            time.sleep(1)

        if not cards_found:
            print("⚠️ Az oldal nem töltött be időben, vagy üres a lista (0 találat).")
            return

        print("⏳ 'Load More' gomb kattintása, amíg a lista véget nem ér...")

        click_count = 0
        while True:
            current_jobs = driver.execute_script(
                "return document.querySelectorAll('span[data-source=\"job\"]').length;")

            driver.execute_script(
                "window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)

            clicked = driver.execute_script("""
                let buttons = document.querySelectorAll('button');
                for (let btn of buttons) {
                    let text = (btn.innerText || btn.textContent || "").toLowerCase();
                    if (text.includes('load more') || text.includes('további') || text.includes('mehr laden')) {
                        if (!btn.disabled && btn.offsetParent !== null) {
                            btn.click();
                            return true;
                        }
                    }
                }
                return false;
            """)

            if clicked:
                click_count += 1
                sys.stdout.write(
                    f"\r   🔄 'Load More' kattintva {click_count} alkalommal... (Látható állások: ~{current_jobs})")
                sys.stdout.flush()

                # Várjuk meg, amíg a kártyák száma megnő
                for _ in range(15):
                    new_count = driver.execute_script(
                        "return document.querySelectorAll('span[data-source=\"job\"]').length;")
                    if new_count > current_jobs:
                        break
                    time.sleep(1)
            else:
                print("\n🏁 Nincs több 'Load More' gomb. A lista teljesen kibontva.")
                break

        print("📄 Az összes állás URL-jének kinyerése a DOM-ból...")

        jobs_on_page = driver.execute_script("""
            let results = [];
            // Megkeressük a lokációt tartalmazó span-okat, és "felfelé" haladva megkeressük a kártyát
            let locSpans = document.querySelectorAll('span[data-source="job"]');
            
            locSpans.forEach(locSpan => {
                let url = "N/A";
                let title = "N/A";
                let loc = locSpan.innerText.trim();
                
                // Megkeressük a fő konténert
                let card = locSpan.closest('div.border-2, div.shadow, li, div.mb-4') || locSpan.parentElement.parentElement.parentElement;
                
                if (card) {
                    let aTag = card.querySelector('a');
                    if (aTag && aTag.href) {
                        url = aTag.href;
                        
                        // A cím általában a text-primary osztályú span-ban vagy az a tag-ben van
                        let titleEl = card.querySelector('span.text-primary') || aTag;
                        title = titleEl.innerText.trim();
                    }
                }
                
                if (url !== "N/A") {
                    results.push({
                        url: url,
                        title: title,
                        location_raw: loc,
                        category: 'Energy / Engineering'
                    });
                }
            });
            return results;
        """)

        for job in jobs_on_page:
            if job['url'] not in unique_urls:
                unique_urls.add(job['url'])
                job_links.append(job)

        print(f"✅ {len(job_links)} egyedi állás azonosítva. Böngésző újraindítása a memóriaszivárgás megelőzése érdekében...")

        if not job_links:
            return

        # --- PRE-PHASE 2: BROWSER REBOOT ---
        try:
            driver.quit()
        except Exception:
            pass
        time.sleep(2)

        driver = create_driver()
        wait = WebDriverWait(driver, 10)

        # --- 2. FÁZIS: RÉSZLETEK ÉS LOKÁCIÓK KINYERÉSE ---
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
                except Exception:
                    print(
                        "   🔄 Kapcsolat megszakadt. Böngésző automatikus újraindítása...")
                    try:
                        driver.quit()
                    except Exception:
                        pass
                    time.sleep(2)
                    driver = create_driver()
                    wait = WebDriverWait(driver, 10)
                    driver.get(job['url'])

                try:
                    wait.until(EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "div[class*='JobAdTemplate_eon_content'], article")))
                    time.sleep(1)
                except:
                    time.sleep(2)

                # Leírás és város kinyerése a belső oldalról
                details = driver.execute_script("""
                    let result = { location: "N/A", description: "" };
                    
                    let locSpans = document.querySelectorAll('span[data-source="job"]');
                    if (locSpans.length > 0) {
                        for (let s of locSpans) {
                            if (s.innerText.includes(',')) {
                                result.location = s.innerText.trim();
                                break;
                            }
                        }
                        if (result.location === "N/A") {
                            result.location = locSpans[0].innerText.trim();
                        }
                    }
                    
                    let desc = document.querySelector("div[class*='JobAdTemplate_eon_content']") || document.querySelector("article");
                    if (desc) result.description = desc.innerHTML;
                    
                    return result;
                """)

                # --- DINAMIKUS LOKÁCIÓ FELDOLGOZÁS ---
                location_raw = details['location'] if details['location'] != "N/A" else job.get(
                    'location_raw', 'N/A')
                city = "N/A"
                country = "N/A"

                if location_raw and location_raw != 'N/A':
                    primary_loc = location_raw.split('|')[0].strip()
                    parts = [p.strip() for p in primary_loc.split(',')]
                    city = parts[0]

                    if len(parts) > 1:
                        # Irányítószám szűrése, ha számokat tartalmaz az utolsó rész
                        if any(c.isdigit() for c in parts[-1]):
                            country = parts[-2] if len(
                                parts) >= 3 else parts[1]
                        else:
                            country = parts[-1]
                    else:
                        country = parts[0]

                print(
                    f"   [{idx}/{len(job_links)}] Formázás: {job['title']} | Hely: {city}, {country}")

                # --- LEÍRÁS FORMÁZÁSA ---
                if details['description']:
                    clean_desc = md(details['description'], heading_style="ATX",
                                    bullets="-", strip=['img', 'script', 'style', 'a'])
                    clean_desc = clean_desc.replace(
                        '•', '-').replace('·', '-').replace('\xa0', ' ')
                    clean_desc = re.sub(r'\n{3,}', '\n\n', clean_desc).strip()
                else:
                    clean_desc = "Leírás nem található."

                # Felesleges szövegek levágása a végéről
                truncation_markers = [
                    "Mit unserer offenen und wertschätzenden Unternehmenskultur", "At E.ON we are committed to"]
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
        print(f"\n✨ SIKER! {saved_count} {COMPANY_NAME} állás mentve.")

    finally:
        try:
            driver.quit()
        except Exception:
            pass


if __name__ == "__main__":
    run_scraper()
