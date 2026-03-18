from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

import sys as _sys
import os as _os
_sys.path.append(_os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))
from driver_setup import get_chrome_driver
import sqlite3
import os
import sys
import time

# Windows terminál UTF-8 kódolásának kikényszerítése
sys.stdout.reconfigure(encoding='utf-8')

# ==========================================
# ⚙️ CONFIGURATION
# ==========================================
COMPANY_NAME = "Knorr-Bremse"
BASE_URL = "https://careers.knorr-bremse.com/content/search/?locale=hu_HU&brand=Knorr-Bremse"

DATA_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
DB_PATH = os.path.join(DATA_FOLDER, "knorrbremse_jobs.db")

JS_DEEP_QUERY = """
function deepQuerySelectorAll(selector, root = document) {
    let results = Array.from(root.querySelectorAll(selector));
    let allElements = root.querySelectorAll('*');
    for (let el of allElements) {
        if (el.shadowRoot) {
            results = results.concat(deepQuerySelectorAll(selector, el.shadowRoot));
        }
    }
    return results;
}
"""


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
    print(
        f"   🏢 Scraper indítása: {COMPANY_NAME} (Shadow DOM + Lapozó mód)...")
    init_db()

    driver = get_chrome_driver()
    try:
        driver.get(BASE_URL)
        print("   ⏳ Várakozás az első kártyákra az Árnyék DOM-ban (max 20 mp)...")

        first_batch_loaded = False
        for _ in range(20):
            time.sleep(1)
            count = driver.execute_script(
                JS_DEEP_QUERY + "return deepQuerySelectorAll('a.search').length;")
            if count > 0:
                first_batch_loaded = True
                break

        if not first_batch_loaded:
            print(f"   ⚠️ Nem töltődött be az első adag állás sem.")
            return

        print("   🔄 Összes állás betöltése (Több betöltése gomb keresése)...")

        click_count = 0
        while True:
            current_cards = driver.execute_script(
                JS_DEEP_QUERY + "return deepQuerySelectorAll('a.search').length;")

            # Gomb keresése, görgetés hozzá, majd kattintás
            click_script = JS_DEEP_QUERY + """
                let btnContainer = deepQuerySelectorAll('.centered.button kb-button')[0] || deepQuerySelectorAll('kb-button')[0];
                
                if (btnContainer) {
                    if (btnContainer.parentElement && window.getComputedStyle(btnContainer.parentElement).display === 'none') {
                        return false;
                    }
                    
                    // Görgetés a gombhoz, hogy biztosan interakcióba léphessen vele a JS
                    btnContainer.scrollIntoView({block: 'center'});
                    
                    if (btnContainer.shadowRoot && btnContainer.shadowRoot.querySelector('button')) {
                        btnContainer.shadowRoot.querySelector('button').click();
                        return true;
                    } else {
                        btnContainer.click();
                        return true;
                    }
                }
                return false;
            """

            clicked = driver.execute_script(click_script)

            if not clicked:
                break  # Nincs több gomb!

            click_count += 1
            sys.stdout.write(
                f"      🖱️ Gomb lenyomva ({click_count}. alkalommal). Várakozás az új kártyákra...\r")
            sys.stdout.flush()

            # 🧠 THE FIX: Dinamikus várakozás az új kártyákra (akár 15 másodpercet is adunk a szervernek)
            new_cards_loaded = False
            for _ in range(15):
                time.sleep(1)
                new_cards = driver.execute_script(
                    JS_DEEP_QUERY + "return deepQuerySelectorAll('a.search').length;")
                if new_cards > current_cards:
                    new_cards_loaded = True
                    break

            if not new_cards_loaded:
                # Ha 15 mp alatt sem jött új kártya, akkor biztosan végeztünk!
                break

        print(f"\n   ✅ Összes oldal betöltve! Adatok kinyerése...")

        # Kinyerés
        extract_script = JS_DEEP_QUERY + """
            let cards = deepQuerySelectorAll('a.search');
            let results = [];
            
            cards.forEach(card => {
                let titleTag = card.querySelector('.job-title');
                if (titleTag) {
                    let title = titleTag.innerText.trim();
                    let url = card.href;
                    
                    let pills = card.querySelectorAll('.pill');
                    let city = 'Budapest';
                    let country = 'Hungary';
                    
                    if (pills.length >= 2) {
                        city = pills[0].innerText.trim();
                        let rawCountry = pills[1].innerText.trim();
                        country = rawCountry.includes('Magyarország') ? 'Hungary' : rawCountry;
                    }
                    
                    results.push({'title': title, 'url': url, 'city': city, 'country': country});
                }
            });
            return results;
        """
        job_targets = driver.execute_script(extract_script)

        job_targets = [dict(t)
                       for t in {tuple(d.items()) for d in job_targets}]
        print(
            f"   🔍 {len(job_targets)} állás hivatkozás megtalálva. Részletek kinyerése...")

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        new_jobs_added = 0

        # Aloldalak
        for job in job_targets:
            job_url = job["url"]
            title = job["title"]
            city = job["city"]
            country = job["country"]

            try:
                driver.get(job_url)

                description = None
                for _ in range(10):
                    time.sleep(1)
                    desc_script = JS_DEEP_QUERY + """
                        let descNode = deepQuerySelectorAll('.jobdescription')[0] || 
                                       deepQuerySelectorAll('.job-description')[0] || 
                                       deepQuerySelectorAll('.desc')[0] ||
                                       deepQuerySelectorAll('.job-content')[0];
                        
                        if (descNode) {
                            return descNode.innerText.trim();
                        }
                        return null;
                    """
                    description = driver.execute_script(desc_script)
                    if description:
                        break

                if not description:
                    description = "A leírás kinyerése nem sikerült az Árnyék DOM-ból."

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

    finally:
        try:
            if 'driver' in locals():
                driver.quit()
        except OSError:
            pass
        except Exception:
            pass


if __name__ == "__main__":
    run_scraper()
