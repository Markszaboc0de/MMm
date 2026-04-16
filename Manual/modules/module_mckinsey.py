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
import urllib.parse

# Force UTF-8 encoding for Windows terminals
sys.stdout.reconfigure(encoding='utf-8')

# ==========================================
# ⚙️ CONFIGURATION
# ==========================================
COMPANY_NAME = "McKinsey & Company"
BASE_URL = "https://www.mckinsey.com/careers/search-jobs"
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Manual\data"
DB_PATH = os.path.join(DATA_FOLDER, "mckinsey_jobs.db")

# The target keywords for filtering
TARGET_KEYWORDS = [
    "trainee", "intern", "internship", "apprenticeship",
    "student", "graduate", "junior", "program"
]


def init_db():
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)
        print(f"   📁 Created new data directory: {DATA_FOLDER}")

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
        description TEXT,
        category TEXT,
        date_found TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    conn.commit()
    conn.close()


def get_country(city):
    """Maps McKinsey global cities to their respective countries."""
    mapping = {
        "abu dhabi": "United Arab Emirates", "amsterdam": "Netherlands", "athens": "Greece",
        "atlanta": "United States", "austin": "United States", "bangkok": "Thailand",
        "barcelona": "Spain", "beijing": "China", "berlin": "Germany", "bogota": "Colombia",
        "boston": "United States", "brussels": "Belgium", "buenos aires": "Argentina",
        "calgary": "Canada", "casablanca": "Morocco", "charlotte": "United States",
        "chicago": "United States", "cleveland": "United States", "cologne": "Germany",
        "columbus": "United States", "connecticut - darien": "United States", "dallas": "United States",
        "denver": "United States", "detroit": "United States", "doha": "Qatar",
        "dubai": "United Arab Emirates", "dusseldorf": "Germany", "frankfurt": "Germany",
        "guatemala city": "Guatemala", "hamburg": "Germany", "helsinki": "Finland",
        "hong kong sar": "Hong Kong", "hong kong": "Hong Kong", "houston": "United States",
        "tokyo": "Japan", "jakarta": "Indonesia", "johannesburg": "South Africa",
        "kuala lumpur": "Malaysia", "kuwait": "Kuwait", "lagos": "Nigeria", "lima": "Peru",
        "lisbon": "Portugal", "luanda": "Angola", "luxembourg": "Luxembourg",
        "madrid": "Spain", "manama": "Bahrain", "manila": "Philippines", "panama city": "Panama",
        "santiago": "Chile", "sao paulo": "Brazil", "medellin": "Colombia",
        "mexico city": "Mexico", "miami": "United States", "minneapolis": "United States",
        "montevideo": "Uruguay", "montreal": "Canada", "munich": "Germany",
        "new jersey": "United States", "new york city": "United States", "new york": "United States",
        "london": "United Kingdom", "rio de janeiro": "Brazil", "seoul": "South Korea",
        "shanghai": "China", "shenzhen": "China", "singapore city": "Singapore",
        "singapore": "Singapore", "taipei": "Taiwan", "osaka": "Japan", "oslo": "Norway",
        "philadelphia": "United States", "pittsburgh": "United States", "quito": "Ecuador",
        "raleigh": "United States", "riyadh": "Saudi Arabia", "san francisco": "United States",
        "san jose": "Costa Rica", "santo domingo": "Dominican Republic", "seattle": "United States",
        "silicon valley": "United States", "southern california": "United States",
        "st. louis": "United States", "stuttgart": "Germany", "vienna": "Austria",
        "toronto": "Canada", "washington dc": "United States", "budapest": "Hungary",
        "prague": "Czech Republic", "warsaw": "Poland", "bucharest": "Romania",
        "milan": "Italy", "rome": "Italy", "paris": "France", "geneva": "Switzerland",
        "zurich": "Switzerland", "stockholm": "Sweden", "copenhagen": "Denmark",
        "istanbul": "Turkey", "dublin": "Ireland", "sydney": "Australia",
        "melbourne": "Australia", "mumbai": "India", "delhi": "India", "bangalore": "India"
    }

    cl = city.lower().strip()
    if cl in mapping:
        return mapping[cl]

    if " - " in city:
        return "United States"
    return "Unknown"


def is_target_job(title):
    """Checks if the job title contains any of our target keywords."""
    title_lower = title.lower()
    return any(keyword in title_lower for keyword in TARGET_KEYWORDS)


def create_driver():
    """Creates a high-speed, auto-updating headless-ready Chrome instance."""
    return get_chrome_driver()


def run_scraper():
    init_db()
    print(
        f"🚀 Starting {COMPANY_NAME} Scraper (Mega-Load Multi-Location Mode)...")

    driver = create_driver()
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        # --- PHASE 1: Mega-Load Link Gathering ---
        mega_url = f"{BASE_URL}?page=100"
        print(f"📂 Triggering Mega-Load at: {mega_url}")

        driver.get(mega_url)

        # Wait for the massive DOM to fully populate
        print("   ⏳ Waiting for all jobs to render...")
        time.sleep(8)

        # Scroll to bottom to trigger any lazy-loaded elements just in case
        driver.execute_script(
            "window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)

        jobs_on_page = driver.execute_script("""
            let results = [];
            let links = document.querySelectorAll('a[href*="/careers/search-jobs/jobs/"]');
            links.forEach(a => {
                let title = a.innerText.trim();
                if (title && a.href) {
                    results.push({ title: title, url: a.href });
                }
            });
            return results;
        """)

        target_jobs = []
        for job in jobs_on_page:
            if is_target_job(job['title']):
                if not any(j['url'] == job['url'] for j in target_jobs):
                    target_jobs.append(job)

        print(f"\n✅ Extracted {len(jobs_on_page)} total jobs in seconds.")
        print(
            f"🎯 Found {len(target_jobs)} matching target jobs. Starting deep extraction...")

        if not target_jobs:
            return

        # --- PHASE 2: Deep Extraction & Multi-Row Spawning ---
        for idx, job in enumerate(target_jobs, 1):
            print(
                f"\n   [{idx}/{len(target_jobs)}] Extracting: {job['title']}")
            driver.get(job['url'])
            time.sleep(2)

            try:
                btn = driver.find_element(
                    By.CSS_SELECTOR, "button[class*='show-more-btn']")
                driver.execute_script("arguments[0].click();", btn)
                time.sleep(0.5)
            except:
                pass

            page_data = driver.execute_script("""
                function walk(el) {
                    let text = "";
                    if (!el) return "";
                    if (el.nodeType === 3) {
                        let val = el.nodeValue.replace(/\\s+/g, ' '); 
                        if (val !== ' ') text += val;
                    } else if (el.nodeType === 1) {
                        let tag = el.tagName.toUpperCase();
                        if (['SCRIPT','STYLE','NAV','FOOTER','BUTTON','SVG'].includes(tag)) return "";
                        if (tag === 'LI') text += "- ";
                        
                        for (let child of el.childNodes) text += walk(child);
                        
                        if (['H1','H2','H3','H4'].includes(tag)) text += "\\n\\n";
                        else if (['P','DIV','BR'].includes(tag)) text += "\\n";
                        else if (tag === 'LI') text += "\\n";
                    }
                    return text;
                }

                let fullDesc = "";
                let p1 = document.querySelector('.JobDescription_mck-c-job-details__info-container__V1E29');
                let p2 = document.querySelector('.JobDescription_mck-c-job-details__qualifications-item__3JCSq');
                
                if (p1) fullDesc += walk(p1).trim() + "\\n\\n";
                if (p2) fullDesc += "REQUIREMENTS:\\n" + walk(p2).trim();
                
                let h1 = document.querySelector('h1.mdc-c-heading');
                let exactTitle = h1 ? h1.innerText.trim() : "";
                
                let cities = [];
                let cityNodes = document.querySelectorAll('.ItemList_mck-c-item-list__items__cqPVb li');
                
                if (cityNodes.length > 0) {
                    cityNodes.forEach(li => {
                        let cText = li.innerText.trim();
                        if (cText && !cText.includes('(')) {
                            cities.push(cText);
                        }
                    });
                } else {
                    let fallback = document.querySelector('.mck-location-icon');
                    if (fallback && fallback.nextSibling) {
                        cities.push(fallback.nextSibling.textContent.trim());
                    }
                }
                
                return {
                    description: fullDesc || 'Description could not be loaded.',
                    exact_title: exactTitle,
                    cities: cities
                };
            """)

            clean_desc = re.sub(r' +', ' ', page_data['description'])
            clean_desc = re.sub(r'\n[ \t]+\n', '\n\n', clean_desc)
            clean_desc = re.sub(r'\n{3,}', '\n\n', clean_desc).strip()

            final_title = page_data.get('exact_title') or job['title']
            cities = page_data.get('cities', [])

            if not cities:
                cities = ["Unknown"]

            print(f"      📍 Found {len(cities)} locations. Generating rows...")

            rows_saved = 0
            for city in cities:
                country = get_country(city)
                unique_city_url = f"{job['url']}?loc={urllib.parse.quote(city)}"

                try:
                    cursor.execute('''
                        INSERT INTO jobs (url, title, company, location_raw, city, country, description, category)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(url) DO UPDATE SET 
                        title=excluded.title, 
                        description=excluded.description
                    ''', (unique_city_url, final_title, COMPANY_NAME, city, city, country, clean_desc, "Junior/Intern/Program"))

                    if cursor.rowcount > 0:
                        rows_saved += 1
                except Exception as e:
                    print(f"      ⚠️ DB Error for {city}: {e}")

            conn.commit()
            print(f"      ✅ Saved {rows_saved} database rows for this job.")

    except Exception as e:
        print(f"❌ Critical error during McKinsey scrape: {e}")

    finally:
        driver.quit()
        conn.close()
        print(f"\n🏁 McKinsey scrape completed successfully.")


if __name__ == "__main__":
    run_scraper()
