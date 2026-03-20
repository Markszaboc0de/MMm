from core.base_scraper import BaseScraper
import os
import sys
import re
import time
from urllib.parse import urljoin
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

# Ensure Python can find the 'core' module and the root driver_setup
root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(root_dir)
from driver_setup import get_chrome_driver

# --- CONFIGURATION ---
TARGETS_FILE = "../targets/hrmaster_targets.txt"
DB_FILE = "../data/hrmaster_jobs.db"


class HrMasterScraper:
    def __init__(self):
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.db_path = os.path.join(self.script_dir, DB_FILE)
        self.targets_path = os.path.join(self.script_dir, TARGETS_FILE)

        self.db_saver = BaseScraper(db_name=self.db_path)
        print("   ⚡ Initializing HRMaster Scraper (Omni-Catcher + Perfect Title Mode)...")

    def load_targets(self):
        if not os.path.exists(self.targets_path):
            print(f"❌ Error: {TARGETS_FILE} not found!")
            return []
        with open(self.targets_path, 'r', encoding='utf-8') as f:
            return [line.strip() for line in f.readlines() if line.strip() and not line.startswith("#")]

    def _get_company_name(self, url):
        domain = url.split('//')[-1].split('/')[0]
        name = domain.replace('.hrmaster.hu', '').capitalize()
        return name

    def _clean_city(self, raw_loc):
        if not raw_loc or raw_loc == "Unknown":
            return "Unknown"

        if " - " in raw_loc:
            parts = raw_loc.split(" - ")
            potential_city = parts[-1].strip()
            if not any(char.isdigit() for char in potential_city):
                return potential_city

        cleaned = re.sub(r'\d+', '', raw_loc)
        parts = cleaned.split(',')
        city = parts[0].strip()
        return city if city else "Unknown"

    def run(self):
        targets = self.load_targets()
        print(f"🚀 Starting Scrape on {len(targets)} HRMaster companies...\n")

        driver = get_chrome_driver()
        # Set a slightly longer wait for HRMaster's dynamic tables
        wait = WebDriverWait(driver, 10)

        total_saved = 0

        try:
            for base_url in targets:
                company_name = self._get_company_name(base_url)
                print(f"--> Fetching Data for: {company_name}")

                try:
                    driver.get(base_url)
                    time.sleep(5)

                    for _ in range(3):
                        driver.execute_script("window.scrollBy(0, 800);")
                        time.sleep(1)

                    # --- THE OMNI-CATCHER ---
                    jobs_on_page = driver.execute_script("""
                        let results = [];
                        let processedPaths = new Set();
                        
                        let elements = document.querySelectorAll('a[href*="/JobAdvertisement/"], [ng-click*="/JobAdvertisement/"]');
                        
                        elements.forEach(el => {
                            let urlPath = "";
                            if (el.hasAttribute('href')) {
                                urlPath = el.getAttribute('href');
                            } else {
                                let match = el.getAttribute('ng-click').match(/,\\s*["']([^"']+)["']/);
                                if (match && match[1]) urlPath = match[1];
                            }
                            
                            if (urlPath && !processedPaths.has(urlPath)) {
                                processedPaths.add(urlPath);
                                let title = (el.innerText || "").trim();
                                
                                let badTitles = ['részletek', 'jelentkezem', 'details', 'apply', 'tovább', ''];
                                if (badTitles.includes(title.toLowerCase())) {
                                    let card = el.closest('div[class*="Card"], div[class*="Item"], div.panel, tr, li');
                                    if (card) {
                                        let header = card.querySelector('h1, h2, h3, h4, [class*="Header"], [class*="Title"], strong');
                                        if (header) title = (header.innerText || "").trim();
                                    }
                                }
                                
                                if (!title) title = "Unknown Position";
                                
                                results.push({
                                    title: title,
                                    url_path: urlPath
                                });
                            }
                        });
                        return results;
                    """)

                    if not jobs_on_page:
                        print(
                            f"   ⚠️ 0 jobs found for {company_name}. (They may have no openings, or use an iframe).")
                        continue

                    print(
                        f"   ⚡ Found {len(jobs_on_page)} jobs. Extracting descriptions & validating titles...")
                    saved_for_company = 0

                    for job in jobs_on_page:
                        full_job_url = urljoin(base_url, job['url_path'])

                        driver.get(full_job_url)

                        try:
                            WebDriverWait(driver, 8).until(
                                lambda d: len(d.find_element(
                                    By.TAG_NAME, "body").text) > 200
                            )
                        except TimeoutException:
                            pass

                        # --- HEAVIEST CONTAINER & PERFECT TITLE EXTRACTOR ---
                        page_data = driver.execute_script("""
                            // Grab Location
                            let locElement = document.querySelector('[data-e2e-testing="Recruitment.Registration.Position.LocationOfWork"]');
                            let exactLoc = locElement ? locElement.innerText.trim() : "Unknown";
                            
                            // THE FIX: Grab Perfect Title from the job description page
                            let titleElement = document.querySelector('[data-e2e-testing="Recruitment.Registration.Position.PositionHeader"]');
                            let exactTitle = titleElement ? titleElement.innerText.trim() : "";
                            
                            function walk(el) {
                                let text = "";
                                if (!el) return "";
                                
                                if (el.nodeType === 3) {
                                    let val = el.nodeValue.replace(/\\s+/g, ' '); 
                                    if (val !== ' ') text += val;
                                } else if (el.nodeType === 1) {
                                    let tag = el.tagName.toUpperCase();
                                    let cls = (el.className && typeof el.className === 'string') ? el.className.toLowerCase() : "";
                                    
                                    if (['SCRIPT','STYLE','NAV','FOOTER','HEADER','BUTTON','SVG'].includes(tag)) return "";
                                    if (cls.includes('share') || cls.includes('apply')) return "";
                                    if (tag === 'LI') text += "- ";
                                    
                                    for (let child of el.childNodes) text += walk(child);
                                    
                                    if (['H1','H2','H3','H4'].includes(tag)) text += "\\n\\n";
                                    else if (['P','DIV','BR'].includes(tag)) text += "\\n";
                                    else if (tag === 'LI') text += "\\n";
                                }
                                return text;
                            }

                            let bestText = '';
                            let mainContainer = document.querySelector('.jobAdvertisement__content, .fr-view');
                            
                            if (mainContainer) {
                                bestText = walk(mainContainer).trim();
                            } else {
                                let allDivs = document.querySelectorAll('div');
                                for (let c of allDivs) {
                                    let currentText = walk(c).trim();
                                    if (currentText.length > bestText.length && currentText.length < 8000) {
                                        bestText = currentText;
                                    }
                                }
                            }
                            
                            return { 
                                location: exactLoc, 
                                description: bestText || 'Description could not be loaded.',
                                exact_title: exactTitle
                            };
                        """)

                        clean_desc = re.sub(
                            r' +', ' ', page_data['description'])
                        clean_desc = re.sub(r'\n[ \t]+\n', '\n\n', clean_desc)
                        clean_desc = re.sub(
                            r'\n{3,}', '\n\n', clean_desc).strip()
                        clean_desc = clean_desc.replace('\xa0', ' ')

                        city = self._clean_city(page_data['location'])

                        # OVERWRITE old title if we found the perfect one
                        final_title = page_data.get(
                            'exact_title') or job['title']

                        # Save to database OR Update existing rows that had bad titles
                        try:
                            conn = sqlite3.connect(self.db_path)
                            cursor = conn.cursor()

                            # Using ON CONFLICT to overwrite the title if the URL already exists
                            cursor.execute("""
                                INSERT INTO jobs (url, title, company, location_raw, city, country, description, category)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                                ON CONFLICT(url) DO UPDATE SET 
                                title=excluded.title, 
                                description=excluded.description,
                                city=excluded.city,
                                location_raw=excluded.location_raw
                            """, (full_job_url, final_title, company_name, page_data['location'], city, "Hungary", clean_desc, "General"))

                            # Check if a new row was added or an old row was updated
                            if cursor.rowcount > 0:
                                saved_for_company += 1

                            conn.commit()
                            conn.close()
                        except Exception as e:
                            print(f"      ⚠️ Database error: {e}")

                    print(
                        f"   ✅ Processed {saved_for_company} jobs (New & Updated).")
                    total_saved += saved_for_company

                except Exception as e:
                    print(f"   ❌ Failed to fetch {company_name}: {e}")

        finally:
            driver.quit()
            print("\n" + "=" * 50)
            print(
                f"🏁 HRMaster Batch Complete. Total Jobs Processed: {total_saved}")


if __name__ == "__main__":
    scraper = HrMasterScraper()
    scraper.run()
