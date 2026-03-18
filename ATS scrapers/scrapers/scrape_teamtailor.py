import os
import requests
import time
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from core.base_scraper import BaseScraper

# --- CONFIGURATION ---
TARGETS_FILE = "../targets/teamtailor_targets.txt"
DB_FILE = "../data/teamtailor_jobs.db"

class TeamtailorScraper:
    def __init__(self):
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.db_path = os.path.join(self.script_dir, DB_FILE)
        self.targets_path = os.path.join(self.script_dir, TARGETS_FILE)

        self.db_saver = BaseScraper(db_name=self.db_path)
        print("   ⚡ Initializing Teamtailor RSS Scraper...")

    def load_targets(self):
        if not os.path.exists(self.targets_path):
            print(f"❌ Error: {TARGETS_FILE} not found!")
            return []
        with open(self.targets_path, 'r', encoding='utf-8') as f:
            return [line.strip() for line in f.readlines() if line.strip() and not line.startswith("#")]

    def run(self):
        targets = self.load_targets()
        print(f" Starting RSS Scrape on {len(targets)} companies...")

        total_new_jobs = 0

        # Define namespace for Teamtailor's custom tags in their RSS
        ns = {'tt': 'https://teamtailor.com/locations'}

        for url in targets:
            company_name = url.split('//')[1].split('.')[0].capitalize()
            rss_url = f"{url.rstrip('/')}/jobs.rss"

            print(f"--> Fetching RSS for: {company_name}")
            try:
                response = requests.get(rss_url, timeout=15)

                if response.status_code != 200:
                    print(f"   ⚠️ Could not load RSS (Status {response.status_code})")
                    continue

                root = ET.fromstring(response.content)
                items = root.findall('./channel/item')

                if not items:
                    print(f"   ⚠️ 0 jobs found in RSS.")
                    continue

                saved_count = 0

                for item in items:
                    title = item.findtext('title', default='Unknown')
                    job_url = item.findtext('link', default='')
                    
                    # Clean up HTML from description
                    html_desc = item.findtext('description', default='')
                    soup = BeautifulSoup(html_desc, 'html.parser')
                    description = soup.get_text(separator='\n').strip()
                    if not description:
                        description = "Description not provided."

                    # Location string logic
                    city = "Unknown"
                    country = "Unknown"
                    raw_location = "Unknown"
                    
                    # Their custom namespace has location details
                    locations = item.find('tt:locations', namespaces=ns)
                    if locations is not None:
                        loc = locations.find('tt:location', namespaces=ns)
                        if loc is not None:
                            city = loc.findtext('tt:city', default='Unknown', namespaces=ns)
                            country = loc.findtext('tt:country', default='Unknown', namespaces=ns)
                            raw_location = f"{city}, {country}".strip(", ")
                    
                    saved = self.db_saver.save_job({
                        "url": job_url,
                        "title": title,
                        "company": company_name,
                        "location_raw": raw_location,
                        "city": city,
                        "country": country,
                        "description": description
                    })

                    if saved:
                        saved_count += 1

                print(f"   ✅ +{saved_count} new jobs saved")
                total_new_jobs += saved_count

            except Exception as e:
                print(f"   ❌ Error fetching {company_name}: {e}")

        print("-" * 50)
        print(f"🏁 Teamtailor Scrape Complete. Total NEW Jobs Added: {total_new_jobs}")

if __name__ == "__main__":
    scraper = TeamtailorScraper()
    scraper.run()
