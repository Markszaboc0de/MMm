import os
import requests
import time
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from core.base_scraper import BaseScraper

# --- CONFIGURATION ---
TARGETS_FILE = "../targets/personio_targets.txt"
DB_FILE = "../data/personio_jobs.db"


class PersonioXmlScraper:
    def __init__(self):
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.db_path = os.path.join(self.script_dir, DB_FILE)
        self.targets_path = os.path.join(self.script_dir, TARGETS_FILE)
        self.db_saver = BaseScraper(db_name=self.db_path)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        })
        print("   ⚡ Initializing Personio XML API Scraper...")

    def clean_html(self, raw_html):
        if not raw_html:
            return "Description not provided."
        soup = BeautifulSoup(raw_html, "html.parser")
        return soup.get_text(separator="\n").strip()

    def run(self):
        if not os.path.exists(self.targets_path):
            print(f"❌ Error: {TARGETS_FILE} not found!")
            return

        with open(self.targets_path, 'r') as f:
            targets = [line.strip() for line in f.readlines()
                       if line.strip() and not line.startswith("#")]

        print(
            f"🚀 Starting XML Scrape on {len(targets)} Personio instances...\n")
        total_saved = 0

        for url in targets:
            domain = url.replace(
                'https://', '').replace('http://', '').rstrip('/')
            company_name = domain.split('.')[0].capitalize()

            # Personio's open XML endpoint
            api_url = f"https://{domain}/xml"
            print(f"--> Fetching XML for: {company_name}")

            try:
                response = self.session.get(api_url, timeout=10)
                if response.status_code != 200:
                    print(
                        f"   ⚠️ Blocked or no XML feed found (Status: {response.status_code})")
                    continue

                # Parse the XML tree
                root = ET.fromstring(response.content)
                positions = root.findall('.//position')

                if not positions:
                    print("   ⚠️ 0 active jobs found.")
                    continue

                saved_for_company = 0
                for job in positions:
                    title = job.findtext('name', 'Unknown')
                    job_id = job.findtext('id', '')

                    # Construct the apply URL
                    job_url = f"https://{domain}/job/{job_id}" if job_id else url

                    raw_loc = job.findtext('office', 'Unknown')

                    # Combine all HTML description blocks
                    desc_html = ""
                    for desc in job.findall('.//jobDescription/value'):
                        if desc.text:
                            desc_html += desc.text + "\n"

                    job_dict = {
                        "url": job_url,
                        "title": title,
                        "company": company_name,
                        "location_raw": raw_loc,
                        "city": raw_loc.split(',')[0] if ',' in raw_loc else raw_loc,
                        "country": "Unknown",
                        "description": self.clean_html(desc_html)
                    }

                    if self.db_saver.save_job(job_dict):
                        saved_for_company += 1
                        if os.environ.get("HEALTH_CHECK_MODE") == "1":
                            break

                print(f"   ✅ Saved {saved_for_company} new jobs.")
                total_saved += saved_for_company

                time.sleep(1)  # Polite delay

            except Exception as e:
                print(f"   ❌ Failed to fetch/parse {company_name}: {e}")

        print("\n" + "=" * 50)
        print(
            f"🏁 Personio Batch Complete. Total NEW Jobs Added: {total_saved}")


if __name__ == "__main__":
    scraper = PersonioXmlScraper()
    scraper.run()
