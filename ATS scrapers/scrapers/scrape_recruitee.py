import os
import requests
import time
from bs4 import BeautifulSoup
from core.base_scraper import BaseScraper

# --- CONFIGURATION ---
TARGETS_FILE = "../targets/recruitee_targets.txt"
DB_FILE = "../data/recruitee_jobs.db"


class RecruiteeApiScraper:
    def __init__(self):
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.db_path = os.path.join(self.script_dir, DB_FILE)
        self.targets_path = os.path.join(self.script_dir, TARGETS_FILE)
        self.db_saver = BaseScraper(db_name=self.db_path)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "application/json"
        })
        print("   ⚡ Initializing Recruitee API Scraper...")

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
            f"🚀 Starting API Scrape on {len(targets)} Recruitee instances...\n")
        total_saved = 0

        for url in targets:
            domain = url.replace(
                'https://', '').replace('http://', '').rstrip('/')
            company_name = domain.split('.')[0].capitalize()

            # Recruitee's open API endpoint
            api_url = f"https://{domain}/api/offers"
            print(f"--> Fetching API for: {company_name}")

            try:
                response = self.session.get(api_url, timeout=10)
                if response.status_code != 200:
                    print(
                        f"   ⚠️ Blocked or no API found (Status: {response.status_code})")
                    continue

                data = response.json()
                offers = data.get('offers', [])

                if not offers:
                    print("   ⚠️ 0 active jobs found.")
                    continue

                saved_for_company = 0
                for job in offers:
                    raw_loc = job.get('location', 'Unknown')
                    city = job.get('city', raw_loc)
                    country = job.get('country', 'Unknown')

                    if job.get('remote'):
                        city = f"Remote - {city}"

                    job_dict = {
                        "url": job.get('careers_url'),
                        "title": job.get('title'),
                        "company": company_name,
                        "location_raw": raw_loc,
                        "city": city,
                        "country": country,
                        "description": self.clean_html(job.get('description', ''))
                    }

                    if self.db_saver.save_job(job_dict):
                        saved_for_company += 1
                        if os.environ.get("HEALTH_CHECK_MODE") == "1":
                            break

                print(f"   ✅ Saved {saved_for_company} new jobs.")
                total_saved += saved_for_company

                # Polite delay to prevent IP bans
                time.sleep(1)

            except Exception as e:
                print(f"   ❌ Failed to fetch {company_name}: {e}")

        print("\n" + "=" * 50)
        print(
            f"🏁 Recruitee Batch Complete. Total NEW Jobs Added: {total_saved}")


if __name__ == "__main__":
    scraper = RecruiteeApiScraper()
    scraper.run()
