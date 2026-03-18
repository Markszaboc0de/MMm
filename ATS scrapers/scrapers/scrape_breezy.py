import os
import requests
import time
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from core.base_scraper import BaseScraper

# --- CONFIGURATION ---
# Notice the new dynamic name from the master finder
TARGETS_FILE = "../targets/breezy_targets.txt"
DB_FILE = "../data/breezy_jobs.db"


class BreezyApiScraper:
    def __init__(self):
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.db_path = os.path.join(self.script_dir, DB_FILE)

        # Read the file generated directly by your Master Finder in the Finders folder
        # Adjust path if you move the targets.txt into the Breezy_Scraper folder instead
        self.targets_path = os.path.join(self.script_dir, TARGETS_FILE)

        self.db_saver = BaseScraper(db_name=self.db_path)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "application/json"
        })
        print("   ⚡ Initializing Breezy HR Hybrid Scraper...")

    def clean_html(self, raw_html):
        if not raw_html:
            return "Description not provided."
        soup = BeautifulSoup(raw_html, "html.parser")
        return soup.get_text(separator="\n").strip()

    def fetch_deep_description(self, job_url):
        """Dives into the specific job page to pull the detailed description"""
        try:
            res = self.session.get(job_url, timeout=7)
            if res.status_code == 200:
                res.encoding = 'utf-8'
                soup = BeautifulSoup(res.text, 'html.parser')
                # Breezy usually wraps descriptions in this specific class
                desc_elem = soup.find('div', class_='description')
                if desc_elem:
                    return self.clean_html(str(desc_elem))
                # Fallback if class changes
                return self.clean_html(res.text)
        except:
            pass
        return "Failed to load description."

    def process_job(self, job, company_name):
        # Safely parse Breezy's nested location JSON structure
        location_data = job.get('location', {})
        city = location_data.get('city', 'Unknown')
        country = location_data.get('country', {}).get('name', 'Unknown')

        raw_loc = f"{city}, {country}" if city != 'Unknown' else "Unknown"

        if location_data.get('is_remote'):
            city = f"Remote - {city}"

        job_url = job.get('url')
        description = self.fetch_deep_description(job_url)

        return {
            "url": job_url,
            "title": job.get('name', 'Unknown'),
            "company": company_name,
            "location_raw": raw_loc,
            "city": city,
            "country": country,
            "description": description
        }

    def run(self):
        if not os.path.exists(self.targets_path):
            print(f"❌ Error: {self.targets_path} not found!")
            print(
                "Please copy the breezy_targets.txt from your Finders folder here first.")
            return

        with open(self.targets_path, 'r') as f:
            targets = [line.strip() for line in f.readlines()
                       if line.strip() and not line.startswith("#")]

        print(
            f"🚀 Starting Hybrid Scrape on {len(targets)} Breezy HR instances...\n")
        total_saved = 0

        for url in targets:
            domain = url.replace(
                'https://', '').replace('http://', '').rstrip('/')
            company_name = domain.split('.')[0].capitalize()

            # Breezy's hidden JSON endpoint
            api_url = f"https://{domain}/json"
            print(f"--> Fetching API for: {company_name}")

            try:
                response = self.session.get(api_url, timeout=10)
                if response.status_code != 200:
                    continue

                jobs_list = response.json()

                if not jobs_list:
                    print("   ⚠️ 0 active jobs found.")
                    continue

                print(
                    f"   ⚡ Found {len(jobs_list)} jobs. Downloading deep descriptions...")
                saved_for_company = 0

                # Multithread the HTML fetching so it runs extremely fast
                with ThreadPoolExecutor(max_workers=10) as executor:
                    futures = [executor.submit(
                        self.process_job, job, company_name) for job in jobs_list]

                    for future in as_completed(futures):
                        try:
                            job_dict = future.result()
                            if job_dict and self.db_saver.save_job(job_dict):
                                saved_for_company += 1
                        except Exception as e:
                            import traceback
                            traceback.print_exc()
                            print(f"Error saving job: {e}")

                print(f"   ✅ Saved {saved_for_company} new jobs.")
                total_saved += saved_for_company

            except Exception as e:
                print(f"   ❌ Failed to fetch {company_name}: {e}")

        print("\n" + "=" * 50)
        print(
            f"🏁 Breezy HR Batch Complete. Total NEW Jobs Added: {total_saved}")


if __name__ == "__main__":
    scraper = BreezyApiScraper()
    scraper.run()
