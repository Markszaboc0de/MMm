import os
import requests
import time
from core.base_scraper import BaseScraper

# --- CONFIGURATION ---
TARGETS_FILE = "../targets/ashby_targets.txt"
DB_FILE = "../data/ashby_jobs.db"


class AshbyApiScraper:
    def __init__(self):
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.db_path = os.path.join(self.script_dir, DB_FILE)
        self.targets_path = os.path.join(self.script_dir, TARGETS_FILE)

        self.db_saver = BaseScraper(db_name=self.db_path)
        print("   ⚡ Initializing Ashby API Scraper (FULL Descriptions)...")

    def load_targets(self):
        if not os.path.exists(self.targets_path):
            print(f"❌ Error: {TARGETS_FILE} not found!")
            return []
        with open(self.targets_path, 'r') as f:
            return [line.strip() for line in f.readlines() if line.strip() and not line.startswith("#")]

    def _split_location(self, raw_loc, is_remote):
        """Clean location parsing using the API's explicit remote flag"""
        if not raw_loc:
            return "Unknown", "Unknown"

        clean = raw_loc.strip()

        # Remove "Remote" from the beginning loosely
        if clean.lower().startswith("remote"):
            clean = clean[6:].strip(" -(),")
        
        # Handle cases like "United States | Canada" simply taking the first one
        if "|" in clean:
            clean = clean.split("|")[0].strip()

        if not clean:
            if is_remote:
                return "Remote", "Remote"
            return "Unknown", "Unknown"

        if "," in clean:
            parts = clean.split(',')
            city = parts[0].strip()
            country = parts[-1].strip()
            if is_remote and "remote" not in city.lower():
                city = f"Remote - {city}"
            return city, country

        if " - " in clean:
            parts = clean.split(" - ")
            return parts[0].strip(), parts[-1].strip()

        # If it's just one word (like "United States") treat it as a country
        if is_remote:
            return "Remote", clean

        return clean, clean

    def run(self):
        targets = self.load_targets()
        print(f" Starting API Scrape on {len(targets)} companies...")

        total_new_jobs = 0

        for url in targets:
            # Extract just the company name from the url
            company_slug = url.split('/')[-1]
            company_name = company_slug.capitalize()

            # The Magic Ashby API Endpoint
            api_url = f"https://api.ashbyhq.com/posting-api/job-board/{company_slug}"

            print(f"--> Fetching API for: {company_name}")
            try:
                response = requests.get(api_url, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}, timeout=10)

                if response.status_code != 200:
                    print(
                        f"   ⚠️ Could not load API (Status {response.status_code})")
                    continue

                data = response.json()
                jobs = data.get('jobs', [])

                if not jobs:
                    print(f"   ⚠️ 0 jobs found in API.")
                    continue

                saved_count = 0

                for job in jobs:
                    title = job.get('title', 'Unknown')
                    job_url = job.get('jobUrl', '')
                    raw_location = job.get('location', 'Unknown')
                    is_remote = job.get('isRemote', False)

                    # Grab the FULL plain text description without any limits
                    description = job.get(
                        'descriptionPlain', 'Description not provided.')

                    city, country = self._split_location(
                        raw_location, is_remote)

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

                print(
                    f"   ✅ +{saved_count} new jobs saved")
                total_new_jobs += saved_count

                time.sleep(0.5)

            except Exception as e:
                print(f"   ❌ Error fetching {company_name}: {e}")

        print("-" * 50)
        print(
            f"🏁 Ashby API Batch Complete. Total NEW Jobs Added: {total_new_jobs}")


if __name__ == "__main__":
    scraper = AshbyApiScraper()
    scraper.run()
