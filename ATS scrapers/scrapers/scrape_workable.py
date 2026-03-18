import os
import requests
import time
from bs4 import BeautifulSoup
from core.base_scraper import BaseScraper

# --- CONFIGURATION ---
TARGETS_FILE = "../targets/workable_targets.txt"
DB_FILE = "../data/workable_jobs.db"


class WorkableApiScraper:
    def __init__(self):
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.db_path = os.path.join(self.script_dir, DB_FILE)
        self.targets_path = os.path.join(self.script_dir, TARGETS_FILE)

        self.db_saver = BaseScraper(db_name=self.db_path)
        print("   ⚡ Initializing Workable API Scraper (Multi-City Engine)...")

    def load_targets(self):
        if not os.path.exists(self.targets_path):
            print(f"❌ Error: {TARGETS_FILE} not found!")
            return []
        with open(self.targets_path, 'r') as f:
            return [line.strip() for line in f.readlines() if line.strip() and not line.startswith("#")]

    def clean_html(self, raw_html):
        """Removes HTML tags from the description."""
        if not raw_html:
            return "Description not provided."
        soup = BeautifulSoup(raw_html, "html.parser")
        return soup.get_text(separator="\n").strip()

    def _parse_location(self, job):
        """Intelligently rebuilds location data from singular AND multi-city arrays."""
        city_list = []
        country_list = []
        raw_location_str = ""

        # Check if the job has the plural "locations" array (Multi-City)
        multi_locations = job.get('locations', [])

        if multi_locations and isinstance(multi_locations, list):
            for loc in multi_locations:
                c = loc.get('city') or ''
                ctry = loc.get('countryName') or loc.get('country') or ''

                if c and c not in city_list:
                    city_list.append(c)
                if ctry and ctry not in country_list:
                    country_list.append(ctry)

            city = " / ".join(city_list) if city_list else "Unknown"
            country = " / ".join(country_list) if country_list else "Unknown"
            raw_location = f"{city}, {country}"

        else:
            # Fallback to the singular "location" object
            loc_data = job.get('location', {})
            city = loc_data.get('city') or ''
            country = loc_data.get('country') or loc_data.get(
                'countryName') or ''
            raw_location = loc_data.get('location_str') or ''

            if not city and not country and raw_location:
                parts = [p.strip() for p in raw_location.split(',')]
                if len(parts) >= 2:
                    city = parts[0]
                    country = parts[-1]
                else:
                    city = raw_location

            city = city if city else "Unknown"
            country = country if country else "Unknown"
            raw_location = raw_location if raw_location else f"{city}, {country}".strip(
                ', ')

        # Remote Logic
        workplace_type = job.get('workplace_type', '')
        telecommuting = job.get('telecommuting', False)

        is_remote = telecommuting or (
            workplace_type and 'remote' in workplace_type.lower())

        if is_remote:
            if city == "Unknown" and country == "Unknown":
                city = "Remote"
                country = "Remote"
            elif "remote" not in city.lower():
                city = f"Remote - {city}"

        return raw_location, city, country

    def run(self):
        targets = self.load_targets()
        print(f"🚀 Starting API Scrape on {len(targets)} Workable companies...")

        total_new_jobs = 0

        for url in targets:
            clean_url = url.strip().rstrip('/')
            company_slug = clean_url.split('/')[-1]
            company_name = company_slug.capitalize()

            api_url = f"https://www.workable.com/api/accounts/{company_slug}?details=true"

            print(f"--> Fetching API for: {company_name}")
            try:
                response = requests.get(api_url, timeout=10)

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
                    job_url = job.get('shortlink', job.get('url', ''))

                    # Run the upgraded Smart Location Parser
                    raw_location, city, country = self._parse_location(job)

                    raw_desc = job.get('description', '')
                    clean_description = self.clean_html(raw_desc)

                    saved = self.db_saver.save_job({
                        "url": job_url,
                        "title": title,
                        "company": company_name,
                        "location_raw": raw_location,
                        "city": city,
                        "country": country,
                        "description": clean_description
                    })

                    if saved:
                        saved_count += 1

                if saved_count > 0:
                    print(
                        f"   ✅ +{saved_count} new jobs saved (Locations Cleaned)")
                    total_new_jobs += saved_count

                time.sleep(0.5)

            except Exception as e:
                print(f"   ❌ Error fetching {company_name}: {e}")

        print("-" * 50)
        print(
            f"🏁 Workable API Batch Complete. Total NEW Jobs Added: {total_new_jobs}")


if __name__ == "__main__":
    scraper = WorkableApiScraper()
    scraper.run()
