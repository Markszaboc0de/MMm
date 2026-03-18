import os
import requests
import time
from bs4 import BeautifulSoup
from core.base_scraper import BaseScraper

# --- CONFIGURATION ---
TARGETS_FILE = "../targets/lever_targets.txt"
DB_FILE = "../data/lever_jobs.db"


class LeverApiScraper:
    def __init__(self):
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.db_path = os.path.join(self.script_dir, DB_FILE)
        self.targets_path = os.path.join(self.script_dir, TARGETS_FILE)

        self.db_saver = BaseScraper(db_name=self.db_path)
        print("   ⚡ Initializing Lever API Scraper (FULL Descriptions)...")

    def load_targets(self):
        if not os.path.exists(self.targets_path):
            print(f"❌ Error: {TARGETS_FILE} not found!")
            return []
        with open(self.targets_path, 'r') as f:
            return [line.strip() for line in f.readlines() if line.strip() and not line.startswith("#")]

    def _split_location(self, raw_loc, workplace_type):
        if not raw_loc:
            return "Unknown", "Unknown"

        clean = raw_loc.strip()
        is_remote = workplace_type and "remote" in workplace_type.lower()

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

        if is_remote:
            return "Remote", "Remote"

        return clean, "Unknown"

    def _build_full_description(self, job):
        """Lever splits descriptions into intro, bullet points, and conclusion. We stitch them together."""
        full_text = []

        # 1. The Intro
        intro = job.get('descriptionPlain', '').strip()
        if intro:
            full_text.append(intro)

        # 2. The Bullet Points (Requirements, Responsibilities, etc.)
        lists = job.get('lists', [])
        for lst in lists:
            # Add the header (e.g., "What you will do")
            header = lst.get('text', '')
            if header:
                full_text.append(f"\n{header}:")

            # Extract bullet points from HTML
            content_html = lst.get('content', '')
            if content_html:
                soup = BeautifulSoup(content_html, 'html.parser')
                bullets = soup.find_all('li')
                if bullets:
                    for li in bullets:
                        full_text.append(f"- {li.get_text(strip=True)}")
                else:
                    # Fallback if no <li> tags
                    full_text.append(soup.get_text(separator='\n'))

        # 3. The Conclusion / EEO statement
        conclusion = job.get('additionalPlain', '').strip()
        if conclusion:
            full_text.append(f"\n{conclusion}")

        return "\n".join(full_text)

    def run(self):
        targets = self.load_targets()
        print(f"🚀 Starting API Scrape on {len(targets)} Lever companies...")

        total_new_jobs = 0

        for url in targets:
            # Extract the slug (e.g., 'spotify' from 'jobs.lever.co/spotify')
            company_slug = url.split('/')[-1]
            company_name = company_slug.capitalize()

            # The Magic Lever API Endpoint
            api_url = f"https://api.lever.co/v0/postings/{company_slug}?mode=json"

            print(f"--> Fetching API for: {company_name}")
            try:
                response = requests.get(api_url, timeout=10)

                if response.status_code != 200:
                    print(
                        f"   ⚠️ Could not load API (Status {response.status_code})")
                    continue

                jobs = response.json()

                if not jobs or not isinstance(jobs, list):
                    print(f"   ⚠️ 0 jobs found in API.")
                    continue

                saved_count = 0

                for job in jobs:
                    title = job.get('text', 'Unknown')
                    job_url = job.get('hostedUrl', '')

                    # Grab the FULL stitched description
                    description = self._build_full_description(job)

                    # Categories hold location data
                    categories = job.get('categories', {})
                    raw_location = categories.get('location', 'Unknown')
                    workplace_type = job.get('workplaceType', '')

                    city, country = self._split_location(
                        raw_location, workplace_type)

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

                if saved_count > 0:
                    print(f"   ✅ +{saved_count} new jobs saved ")
                    total_new_jobs += saved_count

                time.sleep(0.5)

            except Exception as e:
                print(f"   ❌ Error fetching {company_name}: {e}")

        print("-" * 50)
        print(
            f"🏁 Lever API Batch Complete. Total NEW Jobs Added: {total_new_jobs}")


if __name__ == "__main__":
    scraper = LeverApiScraper()
    scraper.run()
