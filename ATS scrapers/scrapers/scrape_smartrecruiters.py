import os
import requests
import time
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from core.base_scraper import BaseScraper

# --- CONFIGURATION ---
TARGETS_FILE = "../targets/smartrecruiters_targets.txt"
DB_FILE = "../data/smartrecruiters_jobs.db"

# 🌍 The Universal ISO Country Translator
ISO_COUNTRIES = {
    'ae': 'United Arab Emirates', 'ar': 'Argentina', 'at': 'Austria', 'au': 'Australia',
    'be': 'Belgium', 'bg': 'Bulgaria', 'br': 'Brazil', 'ca': 'Canada', 'ch': 'Switzerland',
    'cl': 'Chile', 'cn': 'China', 'co': 'Colombia', 'cr': 'Costa Rica', 'cy': 'Cyprus',
    'cz': 'Czechia', 'de': 'Germany', 'dk': 'Denmark', 'ee': 'Estonia', 'eg': 'Egypt',
    'es': 'Spain', 'fi': 'Finland', 'fr': 'France', 'gb': 'United Kingdom', 'gr': 'Greece',
    'hk': 'Hong Kong', 'hr': 'Croatia', 'hu': 'Hungary', 'id': 'Indonesia', 'ie': 'Ireland',
    'il': 'Israel', 'in': 'India', 'is': 'Iceland', 'it': 'Italy', 'jp': 'Japan',
    'kr': 'South Korea', 'lt': 'Lithuania', 'lu': 'Luxembourg', 'lv': 'Latvia', 'ma': 'Morocco',
    'mt': 'Malta', 'mx': 'Mexico', 'my': 'Malaysia', 'nl': 'Netherlands', 'no': 'Norway',
    'nz': 'New Zealand', 'pe': 'Peru', 'ph': 'Philippines', 'pk': 'Pakistan', 'pl': 'Poland',
    'pt': 'Portugal', 'ro': 'Romania', 'rs': 'Serbia', 'ru': 'Russia', 'sa': 'Saudi Arabia',
    'se': 'Sweden', 'sg': 'Singapore', 'si': 'Slovenia', 'sk': 'Slovakia', 'th': 'Thailand',
    'tr': 'Turkey', 'tw': 'Taiwan', 'ua': 'Ukraine', 'us': 'United States', 'vn': 'Vietnam',
    'za': 'South Africa'
}


class SmartRecruitersApiScraper:
    def __init__(self):
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.db_path = os.path.join(self.script_dir, DB_FILE)
        self.targets_path = os.path.join(self.script_dir, TARGETS_FILE)

        self.db_saver = BaseScraper(db_name=self.db_path)
        print(
            "   ⚡ Initializing SmartRecruiters Scraper (Paginated + Country Translation)...")

    def load_targets(self):
        if not os.path.exists(self.targets_path):
            print(f"❌ Error: {TARGETS_FILE} not found!")
            return []
        with open(self.targets_path, 'r') as f:
            return [line.strip() for line in f.readlines() if line.strip() and not line.startswith("#")]

    def clean_html(self, raw_html):
        if not raw_html:
            return ""
        soup = BeautifulSoup(raw_html, "html.parser")
        return soup.get_text(separator="\n").strip()

    def fetch_full_description(self, company_slug, job_id):
        detail_url = f"https://api.smartrecruiters.com/v1/companies/{company_slug}/postings/{job_id}"
        try:
            res = requests.get(detail_url, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}, timeout=5)
            if res.status_code == 200:
                data = res.json()
                job_ad = data.get('jobAd', {}).get('sections', {})

                parts = []
                if job_ad.get('companyDescription', {}).get('text'):
                    parts.append("Company Description:\n" +
                                 self.clean_html(job_ad['companyDescription']['text']))
                if job_ad.get('jobDescription', {}).get('text'):
                    parts.append("Job Description:\n" +
                                 self.clean_html(job_ad['jobDescription']['text']))
                if job_ad.get('qualifications', {}).get('text'):
                    parts.append(
                        "Qualifications:\n" + self.clean_html(job_ad['qualifications']['text']))
                if job_ad.get('additionalInformation', {}).get('text'):
                    parts.append("Additional Information:\n" +
                                 self.clean_html(job_ad['additionalInformation']['text']))

                return "\n\n".join(parts) if parts else "Description not provided."
            return "Could not fetch description."
        except:
            return "Connection error while fetching description."

    def _process_single_job(self, job, company_slug, company_name):
        job_id = job.get('id')
        title = job.get('name', 'Unknown')
        job_url = f"https://jobs.smartrecruiters.com/{company_slug}/{job_id}"

        loc_data = job.get('location', {})
        city = loc_data.get('city') or 'Unknown'

        # 🔄 THE FIX: Translate the ISO country code into a full country name
        raw_country_code = loc_data.get('country', '').lower()
        if raw_country_code:
            # Look up the code in our dictionary. If it's missing, just uppercase the code (e.g., 'BR')
            country = ISO_COUNTRIES.get(
                raw_country_code, raw_country_code.upper())
        else:
            country = 'Unknown'

        raw_location = f"{city}, {country}".strip(', ')

        custom_fields = str(job.get('customField', []))
        if "remote" in custom_fields.lower() or loc_data.get('remote', False):
            if "remote" not in city.lower():
                city = f"Remote - {city}"

        full_description = self.fetch_full_description(company_slug, job_id)

        return {
            "url": job_url,
            "title": title,
            "company": company_name,
            "location_raw": raw_location,
            "city": city,
            "country": country,
            "description": full_description
        }

    def run(self):
        targets = self.load_targets()
        print(
            f"🚀 Starting Turbo API Scrape on {len(targets)} SmartRecruiters companies...\n")

        total_new_jobs = 0

        for url in targets:
            clean_url = url.strip().rstrip('/')
            company_slug = clean_url.split('/')[-1]
            company_name = company_slug.capitalize()

            print(f"--> Fetching API for: {company_name}")

            all_jobs = []
            offset = 0

            while True:
                api_url = f"https://api.smartrecruiters.com/v1/companies/{company_slug}/postings?limit=100&offset={offset}"

                try:
                    response = requests.get(api_url, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}, timeout=10)
                    if response.status_code != 200:
                        print(
                            f"   ⚠️ Could not load API page (Status {response.status_code})")
                        break

                    data = response.json()
                    jobs_page = data.get('content', [])

                    if not jobs_page:
                        break

                    all_jobs.extend(jobs_page)
                    offset += 100
                    if os.environ.get("HEALTH_CHECK_MODE") == "1":
                        break
                    time.sleep(0.5)

                except Exception as e:
                    print(f"   ❌ Error paginating {company_name}: {e}")
                    break

            if not all_jobs:
                print(f"   ⚠️ 0 jobs found in total.")
                continue

            print(
                f"   ⚡ Found {len(all_jobs)} TOTAL jobs. Multithreading descriptions...")
            saved_count = 0

            if os.environ.get("HEALTH_CHECK_MODE") == "1" and all_jobs:
                all_jobs = [all_jobs[0]]

            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = [executor.submit(
                    self._process_single_job, job, company_slug, company_name) for job in all_jobs]

                for future in as_completed(futures):
                    try:
                        job_dict = future.result()
                        saved = self.db_saver.save_job(job_dict)
                        if saved:
                            saved_count += 1
                    except Exception as e:
                        pass

            if saved_count > 0:
                print(f"   ✅ +{saved_count} new jobs saved")
                total_new_jobs += saved_count
            else:
                print(f"   ⚠️ 0 new jobs saved (all were duplicates).")

        print("-" * 50)
        print(
            f"🏁 Turbo Batch Complete. Total NEW Jobs Added: {total_new_jobs}")


if __name__ == "__main__":
    scraper = SmartRecruitersApiScraper()
    scraper.run()
