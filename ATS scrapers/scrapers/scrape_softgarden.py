import os
import requests
import time
import json
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from core.base_scraper import BaseScraper

# --- CONFIGURATION ---
TARGETS_FILE = "../targets/softgarden_targets.txt"
DB_FILE = "../data/softgarden_jobs.db"

class SoftgardenScraper:
    def __init__(self):
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.db_path = os.path.join(self.script_dir, DB_FILE)
        self.targets_path = os.path.join(self.script_dir, TARGETS_FILE)

        self.db_saver = BaseScraper(db_name=self.db_path)
        print("   ⚡ Initializing Softgarden Scraper...")

    def load_targets(self):
        if not os.path.exists(self.targets_path):
            print(f"❌ Error: {TARGETS_FILE} not found!")
            return []
        with open(self.targets_path, 'r', encoding='utf-8') as f:
            return [line.strip() for line in f.readlines() if line.strip() and not line.startswith("#")]

    def run(self):
        targets = self.load_targets()
        print(f" Starting Softgarden Scrape on {len(targets)} companies...")

        total_new_jobs = 0

        for url in targets:
            company_name = url.split('//')[1].split('.')[0].capitalize()
            vacancies_url = f"{url.rstrip('/')}/vacancies"

            print(f"--> Fetching Softgarden for: {company_name}")
            try:
                response = requests.get(vacancies_url, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}, timeout=15)
                time.sleep(1)

                if response.status_code != 200:
                    print(f"   ⚠️ Could not load vacancies (Status {response.status_code})")
                    continue

                response.encoding = 'utf-8'
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Find all links that contain '/job/'
                job_links = set()
                for a in soup.find_all('a', href=True):
                    href = a['href']
                    if '/job/' in href:
                        full_url = urljoin(response.url, href)
                        # Remove URL tracking parameters
                        if '?' in full_url:
                            full_url = full_url.split('?')[0]
                        job_links.add(full_url)

                if not job_links:
                    print(f"   ⚠️ 0 jobs found on vacancies page.")
                    continue

                saved_count = 0

                for job_url in job_links:
                    try:
                        job_res = requests.get(job_url, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}, timeout=10)
                        if job_res.status_code != 200:
                            continue
                            
                        job_res.encoding = 'utf-8'
                        job_soup = BeautifulSoup(job_res.text, 'html.parser')
                        ld_json = job_soup.find('script', type='application/ld+json')
                        
                        title = "Unknown"
                        description = "Description not provided."
                        city = "Unknown"
                        country = "Unknown"
                        raw_location = "Unknown"
                        
                        if ld_json and ld_json.string:
                            try:
                                data = json.loads(ld_json.string)
                                title = data.get('title', 'Unknown')
                                
                                # Sometimes desc is in html
                                raw_desc = data.get('description', '')
                                if raw_desc:
                                    desc_soup = BeautifulSoup(raw_desc, 'html.parser')
                                    description = desc_soup.get_text(separator=' ').strip()
                                
                                job_loc = data.get('jobLocation', {})
                                if isinstance(job_loc, list) and len(job_loc) > 0:
                                    job_loc = job_loc[0]
                                    
                                address = job_loc.get('address', {}) if isinstance(job_loc, dict) else {}
                                if isinstance(address, dict):
                                    city = address.get('addressLocality', 'Unknown')
                                    country = address.get('addressCountry', 'Unknown')
                                    region = address.get('addressRegion', '')
                                    raw_location = ", ".join([v for v in [city, region, country] if v and v != 'Unknown'])
                                    if not raw_location:
                                        raw_location = "Unknown"
                                        
                            except json.JSONDecodeError:
                                pass
                        
                        # Fallback if title lacks ld-json
                        if title == "Unknown" and job_soup.title:
                            title = job_soup.title.text.strip()
                            
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
                            
                    except Exception as e:
                        print(f"   ❌ Error fetching job {job_url}: {e}")
                        
                    time.sleep(0.5)

                print(f"   ✅ +{saved_count} new jobs saved")
                total_new_jobs += saved_count

            except Exception as e:
                print(f"   ❌ Error fetching {company_name}: {e}")

        print("-" * 50)
        print(f"🏁 Softgarden Scrape Complete. Total NEW Jobs Added: {total_new_jobs}")

if __name__ == "__main__":
    scraper = SoftgardenScraper()
    scraper.run()
