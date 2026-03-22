import os
import sys
import requests
import time
from urllib.parse import urlparse

# Ensure Python can find the 'core' module
_ats_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # ATS scrapers/
_root_dir = os.path.dirname(_ats_dir)                                    # project root
for _p in [_ats_dir, _root_dir]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

from core.base_scraper import BaseScraper

# --- CONFIGURATION ---
TARGETS_FILE = "../targets/workday_targets.txt"
DB_FILE = "../data/workday_jobs.db"


class WorkdayScraper:
    def __init__(self):
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.db_path = os.path.join(self.script_dir, DB_FILE)
        self.targets_path = os.path.join(self.script_dir, TARGETS_FILE)

        self.db_saver = BaseScraper(db_name=self.db_path)
        print("   ⚡ Initializing Workday Scraper...")

    def load_targets(self):
        if not os.path.exists(self.targets_path):
            print(f"❌ Error: {TARGETS_FILE} not found!")
            return []
        with open(self.targets_path, 'r', encoding='utf-8') as f:
            return [line.strip() for line in f.readlines() if line.strip() and not line.startswith("#")]

    def get_workday_config(self, url):
        parsed = urlparse(url)
        host = parsed.netloc
        path_parts = parsed.path.strip('/').split('/')
        
        tenant = host.split('.')[0]
        site = path_parts[-1]
        
        api_base = f"https://{host}/wday/cxs/{tenant}/{site}"
        return api_base, tenant, site

    def fetch_jobs(self, api_base, search_text="", max_jobs=None):
        print(f"   Fetching jobs from {api_base}/jobs (search: '{search_text}')...")
        jobs_url = f"{api_base}/jobs"
        all_jobs = []
        offset = 0
        limit = 20
        global_total = None
        
        while True:
            payload = {
                "appliedFacets": {},
                "limit": limit,
                "offset": offset,
                "searchText": search_text
            }
            
            try:
                resp = requests.post(jobs_url, json=payload, timeout=10)
                if resp.status_code != 200:
                    break
                
                data = resp.json()
                jobs = data.get('jobPostings', [])
                current_total = data.get('total', 0)
                
                if global_total is None:
                    global_total = current_total
                    if global_total > 0:
                        print(f"      Total jobs found for '{search_text}': {global_total}")
                
                if not jobs:
                    break
                    
                all_jobs.extend(jobs)
                
                if len(all_jobs) >= global_total:
                    break
                
                if max_jobs and len(all_jobs) >= max_jobs:
                    break
                    
                offset += limit
                time.sleep(0.5) 
                
            except Exception as e:
                print(f"   ❌ Error fetching jobs for '{search_text}': {e}")
                break
                
        return all_jobs

    def fetch_job_details(self, api_base, job_slug):
        if not job_slug:
            return None
        url = f"{api_base}/job/{job_slug}"
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code != 200:
                return None
            data = resp.json()
            
            info = data.get('jobPostingInfo', {})
            description = info.get('jobDescription')
            city = info.get('location')
            country = info.get('country', {}).get('descriptor')
            
            return {
                "description": description,
                "city": city,
                "country": country
            }
        except Exception:
            return None

    def run(self):
        targets = self.load_targets()
        print(f"🚀 Starting Scrape on {len(targets)} Workday companies...\n")
        
        total_saved = 0
        search_terms = ["Hungary", "Budapest", "Magyarország"]
        
        for start_url in targets:
            start_url = start_url.split('?')[0]
            print(f"--> Analyzing {start_url}...")
            
            try:
                api_base, tenant, site = self.get_workday_config(start_url)
                company_name = tenant.capitalize()
                
                # Use a dictionary to deduplicate jobs by externalPath
                unique_jobs = {}
                
                for term in search_terms:
                    jobs = self.fetch_jobs(api_base, search_text=term)
                    for job in jobs:
                        path_key = job.get('externalPath')
                        if path_key and path_key not in unique_jobs:
                            unique_jobs[path_key] = job
                
                jobs_to_process = list(unique_jobs.values())
                
                if not jobs_to_process:
                    print(f"   ⚠️ 0 jobs found matching Hungary/Budapest for {company_name}.")
                    continue
                    
                print(f"   Found {len(jobs_to_process)} unique matching jobs. Fetching details and saving...")
                
                saved_for_company = 0
                for i, job in enumerate(jobs_to_process):
                    external_path = job.get('externalPath', '')
                    parts = external_path.strip('/').split('/')
                    job_slug = parts[-1] if parts else None
                    
                    job_url = f"{start_url.rstrip('/')}{external_path}"
                    
                    details = self.fetch_job_details(api_base, job_slug) or {}
                    
                    city = details.get('city')
                    if not city: city = "Unknown"
                    country = details.get('country')
                    if not country: country = "Unknown"
                    desc = details.get('description')
                    if not desc: desc = "Description not provided"
                    
                    saved = self.db_saver.save_job({
                        "url": job_url,
                        "title": job.get('title', 'Unknown'),
                        "company": company_name,
                        "location_raw": city,
                        "city": city,
                        "country": country,
                        "description": desc
                    })
                    
                    if saved:
                        saved_for_company += 1
                        
                print(f"   ✅ Processed {saved_for_company} NEW jobs for {company_name}.")
                total_saved += saved_for_company
                
                time.sleep(1)
                
            except Exception as e:
                print(f"   ❌ Failed to fetch {start_url}: {e}")
                
        print("\n" + "=" * 50)
        print(f"🏁 Workday Batch Complete. Total NEW Jobs Saved: {total_saved}")


if __name__ == "__main__":
    scraper = WorkdayScraper()
    scraper.run()
