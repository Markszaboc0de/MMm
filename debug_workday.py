import os
import sys
import requests

# Add root directory to path to locate the target scrapers
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

# Dynamically import the workday scraper
target_path = os.path.join(BASE_DIR, "ATS scrapers", "scrapers", "scrape_workday.py")
if not os.path.exists(target_path):
    print(f"❌ Cannot find script at {target_path}")
    sys.exit(1)

# Import it
from importlib.machinery import SourceFileLoader
scrape_workday = SourceFileLoader("scrape_workday", target_path).load_module()

def run_diagnostics():
    print("="*60)
    print(" 🛠️ WORKDAY SCRAPER DIAGNOSTIC TOOL 🛠️")
    print("="*60)
    print("This script will execute the Workday scraping logic step-by-step")
    print("and print exactly what the server is returning to help identify")
    print("why the yield is so low.\n")
    
    try:
        scraper = scrape_workday.WorkdayScraper()
    except Exception as e:
        print(f"❌ Failed to initialize WorkdayScraper: {e}")
        return
        
    targets_file = scraper.targets_path
    
    print(f"[STEP 1] Loading targets from: {targets_file}")
    if not os.path.exists(targets_file):
        print("❌ Error: Targets file not found!")
        return
        
    with open(targets_file, 'r', encoding='utf-8') as f:
        targets = [line.strip() for line in f if line.strip()]
        
    print(f"✅ Successfully loaded {len(targets)} target URLs.")
    
    print("\n[STEP 2] Commencing live HTTP extraction test (limited to first 5 companies)...")
    
    test_targets = targets[:5]
    total_found = 0
    
    for i, target in enumerate(test_targets):
        print(f"\n--- [Company {i+1}/{len(test_targets)}] ---")
        print(f"🔗 Target URL: {target}")
        
        try:
            api_base, tenant, site = scraper.get_workday_config(target)
            print(f"🧩 Parsed Config -> API Base: {api_base} | Tenant: {tenant} | Site: {site}")
            
            jobs_url = f"{api_base}/jobs"
            print(f"📡 Requesting: POST {jobs_url}")
            
            payload = {
                "appliedFacets": {},
                "limit": 20,
                "offset": 0,
                "searchText": ""
            }
            headers = scraper.get_headers()
            
            print(f"📦 Payload sent: {payload}")
            print(f"🪪 User-Agent format: {headers['User-Agent'][:40]}...")
            
            # Send the request just like the scraper does
            resp = requests.post(jobs_url, json=payload, headers=headers, timeout=15)
            
            print(f"📥 Response Status: HTTP {resp.status_code}")
            
            if resp.status_code == 200:
                data = resp.json()
                total = data.get('total', 0)
                returned_jobs = len(data.get('jobPostings', []))
                print(f"✅ Success! API reported {total} total jobs available. This page returned {returned_jobs} jobs.")
                total_found += total
                
                # Try getting the details of the first job to test individual fetches
                if returned_jobs > 0:
                    external_path = data['jobPostings'][0].get('externalPath', '')
                    parts = external_path.strip('/').split('/')
                    first_job_slug = parts[-1] if parts else None
                    
                    if first_job_slug:
                        try:
                            detail_url = f"{api_base}/job/{first_job_slug}"
                            print(f"   🔍 Testing job detail fetch: GET {detail_url}")
                            detail_resp = requests.get(detail_url, headers=headers, timeout=15)
                            if detail_resp.status_code == 200:
                                print(f"   ✅ Job details fetched successfully!")
                                
                                # Step 2.5: Test SQLite Persistence
                                print("   💾 [PIPELINE TEST] Attempting to save job to SQLite (workday_jobs.db)...")
                                try:
                                    scraper.db_saver.save_job({
                                        "url": detail_url,
                                        "title": "Debug Job Title",
                                        "company": tenant,
                                        "location_raw": "Debug Location",
                                        "city": "Debug City",
                                        "country": "Debug Country",
                                        "description": "Debug Description"
                                    })
                                    print("   ✅ SQLite Save Successful!")
                                except Exception as e:
                                    print(f"   ❌ SQLite Save Failed: {e}")
                                    
                                # Step 2.6: Test PostgreSQL Export
                                print("   📤 [PIPELINE TEST] Attempting to push payload to PostgreSQL raw_db...")
                                try:
                                    from postgres_export import push_to_postgres
                                    
                                    # This mimics exactly what ATS scrapers/Run/main.py does
                                    test_payload = [{
                                        'url': detail_url,
                                        'job_title': "Debug Job Title",
                                        'company': tenant,
                                        'location_raw': "Debug Location",
                                        'city': "Debug City",
                                        'country': "Debug Country",
                                        'job_description': "Debug Description",
                                        'date': "2026-03-23 09:17:00"  # SQLite CURRENT_TIMESTAMP format
                                    }]
                                    
                                    push_to_postgres(test_payload)
                                    print("   ✅ PostgreSQL Push Test Finalized!")
                                except Exception as e:
                                    print(f"   ❌ PostgreSQL Push Failed: {type(e).__name__}: {e}")
                            else:
                                print(f"   ❌ Failed to fetch details. HTTP {detail_resp.status_code}")
                        except Exception as e:
                            print(f"   ⚠️ Job detail fetch exception: {e}")
                    else:
                        print(f"   ⚠️ Warning: Could not parse job slug from externalPath: {external_path}")
                
            elif resp.status_code == 403:
                print(f"❌ 403 Forbidden! The Workday server is actively blocking your VM's IP address.")
                print(f"   Reason: This usually implies Cloudflare, Akamai, or Workday's WAF has flagged the VM's datacenter IP as a bot network.")
            elif resp.status_code == 404:
                print(f"❌ 404 Not Found! The parsed API endpoint may be incorrect or the company disabled their JSON API window.")
            else:
                print(f"⚠️ Unexpected server response: {resp.text[:250]}")
                
        except requests.exceptions.Timeout:
            print(f"⏰ Connection perfectly Timed Out! The server took longer than 15s to respond.")
        except requests.exceptions.ConnectionError:
            print(f"🔌 Connection Error! Your VM could not connect to the remote host.")
        except Exception as e:
            print(f"💥 Native Python Exception occurred: {e}")
            
    print("\n" + "="*60)
    print(" 📊 DIAGNOSTIC SUMMARY ")
    print("="*60)
    print(f"Companies tested: {len(test_targets)}")
    print(f"Total jobs mathematically reported by these {len(test_targets)} companies: {total_found}")
    print("If you are seeing 'HTTP 403 Forbidden', it confirms the Workday servers natively block automated requests coming from your Ubuntu VM's IP block.")

if __name__ == '__main__':
    run_diagnostics()
