import csv
import re
import os
from urllib.parse import urlparse

EU_COUNTRIES = [
    "austria", "belgium", "bulgaria", "croatia", "cyprus", "czech republic", "czechia", 
    "denmark", "estonia", "finland", "france", "germany", "greece", "hungary", "ireland", 
    "italy", "latvia", "lithuania", "luxemburg", "luxembourg", "malta", "netherlands", 
    "poland", "portugal", "romania", "slovakia", "slovenia", "spain", "sweden", 
    "switzerland", "norway", "serbia", 
    "nl", "de", "fr", "es", "it", "ie", "ch", # Common country abbreviations
    "deutschland", "österreich", "schweiz", "italia", "españa", "espana", "polska", "nederland", "sverige",
    "suomi", "norge", "danmark", "belgique", "belgie", "magyarország", "magyarorszag",
    # German names
    "kroatien", "tschechien", "polen", "frankreich", "italien", "spanien", "schweden", 
    "finnland", "norwegen", "dänemark", "belgien", "niederlande", "ungarn", "rumänien", 
    "bulgarien", "slowakei", "slowenien", "irland", "griechenland", "lettland", "litauen", 
    "estland", "zypern"
]

EU_CITIES = [
    "paris", "berlin", "madrid", "rome", "bucharest", "vienna", "hamburg", "warsaw", 
    "budapest", "barcelona", "munich", "milan", "prague", "sofia", "brussels", 
    "cologne", "naples", "stockholm", "turin", "valencia", "zagreb", 
    "amsterdam", "frankfurt", "stuttgart", "copenhagen", "helsinki", "dublin", 
    "lisbon", "oslo", "geneva", "zurich", "belgrade", 
    "krakow", "wroclaw", "riga", "vilnius", "tallinn", "bratislava", "ljubljana", 
    "valletta", "nicosia", "berne", "basel", "lausanne", "rotterdam", "antwerp", 
    "ghent", "lyon", "marseille", "toulouse", "bordeaux", "lille", "porto", "seville", 
    "zaragoza", "malaga", "bilbao", "bologna", "florence", "genoa", "palermo", "venice", "leipzig",
    "dresden", "hannover", "nuremberg", "duisburg", "varna", "plovdiv", "cluj-napoca", "timisoara",
    "athens", "thessaloniki", "aarhus", "odense", "gothenburg", "malmo"
]

EU_REGIONS = ["europe", "eu", "dach", "emea"]

def is_eu_location(location_raw, city, country):
    locs = [str(location_raw).lower(), str(city).lower(), str(country).lower()]
    combined = " ".join(locs)
    
    terms = EU_COUNTRIES + EU_CITIES + EU_REGIONS
    
    for term in terms:
        # Avoid matching partial words
        pattern = r'\b' + re.escape(term) + r'\b'
        if re.search(pattern, combined):
            return True
            
    return False

def extract_base_url(url):
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}"

def process_ats(ats_name, root_dir):
    print(f"\n--- Processing {ats_name.upper()} ---")
    csv_path = os.path.join(root_dir, 'data', f'{ats_name}_results.csv')
    targets_path = os.path.join(root_dir, 'targets', f'{ats_name}_targets.txt')
    
    if not os.path.exists(csv_path):
        print(f"Missing data file for {ats_name}. Skipping.")
        return
        
    eu_companies_base_urls = set()
    all_companies_base_urls = set()
    
    print("Reading CSV to find strictly EU URLs...")
    with open(csv_path, 'r', encoding='utf-8') as f:
        try:
            reader = csv.DictReader(f)
            for row in reader:
                url = row.get('url', '').strip()
                if not url:
                    continue
                    
                base_url = extract_base_url(url)
                all_companies_base_urls.add(base_url)
                
                # If we already confirmed this company has an EU job, no need to check again
                if base_url in eu_companies_base_urls:
                    continue
                    
                location_raw = row.get('location_raw', '')
                city = row.get('city', '')
                country = row.get('country', '')
                
                if is_eu_location(location_raw, city, country):
                    eu_companies_base_urls.add(base_url)
        except Exception as e:
            print(f"Error reading CSV: {e}")
                
    print(f"Scraped data contains {len(all_companies_base_urls)} unique company base URLs.")
    print(f"Found {len(eu_companies_base_urls)} company URLs with jobs strictly in the EU (NO UK).")
    
    removed_urls = all_companies_base_urls - eu_companies_base_urls
    print(f"Removed {len(removed_urls)} URLs that had no jobs in the designated EU regions (e.g. only UK, US, etc).")
    
    # Sort for deterministic output
    final_urls = sorted(list(eu_companies_base_urls))
    
    # Write back to targets file
    with open(targets_path, 'w', encoding='utf-8') as f:
        for url in final_urls:
            f.write(url + '\n')
            
    if removed_urls:
        print("\nSample REMOVED URLs (e.g., only UK or other non-EU locations):")
        for r in sorted(list(removed_urls))[:10]:
            print(f" - {r}")
            
    # Also print some kept URLs to visually verify
    if final_urls:
        print("\nSample KEPT URLs (EU jobs verified):")
        for r in final_urls[:10]:
            print(f" - {r}")
            
    print(f"\nFiltered targets for {ats_name} successfully written.")

def main():
    root_dir = os.path.dirname(os.path.abspath(__file__))
    ats_list = ['softgarden', 'teamtailor']
    
    for ats in ats_list:
        process_ats(ats, root_dir)

if __name__ == '__main__':
    main()
