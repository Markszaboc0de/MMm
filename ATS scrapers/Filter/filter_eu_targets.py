import csv
import re
import os

EU_COUNTRIES = [
    "austria", "belgium", "bulgaria", "croatia", "cyprus", "czech republic", "czechia", 
    "denmark", "estonia", "finland", "france", "germany", "greece", "hungary", "ireland", 
    "italy", "latvia", "lithuania", "luxemburg", "luxembourg", "malta", "netherlands", 
    "poland", "portugal", "romania", "slovakia", "slovenia", "spain", "sweden", 
    "switzerland", "norway", "serbia", "uk", "united kingdom", "great britain", "england", "scotland", "wales", "northern ireland",
    "nl", "de", "fr", "es", "it", "ie", "ch" # Common country abbreviations
]

EU_CITIES = [
    "paris", "berlin", "madrid", "rome", "bucharest", "vienna", "hamburg", "warsaw", 
    "budapest", "barcelona", "munich", "milan", "prague", "sofia", "brussels", 
    "birmingham", "cologne", "naples", "stockholm", "turin", "valencia", "zagreb", 
    "amsterdam", "frankfurt", "stuttgart", "copenhagen", "helsinki", "dublin", 
    "lisbon", "oslo", "geneva", "zurich", "belgrade", "london", "manchester", 
    "krakow", "wroclaw", "riga", "vilnius", "tallinn", "bratislava", "ljubljana", 
    "valletta", "nicosia", "berne", "basel", "lausanne", "rotterdam", "antwerp", 
    "ghent", "lyon", "marseille", "toulouse", "bordeaux", "lille", "porto", "seville", 
    "zaragoza", "malaga", "bilbao", "bologna", "florence", "genoa", "palermo", "venice", "leipzig",
    "dresden", "hannover", "nuremberg", "duisburg", "varna", "plovdiv", "cluj-napoca", "timisoara",
    "athens", "thessaloniki", "aarhus", "odense", "gothenburg", "malmo", "bristol", "edinburgh", "glasgow", "leeds"
]

EU_REGIONS = ["europe", "eu", "emea"]

def is_eu_location(location_raw, city, country):
    locs = [str(location_raw).lower(), str(city).lower(), str(country).lower()]
    combined = " ".join(locs)
    
    terms = EU_COUNTRIES + EU_CITIES + EU_REGIONS
    
    for term in terms:
        # Avoid matching partial words (e.g. matching 'uk' inside 'ukraine' or 'unknown')
        pattern = r'\b' + re.escape(term) + r'\b'
        if re.search(pattern, combined):
            return True
            
    return False

def main():
    root_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(root_dir, 'data', 'ashby_results.csv')
    targets_path = os.path.join(root_dir, 'targets', 'ashby_targets.txt')
    
    eu_companies = set()
    
    print("Reading CSV...")
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            company = row.get('company', '').strip()
            if not company:
                continue
                
            # Quick check if already in set
            if company.lower() in eu_companies:
                continue
                
            if is_eu_location(row.get('location_raw', ''), row.get('city', ''), row.get('country', '')):
                eu_companies.add(company.lower())
                
    print(f"Found {len(eu_companies)} companies with EU jobs.")
    
    # Read target URLs
    with open(targets_path, 'r', encoding='utf-8') as f:
        urls = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
        
    print(f"Read {len(urls)} target URLs.")
    
    filtered_urls = []
    removed_urls = []
    
    for url in urls:
        company_slug = url.split('/')[-1]
        
        # Check various formats of company name against the set
        company_slug_lower = company_slug.lower()
        company_capitalized = company_slug.capitalize().lower()
        company_spaced = company_slug.replace('-', ' ').lower()
        
        # The scraper saved it as company_slug.capitalize()
        # Look for a match
        matched = False
        if company_slug_lower in eu_companies or company_capitalized in eu_companies or company_spaced in eu_companies:
            matched = True
        else:
            # Fallback: ignore spaces and hyphens
            for eu_co in eu_companies:
                if eu_co.replace(' ', '').replace('-', '') == company_slug_lower.replace('-', ''):
                    matched = True
                    break
                    
        if matched:
            filtered_urls.append(url)
        else:
            removed_urls.append(url)
            
    print(f"Keeping {len(filtered_urls)} URLs. Removing {len(removed_urls)} URLs.")
    
    # Write back to targets file
    with open(targets_path, 'w', encoding='utf-8') as f:
        for url in filtered_urls:
            f.write(url + '\n')
            
    # Optionally list some removed targets to verify
    if removed_urls:
        print("\nSample removed URLs:")
        for r in removed_urls[:10]:
            print(f" - {r}")
            
    print("\nFiltered targets successfully updated.")

if __name__ == '__main__':
    main()
