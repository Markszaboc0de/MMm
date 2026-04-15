import time
import os
import re
import requests
from bs4 import BeautifulSoup
from core.base_scraper import BaseScraper


class GreenhouseAdapter:
    def __init__(self, target_filename="../targets/greenhouse_targets.txt", db_filename="../data/greenhouse_jobs.db"):
        self.script_location = os.path.dirname(os.path.abspath(__file__))
        full_db_path = os.path.join(self.script_location, db_filename)
        self.scraper = BaseScraper(db_name=full_db_path)
        full_target_path = os.path.join(self.script_location, target_filename)
        self.targets = self._load_targets(full_target_path)

        # --- COMPANY NAME FIXES ---
        self.name_map = {
            "realtimeboardglobal": "Miro",
            "remotecom": "Remote",
            "scalapaysrl": "Scalapay",
            "urbansportsclub": "Urban Sports Club",
            "getyourguide": "GetYourGuide",
            "autoscout24": "AutoScout24",
            "modulrfinance": "Modulr",
            "purestorage": "Pure Storage",
            "hellofresh": "HelloFresh",
            "tripadvisor": "TripAdvisor",
            "uipath": "UiPath",
            "contentful": "Contentful",
            "justeattakeaway": "Just Eat Takeaway",
            "tiermobility": "Tier",
            "voitechnology": "Voi",
            "worldremit": "Zepz (WorldRemit)",
            "taxfix": "Taxfix",
            "gocardless": "GoCardless",
            "soundcloud": "SoundCloud",
            "freshworks": "Freshworks",
            "booking": "Booking.com",
            "doctolib": "Doctolib"
        }

    def _load_targets(self, filepath):
        print(f"📂 Loaded targets from: {filepath}")
        try:
            with open(filepath, 'r') as f:
                return [line.strip() for line in f.readlines() if line.strip() and not line.startswith("#")]
        except FileNotFoundError:
            print("❌ CRITICAL ERROR: File '../targets/targets.txt' not found.")
            exit()

    def _clean_html(self, html_content):
        """Converts raw HTML description into readable text."""
        if not html_content:
            return ""
        soup = BeautifulSoup(html_content, 'html.parser')
        return soup.get_text(separator="\n").strip()

    def _clean_title(self, title):
        """
        Aggressively cleans job titles.
        Handles: [Tags], (Parentheses), En-dashes, Commas, Pipes.
        """
        if not title:
            return "Unknown"

        # 1. Remove [Bracketed] and (Parenthesized) text
        # e.g. "[ITO] Project Manager (Remote)" -> "Project Manager "
        title = re.sub(r'\[.*?\]', '', title)
        title = re.sub(r'\(.*?\)', '', title)

        # 2. Split by any dash-like separator
        # Hyphen (-), En-dash (–), Em-dash (—), Pipe (|)
        for sep in [' - ', ' – ', ' — ', ' | ']:
            if sep in title:
                title = title.split(sep)[0]

        # 3. Handle specific comma-location patterns (Graphcore/Agoda)
        # e.g. "Engineer, Bengaluru" -> "Engineer"
        if ", Bengaluru" in title:
            title = title.split(", Bengaluru")[0]
        if ", Bangkok" in title:
            title = title.split(", Bangkok")[0]

        # 4. Final Cleanup
        return title.strip()

    def _split_location(self, raw_loc):
        if not raw_loc or raw_loc in ["Unknown", "See URL"]:
            return "Unknown", "Unknown"

        clean = raw_loc.replace(";", "").replace("(", "").replace(")", "").strip()

        is_remote = False
        # Remove Remote tags to find actual locations
        lower_clean = clean.lower()
        if any(x in lower_clean for x in ["remote", "home based", "everywhere"]):
            is_remote = True
            # Try to strip Remote out of the string so we can parse the location
            clean = re.sub(r'(?i)\b(remote|home based|everywhere)\b', '', clean).strip(" -/,")

        if not clean:
            return "Remote", "Remote"

        # City, Country
        if "," in clean:
            parts = clean.split(',')
            city = parts[0].strip()
            country = parts[-1].strip()
            if is_remote and "remote" not in city.lower():
                city = f"Remote - {city}"
            return city, country

        # Region - Country
        if " - " in clean:
            parts = clean.split(" - ")
            country = parts[-1].strip()
            city = parts[0].strip()
            if is_remote:
                 city = f"Remote - {city}"
            return city, country

        if is_remote:
            return f"Remote - {clean}", clean

        return clean, clean

    def _upgrade_url(self, url):
        """Forces 'boards' (Description) instead of 'job-boards' (Application Form)."""
        if "job-boards.greenhouse.io" in url:
            return url.replace("job-boards.greenhouse.io", "boards.greenhouse.io")
        if "job-boards.eu.greenhouse.io" in url:
            return url.replace("job-boards.eu.greenhouse.io", "boards.eu.greenhouse.io")
        return url

    def _get_pretty_name(self, slug):
        slug_lower = slug.lower()
        if slug_lower in self.name_map:
            return self.name_map[slug_lower]
        return slug.capitalize()

    def run(self):
        print(f"🚀 Starting Scrape (Aggressive Title Clean + Names)...")
        print("-" * 50)

        total_jobs = 0

        for company_url in self.targets:
            slug = company_url.split('/')[-1]
            if not slug:
                continue

            company_count = 0

            # 1. Try API (US & EU)
            variations = [slug, f"{slug}-inc", f"{slug}careers", f"{slug}jobs"]
            api_worked = False

            for v in variations:
                count = self.scrape_via_api(v, company_url, region="us")
                if count > 0:
                    company_count = count
                    api_worked = True
                    break
                count = self.scrape_via_api(v, company_url, region="eu")
                if count > 0:
                    company_count = count
                    api_worked = True
                    break

            # 2. Try HTML Fallback
            if not api_worked:
                html = self.scraper.get_page(company_url)
                if html:
                    company_count = self.parse_html(html, company_url)

            # Summary Print
            display_name = self._get_pretty_name(slug)

            if company_count > 0:
                print(f"✅ {display_name:<20} | +{company_count} jobs")
                total_jobs += company_count
            else:
                print(f"⚠️ {display_name:<20} | 0 jobs found")

            time.sleep(0.5)

        self.scraper.close()
        print("-" * 50)
        print(f"🏁 Batch complete. Total Jobs: {total_jobs}")

    def scrape_via_api(self, company_slug, source_url, region="us"):
        domain = "boards-api.eu.greenhouse.io" if region == "eu" else "boards-api.greenhouse.io"
        api_url = f"https://{domain}/v1/boards/{company_slug}/jobs?content=true"

        try:
            headers = {'User-Agent': 'Mozilla/5.0'}
            response = requests.get(api_url, headers=headers, timeout=5)

            if response.status_code == 200:
                data = response.json()
                jobs = data.get('jobs', [])
                if not jobs:
                    return 0

                count = 0
                for job in jobs:
                    raw_title = job.get('title', 'Unknown')
                    # CLEAN THE TITLE
                    title = self._clean_title(raw_title)

                    location_text = job.get(
                        'location', {}).get('name', 'Unknown')

                    raw_html = job.get('content', '')
                    description_text = self._clean_html(raw_html)

                    url = job.get('absolute_url', source_url)
                    url = self._upgrade_url(url)

                    if location_text == "Unknown" and " - " in raw_title:
                        parts = raw_title.split(" - ")
                        if len(parts) > 1:
                            location_text = parts[-1]

                    city, country = self._split_location(location_text)
                    company_name = self._get_pretty_name(
                        source_url.split('/')[-1])

                    saved = self.scraper.save_job({
                        "url": url,
                        "title": title,
                        "company": company_name,
                        "location_raw": location_text,
                        "city": city,
                        "country": country,
                        "description": description_text
                    })
                    if saved:
                        count += 1
                        if os.environ.get("HEALTH_CHECK_MODE") == "1":
                            break
                return count
            return 0
        except Exception as e:
            print(f"   ❌ Error fetching from API: {e}")
            return 0

    def parse_html(self, html, source_url):
        soup = BeautifulSoup(html, 'html.parser')
        base_domain = "https://boards.greenhouse.io"
        if "job-boards.greenhouse.io" in str(soup):
            base_domain = "https://job-boards.greenhouse.io"

        count = 0
        for link in soup.find_all('a', href=True):
            href = link['href']
            if ("/jobs/" in href or "/embed/" in href) and "mailto:" not in href:
                raw_title = link.get_text(strip=True)
                title = self._clean_title(raw_title)

                full_link = f"{base_domain}{href}" if not href.startswith(
                    "http") else href
                full_link = self._upgrade_url(full_link)

                location_text = "Unknown"
                if " - " in raw_title:
                    parts = raw_title.split(" - ")
                    location_text = parts[-1]

                city, country = self._split_location(location_text)
                company_name = self._get_pretty_name(source_url.split('/')[-1])

                saved = self.scraper.save_job({
                    "url": full_link,
                    "title": title,
                    "company": company_name,
                    "location_raw": location_text,
                    "city": city,
                    "country": country,
                    "description": "See URL"
                })
                if saved:
                    count += 1
                    if os.environ.get("HEALTH_CHECK_MODE") == "1":
                        break
        return count


if __name__ == "__main__":
    adapter = GreenhouseAdapter()
    adapter.run()
