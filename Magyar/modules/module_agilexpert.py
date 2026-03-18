import requests
from bs4 import BeautifulSoup
import sqlite3
import os
import sys

# Force UTF-8 encoding for Windows terminals
sys.stdout.reconfigure(encoding='utf-8')

# ==========================================
# ⚙️ CONFIGURATION
# ==========================================
COMPANY_NAME = "AgileXpert"
BASE_URL = "https://agilexpert.hu/karrier/"

# 🎯 The EXACT custom data folder path
DATA_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
DB_PATH = os.path.join(DATA_FOLDER, "agilexpert_jobs.db")


def init_db():
    """Ensure the data directory and database exist before trying to insert."""
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)
        print(f"   📁 Created new data directory: {DATA_FOLDER}")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        url TEXT UNIQUE NOT NULL,
        title TEXT NOT NULL,
        company TEXT NOT NULL,
        location_raw TEXT,
        city TEXT,
        country TEXT,
        description TEXT
    )
    ''')
    conn.commit()
    conn.close()


def run_scraper():
    print(f"   🏢 Starting scraper for {COMPANY_NAME}...")
    init_db()

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    try:
        # 1. Fetch the main career page
        response = requests.get(BASE_URL, headers=headers, timeout=15)
        response.encoding = 'utf-8'  # Force UTF-8 for Hungarian accents
        soup = BeautifulSoup(response.text, 'html.parser')

        # 2. Find all job links based on the specific WordPress button class
        job_links = set()
        buttons = soup.find_all('a', class_='wp-block-button__link')

        for btn in buttons:
            href = btn.get('href')
            if href and "mailto" not in href:
                # Reconstruct the absolute URL if it's a relative link
                if not href.startswith("http"):
                    clean_href = href.strip('/')
                    full_url = f"https://agilexpert.hu/karrier/{clean_href}/"
                else:
                    full_url = href

                job_links.add(full_url)

        if not job_links:
            print(
                f"   ⚠️ No jobs found on {COMPANY_NAME}. They might not be hiring right now.")
            return

        print(f"   🔍 Found {len(job_links)} job links. Extracting details...")

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        new_jobs_added = 0

        # 3. Visit each job page to extract the Title and Description
        for job_url in job_links:
            try:
                job_res = requests.get(job_url, headers=headers, timeout=10)
                job_res.encoding = 'utf-8'
                job_soup = BeautifulSoup(job_res.text, 'html.parser')

                # Extract Title
                title_tag = job_soup.find('h2', class_='uagb-heading-text')
                if not title_tag:
                    continue

                title = title_tag.get_text(strip=True)

                # 🧠 THE FIX: Removed the junior filter to scrape EVERYTHING.
                # 🧠 THE FIX: Sequential scanning downward to bypass broken WordPress containers.

                description_lines = []

                # Scan every paragraph, heading, and list that comes AFTER the title
                for tag in title_tag.find_all_next(['p', 'h3', 'h4', 'ul', 'ol']):

                    # Prevent duplicates: If a <p> is weirdly inside a <ul>, skip it because we process the <ul> directly
                    if tag.find_parent(['ul', 'ol']):
                        continue

                    text = tag.get_text(strip=True)
                    if not text:
                        continue

                    # Handle lists beautifully
                    if tag.name in ['ul', 'ol']:
                        for li in tag.find_all('li'):
                            li_text = li.get_text(strip=True)
                            if li_text:
                                description_lines.append(f"- {li_text}")
                        # Add a blank line after the list
                        description_lines.append("")

                    # Handle normal paragraphs and subheadings
                    else:
                        description_lines.append(text)

                    # The Ultimate Stop Condition!
                    if "hr@agilexpert.hu" in text:
                        break

                description = "\n".join(description_lines).strip()

                location_raw = "Magyarország"
                city = "Budapest"
                country = "Hungary"

                # 4. Save to Database
                cursor.execute('''
                    INSERT OR IGNORE INTO jobs (url, title, company, location_raw, city, country, description)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (job_url, title, COMPANY_NAME, location_raw, city, country, description))

                if cursor.rowcount > 0:
                    new_jobs_added += 1

            except Exception as e:
                print(f"   ❌ Failed to scrape job {job_url}: {e}")

        conn.commit()
        conn.close()

        print(
            f"   ✅ {COMPANY_NAME} complete! {new_jobs_added} new jobs saved to database.")

    except Exception as e:
        print(f"   ❌ Critical error scraping {COMPANY_NAME}: {e}")


if __name__ == "__main__":
    run_scraper()
