import requests
import sqlite3
import os
import sys
import time
import re
import html

sys.stdout.reconfigure(encoding='utf-8')

# --- CONFIGURATION ---
COMPANY_NAME = "Schneider Electric"

# The internal API endpoint
API_URL = "https://careers.se.com/api/jobs"
BASE_JOB_URL = "https://careers.se.com/jobs/"

DATA_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
DB_PATH = os.path.join(DATA_FOLDER, "schneider_jobs.db")

# The EU Countries you want to filter by
EU_COUNTRIES = [
    "France", "Spain", "Italy", "Germany", "Hungary", "Sweden", "Poland",
    "Denmark", "Slovakia", "Switzerland", "Norway", "Portugal", "Finland",
    "Netherlands", "Bulgaria", "Czech Republic", "Austria", "Belgium",
    "Greece", "Latvia", "Romania", "Lithuania"
]


def init_db():
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)
    conn = sqlite3.connect(DB_PATH)

    conn.execute('''CREATE TABLE IF NOT EXISTS jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT, url TEXT UNIQUE, title TEXT, 
        company TEXT, location_raw TEXT, city TEXT, country TEXT, description TEXT, category TEXT,
        date_found TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')

    conn.commit()
    conn.close()


def clean_html(raw_html):
    """Safely converts HTML into beautiful, readable plain text"""
    if not raw_html:
        return ""

    # Force newlines for block elements so sentences don't merge
    text = re.sub(r'</?(div|p|h[1-6]|ul|ol|table|tr|section|br)[^>]*>',
                  '\n\n', raw_html, flags=re.IGNORECASE)
    # Add dashes for list items
    text = re.sub(r'<li[^>]*>', '\n- ', text, flags=re.IGNORECASE)
    # Remove all other HTML tags
    text = re.sub(r'<[^>]+>', '', text)
    # Decode HTML entities (like &amp; or &nbsp;)
    text = html.unescape(text)

    # Clean up whitespace
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+\n', '\n\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)

    return text.strip()


def run_scraper():
    init_db()
    print(f"🚀 Starting {COMPANY_NAME} Scraper (High-Speed API Mode)...")

    job_links = []
    unique_urls = set()

    # Disguise our API request to look like a normal browser
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json"
    }

    # --- PHASE 1: HIGH SPEED API FETCHING ---
    page = 1
    while True:
        print(f"📄 Fetching API Page {page}...")

        # We send your exact filters directly to the API
        params = {
            "page": page,
            "limit": 100,  # Maximize items per request for speed
            "country": "|".join(EU_COUNTRIES)
        }

        try:
            response = requests.get(
                API_URL, headers=headers, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            print(f"⚠️ Error calling API: {e}")
            break

        jobs = data.get("jobs", [])
        if not jobs:
            print("🏁 Reached the end of the API results.")
            break

        for item in jobs:
            # The API usually wraps the actual job data inside a 'data' dictionary
            job_data = item.get("data", item)

            req_id = job_data.get("req_id", "")
            title = job_data.get("title", "N/A")

            # Perfect location separation
            city = job_data.get("city", "N/A")
            country = job_data.get("country", "N/A")
            location_raw = job_data.get("location", f"{city}, {country}")

            # ✨ THE MAGIC API FIX: Safely parse category even if it's a dict, list, or string ✨
            categories = job_data.get("categories", [])
            category = "N/A"
            if isinstance(categories, list) and len(categories) > 0:
                first_cat = categories[0]
                if isinstance(first_cat, dict):
                    # If it's a dict, safely try to get a name/title out of it
                    category = first_cat.get("name", str(first_cat))
                else:
                    category = str(first_cat)
            elif isinstance(categories, str):
                category = categories

            # Form the exact URL
            url = f"{BASE_JOB_URL}{req_id}" if req_id else "N/A"

            # The API serves the description right in the JSON!
            raw_desc = job_data.get("description", "")
            clean_desc = clean_html(raw_desc)

            # Slice off Schneider's generic marketing footers
            markers_to_remove = ["Let us learn about you!",
                                 "Looking to make an IMPACT", "Why us?", "Schedule:"]
            for marker in markers_to_remove:
                if marker in clean_desc:
                    clean_desc = clean_desc.split(marker)[0].strip()

            if url != "N/A" and url not in unique_urls:
                unique_urls.add(url)
                job_links.append({
                    "url": url,
                    "title": title,
                    "location_raw": location_raw,
                    "city": city,
                    "country": country,
                    "category": category,
                    "description": clean_desc
                })

        print(
            f"   📊 Extracted {len(jobs)} jobs from this API page. Total so far: {len(job_links)}")
        page += 1
        time.sleep(1)  # Be polite to their API servers

    print(
        f"\n✅ Identified {len(job_links)} unique jobs total! Saving directly to database...")

    if not job_links:
        return

    # --- PHASE 2: INSTANT DATABASE SAVING ---
    conn = sqlite3.connect(DB_PATH)

    saved_count = 0
    for idx, job in enumerate(job_links, 1):
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM jobs WHERE url = ?", (job['url'],))
            if cursor.fetchone():
                print(
                    f"   [{idx}/{len(job_links)}] {job['title']} (Already in DB, skipping...)")
                continue

            print(
                f"   [{idx}/{len(job_links)}] Saving: {job['title']} in {job['city']}, {job['country']}")

            # Safely cast every parameter to a string during insert to guarantee no SQLite crashes
            conn.execute('''INSERT INTO jobs (url, title, company, location_raw, city, country, description, category) 
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                         (str(job['url']), str(job['title']), COMPANY_NAME, str(job['location_raw']), str(job['city']), str(job['country']), str(job['description']), str(job['category'])))
            conn.commit()
            saved_count += 1

        except Exception as e:
            print(f"      ⚠️ Error saving job {idx}: {e}")

    conn.close()
    print(
        f"\n✨ SUCCESS! {saved_count} new {COMPANY_NAME} jobs saved to the database in record time.")


if __name__ == "__main__":
    run_scraper()
