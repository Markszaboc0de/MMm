from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

import sys as _sys
import os as _os
_sys.path.append(_os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))
from driver_setup import get_chrome_driver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import sqlite3
import time
import re
import os
import sys

sys.stdout.reconfigure(encoding='utf-8')

# --- CONFIGURATION ---
COMPANY_NAME = "Morgan Stanley"
BASE_URL = "https://www.morganstanley.com/careers/career-opportunities-search?opportunity=sg"
DATA_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
DB_PATH = os.path.join(DATA_FOLDER, "morganstanley_jobs.db")


def setup_database():
    os.makedirs(DATA_FOLDER, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT, 
            url TEXT UNIQUE, 
            title TEXT, 
            company TEXT, 
            location_raw TEXT, 
            city TEXT, 
            country TEXT, 
            description TEXT, 
            category TEXT, 
            date_found TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    return conn


def clean_city(raw_location):
    if not raw_location or raw_location == "N/A":
        return "N/A"
    cleaned = re.sub(r'\d+', '', raw_location)
    parts = cleaned.split(',')
    return parts[0].strip()


def get_country(raw_location):
    if not raw_location or raw_location == "N/A":
        return "N/A"
    parts = raw_location.split(',')
    if len(parts) > 1:
        return parts[-1].strip()
    return "N/A"


def accept_cookies(driver):
    js_script = """
    try {
        let otBtn = document.querySelector('#onetrust-accept-btn-handler');
        if (otBtn) otBtn.click();
        
        let buttons = document.querySelectorAll('button');
        for (let btn of buttons) {
            let text = (btn.innerText || "").toLowerCase().trim();
            if (text === 'accept' || text === 'accept all' || text === 'elfogadom' || text === 'ok') {
                btn.click();
            }
        }
        
        let oneTrust = document.querySelector('#onetrust-consent-sdk');
        if (oneTrust) oneTrust.remove();
    } catch(e) {}
    """
    driver.execute_script(js_script)
    time.sleep(1)


def run_scraper():
    conn = setup_database()
    cursor = conn.cursor()

    print(
        f"🚀 Starting {COMPANY_NAME} Scraper (ATS Strict Mode + Heaviest Container Walker)...")

    options.add_argument("--disable-popup-blocking")
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    driver = get_chrome_driver()
    job_links = []
    unique_urls = set()

    try:
        # --- PHASE 1: GATHERING LINKS ---
        print("📂 Opening Morgan Stanley career site...")
        driver.get(BASE_URL)
        time.sleep(6)

        accept_cookies(driver)

        page = 1
        while True:
            print(f"📄 Scraping list page {page}...")

            cards_found = False
            for _ in range(15):
                count = driver.execute_script(
                    "return document.querySelectorAll('a[data-analytics-job-card]').length;")
                if count > 0:
                    cards_found = True
                    break
                time.sleep(1)

            if not cards_found:
                print(
                    "⚠️ No job cards found. The page might be empty or loading failed.")
                break

            jobs_on_page = driver.execute_script("""
                let results = [];
                let links = document.querySelectorAll('a[data-analytics-job-card]');
                
                links.forEach(a => {
                    if (a.href && a.href.includes('tal.net')) {
                        let dataAttr = a.getAttribute('data-analytics-job-card') || "";
                        let parts = dataAttr.split('|').map(p => p.trim());
                        
                        let title = parts.length > 1 ? parts[1] : "Unknown Title";
                        let loc = parts.length > 2 ? parts[2] : "N/A";
                        let cat = parts.length > 3 ? parts[3] : "General";
                        
                        results.push({
                            url: a.href, 
                            title: title, 
                            location_raw: loc,
                            category: cat
                        });
                    }
                });
                return results;
            """)

            for job in jobs_on_page:
                if job['url'] not in unique_urls:
                    unique_urls.add(job['url'])
                    job_links.append(job)

            has_next_page = driver.execute_script("""
                let nextBtn = document.querySelector('a.arrow.next');
                if (nextBtn && window.getComputedStyle(nextBtn).pointerEvents !== 'none' && window.getComputedStyle(nextBtn).opacity !== '0.3') {
                    nextBtn.click();
                    return true;
                }
                return false;
            """)

            if has_next_page:
                page += 1
                time.sleep(4)
            else:
                print("🏁 Reached the last page.")
                break

        print(
            f"✅ Found {len(job_links)} unique ATS jobs. Starting description extraction...")

        if not job_links:
            return

        # --- PHASE 2: DEEP SCRAPING DESCRIPTIONS ---
        wait = WebDriverWait(driver, 15)

        for idx, job in enumerate(job_links, 1):

            # Use strict tuple matching for safety
            cursor.execute(
                "SELECT id FROM jobs WHERE url = ? AND description IS NOT NULL AND description != ''", (job['url'],))
            if cursor.fetchone():
                print(
                    f"   [{idx}/{len(job_links)}] {job['title']} (Already deeply scraped, skipping...)")
                continue

            print(f"   [{idx}/{len(job_links)}] Extracting: {job['title']}")
            driver.get(job['url'])

            # Ensure Oleeo forms have rendered
            try:
                wait.until(lambda d: d.execute_script(
                    "return document.querySelectorAll('.form-control-static[role=\"definition\"]').length > 0;"))
                time.sleep(1.5)
            except:
                print(
                    f"      ⚠️ Timeout waiting for description containers on {job['url']}")

            accept_cookies(driver)

            # --- THE FIX: Iterate through all containers and grab the heaviest one ---
            raw_description = driver.execute_script("""
                function walk(el) {
                    let text = "";
                    if (!el) return "";
                    
                    if (el.nodeType === 3) {
                        let val = el.nodeValue.replace(/\\s+/g, ' '); 
                        if (val !== ' ') text += val;
                    } else if (el.nodeType === 1) {
                        let tag = el.tagName.toUpperCase();
                        let cls = (el.className && typeof el.className === 'string') ? el.className.toLowerCase() : "";

                        if (['SCRIPT','STYLE','NAV','FOOTER','HEADER','BUTTON','SVG'].includes(tag)) return "";
                        if (cls.includes('share') || cls.includes('apply')) return "";

                        if (tag === 'LI') text += "- ";
                        
                        for (let child of el.childNodes) {
                            text += walk(child);
                        }
                        
                        if (['H1','H2','H3','H4'].includes(tag)) text += "\\n\\n";
                        else if (['P','DIV','BR'].includes(tag)) text += "\\n";
                        else if (tag === 'LI') text += "\\n";
                    }
                    return text;
                }

                let containers = document.querySelectorAll('.form-control-static[role="definition"], .opportunity-description');
                let bestText = '';
                
                for (let c of containers) {
                    let currentText = walk(c).trim();
                    if (currentText.length > bestText.length) {
                        bestText = currentText;
                    }
                }
                
                return bestText;
            """)

            if not raw_description:
                raw_description = "Description could not be loaded."

            city = clean_city(job["location_raw"])
            country = get_country(job["location_raw"])

            clean_desc = raw_description.replace('\xa0', ' ')
            clean_desc = re.sub(r' +', ' ', clean_desc)
            clean_desc = re.sub(r'\n[ \t]+\n', '\n\n', clean_desc)
            clean_desc = re.sub(r'\n{3,}', '\n\n', clean_desc).strip()

            try:
                # Update existing rows if they exist but lack descriptions (from the last run)
                cursor.execute("""
                    INSERT INTO jobs (url, title, company, location_raw, city, country, description, category)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(url) DO UPDATE SET description=excluded.description
                """, (job["url"], job["title"], COMPANY_NAME, job["location_raw"], city, country, clean_desc, job["category"]))
                conn.commit()
            except Exception as e:
                print(f"      ⚠️ Error saving job {idx}: {e}")

    finally:
        driver.quit()
        conn.close()
        print("\n✨ Scraping completed.")


if __name__ == "__main__":
    run_scraper()
