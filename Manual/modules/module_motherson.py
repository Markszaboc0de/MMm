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
import os
import sys
import time
import re
from bs4 import BeautifulSoup
from markdownify import markdownify as md

sys.stdout.reconfigure(encoding='utf-8')

# --- CONFIGURATION ---
COMPANY_NAME = "Motherson"

# Your exact filtered URL
BASE_URL = "https://careers.motherson.com/en/jobs?country=Hungary&country=CzechRepublic&country=Estonia&country=France&country=Germany&country=Lithuania&country=Poland&country=Spain&experience=Entrylevel&experience=Kezd%25C5%2591%25C3%25A9sgyakornok%252CStudentsInterns%252CDualstudies&experience=Internship%252CThesis"

DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Manual\data"
DB_PATH = os.path.join(DATA_FOLDER, "motherson_jobs.db")


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


def run_scraper():
    init_db()
    print(f"🚀 Starting {COMPANY_NAME} Scraper (React DOM + Markdown Mode)...")

    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    driver = get_chrome_driver()

    job_links = []
    unique_urls = set()

    try:
        # --- PHASE 1: LOAD MAIN PAGE & SCROLL ---
        print("📂 Visiting Motherson filtered careers page...")
        driver.get(BASE_URL)

        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "a[href*='/job/']"))
            )
            time.sleep(3)
        except:
            print("⚠️ Initial load failed or no jobs found. Check the URL filters.")
            return

        print("⏳ Scrolling to load all dynamic job cards...")
        driver.execute_script(
            "window.scrollTo(0, document.body.scrollHeight);")

        scroll_attempts = 0
        last_height = driver.execute_script(
            "return document.body.scrollHeight")

        while scroll_attempts < 5:
            # Click "Load More" buttons if they pop up
            driver.execute_script("""
                let buttons = Array.from(document.querySelectorAll('button'));
                let loadMore = buttons.find(b => (b.innerText || '').toLowerCase().includes('more') || (b.innerText || '').toLowerCase().includes('további'));
                if(loadMore) { loadMore.click(); }
            """)

            driver.execute_script(
                "window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2.5)

            new_height = driver.execute_script(
                "return document.body.scrollHeight")
            if new_height == last_height:
                scroll_attempts += 1
            else:
                scroll_attempts = 0
                last_height = new_height

        print("📄 Extracting data from all rendered cards...")

        # Perfected DOM Parser
        jobs_on_page = driver.execute_script("""
            let results = [];
            let cards = document.querySelectorAll('a[href*="/job/"]');
            
            cards.forEach(aTag => {
                if (aTag && aTag.getAttribute('href')) {
                    let href = aTag.getAttribute('href');
                    let url = href.startsWith('http') ? href : window.location.origin + (href.startsWith('/') ? '' : '/') + href;
                    
                    let title = (aTag.innerText || aTag.textContent || "").trim();
                    
                    // Grab the "m/w/d" suffix if present
                    let suffixEl = aTag.parentElement.nextElementSibling;
                    if(suffixEl && suffixEl.innerText && suffixEl.innerText.includes('(')) {
                        title = title + " " + suffixEl.innerText.trim();
                    }
                    
                    let loc_raw = "N/A";
                    let country = "N/A";
                    let city = "N/A";
                    
                    // Climb up to the main card container
                    let masterCard = aTag.closest('div[class*="e8nfvi23"]') || aTag.parentElement.parentElement.parentElement;
                    
                    if (masterCard) {
                        let allDivs = masterCard.querySelectorAll('div');
                        for (let d of allDivs) {
                            let html = d.innerHTML || "";
                            
                            // Target the exact div containing the <br> tag (e.g., "Lithuania<br>Panevėžys")
                            if (html.includes('<br>') && !html.includes('<div') && !html.includes('<a')) {
                                let parts = html.split('<br>');
                                if (parts.length >= 2) {
                                    // Strip any rogue tags and clean the text
                                    country = parts[0].replace(/<[^>]+>/g, '').trim();
                                    city = parts[1].replace(/<[^>]+>/g, '').trim();
                                    loc_raw = country + ", " + city;
                                    break;
                                }
                            }
                        }
                    }
                    
                    let cat = "N/A";
                    if (masterCard) {
                        let spans = masterCard.querySelectorAll('span');
                        if (spans.length > 1) {
                            cat = (spans[spans.length - 1].innerText || "").trim(); 
                        }
                    }
                    
                    results.push({ url: url, title: title, location_raw: loc_raw, city: city, country: country, category: cat });
                }
            });
            return results;
        """)

        for job in jobs_on_page:
            if job['url'] not in unique_urls:
                unique_urls.add(job['url'])
                job_links.append(job)

        print(
            f"✅ Identified {len(job_links)} unique jobs total! Starting deep scrape...")

        if not job_links:
            return

        # --- PHASE 2: DEEP SCRAPING DESCRIPTIONS ---
        conn = sqlite3.connect(DB_PATH)
        wait = WebDriverWait(driver, 10)

        for idx, job in enumerate(job_links, 1):
            try:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT id FROM jobs WHERE url = ?", (job['url'],))
                if cursor.fetchone():
                    print(
                        f"   [{idx}/{len(job_links)}] {job['title']} (Already in DB, skipping...)")
                    continue

                print(
                    f"   [{idx}/{len(job_links)}] Formatting: {job['title']} in {job['city']}, {job['country']}...")

                # Fetch the actual job page
                driver.get(job['url'])

                try:
                    # Wait for the description container
                    wait.until(EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "div[class*='css-yj3pth'], div[class*='e1cpgz7q']")))
                    time.sleep(1)
                except:
                    time.sleep(2)

                # Get the raw HTML
                html_source = driver.execute_script("""
                    let desc = document.querySelector("div[class*='css-yj3pth']") || document.querySelector("div[class*='e1cpgz7q']");
                    return desc ? desc.innerHTML : "";
                """)

                if html_source:
                    soup = BeautifulSoup(html_source, 'html.parser')
                    clean_html = str(soup)

                    # Convert HTML directly to Markdown
                    clean_desc = md(clean_html, heading_style="ATX", bullets="-",
                                    strip=['img', 'script', 'style', 'table', 'tr', 'td'])

                    # Clean up encoding artifacts and excessive blank lines
                    clean_desc = clean_desc.replace(
                        '•', '-').replace('\xa0', ' ')
                    clean_desc = re.sub(r'\n{3,}', '\n\n', clean_desc).strip()

                else:
                    clean_desc = "Description could not be loaded."

                # Save to Database
                conn.execute('''INSERT INTO jobs (url, title, company, location_raw, city, country, description, category) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                             (str(job['url']), str(job['title']), COMPANY_NAME, str(job['location_raw']),
                              str(job['city']), str(job['country']), str(clean_desc), str(job['category'])))
                conn.commit()

            except Exception as e:
                print(f"      ⚠️ Error saving job {idx}: {e}")

        conn.close()
        print(
            f"\n✨ SUCCESS! {COMPANY_NAME} jobs perfectly formatted in Markdown and saved.")

    finally:
        try:
            driver.quit()
        except:
            pass


if __name__ == "__main__":
    run_scraper()
