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
import json
from markdownify import markdownify as md

sys.stdout.reconfigure(encoding='utf-8')

# --- CONFIGURATION ---
COMPANY_NAME = "Erste Group"

# The URL with your pre-applied Job Level filters (Apprentice, Internship, Junior, Trainee)
BASE_URL = "https://www.erstegroup.com/en/career/positions-offered#/joblist/level_of_experties/Apprentice%2CInternship%2CJunior%2CTrainee"

DATA_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
DB_PATH = os.path.join(DATA_FOLDER, "erste_jobs.db")


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


# Using centralized get_chrome_driver instead of broken local create_driver


def run_scraper():
    init_db()
    print(
        f"🚀 Starting {COMPANY_NAME} Scraper (JSON-LD Data Extraction Mode)...")

    HEALTH_CHECK = os.environ.get("HEALTH_CHECK_MODE") == "1"
    driver = get_chrome_driver()
    job_links = []
    unique_urls = set()

    try:
        # --- PHASE 1: GATHERING LINKS ---
        print("📂 Opening Erste Group careers page with pre-set filters...")
        driver.get(BASE_URL)
        time.sleep(8)

        # Handle Cookie Banner
        try:
            cookie_btn = driver.find_element(
                By.XPATH, "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'accept') or contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'allow') or contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'agree')]")
            driver.execute_script("arguments[0].click();", cookie_btn)
            time.sleep(1.5)
        except:
            pass

        print("⏳ Waiting for job list to render...")
        try:
            WebDriverWait(driver, 20).until(
                lambda d: d.execute_script(
                    "return document.querySelectorAll('a[href*=\"/job-detail/\"]').length > 0;")
            )
            time.sleep(2)
        except:
            print("⚠️ Initial load failed. The API might be slow or returning 0 jobs.")
            return

        print("⏳ Expanding job list (clicking 'Load More' if available)...")
        click_count = 0
        while True:
            driver.execute_script(
                "window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)

            clicked = driver.execute_script("""
                let buttons = document.querySelectorAll('button');
                for (let btn of buttons) {
                    let text = (btn.innerText || btn.textContent || "").toLowerCase();
                    if (text.includes('load more') || text.includes('mehr laden') || text.includes('weitere laden')) {
                        if (!btn.disabled && btn.offsetParent !== null) {
                            btn.click();
                            return true;
                        }
                    }
                }
                return false;
            """)

            if clicked:
                click_count += 1
                sys.stdout.write(
                    f"\r   🔄 Clicked 'Load More' {click_count} times...")
                sys.stdout.flush()
                time.sleep(3)
                if HEALTH_CHECK:
                    print("\n⚡ HEALTH_CHECK_MODE: stopping after 1 Load More click.")
                    break
            else:
                print("\n🏁 List is fully expanded.")
                break

        print("📄 Extracting job data from the list...")

        jobs_on_page = driver.execute_script("""
            let results = [];
            // STRICT FILTER: Only grab links that actually point to a job detail page!
            let links = document.querySelectorAll('a[href*="/job-detail/"]');
            
            links.forEach(a => {
                if (a.href) {
                    let title = a.innerText.trim();
                    if (!title) {
                        let span = a.querySelector('span.link__content');
                        if (span) title = span.innerText.trim();
                    }

                    results.push({
                        url: a.href,
                        title: title,
                        category: 'Finance / Banking'
                    });
                }
            });
            return results;
        """)

        for job in jobs_on_page:
            if job['url'] not in unique_urls:
                unique_urls.add(job['url'])
                job_links.append(job)

        print(
            f"✅ Identified {len(job_links)} unique jobs. Rebooting browser for deep extraction...")

        if not job_links:
            return

        # --- PRE-PHASE 2: BROWSER REBOOT ---
        try:
            driver.quit()
        except:
            pass
        time.sleep(2)

        driver = get_chrome_driver()
        wait = WebDriverWait(driver, 10)

        # --- PHASE 2: DEEP SCRAPING VIA JSON-LD ---
        conn = sqlite3.connect(DB_PATH)
        saved_count = 0

        max_jobs = 1 if HEALTH_CHECK else len(job_links)
        for idx, job in enumerate(job_links[:max_jobs], 1):
            try:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT id FROM jobs WHERE url = ?", (job['url'],))
                if cursor.fetchone():
                    print(
                        f"   [{idx}/{len(job_links)}] {job['title']} (Already in DB, skipping...)")
                    continue

                try:
                    driver.get(job['url'])
                except:
                    print("   🔄 Connection lost. Auto-recovering browser...")
                    try:
                        driver.quit()
                    except:
                        pass
                    time.sleep(2)
                    driver = get_chrome_driver()
                    wait = WebDriverWait(driver, 10)
                    driver.get(job['url'])

                try:
                    wait.until(EC.presence_of_element_located(
                        (By.TAG_NAME, "h1")))
                    time.sleep(1)
                except:
                    time.sleep(2)

                # The Ultimate Extractor: JSON-LD primary, un-hidden DOM secondary
                details = driver.execute_script("""
                    let result = { location: "N/A", description: "" };
                    
                    // --- 1. JSON-LD EXTRACTION (The Cheat Code) ---
                    try {
                        let jsonNodes = document.querySelectorAll('script[data-testid="jobJsonLd"], script[type="application/ld+json"]');
                        for (let node of jsonNodes) {
                            let data = JSON.parse(node.innerText || node.textContent);
                            let jobData = Array.isArray(data) ? data.find(d => d['@type'] === 'JobPosting') : data;
                            
                            if (jobData && jobData['@type'] === 'JobPosting') {
                                // Extract Location
                                if (jobData.jobLocation) {
                                    let locs = Array.isArray(jobData.jobLocation) ? jobData.jobLocation : [jobData.jobLocation];
                                    let cities = [];
                                    for (let l of locs) {
                                        if (l.address && l.address.addressLocality) {
                                            cities.push(l.address.addressLocality.trim());
                                        }
                                    }
                                    if (cities.length > 0) result.location = cities.join(' | ');
                                }
                                
                                // Extract HTML Description
                                if (jobData.description) {
                                    result.description = jobData.description;
                                }
                            }
                        }
                    } catch(e) {}
                    
                    // --- 2. DOM FALLBACK EXTRACTION (If JSON is missing) ---
                    if (!result.description || result.location === "N/A") {
                        
                        // Forcefully un-hide all accordions by removing React's 'inert' attribute
                        document.querySelectorAll('[inert]').forEach(el => el.removeAttribute('inert'));
                        document.querySelectorAll('[data-testid="toggle-content"]').forEach(el => {
                            el.style.display = 'block';
                            el.style.visibility = 'visible';
                            el.style.height = 'auto';
                        });
                        
                        // Location Fallback A: <dd> tag (e.g. Kufstein)
                        if (result.location === "N/A") {
                            let dts = document.querySelectorAll('dt');
                            for (let dt of dts) {
                                let txt = (dt.innerText || "").toLowerCase();
                                if (txt.includes('dienstort') || txt.includes('location') || txt.includes('ort')) {
                                    let dd = dt.nextElementSibling;
                                    if (dd && dd.tagName.toLowerCase() === 'dd') {
                                        result.location = dd.innerText.trim();
                                    }
                                }
                            }
                        }
                        
                        // Location Fallback B: <li> tag (e.g. Bezirk Feldkirch)
                        if (result.location === "N/A") {
                            let lis = document.querySelectorAll('li');
                            for (let li of lis) {
                                let txt = li.innerText || "";
                                if (txt.includes('Dienstort:') || txt.includes('Location:')) {
                                    result.location = txt.replace(/Dienstort:\\s*/i, '').replace(/Location:\\s*/i, '').trim();
                                }
                            }
                        }
                        
                        // Description Fallback: Grab the main wrapper
                        if (!result.description) {
                            let h1 = document.querySelector('h1');
                            if (h1 && h1.parentElement) {
                                let clone = h1.parentElement.cloneNode(true);
                                
                                // Remove 'Apply Now' buttons and the title
                                let applyDiv = clone.querySelector('div.ta-c.f-r');
                                if (applyDiv) applyDiv.remove();
                                let h1Clone = clone.querySelector('h1');
                                if (h1Clone) h1Clone.remove();
                                
                                result.description = clone.innerHTML;
                            }
                        }
                    }
                    
                    return result;
                """)

                # --- FORMAT LOCATION ---
                location_raw = details['location'] if details['location'] != 'N/A' else 'N/A'
                city = location_raw
                country = "Austria"

                print(
                    f"   [{idx}/{len(job_links)}] Formatting: {job['title']} in {city}, {country}")

                # --- FORMAT DESCRIPTION ---
                if details['description']:
                    # Markdownify flawlessly converts the JSON HTML into clean text
                    clean_desc = md(details['description'], heading_style="ATX", bullets="-", strip=[
                                    'img', 'script', 'style', 'a', 'svg', 'canvas', 'button'])
                    clean_desc = clean_desc.replace(
                        '•', '-').replace('·', '-').replace('\xa0', ' ')
                    clean_desc = re.sub(r'\n{3,}', '\n\n', clean_desc).strip()
                else:
                    clean_desc = "Description could not be loaded."

                conn.execute('''INSERT INTO jobs (url, title, company, location_raw, city, country, description, category) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                             (job['url'], job['title'], COMPANY_NAME, location_raw, city, country, clean_desc, job['category']))
                conn.commit()
                saved_count += 1

            except Exception as e:
                print(f"      ⚠️ Error on job {idx}: {e}")

        conn.close()
        print(
            f"\n✨ SUCCESS! {saved_count} {COMPANY_NAME} jobs safely saved with complete descriptions.")

    finally:
        try:
            driver.quit()
        except:
            pass


if __name__ == "__main__":
    run_scraper()
