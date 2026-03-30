import os
import sqlite3
import time
import shutil
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.os_manager import ChromeType


class BaseScraper:
    def __init__(self, db_name="jobs_database.db"):
        self.db_name = db_name
        self._setup_database()
        

    def _setup_driver(self):
        # Silent initialization
        options = Options()
        options.page_load_strategy = 'eager'
        options.add_argument("--blink-settings=imagesEnabled=false")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

        # Robust binary detection
        chrome_candidates = [
            shutil.which("google-chrome"),
            shutil.which("google-chrome-stable"),
            shutil.which("chromium"),
            shutil.which("chromium-browser"),
            "/usr/bin/google-chrome",
            "/usr/bin/google-chrome-stable",
            "/usr/bin/chromium-browser",
            "/usr/bin/chromium",
            "/snap/bin/chromium",
        ]
        
        def find_binary(candidates):
            for path in candidates:
                if path and os.path.isfile(path):
                    return path
            return None

        chrome_binary = find_binary(chrome_candidates)
        if chrome_binary:
            options.binary_location = chrome_binary

        if not hasattr(self, 'driver') or self.driver is None:
            try:
                chrome_type = ChromeType.CHROMIUM if (chrome_binary and "chrom" in chrome_binary.lower()) else ChromeType.GOOGLE
                driver_path = ChromeDriverManager(chrome_type=chrome_type).install()
                service = Service(executable_path=driver_path)
                self.driver = webdriver.Chrome(service=service, options=options)
            except Exception as e:
                print(f"   ⚠️ WebDriverManager failed ({e}), searching for local drivers...")
                driver_candidates = [
                    shutil.which("chromedriver"),
                    "/usr/bin/chromedriver",
                    "/usr/lib/chromium-browser/chromedriver",
                    "/snap/bin/chromium.chromedriver",
                ]
                chromedriver_path = find_binary(driver_candidates)
                if not chromedriver_path:
                     raise RuntimeError(f"chromedriver not found and manager failed. Error: {e}")
                service = Service(executable_path=chromedriver_path)
                self.driver = webdriver.Chrome(service=service, options=options)

    def _setup_database(self):
        try:
            os.makedirs(os.path.dirname(os.path.abspath(self.db_name)), exist_ok=True)
            conn = sqlite3.connect(self.db_name, timeout=15.0)
            cursor = conn.cursor()
            # Added 'description' column
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT UNIQUE,
                    title TEXT,
                    company TEXT,
                    location_raw TEXT,
                    city TEXT,
                    country TEXT,
                    description TEXT, 
                    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"   ❌ CRITICAL DATABASE ERROR: {e}")

    def get_page(self, url):
        if not hasattr(self, 'driver') or self.driver is None:
            self._setup_driver()

        try:
            self.driver.get(url)
            time.sleep(2)
            return self.driver.page_source
        except Exception:
            return None

    def save_job(self, job_data, retry_count=0):
        if not job_data:
            return False

        saved = False
        try:
            conn = sqlite3.connect(self.db_name, timeout=15.0)
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO jobs (url, title, company, location_raw, city, country, description)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                job_data['url'],
                job_data['title'],
                job_data['company'],
                job_data['location_raw'],
                job_data.get('city', 'Unknown'),
                job_data.get('country', 'Unknown'),
                # Saving the full text
                job_data.get('description', 'No description')
            ))
            conn.commit()
            saved = True
        except sqlite3.IntegrityError:
            pass
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e).lower() and retry_count < 3:
                time.sleep(1)
                return self.save_job(job_data, retry_count + 1)
            else:
                self._setup_database()
                if retry_count == 0:
                    return self.save_job(job_data, 1)
        finally:
            conn.close()
        return saved

    def close(self):
        try:
            self.driver.quit()
        except:
            pass
