import sqlite3
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager


class BaseScraper:
    def __init__(self, db_name="jobs_database.db"):
        self.db_name = db_name
        self._setup_database()
        

    def _setup_driver(self):
        # Silent initialization
        options = Options()
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--headless=new")
        options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")

        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=options)

    def _setup_database(self):
        try:
            conn = sqlite3.connect(self.db_name)
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

    def save_job(self, job_data):
        if not job_data:
            return False

        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        saved = False
        try:
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
        except sqlite3.OperationalError:
            self._setup_database()
            return self.save_job(job_data)
        finally:
            conn.close()
        return saved

    def close(self):
        try:
            self.driver.quit()
        except:
            pass
