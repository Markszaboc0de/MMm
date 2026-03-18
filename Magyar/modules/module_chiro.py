import requests
from bs4 import BeautifulSoup
import sqlite3
import os
import sys
import time

# Windows terminál UTF-8 kódolásának kikényszerítése
sys.stdout.reconfigure(encoding='utf-8')

# ==========================================
# ⚙️ CONFIGURATION
# ==========================================
COMPANY_NAME = "Chiro Marketing"
BASE_URL = "https://chiro.hu/karrier/"

DATA_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
DB_PATH = os.path.join(DATA_FOLDER, "chiro_jobs.db")


def init_db():
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)
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
    print(
        f"   🏢 Scraper indítása: {COMPANY_NAME} (Villámgyors Requests mód)...")
    init_db()

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    try:
        response = requests.get(BASE_URL, headers=headers, timeout=15)
        response.encoding = 'utf-8'
        soup = BeautifulSoup(response.text, 'html.parser')

        # Az Elementor loop itemek keresése
        job_cards = soup.find_all(
            'div', attrs={'data-elementor-type': 'loop-item'})
        job_targets = []

        for card in job_cards:
            classes = card.get('class', [])
            card_text = card.get_text(strip=True).lower()

            # 🛑 THE FIX: Kiszűrjük a zárt állásokat!
            if 'zart-karrier' in classes or 'zárva' in card_text:
                continue

            a_tag = card.find('a', href=True)
            title_tag = card.find('h3')

            if a_tag and title_tag:
                job_url = a_tag['href']
                job_title = title_tag.get_text(strip=True)

                job_targets.append({
                    "url": job_url,
                    "title": job_title
                })

        # Duplikációk szűrése
        job_targets = [dict(t)
                       for t in {tuple(d.items()) for d in job_targets}]

        if not job_targets:
            print(
                f"   ⚠️ Nincsenek nyitott állások a(z) {COMPANY_NAME} oldalon (csak zártakat találtunk).")
            return

        print(
            f"   🔍 {len(job_targets)} nyitott állás megtalálva. Részletek kinyerése...")

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        new_jobs_added = 0

        # Végigmegyünk az aloldalakon
        for job in job_targets:
            job_url = job["url"]
            title = job["title"]

            try:
                job_res = requests.get(job_url, headers=headers, timeout=10)
                job_res.encoding = 'utf-8'
                job_soup = BeautifulSoup(job_res.text, 'html.parser')

                description_lines = []

                # WordPress Elementor tartalom keresése
                main_content = job_soup.find(
                    'div', class_='elementor-widget-theme-post-content') or job_soup.find('main') or job_soup.find('article')

                if main_content:
                    for tag in main_content.find_all(['p', 'h2', 'h3', 'h4', 'ul', 'ol']):

                        # Kiszűrjük a navigációs elemeket
                        tag_classes = str(tag.get('class', '')).lower()
                        if any(nav in tag_classes for nav in ['nav', 'menu', 'footer', 'header', 'share']):
                            continue

                        if tag.name in ['ul', 'ol']:
                            for li in tag.find_all('li'):
                                li_text = li.get_text(strip=True)
                                if li_text:
                                    description_lines.append(f"- {li_text}")
                            description_lines.append("")
                        else:
                            text = tag.get_text(strip=True)
                            if text and text != title and text not in description_lines:
                                description_lines.append(text)

                description = "\n".join(description_lines).strip()
                if not description:
                    description = "A leírás kinyerése nem sikerült."

                # Alapértelmezett lokáció
                location_raw = "Magyarország"
                city = "Budapest"
                country = "Hungary"

                # Mentés az adatbázisba
                cursor.execute('''
                    INSERT OR IGNORE INTO jobs (url, title, company, location_raw, city, country, description)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (job_url, title, COMPANY_NAME, location_raw, city, country, description))

                if cursor.rowcount > 0:
                    new_jobs_added += 1

                time.sleep(0.5)

            except Exception as e:
                print(
                    f"   ❌ Hiba az állás feldolgozása közben ({job_url}): {e}")

        conn.commit()
        conn.close()

        print(
            f"   ✅ {COMPANY_NAME} kész! {new_jobs_added} új nyitott állás lementve az adatbázisba.")

    except Exception as e:
        print(
            f"   ❌ Kritikus hiba a(z) {COMPANY_NAME} oldal futtatása közben: {e}")


if __name__ == "__main__":
    run_scraper()
