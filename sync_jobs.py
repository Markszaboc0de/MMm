"""
This script synchronizes scraped jobs from the local `raw_db` 
to the destination `jobs_db` (either remote on VM1 or local duplicate).

It uses "UPSERT" logic so it's safe to run repeatedly. New jobs 
are inserted, and existing jobs (matched by URL) are updated.

Usage:
    python sync_jobs.py
"""

import os
import psycopg2
from psycopg2.extras import execute_values

# ==========================================
# ⚙️ CONFIGURATION
# ==========================================

# SOURCE Database (where scrapers save to, usually local)
SRC_HOST = os.getenv("SRC_PG_HOST", "localhost")
SRC_PORT = os.getenv("SRC_PG_PORT", "5432")
SRC_DB   = os.getenv("SRC_PG_DATABASE", "raw_db")
SRC_USER = os.getenv("SRC_PG_USER", "postgres")
SRC_PASS = os.getenv("SRC_PG_PASSWORD", "postgres")

# TARGET Database (the main app database on VM1)
DEST_HOST = os.getenv("DEST_PG_HOST", "192.168.1.151")
DEST_PORT = os.getenv("DEST_PG_PORT", "5432")
DEST_DB   = os.getenv("DEST_PG_DATABASE", "job_match_db")
DEST_USER = os.getenv("DEST_PG_USER", "app_user")
DEST_PASS = os.getenv("DEST_PG_PASSWORD", "Mindenszarhoz")

# Table Names
SRC_TABLE  = "scraped_jobs"
DEST_TABLE = "job_descriptions" # Automatically insert into the real dataset

def sync_databases():
    print(f"🔄 Starting database sync from {SRC_DB} to {DEST_DB}...")

    src_conn = None
    dest_conn = None

    try:
        # 1. Connect to Source
        src_conn = psycopg2.connect(
            host=SRC_HOST, port=SRC_PORT, dbname=SRC_DB, user=SRC_USER, password=SRC_PASS
        )
        src_cursor = src_conn.cursor()

        # 2. Connect to Destination
        dest_conn = psycopg2.connect(
            host=DEST_HOST, port=DEST_PORT, dbname=DEST_DB, user=DEST_USER, password=DEST_PASS
        )
        dest_cursor = dest_conn.cursor()

        # 4. Read all data from source
        print("📥 Reading data from source database...")
        # Only select the columns we need for job_descriptions
        src_cursor.execute(f'SELECT "Company", "Job Title", "City", "Country", "Job Description", "URL" FROM {SRC_TABLE}')
        raw_rows = src_cursor.fetchall()
        
        if not raw_rows:
            print("⚠️ No data found in source database.")
            return

        # Map strictly to destination schema
        # jd_id SERIAL PRIMARY KEY, company, title, city, country, raw_text, url, employer_id
        formatted_rows = []
        for row in raw_rows:
            c_company, c_title, c_city, c_country, c_raw_text, c_url = row
            employer_id = c_company # Use company name for employer_id
            formatted_rows.append((c_company, c_title, c_city, c_country, c_raw_text, c_url, employer_id))

        print(f"📤 Preparing to sync {len(formatted_rows)} rows to destination database...")

        # 5. Upsert into destination
        upsert_query = f'''
            INSERT INTO {DEST_TABLE} (company, title, city, country, raw_text, url, employer_id)
            VALUES %s
            ON CONFLICT (url) DO UPDATE SET
                company = EXCLUDED.company,
                title = EXCLUDED.title,
                city = EXCLUDED.city,
                country = EXCLUDED.country,
                raw_text = EXCLUDED.raw_text,
                employer_id = EXCLUDED.employer_id;
        '''
        
        execute_values(dest_cursor, upsert_query, formatted_rows)
        dest_conn.commit()

        print(f"✅ Successfully synchronized {len(formatted_rows)} jobs to {DEST_DB} at {DEST_HOST}!")

    except Exception as e:
        print(f"❌ Error during synchronization: {e}")
        if dest_conn:
            dest_conn.rollback()
    
    finally:
        if src_conn:
            src_conn.close()
        if dest_conn:
            dest_conn.close()

if __name__ == "__main__":
    sync_databases()
