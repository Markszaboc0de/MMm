"""
This script synchronizes scraped jobs from the local `raw_db` 
to the destination `job_match_db` (VM1) into a staging table `scraped_jobs`.

It uses "UPSERT" logic via a temp table so it's safe to run repeatedly. New jobs 
are inserted, and existing jobs (matched by URL) are updated.

Usage:
    python sync_jobs.py
"""

import os
import sys
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Load environment variables securely from .env
load_dotenv()

# ==========================================
# ⚙️ CONFIGURATION
# ==========================================

# SOURCE Database (where scrapers save to, usually local)
SRC_HOST = os.getenv("SRC_PG_HOST", "localhost")
SRC_PORT = os.getenv("SRC_PG_PORT", "5432")
SRC_DB   = os.getenv("SRC_PG_DATABASE", "raw_db")
SRC_USER = os.getenv("SRC_PG_USER", "postgres")
SRC_PASS = os.getenv("SRC_PG_PASSWORD") # NO HARDCODED PASSWORDS

# TARGET Database (the main app database on VM1)
DEST_HOST = os.getenv("DEST_PG_HOST", "10.0.0.74")
DEST_PORT = os.getenv("DEST_PG_PORT", "5432")
DEST_DB   = os.getenv("DEST_PG_DATABASE", "job_match_db")
DEST_USER = os.getenv("DEST_PG_USER", "app_user")
DEST_PASS = os.getenv("DEST_PG_PASSWORD")

if not SRC_PASS or not DEST_PASS:
    raise ValueError("Critical Security Error: Both SRC_PG_PASSWORD and DEST_PG_PASSWORD must be set in your .env file!")

# Table Names
SRC_TABLE  = "scraped_jobs"
DEST_TABLE = "scraped_jobs" # The user created a new table for this

def sync_databases():
    print(f"🔄 Starting database sync from {SRC_DB} to {DEST_DB}...")

    src_conn = None
    dest_conn = None

    try:
        # 1. Connect to Source
        print(f"🔌 Connecting to Source Database ({SRC_HOST}:{SRC_PORT})...")
        src_conn = psycopg2.connect(
            host=SRC_HOST, port=SRC_PORT, dbname=SRC_DB, user=SRC_USER, password=SRC_PASS, connect_timeout=10
        )
        src_cursor = src_conn.cursor()
        print("✅ Connected to Source.")

        # 2. Connect to Destination
        print(f"🔌 Connecting to Destination Database ({DEST_HOST}:{DEST_PORT})...")
        dest_conn = psycopg2.connect(
            host=DEST_HOST, port=DEST_PORT, dbname=DEST_DB, user=DEST_USER, password=DEST_PASS, connect_timeout=10
        )
        dest_cursor = dest_conn.cursor()
        print("✅ Connected to Destination.")

        # 3. Create destination table if it doesn't exist (matching raw_db schema)
        dest_cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {DEST_TABLE} (
                id SERIAL PRIMARY KEY,
                company TEXT,
                title TEXT,
                city TEXT,
                country TEXT,
                raw_text TEXT,
                url TEXT
            );
        ''')
        dest_conn.commit()

        # 4. Read all data from source
        print("📥 Reading data from source database...")
        src_cursor.execute(f'SELECT "Company", "Job Title", "City", "Country", "Job Description", "URL" FROM {SRC_TABLE}')
        rows = src_cursor.fetchall()
        
        if not rows:
            print("⚠️ No data found in source database.")
            return

        print(f"📤 Preparing to sync {len(rows)} rows to destination database...")

        # 5. Upsert into destination using a temporary table (avoids ON CONFLICT constraint error)
        dest_cursor.execute('''
            CREATE TEMP TABLE temp_sync (
                company TEXT,
                title TEXT,
                city TEXT,
                country TEXT,
                raw_text TEXT,
                url TEXT
            ) ON COMMIT DROP;
        ''')

        execute_values(
            dest_cursor,
            '''INSERT INTO temp_sync (company, title, city, country, raw_text, url) 
               VALUES %s''',
            rows
        )

        # Update existing records
        dest_cursor.execute(f'''
            UPDATE {DEST_TABLE} j
            SET company = t.company,
                title = t.title,
                city = t.city,
                country = t.country,
                raw_text = t.raw_text
            FROM temp_sync t
            WHERE j.url = t.url;
        ''')

        # Insert new records
        dest_cursor.execute(f'''
            INSERT INTO {DEST_TABLE} (company, title, city, country, raw_text, url)
            SELECT t.company, t.title, t.city, t.country, t.raw_text, t.url
            FROM temp_sync t
            WHERE NOT EXISTS (
                SELECT 1 FROM {DEST_TABLE} j WHERE j.url = t.url
            );
        ''')
        
        dest_conn.commit()

        print(f"✅ Successfully synchronized {len(rows)} jobs to {DEST_DB} ({DEST_TABLE} table) at {DEST_HOST}!")

    except Exception as e:
        print(f"❌ Error during synchronization: {e}")
        if dest_conn:
            dest_conn.rollback()
        sys.exit(1)
    
    finally:
        if src_conn:
            src_conn.close()
        if dest_conn:
            dest_conn.close()

if __name__ == "__main__":
    sync_databases()
