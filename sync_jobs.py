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

# TARGET Database (the main app database on VM1, or local duplicate)
DEST_HOST = os.getenv("DEST_PG_HOST", "localhost") # Change this to VM1's IP if remote!
DEST_PORT = os.getenv("DEST_PG_PORT", "5432")
DEST_DB   = os.getenv("DEST_PG_DATABASE", "jobs_db")
DEST_USER = os.getenv("DEST_PG_USER", "postgres")
DEST_PASS = os.getenv("DEST_PG_PASSWORD", "postgres")

# Table Names
SRC_TABLE  = "scraped_jobs"
DEST_TABLE = "scraped_jobs" # Ensure this matches your destination schema

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

        # 3. Create destination table if it doesn't exist (matching raw_db schema)
        dest_cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {DEST_TABLE} (
                "ID" SERIAL PRIMARY KEY,
                "Company" TEXT,
                "Job Title" TEXT,
                "City" TEXT,
                "Country" TEXT,
                "Job Description" TEXT,
                "URL" TEXT UNIQUE,
                "Date" TEXT
            );
        ''')
        dest_conn.commit()

        # 4. Read all data from source
        print("📥 Reading data from source database...")
        src_cursor.execute(f'SELECT "Company", "Job Title", "City", "Country", "Job Description", "URL", "Date" FROM {SRC_TABLE}')
        rows = src_cursor.fetchall()
        
        if not rows:
            print("⚠️ No data found in source database.")
            return

        print(f"📤 Preparing to sync {len(rows)} rows to destination database...")

        # 5. Upsert into destination
        upsert_query = f'''
            INSERT INTO {DEST_TABLE} ("Company", "Job Title", "City", "Country", "Job Description", "URL", "Date")
            VALUES %s
            ON CONFLICT ("URL") DO UPDATE SET
                "Company" = EXCLUDED."Company",
                "Job Title" = EXCLUDED."Job Title",
                "City" = EXCLUDED."City",
                "Country" = EXCLUDED."Country",
                "Job Description" = EXCLUDED."Job Description",
                "Date" = EXCLUDED."Date";
        '''
        
        execute_values(dest_cursor, upsert_query, rows)
        dest_conn.commit()

        print(f"✅ Successfully synchronized {len(rows)} jobs to {DEST_DB} at {DEST_HOST}!")

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
