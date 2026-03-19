import psycopg2
from psycopg2.extras import execute_values
import os
from dotenv import load_dotenv

# Load environment variables from a .env file
load_dotenv()

# Database connection details (NO hardcoded passwords!)
DB_HOST = os.getenv("PG_HOST", "localhost")
DB_PORT = os.getenv("PG_PORT", "5432")
DB_NAME = os.getenv("PG_DATABASE", "raw_db")
DB_USER = os.getenv("PG_USER", "postgres")
# Do not provide a default password string in source code!
DB_PASSWORD = os.getenv("PG_PASSWORD")

if not DB_PASSWORD:
    raise ValueError("Critical Security Error: PG_PASSWORD environment variable is not set! Please configure your .env file.")

def get_connection():
    """Returns a connection to the PostgreSQL database."""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

def setup_postgres_table():
    """Ensures the target table exists with the requested schema."""
    conn = get_connection()
    cursor = conn.cursor()
    
    # ID, Company, Job Title, City, Country, Job Description, URL, Date where Date is the date of scraping
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS scraped_jobs (
        id SERIAL PRIMARY KEY,
        "Company" VARCHAR(255),
        "Job Title" VARCHAR(255),
        "City" VARCHAR(255),
        "Country" VARCHAR(255),
        "Job Description" TEXT,
        "URL" TEXT UNIQUE,
        "Date" DATE
    );
    '''
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    conn.close()

def push_to_postgres(jobs_data):
    """
    Pushes a list of dictionaries to the PostgreSQL database.
    Expected keys in each dict (case-insensitive in logic): 
    company, job_title, city, country, job_description, url, date
    """
    if not jobs_data:
        print("No data provided to push to PostgreSQL.")
        return

    setup_postgres_table()
    
    conn = get_connection()
    cursor = conn.cursor()

    # Prepare data for execute_values
    # Ensure order matches: Company, Job Title, City, Country, Job Description, URL, Date
    insert_query = '''
    INSERT INTO scraped_jobs (
        "Company", "Job Title", "City", "Country", "Job Description", "URL", "Date"
    ) VALUES %s
    ON CONFLICT ("URL") DO UPDATE SET
        "Company" = EXCLUDED."Company",
        "Job Title" = EXCLUDED."Job Title",
        "City" = EXCLUDED."City",
        "Country" = EXCLUDED."Country",
        "Job Description" = EXCLUDED."Job Description",
        "Date" = EXCLUDED."Date";
    '''

    values_list = []
    skipped = 0
    for job in jobs_data:
        try:
            values_list.append((
                job.get('company', 'Unknown'),
                job.get('job_title', 'Unknown'),
                job.get('city', ''),
                job.get('country', ''),
                job.get('job_description', ''),
                job.get('url'),
                job.get('date')
            ))
        except Exception as e:
            skipped += 1
            pass

    if values_list:
        try:
            execute_values(cursor, insert_query, values_list)
            conn.commit()
            print(f"✅ Successfully upserted {len(values_list)} records to PostgreSQL database '{DB_NAME}'.")
        except Exception as e:
            conn.rollback()
            print(f"❌ Error inserting into PostgreSQL: {e}")
    if skipped > 0:
        print(f"⚠️ Skipped {skipped} records due to missing data formatting.")

    cursor.close()
    conn.close()
