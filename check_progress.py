import os
import glob
import sqlite3
try:
    import psycopg2
    from dotenv import load_dotenv
    load_dotenv()
    HAS_PG = True
except ImportError:
    HAS_PG = False

def count_sqlite_jobs():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_dirs = {
        "ATS": os.path.join(base_dir, "ATS scrapers", "data"),
        "Magyar": os.path.join(base_dir, "Magyar", "data"),
        "Manual": os.path.join(base_dir, "Manual", "data")
    }
    
    total_sqlite = 0
    print("\n" + "="*50)
    print(" 📂 LOCAL SQLITE CACHE STORAGE ")
    print("="*50)
    
    for name, d in data_dirs.items():
        if not os.path.exists(d):
            print(f" {name}: 0 jobs (Directory missing)")
            continue
            
        dbs = glob.glob(os.path.join(d, "*.db")) + glob.glob(os.path.join(d, "*.sqlite"))
        if not dbs:
            print(f" {name}: 0 jobs (No databases yet)")
            continue
            
        folder_total = 0
        for db in dbs:
            try:
                conn = sqlite3.connect(db)
                c = conn.cursor()
                # Check for table existence mapping
                c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='jobs'")
                if c.fetchone():
                    c.execute("SELECT COUNT(*) FROM jobs")
                    folder_total += c.fetchone()[0]
                conn.close()
            except Exception:
                pass
                
        print(f" {name:<10}: {folder_total} jobs collected")
        total_sqlite += folder_total
        
    print("-" * 50)
    print(f" 🧮 TOTAL JOBS ON DISK: {total_sqlite}\n")

def count_postgres_jobs():
    print("="*50)
    print(" 🐘 POSTGRESQL FINAL DESTINATION (raw_db) ")
    print("="*50)
    
    if not HAS_PG:
        print(" ❌ psycopg2 or python-dotenv not installed. Cannot check Postgres.")
        return
        
    db_host = os.getenv("PG_HOST", "localhost")
    db_port = os.getenv("PG_PORT", "5432")
    db_name = os.getenv("PG_DATABASE", "raw_db")
    db_user = os.getenv("PG_USER", "postgres")
    db_pass = os.getenv("PG_PASSWORD")
    
    if not db_pass:
        print(" ⚠️ PG_PASSWORD not found in .env. Skipping Postgres check.")
        return
        
    try:
        conn = psycopg2.connect(
            host=db_host, port=db_port, dbname=db_name, user=db_user, password=db_pass, connect_timeout=5
        )
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM scraped_jobs")
        count = cursor.fetchone()[0]
        print(f" 🟢 SUCCESS! Currently holding: {count} jobs inside 'scraped_jobs' table.")
        
        # Show top 5 companies by representation to prove data spread
        cursor.execute('SELECT "Company", COUNT(*) FROM scraped_jobs GROUP BY "Company" ORDER BY COUNT(*) DESC LIMIT 5')
        print("\n 🏢 Top 5 Companies represented in PostgreSQL:")
        for company, c_count in cursor.fetchall():
            print(f"    - {company[:30]:<30} | {c_count} jobs")
            
        conn.close()
    except psycopg2.errors.UndefinedTable:
        print(" ⚠️ Table 'scraped_jobs' does not exist yet. It will be created shortly.")
    except Exception as e:
        print(f" ❌ Postgres Connection Failed: {e}")

if __name__ == "__main__":
    count_sqlite_jobs()
    count_postgres_jobs()
    print("\n💡 TIP: Run this script repeatedly to watch the numbers grow in real time!\n")
