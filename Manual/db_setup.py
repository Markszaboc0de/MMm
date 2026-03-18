import sqlite3
import os

DB_NAME = "hungary_jobs.db"


def setup_database():
    print(f"⚙️ Initializing Master Database: {DB_NAME}...")
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # Updated to match your exact requested format
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
    print("✅ Database setup complete. Ready to receive data!")


if __name__ == "__main__":
    setup_database()
