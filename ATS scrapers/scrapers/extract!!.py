import sqlite3
import csv
import os
import glob
from datetime import datetime

# ==========================================
# ⚙️ CONFIGURATION
# ==========================================
# Updated paths to match your new ATS Scrapers architecture
DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\ATS scrapers\data"
OUTPUT_CSV = r"C:\Users\kgyoz\Documents\Projekt\ATS scrapers\ats_jobs_export.csv"


def fix_schema(db_path, db_name):
    """Ellenőrzi és pótolja a hiányzó oszlopokat az adatbázisban."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Megnézzük, milyen oszlopok vannak most
    cursor.execute("PRAGMA table_info(jobs)")
    columns = [info[1] for info in cursor.fetchall()]

    # 1. Country pótlása (ha hiányzik)
    if 'country' not in columns:
        conn.execute(
            "ALTER TABLE jobs ADD COLUMN country TEXT DEFAULT 'Hungary'")
        conn.commit()

    # 2. Company pótlása (ha hiányzik) - a fájlnévből kitaláljuk a cégnevet
    if 'company' not in columns:
        guessed_company = db_name.replace("_jobs.db", "").upper()
        conn.execute(
            f"ALTER TABLE jobs ADD COLUMN company TEXT DEFAULT '{guessed_company}'")
        conn.commit()

    conn.close()


def export_all_databases():
    print(f"📦 Keresés a következő mappában: {DATA_FOLDER}...")
    db_files = glob.glob(os.path.join(DATA_FOLDER, "*.db"))

    if not db_files:
        print("❌ Hiba: Nem találtam egyetlen .db fájlt sem a mappában.")
        return

    print(f"🔍 {len(db_files)} darab adatbázist találtam.\n")
    all_jobs = []

    for db_path in db_files:
        db_name = os.path.basename(db_path)
        print(f"   ▶️ Feldolgozás: {db_name}...")

        # --- JAVÍTÁS INDÍTÁSA ---
        try:
            fix_schema(db_path, db_name)
        except Exception as e:
            print(f"      ⚠️ Séma javítási hiba: {e}")
        # ------------------------

        timestamp = os.path.getmtime(db_path)
        scrape_date = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')

        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            cursor.execute(
                "SELECT company, title, city, country, description, url FROM jobs")
            rows = cursor.fetchall()

            for row in rows:
                company = row[0]
                title = row[1]
                city = row[2]

                # --- THE OVERRIDE: Forcing Country to be exactly "Hungary" ---
                country = "Hungary"

                description = row[4]
                url = row[5]

                all_jobs.append([company, title, city, country,
                                description, url, scrape_date])

            conn.close()
        except sqlite3.OperationalError as e:
            print(f"      ⚠️ Hiba a(z) {db_name} olvasásakor: {e}")

    if not all_jobs:
        print("\n⚠️ Az adatbázisok üresek. Nincs mit exportálni.")
        return

    # Sort alphabetically by Company, then by Job Title
    all_jobs.sort(key=lambda x: (str(x[0]), str(x[1])))

    # Using utf-8-sig ensures Excel reads Hungarian characters (á, é, ő) perfectly
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8-sig") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["company", "title", "city", "country",
                        "description", "url", "date_of_scraping"])
        writer.writerows(all_jobs)

    print(f"\n✨ SIKER! Az összesített fájl mentve: {OUTPUT_CSV}")


if __name__ == "__main__":
    export_all_databases()
