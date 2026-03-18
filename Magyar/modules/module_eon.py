import requests
import sqlite3
import os

# --- KONFIGURÁCIÓ ---
COMPANY_NAME = "E.ON"
# Ezt az URL-t most tisztán, paraméterek nélkül használjuk, a Payload-ot külön adjuk át
API_URL = "https://v09fm4cjghdr23p7p.a1.typesense.net/multi_search"

DATA_FOLDER = r"C:\Users\kgyoz\Documents\Projekt\Magyar\data"
DB_PATH = os.path.join(DATA_FOLDER, "eon_jobs.db")


def run_scraper():
    print(f"🚀 {COMPANY_NAME} Végső kísérlet (Böngésző imitáció)...")

    # A böngésződ pontosan ezt küldi el a háttérben
    payload = {
        "searches": [
            {
                "collection": "jobs",
                "q": "*",
                "query_by": "data.title,data.locations.city,data.locations.state,data.locations.country,data.company,data.jobField",
                "sort_by": "data.postingDate_timestamp:desc",
                "per_page": 250
            }
        ]
    }

    # Fontos: Az x-typesense-api-key mellett kell az Origin is!
    headers = {
        "x-typesense-api-key": "AGw1v6TzYYkkQyvvf6uFvHXuO3DML7AD",
        "Origin": "https://allas.eon.hu",
        "Referer": "https://allas.eon.hu/",
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    }

    try:
        # Most csak az API kulcsot adjuk meg, paraméterek nélkül az URL-ben
        response = requests.post(API_URL, json=payload, headers=headers)

        print(f"DEBUG: Státusz kód: {response.status_code}")

        if response.status_code == 200:
            results = response.json()
            # Nézzük meg, mi van a válaszban
            hits = results['results'][0].get('hits', [])
            print(f"📦 API válasz érkezett. Nyers találatok száma: {len(hits)}")

            if len(hits) == 0:
                print(
                    "⚠️ Az API válaszolt, de a lista üres. Valószínűleg a Typesense IP-alapú blokkolást alkalmaz.")
                return

            conn = sqlite3.connect(DB_PATH)
            saved = 0
            for hit in hits:
                doc = hit['document']
                data = doc['data']
                title = data.get('title', 'Nincs cím')

                # Ellenőrizzük, hogy magyar állás-e
                if data.get('language') == 'HU':
                    job_id = doc.get('id')
                    job_url = f"https://allas.eon.hu/hu/allas/{job_id}"
                    city = data.get('locations', [{}])[0].get('city', 'N/A')

                    conn.execute("INSERT OR REPLACE INTO jobs (url, title, company, city) VALUES (?, ?, ?, ?)",
                                 (job_url, title, "E.ON", city))
                    saved += 1

            conn.commit()
            conn.close()
            print(f"✨ SIKER! {saved} magyar állás elmentve.")
        else:
            print(f"❌ Hiba: {response.status_code}")
            print(f"Válasz: {response.text}")

    except Exception as e:
        print(f"💥 Hiba: {e}")


if __name__ == "__main__":
    run_scraper()
