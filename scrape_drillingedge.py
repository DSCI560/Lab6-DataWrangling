import pymysql
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# ===============================
# CONFIG
# ===============================

BASE_URL = "https://www.drillingedge.com"
SEARCH_URL = "https://www.drillingedge.com/search?q="

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "admin123",
    "database": "oil_wells",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

# ===============================
# DB CONNECTION
# ===============================

conn = pymysql.connect(**DB_CONFIG)
cursor = conn.cursor()

# ===============================
# GET WELLS TO ENRICH
# ===============================

def get_wells_to_scrape():
    """
    Only scrape:
    - QC valid wells
    - Wells with API
    - Wells not already enriched
    """
    cursor.execute("""
        SELECT w.id, w.api
        FROM wells w
        LEFT JOIN drillingedge_extra d
            ON w.id = d.well_id
        WHERE w.qc_status = 'valid'
        AND w.api IS NOT NULL
        AND d.id IS NULL
    """)
    return cursor.fetchall()

# ===============================
# SCRAPE WELL PAGE
# ===============================

def scrape_well_data(api):
    search_url = SEARCH_URL + api
    print(f"Searching: {search_url}")

    try:
        r = requests.get(search_url, headers=HEADERS, timeout=15)
    except Exception as e:
        print(f"Search request failed: {e}")
        return None

    if r.status_code != 200:
        print("Search request returned non-200 status")
        return None

    soup = BeautifulSoup(r.text, "html.parser")

    # Find first well link
    well_link = None
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/well/" in href:
            well_link = urljoin(BASE_URL, href)
            break

    if not well_link:
        print("No well link found in search results")
        return None

    if well_link.rstrip("/") == BASE_URL:
        print("Homepage link detected — skipping")
        return None

    print(f"Found well page: {well_link}")

    try:
        r2 = requests.get(well_link, headers=HEADERS, timeout=15)
    except Exception as e:
        print(f"Well page request failed: {e}")
        return None

    if r2.status_code != 200:
        print("Failed to load well page")
        return None

    soup2 = BeautifulSoup(r2.text, "html.parser")
    text = soup2.get_text(separator="\n")

    def extract_field(label):
        for line in text.split("\n"):
            if label.lower() in line.lower():
                parts = line.split(":")
                if len(parts) > 1:
                    return parts[-1].strip()
        return None

    well_status = extract_field("Well Status")
    well_type = extract_field("Well Type")
    closest_city = extract_field("Closest City")
    barrels = extract_field("Barrels of Oil")
    gas = extract_field("MCF Gas")

    # Basic sanity check
    if not any([well_status, well_type, closest_city, barrels, gas]):
        print("No meaningful data extracted")
        return None

    return {
        "well_status": well_status,
        "well_type": well_type,
        "closest_city": closest_city,
        "barrels_of_oil": barrels,
        "mcf_gas": gas,
        "scraped_url": well_link
    }

# ===============================
# SAVE ENRICHMENT (UPSERT)
# ===============================

def save_enrichment(well_id, data):
    cursor.execute("""
        INSERT INTO drillingedge_extra (
            well_id,
            well_status,
            well_type,
            closest_city,
            barrels_of_oil,
            mcf_gas,
            scraped_url
        ) VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
            well_status = VALUES(well_status),
            well_type = VALUES(well_type),
            closest_city = VALUES(closest_city),
            barrels_of_oil = VALUES(barrels_of_oil),
            mcf_gas = VALUES(mcf_gas),
            scraped_url = VALUES(scraped_url)
    """, (
        well_id,
        data["well_status"],
        data["well_type"],
        data["closest_city"],
        data["barrels_of_oil"],
        data["mcf_gas"],
        data["scraped_url"]
    ))

    conn.commit()

# ===============================
# MAIN
# ===============================

def main():
    wells = get_wells_to_scrape()

    print(f"Found {len(wells)} wells to enrich")

    for well_id, api in wells:
        print(f"\nEnriching Well ID {well_id} (API {api})")

        data = scrape_well_data(api)

        if data:
            save_enrichment(well_id, data)
            print("→ Enrichment saved")
        else:
            print("→ No data saved")

if __name__ == "__main__":
    main()