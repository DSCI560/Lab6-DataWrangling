import os
import pymysql.cursors
import datetime

db_conf = {
    "host": os.environ.get("DB_HOST", "localhost"),
    "user": os.environ.get("DB_USER", "root"),
    "password": os.environ.get("DB_PASS", "password"),
    "db": os.environ.get("DB_NAME", "oil_wells"),
    "cursorclass": pymysql.cursors.DictCursor,
    "charset": "utf8mb4"
}

def get_conn():
    return pymysql.connect(**db_conf)

def has_wells(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(1) as c FROM wells")
        return cur.fetchone()['c'] > 0

def insert_sample(conn):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO wells (filename, file_hash, api, well_name, well_number, address, latitude, longitude, county, state, operator, qc_status, raw_text, created_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                "sample.txt",
                "hashsample0001",
                "12345678901234",
                "Sample Well A",
                "W-100",
                "100 Main St",
                29.7604,
                -95.3698,
                "Harris",
                "TX",
                "Sample Operator",
                "verified",
                "raw text sample",
                datetime.datetime.utcnow()
            )
        )
        wid = cur.lastrowid
        cur.execute(
            "INSERT INTO stimulations (well_id, date_stimulated, stimulated_formation, top_ft, bottom_ft, stages, volume, volume_units, treatment_pressure, max_treatment_rate, treatment_type, lbs_proppant, acid_percent, additional_info) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                wid,
                datetime.date(2023, 6, 1),
                "Formation A",
                1000,
                1500,
                12,
                2500.0,
                "bbl",
                5000.0,
                120.0,
                "frack",
                500000,
                0.0,
                "sample stim"
            )
        )
    conn.commit()

def main():
    conn = get_conn()
    try:
        if not has_wells(conn):
            insert_sample(conn)
    finally:
        conn.close()

if __name__ == '__main__':
    main()