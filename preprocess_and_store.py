# preprocess_and_store.py
import re
import mysql.connector
from datetime import datetime
import os

DB_HOST = os.environ.get("DB_HOST", "127.0.0.1")
DB_USER = os.environ.get("DB_USER", "root")
DB_PASS = os.environ.get("DB_PASS", "admin123")
DB_NAME = os.environ.get("DB_NAME", "oil_wells")

def clean_text(s):
    if s is None:
        return None
    s = re.sub(r'\s+', ' ', s)
    s = s.strip()
    # remove weird unicode / control characters
    s = re.sub(r'[^\x00-\x7F]+',' ', s)
    return s

def normalize_coords(lat, lon):
    try:
        if lat is None or lon is None:
            return None, None
        latf = float(lat)
        lonf = float(lon)
        if abs(latf) > 90 or abs(lonf) > 180:
            return None, None
        return latf, lonf
    except:
        return None, None

def main():
    conn = mysql.connector.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME)
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM wells WHERE (latitude IS NULL OR longitude IS NULL) OR (operator IS NULL)")
    rows = cur.fetchall()
    for r in rows:
        # simple clean
        upd = {}
        if r['address']:
            upd['address'] = clean_text(r['address'])
        if r['well_name']:
            upd['well_name'] = clean_text(r['well_name'])
        lat, lon = normalize_coords(r['latitude'], r['longitude'])
        # write back
        qparts = []
        vals = []
        for k,v in upd.items():
            qparts.append(f"{k}=%s"); vals.append(v)
        if lat is not None and lon is not None:
            qparts.append("latitude=%s"); vals.append(lat)
            qparts.append("longitude=%s"); vals.append(lon)
        if qparts:
            vals.append(r['id'])
            sql = "UPDATE wells SET " + ", ".join(qparts) + " WHERE id=%s"
            cur2 = conn.cursor()
            cur2.execute(sql, vals)
            conn.commit()
            cur2.close()
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()