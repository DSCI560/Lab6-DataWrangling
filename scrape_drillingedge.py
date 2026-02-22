# scrape_drillingedge.py
import requests
from bs4 import BeautifulSoup
import mysql.connector
import time
import os

DB_HOST = os.environ.get("DB_HOST", "127.0.0.1")
DB_USER = os.environ.get("DB_USER", "root")
DB_PASS = os.environ.get("DB_PASS", "admin123")
DB_NAME = os.environ.get("DB_NAME", "oil_wells")

BASE_SEARCH = "https://www.drillingedge.com/search?q={query}"

def db_connect():
    return mysql.connector.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME)

def search_and_scrape(api, well_name):
    query = api or well_name
    if not query:
        return None
    url = BASE_SEARCH.format(query=requests.utils.quote(query))
    r = requests.get(url, timeout=15)
    if r.status_code != 200:
        return None
    soup = BeautifulSoup(r.text, "html.parser")
    # heuristics: find first result link
    link = soup.select_one("a.result-link") or soup.select_one("a")
    if not link:
        return None
    href = link.get("href")
    if href.startswith("/"):
        href = "https://www.drillingedge.com" + href
    # fetch well page
    wp = requests.get(href, timeout=15)
    if wp.status_code != 200:
        return None
    wsoup = BeautifulSoup(wp.text, "html.parser")
    # Now extract fields: look for labels or highlighted spans
    def get_text_by_label(lbl):
        el = wsoup.find(text=lambda t: t and lbl in t)
        if el:
            parent = el.parent
            nxt = parent.find_next_sibling(text=True)
            if nxt:
                return nxt.strip()
        return None
    # fallback: look for numeric badges
    barrels = None
    b_el = wsoup.find(lambda tag: tag.name=="span" and "Barrels" in tag.text)
    if b_el:
        barrels = b_el.text.strip()
    # try well status / type / closest city using label text on page
    status = None
    wtype = None
    city = None
    # generic approach: scan small tables
    for tr in wsoup.select("table tr"):
        tds = [td.get_text(separator=" ", strip=True) for td in tr.find_all(["td","th"])]
        if len(tds) >= 2:
            key = tds[0].lower()
            val = tds[1]
            if "status" in key and not status:
                status = val
            if "type" in key and not wtype:
                wtype = val
            if "closest city" in key or "closest" in key:
                city = val
    return {"url": href, "status": status, "type": wtype, "city": city, "barrels": barrels}

def main():
    conn = db_connect()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT id, api, well_name FROM wells")
    rows = cur.fetchall()
    for r in rows:
        res = search_and_scrape(r['api'], r['well_name'])
        if res:
            cur2 = conn.cursor()
            cur2.execute("""
                INSERT INTO drillingedge_extra (well_id, well_status, well_type, closest_city, barrels_of_oil, scraped_url)
                VALUES (%s,%s,%s,%s,%s,%s)
            """, (r['id'], res.get('status'), res.get('type'), res.get('city'), res.get('barrels'), res.get('url')))
            conn.commit()
            cur2.close()
        time.sleep(1)  # avoid hammering the site
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()