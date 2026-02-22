# ocr_and_extract.py
import os, hashlib, sys, subprocess, tempfile
from pdf2image import convert_from_path
from pytesseract import image_to_string
from parse_utils import extract_api, extract_coords, extract_wellname, parse_stimulation_table
import mysql.connector
from tqdm import tqdm

DB_HOST = os.environ.get("DB_HOST", "127.0.0.1")
DB_USER = os.environ.get("DB_USER", "root")
DB_PASS = os.environ.get("DB_PASS", "admin123")
DB_NAME = os.environ.get("DB_NAME", "oil_wells")

PDF_DIR = sys.argv[1] if len(sys.argv) > 1 else "./pdfs"

def file_hash(path):
    h = hashlib.sha256()
    with open(path,'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            h.update(chunk)
    return h.hexdigest()

def db_connect():
    return mysql.connector.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME)

def already_processed(cur, fhash):
    cur.execute("SELECT id FROM wells WHERE file_hash=%s", (fhash,))
    return cur.fetchone() is not None

def save_well(cur, filename, fhash, api, name, lat, lon, address, operator=None):
    cur.execute("""
      INSERT INTO wells (filename, file_hash, api, well_name, address, latitude, longitude, operator)
      VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    """, (filename, fhash, api, name, address, lat, lon, operator))
    return cur.lastrowid

def save_stim(cur, well_id, stim_line):
    # for now store entire line as additional_info; later parse into structured fields
    cur.execute("""
      INSERT INTO stimulations (well_id, additional_info)
      VALUES (%s,%s)
    """, (well_id, stim_line))

def ocr_pdf_to_text(pdf_path):
    # try ocrmypdf to produce OCRed PDF and read text via pdftotext
    try:
        out = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
        out.close()
        subprocess.check_call(["ocrmypdf", "--force-ocr", pdf_path, out.name])
        # now use pdftotext (poppler)
        txt = subprocess.check_output(["pdftotext", "-layout", out.name, "-"]).decode('utf8', errors='ignore')
        os.unlink(out.name)
        return txt
    except Exception as e:
        # fallback: render pages to images and apply pytesseract
        pages = convert_from_path(pdf_path, dpi=200)
        texts = []
        for im in pages:
            texts.append(image_to_string(im))
        return "\n".join(texts)

def process_file(cur, path):
    print("Processing:", path)
    fh = file_hash(path)
    if already_processed(cur, fh):
        print(" - already processed, skipping.")
        return
    text = ocr_pdf_to_text(path)
    api = extract_api(text)
    lat, lon = extract_coords(text)
    name = extract_wellname(text)
    address = None
    # try to find address lines heuristically
    for line in text.splitlines():
        if "County" in line or "State" in line or "Address" in line:
            address = (address or "") + " " + line.strip()
    wid = save_well(cur, os.path.basename(path), fh, api, name, lat, lon, address, operator=None)
    # parse stimulation table heuristically
    stims = parse_stimulation_table(text)
    for s in stims:
        save_stim(cur, wid, s)
    print(" - saved well id", wid)

def main():
    conn = db_connect()
    cur = conn.cursor()
    for fname in tqdm(sorted(os.listdir(PDF_DIR))):
        if not fname.lower().endswith(".pdf"):
            continue
        try:
            process_file(cur, os.path.join(PDF_DIR, fname))
            conn.commit()
        except Exception as e:
            print("Error processing", fname, e)
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()