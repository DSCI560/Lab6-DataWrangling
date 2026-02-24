import os
import hashlib
import pymysql
import pytesseract
from pdf2image import convert_from_path
from PyPDF2 import PdfReader
import gc

from parse_utils import (
    clean_text,
    extract_api,
    extract_well_name,
    extract_operator,
    extract_county_state,
    extract_address,
    extract_coordinates,
    parse_all_stim_and_extended,
    is_valid_nd_coordinate
)

pdf_folder = "pdfs"
db_config = {
    "host": "localhost",
    "user": "root",
    "password": "admin123",
    "database": "oil_wells",
}

conn = pymysql.connect(**db_config)
cursor = conn.cursor()

def get_file_hash(filepath):
    hasher = hashlib.sha256()
    with open(filepath, "rb") as f:
        while chunk := f.read(8192):
            hasher.update(chunk)
    return hasher.hexdigest()

def already_processed(file_hash):
    cursor.execute("SELECT id FROM wells WHERE file_hash = %s", (file_hash,))
    return cursor.fetchone() is not None

def ocr_pdf_to_text(filepath):
    text = ""

    try:
        reader = PdfReader(filepath)
        for page in reader.pages:
            extracted = page.extract_text()
            if extracted:
                text += extracted + "\n"
        if len(text.strip()) > 400 and extract_api(text):
            print("Using direct PDF text extraction")
            return clean_text(text)
        if text.strip():
            print("Text extracted but API not found, running OCR fallback")
    except Exception:
        pass

    try:
        reader = PdfReader(filepath)
        total_pages = len(reader.pages)
    except Exception:
        total_pages = None

    batch_size = 12
    current_page = 1

    while total_pages and current_page <= total_pages:
        try:
            last_page = min(current_page + batch_size - 1, total_pages)
            print(f"OCR pages {current_page} to {last_page}")

            images = convert_from_path(
                filepath,
                dpi=225,
                first_page=current_page,
                last_page=last_page
            )

            for img in images:
                text += pytesseract.image_to_string(img, config="--psm 6") + "\n"
                del img

            del images
            gc.collect()

            stim_rows, ext = parse_all_stim_and_extended(text)
            api_found = extract_api(text)
            coords = extract_coordinates(text)
            coords_ok = coords[0] is not None and coords[1] is not None

            stim_present = False
            if stim_rows:
                stim_present = True
            if ext.get('treatment_type') or ext.get('lbs_proppant') or ext.get('treatment_pressure'):
                stim_present = True

            if api_found and coords_ok and stim_present:
                print("Required metadata and stimulation found, stopping OCR early")
                break

            current_page = last_page + 1

        except MemoryError:
            print("MemoryError, reducing batch size")
            batch_size = max(4, batch_size // 2)
            gc.collect()
        except Exception as e:
            print(f"OCR batch error: {e}")
            batch_size = max(4, batch_size // 2)
            gc.collect()

    return clean_text(text)

def validate_well_record(data):
    if not data.get("api"):
        return "invalid"
    if data.get("latitude") is None or data.get("longitude") is None:
        return "needs_review"
    if not is_valid_nd_coordinate(data["latitude"], data["longitude"]):
        return "needs_review"
    if not data.get("well_name") or len(str(data.get("well_name")).strip()) < 3:
        return "needs_review"
    return "valid"

def save_well(data):
    try:
        if data.get("address") and len(data["address"]) > 500:
            data["address"] = data["address"][:500]

        cursor.execute("""
            INSERT INTO wells (
                filename, file_hash, api, well_name, address,
                latitude, longitude, county, state, operator,
                qc_status, raw_text
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                raw_text = VALUES(raw_text),
                latitude = VALUES(latitude),
                longitude = VALUES(longitude),
                qc_status = VALUES(qc_status)
        """, (
            data.get("filename"),
            data.get("file_hash"),
            data.get("api"),
            data.get("well_name"),
            data.get("address"),
            data.get("latitude"),
            data.get("longitude"),
            data.get("county"),
            data.get("state"),
            data.get("operator"),
            data.get("qc_status"),
            data.get("raw_text")
        ))
        conn.commit()
        cursor.execute("SELECT id FROM wells WHERE api = %s", (data.get("api"),))
        row = cursor.fetchone()
        return row[0] if row else None
    except Exception as e:
        print(f"DB error while saving well: {e}")
        conn.rollback()
        return None

def save_stimulations(well_id, stim_rows, ext):
    if not stim_rows and not any([ext.get('treatment_type'), ext.get('lbs_proppant'), ext.get('treatment_pressure'), ext.get('max_treatment_rate')]):
        return
    try:
        for stim in stim_rows or [{}]:
            cursor.execute("""
                INSERT INTO stimulations (
                    well_id,
                    date_stimulated,
                    stimulated_formation,
                    top_ft,
                    bottom_ft,
                    stages,
                    volume,
                    volume_units,
                    treatment_type,
                    lbs_proppant,
                    acid_percent,
                    treatment_pressure,
                    max_treatment_rate,
                    additional_info
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                well_id,
                stim.get("date_stimulated"),
                stim.get("stimulated_formation"),
                stim.get("top_ft"),
                stim.get("bottom_ft"),
                stim.get("stages"),
                stim.get("volume"),
                stim.get("volume_units"),
                ext.get("treatment_type"),
                ext.get("lbs_proppant"),
                ext.get("acid_percent"),
                ext.get("treatment_pressure"),
                ext.get("max_treatment_rate"),
                stim.get("additional_info") or ext.get("details_text")
            ))
        conn.commit()
    except Exception as e:
        print(f"DB error while saving stimulations: {e}")
        conn.rollback()

def process_file(filepath):
    print(f"\nProcessing {filepath}")
    file_hash = get_file_hash(filepath)
    if already_processed(file_hash):
        print("Skipping (already processed)")
        return

    text = ocr_pdf_to_text(filepath)

    api = extract_api(text)
    well_name = extract_well_name(text)
    operator = extract_operator(text)
    county, state = extract_county_state(text)
    address = extract_address(text)
    latitude, longitude = extract_coordinates(text)
    stim_rows, ext = parse_all_stim_and_extended(text)

    well_data = {
        "filename": os.path.basename(filepath),
        "file_hash": file_hash,
        "api": api,
        "well_name": well_name,
        "address": address,
        "latitude": latitude,
        "longitude": longitude,
        "county": county,
        "state": state or "North Dakota",
        "operator": operator,
        "raw_text": text
    }

    qc_status = validate_well_record(well_data)
    well_data["qc_status"] = qc_status

    print(f"QC status: {qc_status}")
    print(f"API: {api}")
    print(f"Coordinates: {latitude}, {longitude}")

    if qc_status == "invalid":
        print("Record rejected (invalid)")
        return

    well_id = save_well(well_data)

    if well_id:
        save_stimulations(well_id, stim_rows, ext)
        if stim_rows:
            print(f"Inserted {len(stim_rows)} stim rows")
        else:
            print("No structured stim rows; saved extended stim summary")

    print("Done")

def main():
    for file in os.listdir(pdf_folder):
        if file.lower().endswith(".pdf"):
            process_file(os.path.join(pdf_folder, file))

if __name__ == "__main__":
    main()
