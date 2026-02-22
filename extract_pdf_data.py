import os
import re
import pytesseract
from PyPDF2 import PdfReader
from pdf2image import convert_from_path
import psycopg2

# ===============================
# CONFIGURATION
# ===============================
PDF_FOLDER = "pdfs/"
OCR_FOLDER = "ocr_output/"

# If using Windows, set your Tesseract path:
pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"

# Create OCR folder if not exists
if not os.path.exists(OCR_FOLDER):
    os.makedirs(OCR_FOLDER)

# ===============================
# DATABASE CONNECTION
# ===============================
db = psycopg2.connect(
    host="localhost",
    user="postgres",
    password="admin123",
    database="oil_wells"
)
cursor = db.cursor()

# ===============================
# HELPER FUNCTIONS
# ===============================

def clean_text(text):
    text = re.sub(r'\n+', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'[^\x00-\x7F]+', ' ', text)  # remove weird OCR chars
    return text.strip()


def extract_fields(text):
    data = {}

    api_match = re.search(r'API[:\s]*([\d\-]+)', text)
    lat_match = re.search(r'Latitude[:\s]*([^\s,]+)', text)
    long_match = re.search(r'Longitude[:\s]*([^\s,]+)', text)
    name_match = re.search(r'Well Name[:\s]*([A-Za-z0-9\-\s]+)', text)

    data['api'] = api_match.group(1) if api_match else "N/A"
    data['latitude'] = lat_match.group(1) if lat_match else "N/A"
    data['longitude'] = long_match.group(1) if long_match else "N/A"
    data['well_name'] = name_match.group(1).strip() if name_match else "N/A"

    return data


def insert_into_db(data):
    sql = """
        INSERT INTO wells (api_number, well_name, latitude, longitude)
        VALUES (%s, %s, %s, %s)
    """
    cursor.execute(sql, (
        data['api'],
        data['well_name'],
        data['latitude'],
        data['longitude']
    ))
    db.commit()


def extract_text_from_pdf(path):
    text = ""
    reader = PdfReader(path)

    for page in reader.pages:
        extracted = page.extract_text()
        if extracted:
            text += extracted

    return text


def apply_ocr(path):
    print("   → No embedded text found. Applying OCR...")
    text = ""
    images = convert_from_path(path)

    for image in images:
        text += pytesseract.image_to_string(image)

    return text


# ===============================
# MAIN LOOP
# ===============================

for file in os.listdir(PDF_FOLDER):
    if file.endswith(".pdf"):
        print(f"\nProcessing: {file}")
        path = os.path.join(PDF_FOLDER, file)

        try:
            text = extract_text_from_pdf(path)

            # If no text found → use OCR
            if text.strip() == "":
                text = apply_ocr(path)

            text = clean_text(text)
            data = extract_fields(text)

            insert_into_db(data)

            print("   ✓ Inserted into database")

        except Exception as e:
            print(f"   ✗ Error processing {file}: {e}")

print("\nPDF extraction complete.")
cursor.close()
db.close()