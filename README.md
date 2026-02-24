# Oil Wells Data Wrangling – Lab 6

Team Name: GamerSups  
Members: Bhargav Limbasia, Harsh Marar, Nisharksh Mittal  
Assignment: Lab 6 – Oil Wells Data Wrangling  

This project extracts well data from scanned PDFs, stores it in a MySQL database, enriches it using web scraping, and visualizes everything on an interactive map.

Overview

The workflow has four main parts:

1. PDF Extraction (OCR + parsing)
2. Database storage
3. Web scraping (DrillingEdge)
4. Web visualization (Flask + Leaflet map)

Each PDF corresponds to one well-specific dataset.

1. PDF Extraction

Script: ocr_and_extract.py

What it does:
- Iterates through all PDFs in the pdfs/ folder.
- Uses PyPDF2 first (fast path).
- Falls back to full OCR using PyTesseract for scanned pages.
- OCRs all pages (no early stopping).
- Extracts:
  - API number
  - Well name
  - Latitude & longitude (DMS + decimal support)
  - Operator
  - County, state
  - Address
  - Stimulation data:
    - Date stimulated
    - Formation
    - Top / bottom depth
    - Stages
    - Volume & units
    - Treatment type
    - Lbs proppant
    - Acid %
    - Maximum treatment pressure
    - Maximum treatment rate

All extracted data is cleaned and inserted into MySQL.

UPSERT logic is used to avoid duplicate API crashes.

2. Database

Database: oil_wells

Tables:

wells
Stores:
- filename
- file_hash
- API
- well name
- coordinates
- operator
- county/state
- qc_status
- raw_text

stimulations
Stores:
- well_id (foreign key)
- date_stimulated
- stimulated_formation
- top_ft
- bottom_ft
- stages
- volume
- volume_units
- treatment_type
- lbs_proppant
- acid_percent
- treatment_pressure
- max_treatment_rate
- additional_info

3. Web Scraping

Integrated inside ingestion pipeline.

For each well:
- Uses API and well name
- Queries drillingedge.com
- Extracts:
  - Well status
  - Well type
  - Closest city
  - Oil production
  - Gas production

Data is appended to existing DB records.

Missing values are replaced with 0 or N/A as required.

4. Flask API

File: app.py

Provides:

GET /api/wells
Returns:
- Well metadata
- All stimulation rows
- Production info

GET /api/wells/<id>
Returns:
- Full detailed record including raw_text

Runs using:
python app.py

Server runs at:
http://localhost:5000

5. Map Visualization

Frontend: index.html  
Uses:
- Leaflet
- OpenStreetMap tiles

What it does:
- Fetches /api/wells
- Plots push pins using latitude/longitude
- Popup shows:
  - API
  - Operator
  - County/state
  - QC status
  - Stimulation table
  - Lbs proppant

Map loads automatically when Flask server is running.

6. Sample Data Loader (Optional)

Script: load_data.py

Purpose:
- Inserts a sample well if database is empty.
- Useful for testing the map without running OCR.

How to Run

Step 1 – Setup MySQL
Create database:
CREATE DATABASE oil_wells;

Run required ALTER statements for stimulation fields if needed.

Step 2 – Install Dependencies
pip install pytesseract pdf2image PyPDF2 pymysql flask flask-cors

Make sure Tesseract and Poppler are installed (Linux environment).

Step 3 – Run PDF Extraction
python ocr_and_extract.py

Step 4 – Run Web App
python app.py

Open in browser:
http://localhost:5000


Notes

- Each PDF is treated as one well dataset.
- OCR processes all pages.
- Multi-well administrative references inside PDFs are not split into separate records (aligned with assignment intent).
- Data is cleaned before DB storage.
