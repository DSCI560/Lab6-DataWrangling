# Assignment – Part 1  
## Data Collection, Wrangling, and Storage

This assignment required converting scanned oil well PDF documents into structured, enriched, and validated database records. The approach was designed to systematically handle unstructured OCR text, prevent duplicate processing, and store clean relational data for further analysis.

The overall workflow follows a linear but modular pipeline:

1. Initialize the database.
2. OCR and extract text from PDFs.
3. Parse structured fields from noisy text.
4. Enrich records using drillingedge.com.
5. Clean and normalize extracted data.
6. Store validated results in relational tables.

The system was designed to handle inconsistencies in scanned documents while maintaining database integrity and incremental processing.

---

### create_tables.sql – Database Initialization Logic

This script creates the relational schema for storing all extracted and enriched data. The design separates well metadata, stimulation data, and web-enriched data into independent tables. Primary and foreign keys ensure referential integrity.

A UNIQUE constraint is applied to the file hash field to prevent duplicate processing. Each PDF is uniquely identified by its SHA256 hash. This ensures that even if a file is renamed, it will not be reprocessed.

The logic implemented:
- Normalize the schema to avoid redundancy.
- Enforce uniqueness at the database level.
- Use foreign keys to preserve relationships.
- Allow NULL values for missing or unavailable fields.

---

### ocr_and_extract.py – OCR and Base Extraction Logic

This script processes each PDF document and extracts raw text.

Since scanned PDFs contain images rather than selectable text, OCR is required. The primary method uses `ocrmypdf` to generate a searchable text layer. If this fails, a fallback using `pytesseract` is applied.

Before processing a file, a SHA256 hash is generated and checked against the database. If the hash already exists, the file is skipped. This prevents redundant processing and ensures incremental execution.

After OCR:
- API number, well name, and coordinates are extracted.
- Stimulation sections are located heuristically.
- Raw stimulation lines are stored for structured refinement later.

The logic implemented:
- Always convert images to machine-readable text first.
- Apply extraction only after OCR validation.
- Prevent duplicate processing using hash comparison.
- Store minimally structured data first, refine later.

---

### parse_utils.py – Parsing and Field Extraction Logic

This module handles structured data extraction from noisy OCR text.

Because OCR output may contain inconsistent formatting, layered extraction logic is used:
- First attempt labeled pattern matching (e.g., “API:”).
- If labels are missing, fallback to numeric pattern detection.
- Support both decimal and DMS coordinate formats.
- Convert DMS coordinates into decimal degrees.

For stimulation data, instead of assuming strict table formatting, the script identifies stimulation-related sections and captures relevant lines. This avoids failure when tables are misaligned during OCR.

The logic implemented:
- Use regex patterns with fallback strategies.
- Convert all geographic data to a standardized numeric format.
- Avoid strict table dependency due to OCR noise.
- Capture raw stimulation data when the structure is uncertain.

---

### scrape_drillingedge.py – Web Enrichment Logic

This script enriches each well record using drillingedge.com.

The API number is used as the primary search key. If unavailable, the well name is used as a fallback. The script queries the search page, identifies the relevant well result, and extracts required fields such as well status, well type, closest city, and production values.

Extracted enrichment data is stored in a separate table linked to the base well record.

The logic implemented:
- Use the most reliable identifier (API) first.
- Apply fallback query logic when necessary.
- Validate extracted values before insertion.
- Maintain separation between extracted and scraped data.
- Add request delay to avoid excessive server load.

---

### preprocess_and_store.py – Cleaning and Normalization Logic

This script cleans and standardizes extracted data before final storage validation.

OCR often introduces formatting artifacts such as extra whitespace or invalid characters. The script removes non-ASCII characters and normalizes whitespace.

Coordinates are validated to ensure they fall within valid geographic ranges. If invalid, they are reset to NULL to prevent corrupt spatial data.

The logic implemented:
- Clean text before committing updates.
- Normalize numeric fields to correct types.
- Validate coordinate ranges.
- Update incomplete records incrementally.

---

### Overall Approach and Problem Handling

The system was designed to address the core challenges of the assignment:

- Scanned PDFs containing image-based text.
- Noisy and inconsistent OCR output.
- Irregular formatting of stimulation tables.
- Risk of duplicate processing.
- External enrichment requirement.
- Need for structured relational storage.

The solution strategy was:

- Convert unstructured image data into text using OCR.
- Apply layered parsing logic with fallback mechanisms.
- Normalize extracted values before storage.
- Enforce database-level duplicate protection.
- Separate core extraction from enrichment.
- Maintain modular scripts for controlled execution.

The implemented pipeline converts unstructured scanned documents into validated, enriched, and relationally consistent database records using incremental and defensive processing logic.
