# parse_utils.py
import re
from datetime import datetime

# Example regex patterns; you will tune these per real PDF text
API_RE = re.compile(r'\bAPI[:\s]*([0-9\-]{5,})\b', re.I)
COORD_DMS_RE = re.compile(r'([0-9]{1,3})[°\s]+([0-9]{1,2})[\'\s]+([0-9]{1,2}(?:\.\d+)?)\s*([NnSsEeWw])')
COORD_DEC_RE = re.compile(r'Latitude[:\s]*([+-]?\d+\.\d+)|Lat[:\s]*([+-]?\d+\.\d+)', re.I)
LONG_DEC_RE = re.compile(r'Longitude[:\s]*([+-]?\d+\.\d+)|Long[:\s]*([+-]?\d+\.\d+)', re.I)
WELL_NAME_RE = re.compile(r'Well Name[:\s]*([\w \-\,\.0-9]+)', re.I)
WELL_LINE_RE = re.compile(r'Operator[:\s]*(.+?)\s+Well Name[:\s]*(.+?)\s+API[:\s]*(\d+)', re.I|re.S)

def dms_to_decimal(deg, minutes, seconds, hemi):
    dec = float(deg) + float(minutes)/60.0 + float(seconds)/3600.0
    if hemi.upper() in ('S','W'):
        dec = -dec
    return dec

def extract_api(text):
    m = API_RE.search(text)
    if m:
        return m.group(1).strip()
    # fallback: try find 10+ digit numbers that look like API
    m2 = re.search(r'\b(\d{10,})\b', text)
    return m2.group(1) if m2 else None

def extract_coords(text):
    # first try decimal style
    latm = COORD_DEC_RE.search(text)
    lonm = LONG_DEC_RE.search(text)
    if latm and lonm:
        lat = latm.group(1) or latm.group(2)
        lon = lonm.group(1) or lonm.group(2)
        try:
            return float(lat), float(lon)
        except:
            pass
    # fallback: DMS search find two occurrences
    dms = COORD_DMS_RE.findall(text)
    if len(dms) >= 2:
        lat = dms[0]
        lon = dms[1]
        return dms_to_decimal(lat[0], lat[1], lat[2], lat[3]), dms_to_decimal(lon[0], lon[1], lon[2], lon[3])
    return None, None

def extract_wellname(text):
    m = WELL_NAME_RE.search(text)
    if m:
        return m.group(1).strip()
    # fallback to first large uppercase line
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    if lines:
        candidate = lines[0]
        if len(candidate) < 120:
            return candidate
    return None

# parse stimulation table: naive approach
def parse_stimulation_table(text):
    # find "Well Specific Stimulations" or "Stimulation" block
    block = None
    start = text.find("Well Specific Stimulations")
    if start >= 0:
        block = text[start:start+2000]
    else:
        # try search for "Stimulation Data" or "Type Treatment"
        m = re.search(r'(Stimulation|Type Treatment|Top \(Ft\)).{0,1200}', text, re.I|re.S)
        if m:
            block = text[m.start(): m.start()+1200]
    if not block:
        return []
    # simple extraction: look for lines with dates or numbers
    results = []
    # naive: find lines that contain 'Date' or 'Top' and split
    for line in block.splitlines():
        if re.search(r'\bDate\b|\bTop\b|\bVolume\b', line, re.I):
            results.append(line.strip())
    return results