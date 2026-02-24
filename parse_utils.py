# parse_utils.py
import re
from datetime import datetime
from typing import Tuple, List, Dict, Optional

nd_lat_range = (45.0, 50.0)
nd_lon_range = (-105.0, -96.0)

header_blacklist = [
    "24-HOUR PRODUCTION",
    "PLEASE READ INSTRUCTIONS",
    "FOR STATE USE ONLY",
    "DETAILS OF WORK",
    "INDUSTRIAL COMMISSION",
]

def clean_text(text: str) -> str:
    if text is None:
        return ""
    text = text.replace('\r', '\n')
    text = re.sub(r'\t', ' ', text)
    text = re.sub(r' +', ' ', text)
    text = re.sub(r'\n+', '\n', text)
    return text.strip()

def is_valid_nd_coordinate(lat: Optional[float], lon: Optional[float]) -> bool:
    if lat is None or lon is None:
        return False
    try:
        return nd_lat_range[0] <= float(lat) <= nd_lat_range[1] and nd_lon_range[0] <= float(lon) <= nd_lon_range[1]
    except Exception:
        return False

def dms_to_decimal(deg: float, minutes: float, seconds: float, hemi: str) -> float:
    val = float(deg) + float(minutes)/60.0 + float(seconds)/3600.0
    hemi = (hemi or "").upper()
    if hemi in ('S','W'):
        val = -abs(val)
    return val

def _digits_only(s: str) -> str:
    return re.sub(r'\D', '', s or '')

def normalize_api_from_digits(digits: str) -> Optional[str]:
    if not digits or len(digits) != 10:
        return None
    return f"{digits[:2]}-{digits[2:5]}-{digits[5:]}"

def extract_api(text: str) -> Optional[str]:
    if not text:
        return None
    t = text

    m = re.search(r'\bAPI[:\s]*([0-9\-\s]{8,20})\b', t, flags=re.IGNORECASE)
    if m:
        candidate = m.group(1)
        digits = _digits_only(candidate)
        api = normalize_api_from_digits(digits)
        if api:
            return api

    m = re.search(r'\b33[-\s]?\d{3}[-\s]?\d{5}\b', t)
    if m:
        candidate = m.group(0)
        digits = _digits_only(candidate)
        return normalize_api_from_digits(digits)

    m = re.search(r'\b33\d{8}\b', t)
    if m:
        return normalize_api_from_digits(m.group(0))

    return None

def extract_well_name(text: str) -> Optional[str]:
    if not text:
        return None
    t = text

    m = re.search(r'Well Name(?: and Number|/Number| Number)?[:\s\-]*([^\n\r]{3,120})', t, flags=re.IGNORECASE)
    if m:
        candidate = m.group(1).strip()
        if len(candidate) > 3 and any(ch.isalpha() for ch in candidate):
            if not any(h in candidate.upper() for h in header_blacklist):
                return candidate

    m = re.search(r'Well Name(?: and Number|/Number| Number)?\s*\n\s*([^\n\r]{3,120})', t, flags=re.IGNORECASE)
    if m:
        candidate = m.group(1).strip()
        if len(candidate) > 3:
            return candidate

    for line in t.splitlines():
        if re.search(r'[A-Za-z].*\d', line) and len(line) < 120 and ':' not in line:
            if not any(h in line.upper() for h in header_blacklist):
                return line.strip()
    return None

def extract_operator(text: str) -> Optional[str]:
    if not text:
        return None
    m = re.search(r'Operator[:\s]*([^\n\r]{2,200})', text, flags=re.IGNORECASE)
    if m:
        return m.group(1).strip()
    m = re.search(r'Operator\s*\n\s*([^\n\r]{2,200})', text, flags=re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return None

def extract_county_state(text: str) -> Tuple[Optional[str], Optional[str]]:
    county = None
    state = None
    if not text:
        return county, state

    m = re.search(r'County[, ]+State[:\s]*([^\n\r]+)', text, flags=re.IGNORECASE)
    if m:
        full = m.group(1).strip()
        parts = [p.strip() for p in re.split(r',', full)]
        if parts:
            county = parts[0]
            if len(parts) > 1:
                state = parts[1]
    if not county:
        m = re.search(r'County[:\s]*([^\n\r]{2,120})', text, flags=re.IGNORECASE)
        if m:
            county = m.group(1).strip()
    if not state:
        m = re.search(r'State[:\s]*([^\n\r]{2,120})', text, flags=re.IGNORECASE)
        if m:
            state = m.group(1).strip()
    if state and 'dakota' in state.lower():
        state = 'North Dakota'
    return county, state

def extract_address(text: str) -> Optional[str]:
    if not text:
        return None
    m = re.search(r'Address[:\s]*\n\s*(.+?)\n\s*City[:\s]*\n\s*(.+?)\n\s*State[:\s]*\n\s*(.+?)\n\s*(?:Zip|Zip Code)[:\s]*\n\s*(\d{5})', text, flags=re.IGNORECASE | re.DOTALL)
    if m:
        street = ' '.join(m.group(1).split())
        city = ' '.join(m.group(2).split())
        state = ' '.join(m.group(3).split())
        zipc = m.group(4)
        return f"{street}, {city}, {state} {zipc}"[:500]

    m = re.search(r'Address[:\s]*([^\n\r]{5,500})', text, flags=re.IGNORECASE)
    if m:
        addr = ' '.join(m.group(1).split())
        return addr[:500]

    m = re.search(r'Operator[:\s]*[^\n\r]+\n(.{5,240})', text, flags=re.IGNORECASE)
    if m:
        candidate = re.sub(r'\s+', ' ', m.group(1).strip())
        return candidate[:500]
    return None

def _parse_dms_flexible(s: str) -> Optional[Tuple[float,float]]:
    if not s:
        return None

    lat_patterns = [
        r'Latitude[:\s]*([0-9]{1,3})[°\s]\s*([0-9]{1,2})[\'\s]\s*([0-9\.]+)\s*([NS])',
        r'Lat[:\s]*([0-9]{1,3})[°\s]\s*([0-9]{1,2})[\'\s]\s*([0-9\.]+)\s*([NS])'
    ]
    lon_patterns = [
        r'Longitude[:\s]*([0-9]{1,3})[°\s]\s*([0-9]{1,2})[\'\s]\s*([0-9\.]+)\s*([EW])',
        r'Lon[:\s]*([0-9]{1,3})[°\s]\s*([0-9]{1,2})[\'\s]\s*([0-9\.]+)\s*([EW])'
    ]
    for lp in lat_patterns:
        for lnp in lon_patterns:
            mlat = re.search(lp, s, flags=re.IGNORECASE)
            mlon = re.search(lnp, s, flags=re.IGNORECASE)
            if mlat and mlon:
                try:
                    lat = dms_to_decimal(mlat.group(1), mlat.group(2), mlat.group(3), mlat.group(4))
                    lon = dms_to_decimal(mlon.group(1), mlon.group(2), mlon.group(3), mlon.group(4))
                    return (lat, lon)
                except Exception:
                    pass

    m = re.search(r'([0-9]{1,3})\s+([0-9]{1,2})\s+([0-9\.]+)\s*([NS])\D+([0-9]{1,3})\s+([0-9]{1,2})\s+([0-9\.]+)\s*([EW])', s, flags=re.IGNORECASE)
    if m:
        try:
            lat = dms_to_decimal(m.group(1), m.group(2), m.group(3), m.group(4))
            lon = dms_to_decimal(m.group(5), m.group(6), m.group(7), m.group(8))
            return (lat, lon)
        except Exception:
            pass

    m = re.search(r'Latitude[:\s]*([\-]?\d+\.\d+)', s, flags=re.IGNORECASE) or re.search(r'Lat[:\s]*([\-]?\d+\.\d+)', s, flags=re.IGNORECASE)
    n = re.search(r'Longitude[:\s]*([\-]?\d+\.\d+)', s, flags=re.IGNORECASE) or re.search(r'Lon[:\s]*([\-]?\d+\.\d+)', s, flags=re.IGNORECASE)
    if m and n:
        try:
            lat = float(m.group(1))
            lon = float(n.group(1))
            return (lat, lon)
        except Exception:
            pass

    decimals = re.findall(r'([\-]?\d{1,3}\.\d+)', s)
    if len(decimals) >= 2:
        for i in range(len(decimals)-1):
            try:
                lat = float(decimals[i])
                lon = float(decimals[i+1])
                if is_valid_nd_coordinate(lat, lon):
                    return (lat, lon)
                if is_valid_nd_coordinate(float(decimals[i+1]), float(decimals[i])):
                    return (float(decimals[i+1]), float(decimals[i]))
            except Exception:
                continue
    return None

def extract_coordinates(text: str) -> Tuple[Optional[float], Optional[float]]:
    if not text:
        return None, None

    parsed = _parse_dms_flexible(text)
    if parsed:
        return parsed

    for line in text.splitlines():
        if 'latitude' in line.lower() or 'longitude' in line.lower():
            parsed = _parse_dms_flexible(line)
            if parsed:
                return parsed

    lines = text.splitlines()
    for i in range(len(lines)-2):
        window = '\n'.join(lines[i:i+3])
        parsed = _parse_dms_flexible(window)
        if parsed:
            return parsed
    return None, None

def _find_stim_section(text: str) -> Optional[str]:
    if not text:
        return None
    headings = [
        r'Well Specific Stimulations',
        r'Well Specific Stimulation',
        r'Stimulation Data',
        r'Well Specific Fracture',
        r'Well Specific Fractures',
        r'Date Stimulated'
    ]
    for h in headings:
        m = re.search(h, text, flags=re.IGNORECASE)
        if m:
            start = m.start()
            end_match = re.search(r'ADDITIONAL INFORMATION|ADDITIONAL NOTES|DETAILS|SIGNATURE|CERTIFICATION|Figure\s', text[m.end():], flags=re.IGNORECASE)
            if end_match:
                end = m.end() + end_match.start()
            else:
                end = min(len(text), m.end() + 2000)
            return text[start:end]
    m = re.search(r'Date\s+Stimulated.*?Stimulated Formation', text, flags=re.IGNORECASE | re.DOTALL)
    if m:
        start = m.start()
        end = min(len(text), start + 2000)
        return text[start:end]
    return None

def parse_stimulations(text: str) -> List[Dict]:
    result = []
    if not text:
        return result

    section = _find_stim_section(text)
    if not section:
        return result

    sec = re.sub(r'\r', '\n', section)
    sec = re.sub(r'\n+', '\n', sec).strip()
    lines = sec.splitlines()

    additional_info_lines = []
    header_idx = None
    for idx, ln in enumerate(lines):
        if re.search(r'Date\s+Stimulated', ln, flags=re.IGNORECASE) and re.search(r'Stimulated\s+Formation', ln, flags=re.IGNORECASE):
            header_idx = idx
            break

    rows_start = header_idx + 1 if header_idx is not None else 0

    row_pattern = re.compile(
        r'(?P<date>\d{1,2}/\d{1,2}/\d{4})\s+'
        r'(?P<formation>[A-Za-z0-9\-\s\/\(\)]+?)\s+'
        r'(?P<top>\d{3,6})\s+'
        r'(?P<bottom>\d{3,6})\s+'
        r'(?P<stages>\d{1,4})\s+'
        r'(?P<volume>[\d,]+)\s+'
        r'(?P<units>\bBarrels\b|\bBBL\b|\bGallons\b|\bMCF\b|\bBBLS\/Min\b)?',
        flags=re.IGNORECASE
    )

    for ln in lines[rows_start:]:
        ln_stripped = ln.strip()
        if not ln_stripped:
            continue
        m = row_pattern.search(ln_stripped)
        if m:
            date_str = m.group('date')
            try:
                date_obj = datetime.strptime(date_str, "%m/%d/%Y").date()
            except Exception:
                date_obj = None
            formation = m.group('formation').strip()
            try:
                top_ft = int(m.group('top'))
            except:
                top_ft = None
            try:
                bottom_ft = int(m.group('bottom'))
            except:
                bottom_ft = None
            try:
                stages = int(m.group('stages'))
            except:
                stages = None
            try:
                volume = float(m.group('volume').replace(',', ''))
            except:
                volume = None
            units = m.group('units') if m.group('units') else None

            result.append({
                "date_stimulated": date_obj,
                "stimulated_formation": formation,
                "top_ft": top_ft,
                "bottom_ft": bottom_ft,
                "stages": stages,
                "volume": volume,
                "volume_units": units,
                "additional_info": None
            })
        else:
            if re.search(r'\d', ln_stripped) and (len(ln_stripped) < 200):
                additional_info_lines.append(ln_stripped)

    if not result:
        for ln in lines:
            m2 = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', ln)
            if m2:
                parts = re.split(r'\s{2,}', ln)
                if len(parts) >= 6:
                    date_str = parts[0].strip()
                    try:
                        date_obj = datetime.strptime(date_str, "%m/%d/%Y").date()
                    except:
                        date_obj = None
                    formation = parts[1].strip()
                    def to_int(s):
                        s2 = re.sub(r'[^\d]', '', s or '')
                        return int(s2) if s2 else None
                    top_ft = to_int(parts[2]) if len(parts) > 2 else None
                    bottom_ft = to_int(parts[3]) if len(parts) > 3 else None
                    stages = to_int(parts[4]) if len(parts) > 4 else None
                    vol = None
                    units = None
                    if len(parts) > 5:
                        vmatch = re.search(r'([\d,]+)', parts[5])
                        if vmatch:
                            vol = float(vmatch.group(1).replace(',', ''))
                            if len(parts) > 6:
                                units = parts[6].strip()
                    result.append({
                        "date_stimulated": date_obj,
                        "stimulated_formation": formation,
                        "top_ft": top_ft,
                        "bottom_ft": bottom_ft,
                        "stages": stages,
                        "volume": vol,
                        "volume_units": units,
                        "additional_info": None
                    })

    additional_info = '\n'.join(additional_info_lines).strip() if additional_info_lines else None
    if additional_info:
        for r in result:
            r["additional_info"] = additional_info

    return result

def extract_extended_stim_data(text: str) -> Dict[str, Optional[object]]:
    out = {
        "treatment_type": None,
        "lbs_proppant": None,
        "acid_percent": None,
        "treatment_pressure": None,
        "max_treatment_rate": None,
        "details_text": None
    }
    if not text:
        return out

    sec = _find_stim_section(text) or text

    m = re.search(r'(?:Type\s*Treatment|Treatment Type|Type Treatment)[:\s]*([A-Za-z0-9\-\s/]+)', sec, flags=re.IGNORECASE)
    if m:
        out['treatment_type'] = m.group(1).strip()

    m = re.search(r'(?:Lbs\s+Proppant|LBS\s+Proppant)[:\s]*([\d,]{3,})', sec, flags=re.IGNORECASE)
    if not m:
        m = re.search(r'\b([\d,]{5,})\b\s*(?:lbs|LBS)?\s*(?:Proppant)?', sec)
    if m:
        try:
            out['lbs_proppant'] = int(m.group(1).replace(',', ''))
        except:
            out['lbs_proppant'] = None

    m = re.search(r'Acid\s*(?:%|percent)?[:\s]*([\d\.]+)', sec, flags=re.IGNORECASE)
    if m:
        try:
            out['acid_percent'] = float(m.group(1))
        except:
            out['acid_percent'] = None

    m = re.search(r'(?:Maximum Treatment Pressure|Max(?:imum)? Treatment Pressure).*?([0-9]{2,6})', sec, flags=re.IGNORECASE)
    if m:
        try:
            out['treatment_pressure'] = float(m.group(1))
        except:
            out['treatment_pressure'] = None

    m = re.search(r'(?:Maximum Treatment Rate|Max(?:imum)? Treatment Rate).*?([0-9]+(?:\.[0-9]+)?)', sec, flags=re.IGNORECASE)
    if m:
        try:
            out['max_treatment_rate'] = float(m.group(1))
        except:
            out['max_treatment_rate'] = None

    detail_lines = []
    for ln in sec.splitlines():
        if re.search(r'\b(Mesh|White|30/50|40/70|100 Mesh)\b', ln, flags=re.IGNORECASE) or re.search(r'\:\s*\d{3,}', ln):
            detail_lines.append(ln.strip())
    out['details_text'] = '\n'.join(detail_lines).strip() if detail_lines else None

    return out

def parse_all_stim_and_extended(text: str):
    stim_rows = parse_stimulations(text)
    ext = extract_extended_stim_data(text)
    return stim_rows, ext

if __name__ == "__main__":
    print("parse_utils loaded")