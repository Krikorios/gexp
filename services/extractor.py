import base64
import json
import re
from pathlib import Path

from config import (
    ANTHROPIC_API_KEY, GEMINI_API_KEY,
    CLAUDE_MODEL, GEMINI_MODEL, DEFAULT_PROVIDER, UPLOAD_DIR,
)

SYSTEM_PROMPT = """You are an expert OCR assistant specializing in Lebanese government real estate documents written in Arabic (right-to-left). Your task is to extract structured data from scanned document images of "بطاقة معلومات عن الملكية العقارية" (Real Estate Property Information Cards) issued by the Lebanese Ministry of Real Estate Affairs.

CRITICAL INSTRUCTIONS:
1. Extract ALL text exactly as written in Arabic — do not translate or transliterate.
2. For dates, preserve the original format as printed (e.g., ٢٩-٣-٢٠٢٦ or 29/03/2026).
3. For the properties table, extract EVERY row — documents often have 30+ rows. If the document prominently states "لا يملك", set owns_properties to false and return an empty array [] for properties.
4. If a field is illegible or absent, use null.
5. Numbers may be in Arabic-Indic digits (٠١٢٣٤٥٦٧٨٩) or Western digits — preserve exactly as-is.
6. The header and footer sections contain these labeled fields:
   - الاسم (first_name), اسم الأب (father_name), اسم الأم (mother_name)
   - الشهرة (family_name / family_origin), الجنسية (nationality)
   - تاريخ الولادة (birth_date), رقم السجل (registry_number), مكان السجل (registry_place)
   - رقم الطلب (request_number), تاريخ الطلب (request_date)
   - اسم المستدعي/الوكيل (applicant_name_raw)
   - الغاية من الطلب (request_purpose)
   - نطاق البحث contains القضاء or المحافظات (qaza/district/governorate) — extract as search_scope
   - in the bottom footer, find the page fraction (e.g. "صفحة 1 من 3" or "1 / 3") and extract as page_info.
   - معلومات محولة لغاية (data_valid_until) found near the bottom.
   - أمانة السجل (registry_office) found at the bottom right.
   - عدد العقارات: (declared_property_count) located right under the properties table.
7. The properties table has 8 columns in order from RIGHT to LEFT as they appear on the page:
   col1(rightmost)=اسم الفريق, col2=رقم العقار, col3=القسم, col4=البلوك,        
   col5=المنطقة العقارية, col6=القضاء, col7=عدد الأسهم, col8(leftmost)=نوع الملكية
   Map these to JSON keys: party_name, property_number, section, block, real_estate_district, qaza, num_shares, ownership_type
8. Return ONLY valid JSON matching the schema. No markdown, no explanation."""  

USER_PROMPT = (
    "Extract all data from this Lebanese real estate property card. "
    "Return a single JSON object with these keys: "
    "request_number, request_date, applicant_name_raw, request_purpose, data_valid_until, "
    "registry_office, page_info, search_scope, owns_properties, declared_property_count, "
    "person (object with: first_name, father_name, mother_name, family_name, "  
    "family_origin, nationality, birth_date, registry_number, registry_place), "
    "properties (array of objects with: party_name, property_number, section, block, "
    "real_estate_district, qaza, num_shares, ownership_type), "
    "extraction_notes. "
    "Use null for missing fields. Include every property row from the table (or empty array if none)."   
)

def _resolve_path(image_path: str) -> str:
    if Path(image_path).is_absolute():
        return image_path
    return str(Path(UPLOAD_DIR) / image_path)


def _encode_image(image_path: str) -> tuple[str, str]:
    path = Path(image_path)
    suffix = path.suffix.lower()
    media_type_map = {
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".png": "image/png",
        ".webp": "image/webp",
        ".gif": "image/gif",
    }
    media_type = media_type_map.get(suffix, "image/jpeg")
    with open(image_path, "rb") as f:
        data = base64.standard_b64encode(f.read()).decode("utf-8")
    return data, media_type


def _strip_code_fences(text: str) -> str:
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        text = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])
    return text.strip()


def get_available_providers() -> list[dict]:
    """Return list of all available providers (EasyOCR is always available)."""
    providers = [
        {"id": "easyocr", "name": "EasyOCR (مجاني)", "model": "local"},
    ]
    if ANTHROPIC_API_KEY:
        providers.append({"id": "claude", "name": "Claude (Anthropic)", "model": CLAUDE_MODEL})
    if GEMINI_API_KEY:
        providers.append({"id": "gemini", "name": "Gemini (Google)", "model": GEMINI_MODEL})
    return providers


def get_default_provider() -> str:
    """Return the default provider, falling back to whichever is available."""
    providers = get_available_providers()
    if not providers:
        return "easyocr"
    ids = [p["id"] for p in providers]
    if DEFAULT_PROVIDER in ids:
        return DEFAULT_PROVIDER
    return ids[0]


# ─── Claude extraction ───────────────────────────────────────────

async def _extract_with_claude(image_path: str) -> dict:
    import anthropic

    client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_API_KEY)
    full_path = _resolve_path(image_path)
    img_data, media_type = _encode_image(full_path)

    message = await client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=4096,
        system=[
            {
                "type": "text",
                "text": SYSTEM_PROMPT,
                "cache_control": {"type": "ephemeral"},
            }
        ],
        messages=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": media_type,
                            "data": img_data,
                        },
                    },
                    {"type": "text", "text": USER_PROMPT},
                ],
            }
        ],
    )

    raw_text = _strip_code_fences(message.content[0].text)
    return json.loads(raw_text)


# ─── Gemini extraction ───────────────────────────────────────────

async def _extract_with_gemini(image_path: str) -> dict:
    import asyncio
    from google import genai
    from google.genai import types

    client = genai.Client(api_key=GEMINI_API_KEY)
    full_path = _resolve_path(image_path)

    with open(full_path, "rb") as f:
        image_bytes = f.read()

    suffix = Path(full_path).suffix.lower()
    mime_map = {".jpg": "image/jpeg", ".jpeg": "image/jpeg",
                ".png": "image/png", ".webp": "image/webp"}
    mime_type = mime_map.get(suffix, "image/jpeg")

    # Try primary model first; only fall back on 503 (overloaded), NOT on 429 (quota)
    models_to_try = [GEMINI_MODEL, "gemini-2.0-flash-lite"]

    def _call(model_name: str):
        response = client.models.generate_content(
            model=model_name,
            contents=[
                types.Content(parts=[
                    types.Part(text=SYSTEM_PROMPT + "\n\n" + USER_PROMPT),
                    types.Part(inline_data=types.Blob(mime_type=mime_type, data=image_bytes)),
                ])
            ],
            config=types.GenerateContentConfig(
                temperature=0.1,
                max_output_tokens=8192,
            ),
        )
        return response.text

    last_error = None
    for model_name in models_to_try:
        for attempt in range(2):
            try:
                raw_text = await asyncio.get_event_loop().run_in_executor(None, lambda m=model_name: _call(m))
                raw_text = _strip_code_fences(raw_text)
                return json.loads(raw_text)
            except Exception as e:
                last_error = e
                err_str = str(e)
                # On quota exhaustion, skip to next model immediately (don't waste retries)
                if "429" in err_str or "RESOURCE_EXHAUSTED" in err_str:
                    break
                # On temporary overload, wait and retry same model
                if "503" in err_str or "UNAVAILABLE" in err_str:
                    await asyncio.sleep(3 * (attempt + 1))
                    continue
                raise

    raise last_error


# ─── EasyOCR extraction (free, local) ────────────────────────────

# Cache EasyOCR reader (heavy to initialize)
_easyocr_reader = None


def _get_easyocr_reader():
    global _easyocr_reader
    if _easyocr_reader is None:
        import easyocr
        _easyocr_reader = easyocr.Reader(["ar", "en"], gpu=False)
    return _easyocr_reader


# Known label strings for header fields
_LABEL_MAP = {
    "الاسم":         "first_name",
    "اسم الأب":      "father_name",
    "اسم الأم":      "mother_name",
    "الشهرة":        "family_origin",
    "الجنسية":       "nationality",
    "تاريخ الولادة": "birth_date",
    "رقم السجل":     "registry_number",
    "مكان السجل":    "registry_place",
}

# Table header labels that mark the start of the properties table
_TABLE_HEADERS = {"اسم الفريق", "رقم العقار", "القسم", "البلوك",
                  "المنطقة العقارية", "القضاء", "عدد الأسهم", "نوع الملكية"}

# Column-header text → JSON key (right→left order on page)
_COL_KEYS = [
    ("اسم الفريق",       "party_name"),
    ("رقم العقار",       "property_number"),
    ("القسم",            "section"),
    ("البلوك",           "block"),
    ("المنطقة العقارية", "real_estate_district"),
    ("القضاء",           "qaza"),
    ("عدد الأسهم",       "num_shares"),
    ("نوع الملكية",      "ownership_type"),
]


def _parse_easyocr_results(results: list) -> dict:
    """
    Parse EasyOCR bounding-box results into structured document data.
    Each result is (bbox, text, confidence).
    bbox = [[x1,y1],[x2,y2],[x3,y3],[x4,y4]]

    Strategy:
    1. Build a list of items with (text, x_center, y_center, x_left, x_right).
    2. Detect the table-header row → derive column X-boundary ranges.
    3. Header section (above table): find label items, then take the nearest
       right-adjacent item as the value (RTL: label is in the centre/left,
       value is to the right of it on the same row).
    4. Table body rows: assign each cell to a column by matching its X-center
       to the column boundary derived from the header row.
    """
    if not results:
        return _empty_result("No text detected")

    items = []
    for bbox, text, conf in results:
        xs = [pt[0] for pt in bbox]
        ys = [pt[1] for pt in bbox]
        items.append({
            "text": text.strip(),
            "x": (min(xs) + max(xs)) / 2,
            "y": (min(ys) + max(ys)) / 2,
            "x_left": min(xs),
            "x_right": max(xs),
        })

    items.sort(key=lambda t: t["y"])

    # ── 1. Locate the table header row ──────────────────────────────────────
    header_row_items = []
    header_y = None
    
    # Simple heuristic to find where the table starts:
    # First, locate the word "العقارات" or "المملوكة" or "نطاق البحث"
    for item in items:
        if "العقارات" in item["text"] or "المملوكة" in item["text"]:
            header_y = item["y"]
            break

    if header_y is None:
        for item in items:
            if item["text"] in _TABLE_HEADERS:
                header_y = item["y"]
                break

    for item in items:
        if header_y is not None and 0 < item["y"] - header_y < 80:
             # The table headers are right below "العقارات المملوكة"
             # Let's collect anything that looks like a header column label
             if any(h in item["text"] for h in ["العقار", "الفريق", "القسم", "البلوك", "المنطقة", "قضاء", "أسهم", "نوع"]):
                 header_row_items.append(item)
                 
    if header_row_items:
        # Sort header cells right→left (descending x)
        header_row_items.sort(key=lambda t: -t["x"])

    # ── 2. Build column X-boundaries from header row ─────────────────────────
    col_boundaries = []  # list of (x_left, x_right, json_key)
    
    # Define a fuzzy matching logic or regex to assign headers
    key_keywords = {
        "party_name": ["فريق"],
        "property_number": ["رقم", "عقار"],
        "section": ["قسم"],
        "block": ["بلوك"],
        "real_estate_district": ["منطقة", "عقارية"],
        "qaza": ["قضاء"],
        "num_shares": ["أسهم", "سهم"],
        "ownership_type": ["نوع", "ملكية"],
    }
    
    if header_row_items:
        for it in header_row_items:
            assigned_key = None
            for key, keywords in key_keywords.items():
                if any(kw in it["text"] for kw in keywords):
                    assigned_key = key
                    break
            if assigned_key:
                # Add large horizontal padding to ensure columns grab cells correctly
                col_boundaries.append((it["x_left"] - 40, it["x_right"] + 40, assigned_key))

    table_start_y = header_row_items[0]["y"] if header_row_items else (header_y if header_y is not None else 600)

    # ── 3. Extract header/person fields (above the table) ────────────────────
    header_items = [it for it in items if it["y"] < (header_y if header_y is not None else table_start_y) - 5]

    # Group header items into rows
    header_rows: list[list[dict]] = []
    if header_items:
        current = [header_items[0]]
        for it in header_items[1:]:
            if abs(it["y"] - current[0]["y"]) < 15:
                current.append(it)
            else:
                header_rows.append(sorted(current, key=lambda t: -t["x"]))  # right→left
                current = [it]
        header_rows.append(sorted(current, key=lambda t: -t["x"]))

    person: dict = {}
    request_number = None
    request_date   = None
    applicant_name = None

    for row in header_rows:
        # In RTL layout the label is printed first (rightmost), value follows to its left.
        # We scan each item; if it matches a known label we take the next item as the value.
        i = 0
        while i < len(row):
            txt = row[i]["text"]

            # Strip trailing colon if present
            clean = txt.rstrip(":").strip()

            if clean in _LABEL_MAP and i + 1 < len(row):
                value = row[i + 1]["text"].rstrip(":").strip()
                # Skip if the value looks like another label
                if value not in _LABEL_MAP:
                    person[_LABEL_MAP[clean]] = value
                    i += 2
                    continue

            # رقم الطلب
            if "رقم الطلب" in txt and i + 1 < len(row):
                request_number = row[i + 1]["text"].strip()
                i += 2
                continue
            # تاريخ الطلب
            if "تاريخ الطلب" in txt and i + 1 < len(row):
                request_date = row[i + 1]["text"].strip()
                i += 2
                continue
            # اسم المستدعي / الوكيل
            if "المستدعي" in txt or "الوكيل" in txt:
                if i + 1 < len(row):
                    applicant_name = row[i + 1]["text"].strip()
                    i += 2
                    continue

            i += 1

    # Also try full-text regex as fallback for request fields
    full_text = " ".join(it["text"] for it in items)
    if not request_number:
        m = re.search(r"رقم الطلب[:\s]+([\d٠-٩]+)", full_text)
        if m:
            request_number = m.group(1)
    if not request_date:
        m = re.search(r"تاريخ الطلب[:\s]+([\d٠-٩\-/]+)", full_text)
        if m:
            request_date = m.group(1)

    page_info = None
    m_page = re.search(r"(صفحة\s*[\d٠-٩]+\s*من\s*[\d٠-٩]+|[\d٠-٩]+\s*/\s*[\d٠-٩]+)", full_text)
    if m_page:
        page_info = m_page.group(1).strip()

    # Extract search scope (نطاق البحث / القضاء)
    search_scope = None
    for row in header_rows:
        for j, item in enumerate(row):
            if "القضاء" in item["text"] or "نطاق" in item["text"]:
                # Value is the next item in the row
                if j + 1 < len(row):
                    val = row[j + 1]["text"].rstrip(":").strip()
                    if val and "نطاق" not in val and "القضاء" not in val:
                        search_scope = val
                        break
        if search_scope:
            break
    # Regex fallback
    if not search_scope:
        m_scope = re.search(r"القضاء[:\s]+([^\d٠-٩\-/]+?)(?:\s|$)", full_text)
        if m_scope:
            search_scope = m_scope.group(1).strip().rstrip(":")

    # ── 4. Extract table body rows ────────────────────────────────────────────
    table_items = [it for it in items if it["y"] > table_start_y + 15]
    properties = _group_into_table_rows(table_items, col_boundaries)

    return {
        "request_number": request_number,
        "request_date": request_date,
        "applicant_name_raw": applicant_name,
        "page_info": page_info,
        "search_scope": search_scope,
        "person": person if person else {"first_name": None},
        "properties": properties,
        "extraction_notes": "Extracted with EasyOCR (local). Please review carefully.",
    }


def _group_into_table_rows(items: list[dict], col_boundaries: list) -> list[dict]:
    """
    Group text items into table rows by Y-proximity, then assign each cell to
    the correct column using col_boundaries (list of (x_left, x_right, key)).
    Falls back to positional ordering when no boundaries are available.
    """
    if not items:
        return []

    items_sorted = sorted(items, key=lambda t: t["y"])
    rows: list[list[dict]] = []
    current_row = [items_sorted[0]]

    for item in items_sorted[1:]:
        if abs(item["y"] - current_row[0]["y"]) < 14:
            current_row.append(item)
        else:
            rows.append(current_row)
            current_row = [item]
    if current_row:
        rows.append(current_row)

    properties = []
    key_order = [k for _, k in _COL_KEYS]

    for row_items in rows:
        texts = [c["text"] for c in row_items]
        joined = " ".join(texts)

        # Skip header row and single-word rows
        if any(h in joined for h in _TABLE_HEADERS):
            continue
        if len(row_items) < 2:
            continue

        if col_boundaries:
            # Assign each cell to a column by X-overlap with column boundaries
            prop: dict = {k: None for _, k in _COL_KEYS}
            for cell in row_items:
                best_key = None
                best_overlap = 0
                for x_left, x_right, key in col_boundaries:
                    # Compute horizontal overlap between cell and column boundary
                    overlap = min(cell["x_right"], x_right) - max(cell["x_left"], x_left)
                    if overlap > best_overlap:
                        best_overlap = overlap
                        best_key = key
                if best_key and best_overlap > 0:
                    # Concatenate if multiple cells map to same column
                    existing = prop.get(best_key)
                    prop[best_key] = (existing + " " + cell["text"]) if existing else cell["text"]
        else:
            # Fallback: positional right→left assignment
            cells = sorted(row_items, key=lambda t: -t["x"])
            prop = {key_order[i]: cells[i]["text"] if i < len(cells) else None
                    for i in range(len(key_order))}

        properties.append(prop)

    return properties


def _empty_result(note: str) -> dict:
    return {
        "request_number": None,
        "request_date": None,
        "applicant_name_raw": None,
        "page_info": None,
        "search_scope": None,
        "person": {"first_name": None},
        "properties": [],
        "extraction_notes": note,
    }


async def _extract_with_easyocr(image_path: str) -> dict:
    import asyncio

    full_path = _resolve_path(image_path)

    def _call():
        reader = _get_easyocr_reader()
        results = reader.readtext(full_path)
        return _parse_easyocr_results(results)

    return await asyncio.get_event_loop().run_in_executor(None, _call)


# ─── Public API ───────────────────────────────────────────────────

async def extract_document(image_path: str, provider: str = "") -> dict:
    """
    Extract structured data from a document image using the specified provider.
    Falls back to the default provider if none specified.
    """
    if not provider:
        provider = get_default_provider()

    if provider == "claude":
        if not ANTHROPIC_API_KEY:
            raise ValueError("ANTHROPIC_API_KEY not set")
        return await _extract_with_claude(image_path)
    elif provider == "gemini":
        if not GEMINI_API_KEY:
            raise ValueError("GEMINI_API_KEY not set")
        return await _extract_with_gemini(image_path)
    elif provider == "easyocr":
        return await _extract_with_easyocr(image_path)
    else:
        raise ValueError(f"Unknown provider: {provider}")
