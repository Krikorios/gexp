import json
import asyncio
import re

from fastapi import APIRouter, HTTPException, Request, status
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from config import UPLOAD_DIR
from database.connection import get_db
from services.extractor import (
    extract_document,
    get_available_providers,
    get_default_provider,
    verify_page_correlation,
)
from services.search_service import normalize_arabic, _normalize_scope

router = APIRouter()
templates = Jinja2Templates(directory="templates")

REVIEWABLE_DOCUMENT_STATUSES = {"extracted", "confirmed", "error"}

# Map Arabic-Indic digits to Western
_ARABIC_DIGIT_MAP = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")


def _is_subsequent_page(page_info: str) -> bool:
    """Return True if page_info indicates this is page 2 or later.
    Handles formats like: 'صفحة ٢ من 5', '2 / 3', 'صفحة 2 من 3', '٢ من ٣'
    """
    if not page_info:
        return False
    # Normalize Arabic-Indic digits to Western
    normalized = page_info.translate(_ARABIC_DIGIT_MAP)
    # Find the first number (the page number)
    m = re.search(r"(\d+)", normalized)
    if m:
        page_num = int(m.group(1))
        return page_num > 1
    return False


def _find_page1_candidate(conn, doc: dict) -> tuple[bool, dict | None]:
    """Return whether the doc looks like a later page and the best page-1 candidate."""
    is_subsequent_page = False
    sibling_doc = None

    if doc.get("pdf_group_id") and (doc.get("page_number") or 0) > 1:
        is_subsequent_page = True
        sibling_doc = conn.execute(
            """SELECT * FROM documents
               WHERE pdf_group_id=? AND page_number=1""",
            (doc["pdf_group_id"],),
        ).fetchone()

    if not sibling_doc:
        page_info_str = doc.get("page_info") or ""
        req_num = (doc.get("request_number") or "").strip()
        doc_scope = (doc.get("search_scope") or "").strip()
        if req_num and _is_subsequent_page(page_info_str):
            is_subsequent_page = True
            if doc_scope:
                sibling_doc = conn.execute(
                    """SELECT * FROM documents
                       WHERE request_number=? AND search_scope=? AND id != ?
                       ORDER BY CASE WHEN page_number=1 THEN 0 ELSE 1 END,
                                CASE WHEN person_id IS NOT NULL THEN 0 ELSE 1 END,
                                id
                       LIMIT 1""",
                    (req_num, doc_scope, doc["id"]),
                ).fetchone()

    return is_subsequent_page, dict(sibling_doc) if sibling_doc else None


def _build_correlation_context(doc: dict) -> dict:
    person = doc.get("person") or {}
    return {
        "document_id": doc.get("id"),
        "request_number": doc.get("request_number"),
        "request_date": doc.get("request_date"),
        "page_info": doc.get("page_info"),
        "page_number": doc.get("page_number"),
        "search_scope": doc.get("search_scope"),
        "applicant_name_raw": doc.get("applicant_name_raw"),
        "person": {
            "first_name": person.get("first_name"),
            "father_name": person.get("father_name"),
            "mother_name": person.get("mother_name"),
            "family_name": person.get("family_name"),
            "registry_number": person.get("registry_number"),
            "registry_place": person.get("registry_place"),
        },
    }


def _get_document(doc_id: int) -> dict | None:
    with get_db() as conn:
        row = conn.execute("SELECT * FROM documents WHERE id=?", (doc_id,)).fetchone()
        if not row:
            return None
        doc = dict(row)
        props = conn.execute(
            "SELECT * FROM properties WHERE document_id=? ORDER BY row_order",
            (doc_id,),
        ).fetchall()
        doc["properties"] = [dict(p) for p in props]

        if doc["person_id"]:
            person = conn.execute(
                "SELECT * FROM persons WHERE id=?", (doc["person_id"],)
            ).fetchone()
            doc["person"] = dict(person) if person else {}
        elif doc["raw_extraction_json"]:
            try:
                extracted = json.loads(doc["raw_extraction_json"])
                doc["person"] = extracted.get("person", {})
            except Exception:
                doc["person"] = {}
        else:
            doc["person"] = {}

        # For multi-page documents (page 2+), inherit person info & doc fields from page 1.
        # Works for both PDF splits (pdf_group_id) and individually uploaded images (via request_number + page_info).
        doc["inherited_from_page1"] = False
        doc["page1_person_id"] = None
        doc["page1_doc_id"] = None

        current_first = (doc["person"].get("first_name") or "").strip()
        is_subsequent_page, sibling_doc = _find_page1_candidate(conn, doc)

        if sibling_doc:
            doc["page1_doc_id"] = sibling_doc.get("id")
            if sibling_doc.get("person_id"):
                doc["page1_person_id"] = sibling_doc["person_id"]

            if sibling_doc.get("person_id") and not current_first:
                person = conn.execute(
                    "SELECT * FROM persons WHERE id=?", (sibling_doc["person_id"],)
                ).fetchone()
                if person:
                    doc["person"] = dict(person)
                    doc["inherited_from_page1"] = True
            elif sibling_doc.get("raw_extraction_json") and not current_first:
                try:
                    sib_extracted = json.loads(sibling_doc["raw_extraction_json"])
                    sib_person = sib_extracted.get("person", {})
                    if (sib_person.get("first_name") or "").strip():
                        doc["person"] = sib_person
                        doc["inherited_from_page1"] = True
                except Exception:
                    pass

            # Inherit document-level fields if missing
            inherit_fields = [
                "request_number", "request_date", "search_scope",
                "request_purpose", "data_valid_until", "registry_office",
                "applicant_name_raw",
            ]
            for field in inherit_fields:
                if not (doc.get(field) or "").strip() and (sibling_doc.get(field) or "").strip():
                    doc[field] = sibling_doc[field]

        doc["is_subsequent_page"] = is_subsequent_page

    return doc


def _parse_optional_bool(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)

    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off", ""}:
        return False
    return None


def _get_merge_candidates(
    first_name: str,
    father_name: str = "",
    family_name: str = "",
    search_scope: str = "",
    registry_number: str = "",
) -> list[dict]:
    if not first_name:
        return []

    first_norm = normalize_arabic(first_name.strip())
    family_norm = normalize_arabic(family_name.strip()) if family_name else ""
    normalized_scope = _normalize_scope(search_scope)
    normalized_registry = (registry_number or "").strip()

    with get_db() as conn:
        if family_norm:
            rows = conn.execute(
                """SELECT p.*,
                          COUNT(DISTINCT pr.id) AS property_count,
                          COUNT(DISTINCT d.id) AS document_count,
                          GROUP_CONCAT(DISTINCT NULLIF(TRIM(d.search_scope), '')) AS search_scopes
                   FROM persons p
                   LEFT JOIN documents d ON d.person_id = p.id
                   LEFT JOIN properties pr ON pr.person_id = p.id
                   WHERE p.first_name_norm = ? AND p.family_name_norm = ?
                   GROUP BY p.id""",
                (first_norm, family_norm),
            ).fetchall()
        else:
            rows = conn.execute(
                """SELECT p.*,
                          COUNT(DISTINCT pr.id) AS property_count,
                          COUNT(DISTINCT d.id) AS document_count,
                          GROUP_CONCAT(DISTINCT NULLIF(TRIM(d.search_scope), '')) AS search_scopes
                   FROM persons p
                   LEFT JOIN documents d ON d.person_id = p.id
                   LEFT JOIN properties pr ON pr.person_id = p.id
                   WHERE p.first_name_norm = ?
                   GROUP BY p.id""",
                (first_norm,),
            ).fetchall()

    matches = []
    normalized_father = normalize_arabic(father_name.strip()) if father_name else ""
    for row in rows:
        person = dict(row)
        if normalized_father and person.get("father_name"):
            if normalize_arabic(person["father_name"]) != normalized_father:
                continue
        
        scope_values = [_normalize_scope(s) or s.strip() for s in (person.get("search_scopes") or "").split(",") if s.strip()]
        
        same_scope = False
        if not normalized_scope:
            same_scope = True
        elif not scope_values:
            same_scope = True
        elif normalized_scope == "كل لبنان" or "كل لبنان" in scope_values or _normalize_scope("كل لبنان") in scope_values:
            same_scope = True
        elif normalized_scope in scope_values:
            same_scope = True

        registry_match = bool(
            normalized_registry
            and person.get("registry_number")
            and person["registry_number"].strip() == normalized_registry
        )
        person["search_scope_list"] = [s.strip() for s in (person.get("search_scopes") or "").split(",") if s.strip()]
        person["same_scope"] = same_scope
        person["registry_match"] = registry_match
        person["merge_allowed"] = same_scope or registry_match
        matches.append(person)

    matches.sort(
        key=lambda person: (
            0 if person.get("merge_allowed") else 1,
            0 if person.get("same_scope") else 1,
            0 if person.get("registry_match") else 1,
            -(person.get("document_count") or 0),
            -(person.get("property_count") or 0),
            person.get("id") or 0,
        )
    )
    return matches


@router.get("/review/next")
async def review_next():
    with get_db() as conn:
        row = conn.execute(
            "SELECT id FROM documents WHERE status='extracted' ORDER BY id LIMIT 1"
        ).fetchone()
    if row:
        return RedirectResponse(f"/review/{row['id']}", status_code=302)

    with get_db() as conn:
        pending = conn.execute(
            "SELECT COUNT(*) AS n FROM documents WHERE status='pending'"
        ).fetchone()["n"]

    if pending:
        return HTMLResponse(
            "<html><body style='font-family:sans-serif;text-align:center;padding:3rem'>"
            "<h2>جارٍ معالجة الوثائق...</h2>"
            "<p>أعد التحميل بعد قليل أو اذهب إلى <a href='/documents'>قائمة الوثائق</a>.</p>"
            "</body></html>"
        )

    return HTMLResponse(
        "<html><body style='font-family:sans-serif;text-align:center;padding:3rem'>"
        "<h2>تمت مراجعة جميع الوثائق!</h2>"
        "<p><a href='/search'>البحث في قاعدة البيانات</a> | <a href='/'>رفع المزيد</a></p>"
        "</body></html>"
    )


@router.get("/review/{doc_id}")
async def review_document(request: Request, doc_id: int, wait: int = 0):
    doc = _get_document(doc_id)
    if not doc:
        return HTMLResponse("Document not found", status_code=404)

    if doc["status"] == "pending" and wait:
        return HTMLResponse(
            f"""<html><head><meta http-equiv="refresh" content="3;url=/review/{doc_id}?wait=1">
            <title>Processing...</title></head>
            <body style='font-family:sans-serif;text-align:center;padding:3rem'>
            <h2>جارٍ قراءة الوثيقة...</h2>
            <p>ستتحدث هذه الصفحة تلقائياً.</p>
            <p><a href="/review/{doc_id}">تحديث الآن</a></p>
            </body></html>"""
        )

    return templates.TemplateResponse(
        request,
        "review.html",
        {
            "doc": doc,
            "upload_dir": "/uploads",
            "providers": get_available_providers(),
            "current_provider": doc.get("provider") or get_default_provider(),
            "ai_verification_available": any(
                provider["id"] in {"claude", "gemini"} for provider in get_available_providers()
            ),
        },
    )


@router.post("/review/{doc_id}/verify-correlation")
async def review_verify_correlation(doc_id: int):
    with get_db() as conn:
        row = conn.execute("SELECT * FROM documents WHERE id=?", (doc_id,)).fetchone()
        if not row:
            return JSONResponse({"error": "Document not found"}, status_code=404)

        current_doc = _get_document(doc_id)
        if not current_doc:
            return JSONResponse({"error": "Document not found"}, status_code=404)

        is_subsequent_page, sibling_doc = _find_page1_candidate(conn, dict(row))
        if not is_subsequent_page:
            return JSONResponse(
                {"error": "This document is not identified as page 2 or later."},
                status_code=400,
            )
        if not sibling_doc:
            return JSONResponse(
                {"error": "No page 1 candidate was found for AI verification."},
                status_code=404,
            )

        candidate_doc = _get_document(sibling_doc["id"])
        if not candidate_doc:
            return JSONResponse({"error": "Candidate page not found"}, status_code=404)

    try:
        result = await verify_page_correlation(
            current_doc["image_path"],
            candidate_doc["image_path"],
            _build_correlation_context(current_doc),
            _build_correlation_context(candidate_doc),
        )
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)

    return JSONResponse(
        {
            "ok": True,
            "current_doc_id": current_doc["id"],
            "candidate_doc_id": candidate_doc["id"],
            **result,
        }
    )


@router.get("/api/check-duplicate")
async def check_duplicate(
    first_name: str = "",
    father_name: str = "",
    family_name: str = "",
    search_scope: str = "",
    registry_number: str = "",
):
    """Check if a person with similar name already exists. Called via AJAX from review page."""
    if not first_name:
        return JSONResponse({"matches": []})

    matches = []
    for candidate in _get_merge_candidates(
        first_name,
        father_name,
        family_name,
        search_scope,
        registry_number,
    ):
        matches.append({
            "id": candidate["id"],
            "first_name": candidate["first_name"],
            "father_name": candidate.get("father_name"),
            "family_name": candidate.get("family_name"),
            "family_origin": candidate.get("family_origin"),
            "property_count": candidate.get("property_count", 0),
            "document_count": candidate.get("document_count", 0),
            "search_scopes": candidate.get("search_scope_list", []),
            "same_scope": candidate.get("same_scope", False),
            "registry_match": candidate.get("registry_match", False),
            "merge_allowed": candidate.get("merge_allowed", False),
        })

    return JSONResponse({"matches": matches})


@router.post("/confirm/{doc_id}")
async def confirm_document(doc_id: int, request: Request):
    body = await request.json()

    person_data = body.get("person", {})
    properties_data = body.get("properties", [])
    merge_person_id = body.get("merge_person_id")  # If user chose to merge

    if not isinstance(person_data, dict):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid person payload")
    if not isinstance(properties_data, list):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid properties payload")

    first_name = (person_data.get("first_name") or "").strip()

    registry_number = (person_data.get("registry_number") or "").strip() or None

    with get_db() as conn:
        current_doc = conn.execute(
            "SELECT * FROM documents WHERE id=?",
            (doc_id,),
        ).fetchone()
        if not current_doc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Document not found")

        current_doc = dict(current_doc)

        # For multi-page documents (page 2+), resolve sibling document's person
        # Works via pdf_group_id OR request_number+search_scope + page_info
        page1_person_id = None
        is_subsequent, sibling = _find_page1_candidate(conn, current_doc)
        if sibling and sibling.get("person_id"):
            page1_person_id = sibling["person_id"]

        # Inherit person fields from sibling if not provided
        if page1_person_id and not first_name:
            sibling_person = conn.execute(
                "SELECT * FROM persons WHERE id=?", (page1_person_id,)
            ).fetchone()
            if sibling_person:
                first_name = sibling_person["first_name"] or ""
                person_data["first_name"] = first_name
                for field in ("father_name", "mother_name", "family_name",
                              "family_origin", "nationality", "birth_date",
                              "registry_number", "registry_place"):
                    if not (person_data.get(field) or "").strip() and sibling_person[field]:
                        person_data[field] = sibling_person[field]
                registry_number = (person_data.get("registry_number") or "").strip() or None

        # For subsequent pages, also inherit search_scope from the sibling doc
        # so that merge validation can work (it requires same_scope)
        if page1_person_id and is_subsequent:
            sibling_scope_row = conn.execute(
                "SELECT search_scope FROM documents WHERE person_id=? AND search_scope IS NOT NULL AND TRIM(search_scope) != '' LIMIT 1",
                (page1_person_id,),
            ).fetchone()
            if sibling_scope_row:
                _inherited_scope = (sibling_scope_row["search_scope"] or "").strip()
                # Store for later use if body didn't provide search_scope
                if not (body.get("search_scope") or "").strip() and _inherited_scope:
                    body["search_scope"] = _inherited_scope

        if not first_name:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="First name is required")
        if current_doc["status"] == "pending":
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Document is still being processed",
            )
        if current_doc["status"] not in REVIEWABLE_DOCUMENT_STATUSES:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Document status '{current_doc['status']}' cannot be confirmed",
            )

        def _body_or_existing(field_name: str):
            if field_name in body:
                value = body.get(field_name)
                if isinstance(value, str):
                    value = value.strip()
                return value or None
            return current_doc.get(field_name)

        request_number = _body_or_existing("request_number")
        request_date = _body_or_existing("request_date")
        page_info = _body_or_existing("page_info")
        search_scope = _body_or_existing("search_scope")
        request_purpose = _body_or_existing("request_purpose")
        data_valid_until = _body_or_existing("data_valid_until")
        registry_office = _body_or_existing("registry_office")
        normalized_search_scope = _normalize_scope(search_scope) or ""

        if "owns_properties" in body:
            owns_properties = _parse_optional_bool(body.get("owns_properties"))
        else:
            owns_properties = current_doc.get("owns_properties")

        if "declared_property_count" in body:
            declared_property_count = body.get("declared_property_count")
            if declared_property_count is not None and str(declared_property_count).strip() != "":
                try:
                    declared_property_count = int(declared_property_count)
                except (TypeError, ValueError):
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="Invalid declared property count",
                    )
            else:
                declared_property_count = None
        else:
            declared_property_count = current_doc.get("declared_property_count")

        person_id = None

        # page1_person_id already resolved above for multi-page PDFs

        # Option 1: User explicitly chose to merge with an existing person
        if merge_person_id:
            try:
                person_id = int(merge_person_id)
            except (TypeError, ValueError):
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid merge target")

            existing_person = conn.execute(
                "SELECT * FROM persons WHERE id=?",
                (person_id,),
            ).fetchone()
            if not existing_person:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Merge target not found")

            allowed_merge_ids = {candidate["id"] for candidate in _get_merge_candidates(
                first_name,
                person_data.get("father_name") or "",
                person_data.get("family_name") or "",
                normalized_search_scope,
                registry_number or "",
            ) if candidate.get("merge_allowed")}

            if person_id not in allowed_merge_ids:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Merge target is not a verified match for this search scope",
                )

            # Update person info with latest data
            conn.execute(
                """UPDATE persons SET
                   first_name=?, father_name=?, mother_name=?,
                   family_name=?, family_origin=?, nationality=?,
                   birth_date=?, registry_place=?,
                   registry_number=COALESCE(registry_number, ?),
                   first_name_norm=?, family_name_norm=?,
                   updated_at=CURRENT_TIMESTAMP
                   WHERE id=?""",
                (
                    first_name,
                    person_data.get("father_name"),
                    person_data.get("mother_name"),
                    person_data.get("family_name"),
                    person_data.get("family_origin"),
                    person_data.get("nationality"),
                    person_data.get("birth_date"),
                    person_data.get("registry_place"),
                    registry_number,
                    normalize_arabic(first_name),
                    normalize_arabic(person_data.get("family_name") or ""),
                    person_id,
                ),
            )

        # Option 2: Auto-merge by registry_number
        if not person_id and registry_number:
            existing = conn.execute(
                "SELECT id FROM persons WHERE registry_number=?", (registry_number,)
            ).fetchone()
            if existing:
                person_id = existing["id"]
                conn.execute(
                    """UPDATE persons SET
                       first_name=?, father_name=?, mother_name=?,
                       family_name=?, family_origin=?, nationality=?,
                       birth_date=?, registry_place=?,
                       first_name_norm=?, family_name_norm=?,
                       updated_at=CURRENT_TIMESTAMP
                       WHERE id=?""",
                    (
                        first_name,
                        person_data.get("father_name"),
                        person_data.get("mother_name"),
                        person_data.get("family_name"),
                        person_data.get("family_origin"),
                        person_data.get("nationality"),
                        person_data.get("birth_date"),
                        person_data.get("registry_place"),
                        normalize_arabic(first_name),
                        normalize_arabic(person_data.get("family_name") or ""),
                        person_id,
                    ),
                )

        # Option 2b: Auto-link to page 1's person for multi-page PDFs
        if not person_id and page1_person_id:
            person_id = page1_person_id

        # Option 3: Create new person
        if not person_id:
            existing_person = None
            if current_doc.get("person_id"):
                existing_person = conn.execute(
                    "SELECT id FROM persons WHERE id=?",
                    (current_doc["person_id"],),
                ).fetchone()

            if existing_person:
                person_id = existing_person["id"]
                conn.execute(
                    """UPDATE persons SET
                       first_name=?, father_name=?, mother_name=?,
                       family_name=?, family_origin=?, nationality=?,
                       birth_date=?, registry_number=?, registry_place=?,
                       first_name_norm=?, family_name_norm=?,
                       updated_at=CURRENT_TIMESTAMP
                       WHERE id=?""",
                    (
                        first_name,
                        person_data.get("father_name"),
                        person_data.get("mother_name"),
                        person_data.get("family_name"),
                        person_data.get("family_origin"),
                        person_data.get("nationality"),
                        person_data.get("birth_date"),
                        registry_number,
                        person_data.get("registry_place"),
                        normalize_arabic(first_name),
                        normalize_arabic(person_data.get("family_name") or ""),
                        person_id,
                    ),
                )
            else:
                cursor = conn.execute(
                    """INSERT INTO persons
                       (first_name, father_name, mother_name, family_name, family_origin,
                        nationality, birth_date, registry_number, registry_place,
                        first_name_norm, family_name_norm)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        first_name,
                        person_data.get("father_name"),
                        person_data.get("mother_name"),
                        person_data.get("family_name"),
                        person_data.get("family_origin"),
                        person_data.get("nationality"),
                        person_data.get("birth_date"),
                        registry_number,
                        person_data.get("registry_place"),
                        normalize_arabic(first_name),
                        normalize_arabic(person_data.get("family_name") or ""),
                    ),
                )
                person_id = cursor.lastrowid

        # Replace properties for this document
        conn.execute("DELETE FROM properties WHERE document_id=?", (doc_id,))
        for i, prop in enumerate(properties_data):
            if not isinstance(prop, dict):
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid property payload")
            conn.execute(
                """INSERT INTO properties
                   (document_id, person_id, row_order, party_name, property_number,
                    section, block, real_estate_district, qaza, num_shares, ownership_type)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    doc_id, person_id, i,
                    prop.get("party_name"),
                    prop.get("property_number"),
                    prop.get("section"),
                    prop.get("block"),
                    prop.get("real_estate_district"),
                    prop.get("qaza"),
                    prop.get("num_shares"),
                    prop.get("ownership_type"),
                ),
            )

        conn.execute(
            """UPDATE documents SET status='confirmed', person_id=?,
               request_number=?, request_date=?, page_info=?, search_scope=?,
               request_purpose=?, data_valid_until=?, registry_office=?,
               owns_properties=?, declared_property_count=?,
               updated_at=CURRENT_TIMESTAMP WHERE id=?""",
            (person_id, request_number, request_date, page_info, search_scope,
             request_purpose, data_valid_until, registry_office,
             owns_properties, declared_property_count, doc_id),
        )

    return JSONResponse({"ok": True, "person_id": person_id, "next": "/review/next"})


@router.post("/extract/{doc_id}")
async def retrigger_extraction(doc_id: int, provider: str = ""):
    """Re-run extraction for a document and reset the extracted state first."""
    with get_db() as conn:
        doc = conn.execute(
            "SELECT image_path, provider FROM documents WHERE id=?", (doc_id,)
        ).fetchone()
    if not doc:
        return JSONResponse({"error": "not found"}, status_code=404)

    use_provider = provider or doc["provider"] or get_default_provider()

    with get_db() as conn:
        conn.execute(
            """UPDATE documents
               SET status='pending',
                   person_id=NULL,
                   request_number=NULL,
                   request_date=NULL,
                   applicant_name_raw=NULL,
                   request_purpose=NULL,
                   data_valid_until=NULL,
                   registry_office=NULL,
                   owns_properties=NULL,
                   declared_property_count=NULL,
                   page_info=NULL,
                   search_scope=NULL,
                   raw_extraction_json=NULL,
                   extraction_error=NULL,
                   provider=?,
                   updated_at=CURRENT_TIMESTAMP
               WHERE id=?""",
            (use_provider, doc_id),
        )
        conn.execute("DELETE FROM properties WHERE document_id=?", (doc_id,))

    from routers.upload import _extract_and_save
    asyncio.create_task(_extract_and_save(doc_id, doc["image_path"], use_provider))
    return JSONResponse({"ok": True, "message": "Extraction started"})
