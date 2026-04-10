import json
import asyncio

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from config import UPLOAD_DIR
from database.connection import get_db
from services.extractor import extract_document
from services.search_service import normalize_arabic

router = APIRouter()
templates = Jinja2Templates(directory="templates")


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

    return doc


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
        request, "review.html", {"doc": doc, "upload_dir": "/uploads"}
    )


@router.get("/api/check-duplicate")
async def check_duplicate(
    first_name: str = "",
    father_name: str = "",
    family_name: str = "",
):
    """Check if a person with similar name already exists. Called via AJAX from review page."""
    if not first_name:
        return JSONResponse({"matches": []})

    first_norm = normalize_arabic(first_name.strip())
    family_norm = normalize_arabic(family_name.strip()) if family_name else ""

    with get_db() as conn:
        if family_norm:
            rows = conn.execute(
                """SELECT p.*, COUNT(DISTINCT pr.id) AS property_count
                   FROM persons p
                   LEFT JOIN properties pr ON pr.person_id = p.id
                   WHERE p.first_name_norm = ? AND p.family_name_norm = ?
                   GROUP BY p.id""",
                (first_norm, family_norm),
            ).fetchall()
        else:
            rows = conn.execute(
                """SELECT p.*, COUNT(DISTINCT pr.id) AS property_count
                   FROM persons p
                   LEFT JOIN properties pr ON pr.person_id = p.id
                   WHERE p.first_name_norm = ?
                   GROUP BY p.id""",
                (first_norm,),
            ).fetchall()

        # Further filter by father_name if provided
        matches = []
        for r in rows:
            d = dict(r)
            if father_name and d.get("father_name"):
                if normalize_arabic(father_name.strip()) != normalize_arabic(d["father_name"]):
                    continue
            matches.append({
                "id": d["id"],
                "first_name": d["first_name"],
                "father_name": d.get("father_name"),
                "family_name": d.get("family_name"),
                "family_origin": d.get("family_origin"),
                "property_count": d["property_count"],
            })

    return JSONResponse({"matches": matches})


@router.post("/confirm/{doc_id}")
async def confirm_document(doc_id: int, request: Request):
    body = await request.json()

    person_data = body.get("person", {})
    properties_data = body.get("properties", [])
    merge_person_id = body.get("merge_person_id")  # If user chose to merge

    first_name = (person_data.get("first_name") or "").strip()
    registry_number = (person_data.get("registry_number") or "").strip() or None
    
    # We should update document fields as well since user might have edited them
    request_number = (body.get("request_number") or "").strip() or None
    request_date = (body.get("request_date") or "").strip() or None
    page_info = (body.get("page_info") or "").strip() or None
    search_scope = (body.get("search_scope") or "").strip() or None
    request_purpose = (body.get("request_purpose") or "").strip() or None
    data_valid_until = (body.get("data_valid_until") or "").strip() or None
    registry_office = (body.get("registry_office") or "").strip() or None
    owns_properties = body.get("owns_properties")
    if owns_properties is not None:
        owns_properties = bool(owns_properties)
    declared_property_count = body.get("declared_property_count")
    if declared_property_count is not None:
        try:
            declared_property_count = int(declared_property_count)
        except ValueError:
            declared_property_count = None


    with get_db() as conn:
        person_id = None

        # Option 1: User explicitly chose to merge with an existing person
        if merge_person_id:
            person_id = int(merge_person_id)
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

        # Option 3: Create new person
        if not person_id:
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
    """Re-run extraction for a document (retry after error)."""
    with get_db() as conn:
        doc = conn.execute(
            "SELECT image_path, provider FROM documents WHERE id=?", (doc_id,)
        ).fetchone()
    if not doc:
        return JSONResponse({"error": "not found"}, status_code=404)

    with get_db() as conn:
        conn.execute(
            "UPDATE documents SET status='pending', extraction_error=NULL WHERE id=?",
            (doc_id,),
        )
        conn.execute("DELETE FROM properties WHERE document_id=?", (doc_id,))

    from routers.upload import _extract_and_save
    use_provider = provider or doc["provider"] or ""
    asyncio.create_task(_extract_and_save(doc_id, doc["image_path"], use_provider))
    return JSONResponse({"ok": True, "message": "Extraction started"})
