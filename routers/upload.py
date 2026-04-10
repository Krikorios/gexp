import asyncio
import json
import uuid
from datetime import date
from pathlib import Path

from fastapi import APIRouter, File, Form, Request, UploadFile
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates

from config import UPLOAD_DIR
from database.connection import get_db
from services.extractor import extract_document, get_available_providers, get_default_provider
from services.pdf_handler import pdf_to_images

router = APIRouter()
templates = Jinja2Templates(directory="templates")

ALLOWED_IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp"}
ALLOWED_PDF_EXTS = {".pdf"}
ALLOWED_EXTENSIONS = ALLOWED_IMAGE_EXTS | ALLOWED_PDF_EXTS


def _save_image(file_bytes: bytes, original_name: str) -> str:
    """Save image to uploads/{date}/{uuid}_{name} and return relative path."""
    today = date.today().isoformat()
    dest_dir = Path(UPLOAD_DIR) / today
    dest_dir.mkdir(parents=True, exist_ok=True)
    suffix = Path(original_name).suffix.lower() or ".jpg"
    filename = f"{uuid.uuid4().hex}{suffix}"
    dest = dest_dir / filename
    dest.write_bytes(file_bytes)
    return str(Path(today) / filename)


async def _extract_and_save(doc_id: int, image_path: str, provider: str = ""):
    """Background task: call extractor, parse result, update DB."""
    try:
        data = await extract_document(image_path, provider=provider)
        raw_json = json.dumps(data, ensure_ascii=False)

        with get_db() as conn:
            conn.execute(
                """UPDATE documents SET status='extracted',
                   raw_extraction_json=?,
                   request_number=?,
                   request_date=?,
                   applicant_name_raw=?,
                   request_purpose=?,
                   data_valid_until=?,
                   registry_office=?,
                   owns_properties=?,
                   declared_property_count=?,
                   page_info=?,
                   search_scope=?,
                   updated_at=CURRENT_TIMESTAMP
                   WHERE id=?""",
                (
                    raw_json,
                    data.get("request_number"),
                    data.get("request_date"),
                    data.get("applicant_name_raw"),
                    data.get("request_purpose"),
                    data.get("data_valid_until"),
                    data.get("registry_office"),
                    data.get("owns_properties"),
                    data.get("declared_property_count"),
                    data.get("page_info"),
                    data.get("search_scope"),
                    doc_id,
                ),
            )

            for i, prop in enumerate(data.get("properties", [])):
                conn.execute(
                    """INSERT INTO properties
                       (document_id, row_order, party_name, property_number,
                        section, block, real_estate_district, qaza, num_shares, ownership_type)
                       VALUES (?,?,?,?,?,?,?,?,?,?)""",
                    (
                        doc_id, i,
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

    except Exception as e:
        with get_db() as conn:
            conn.execute(
                """UPDATE documents SET status='error', extraction_error=?,
                   updated_at=CURRENT_TIMESTAMP WHERE id=?""",
                (str(e), doc_id),
            )


@router.get("/")
async def upload_page(request: Request):
    with get_db() as conn:
        stats = conn.execute(
            """SELECT
               COUNT(*) AS total,
               SUM(CASE WHEN status='confirmed' THEN 1 ELSE 0 END) AS confirmed,
               SUM(CASE WHEN status='extracted' THEN 1 ELSE 0 END) AS pending_review,
               SUM(CASE WHEN status='error' THEN 1 ELSE 0 END) AS errors
            FROM documents"""
        ).fetchone()
    return templates.TemplateResponse(
        request, "index.html", {
            "stats": dict(stats) if stats else {},
            "providers": get_available_providers(),
            "default_provider": get_default_provider(),
        }
    )


@router.post("/upload")
async def upload_files(
    request: Request,
    files: list[UploadFile] = File(...),
    provider: str = Form(""),
):
    if not provider:
        provider = get_default_provider()

    doc_ids = []

    for upload in files:
        suffix = Path(upload.filename).suffix.lower()
        if suffix not in ALLOWED_EXTENSIONS:
            continue

        file_bytes = await upload.read()

        if suffix in ALLOWED_PDF_EXTS:
            # PDF: split into per-page images
            pages = pdf_to_images(file_bytes, upload.filename)
            for page_info in pages:
                with get_db() as conn:
                    cursor = conn.execute(
                        """INSERT INTO documents
                           (image_path, status, provider, pdf_group_id, page_number)
                           VALUES (?, 'pending', ?, ?, ?)""",
                        (
                            page_info["image_path"],
                            provider,
                            page_info["pdf_group_id"],
                            page_info["page_number"],
                        ),
                    )
                    doc_ids.append((cursor.lastrowid, page_info["image_path"]))
        else:
            # Image file
            rel_path = _save_image(file_bytes, upload.filename)
            with get_db() as conn:
                cursor = conn.execute(
                    "INSERT INTO documents (image_path, status, provider) VALUES (?, 'pending', ?)",
                    (rel_path, provider),
                )
                doc_ids.append((cursor.lastrowid, rel_path))

    # Fire background extractions
    for doc_id, rel_path in doc_ids:
        asyncio.create_task(_extract_and_save(doc_id, rel_path, provider))

    if len(doc_ids) == 1:
        return RedirectResponse(f"/review/{doc_ids[0][0]}?wait=1", status_code=303)
    return RedirectResponse("/documents?uploaded=1", status_code=303)
