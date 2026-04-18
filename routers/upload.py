import asyncio
import hashlib
import json
import uuid
from datetime import date
from pathlib import Path

from fastapi import APIRouter, File, Form, Request, UploadFile
from fastapi.responses import JSONResponse, RedirectResponse
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


def _hash_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _hash_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _find_duplicate(conn, image_hash: str) -> dict | None:
    """Return an existing non-staged document sharing the same image hash."""
    row = conn.execute(
        """SELECT id, status, image_path, person_id, request_number
           FROM documents
           WHERE image_hash=? AND status != 'staged'
           ORDER BY id LIMIT 1""",
        (image_hash,),
    ).fetchone()
    return dict(row) if row else None


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

            # Flag logical duplicates: same request_number + search_scope + page_number
            req_num = (data.get("request_number") or "").strip()
            scope = (data.get("search_scope") or "").strip()
            page_info = (data.get("page_info") or "").strip()
            if req_num:
                existing = conn.execute(
                    """SELECT id FROM documents
                       WHERE id != ? AND request_number=?
                         AND COALESCE(search_scope,'')=?
                         AND COALESCE(page_info,'')=?
                         AND status IN ('extracted','confirmed')
                       ORDER BY id LIMIT 1""",
                    (doc_id, req_num, scope, page_info),
                ).fetchone()
                if existing:
                    conn.execute(
                        "UPDATE documents SET duplicate_of=? WHERE id=?",
                        (existing["id"], doc_id),
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
            FROM documents WHERE status != 'staged'"""
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
    duplicates = []

    for upload in files:
        suffix = Path(upload.filename).suffix.lower()
        if suffix not in ALLOWED_EXTENSIONS:
            continue

        file_bytes = await upload.read()

        if suffix in ALLOWED_PDF_EXTS:
            # PDF: split into per-page images; hash each rendered page
            pages = pdf_to_images(file_bytes, upload.filename)
            for page_info in pages:
                page_path = Path(UPLOAD_DIR) / page_info["image_path"]
                page_hash = _hash_file(page_path)
                with get_db() as conn:
                    dup = _find_duplicate(conn, page_hash)
                    if dup:
                        # Discard the freshly rendered duplicate page
                        try:
                            page_path.unlink(missing_ok=True)
                        except OSError:
                            pass
                        duplicates.append({
                            "name": f"{upload.filename} (p{page_info['page_number']})",
                            "existing_id": dup["id"],
                            "status": dup["status"],
                        })
                        continue
                    cursor = conn.execute(
                        """INSERT INTO documents
                           (image_path, image_hash, status, provider, pdf_group_id, page_number)
                           VALUES (?, ?, 'pending', ?, ?, ?)""",
                        (
                            page_info["image_path"],
                            page_hash,
                            provider,
                            page_info["pdf_group_id"],
                            page_info["page_number"],
                        ),
                    )
                    doc_ids.append((cursor.lastrowid, page_info["image_path"]))
        else:
            # Image file
            image_hash = _hash_bytes(file_bytes)
            with get_db() as conn:
                dup = _find_duplicate(conn, image_hash)
                if dup:
                    duplicates.append({
                        "name": upload.filename,
                        "existing_id": dup["id"],
                        "status": dup["status"],
                    })
                    continue
                rel_path = _save_image(file_bytes, upload.filename)
                cursor = conn.execute(
                    "INSERT INTO documents (image_path, image_hash, status, provider) VALUES (?, ?, 'pending', ?)",
                    (rel_path, image_hash, provider),
                )
                doc_ids.append((cursor.lastrowid, rel_path))

    # Fire background extractions
    for doc_id, rel_path in doc_ids:
        asyncio.create_task(_extract_and_save(doc_id, rel_path, provider))

    if len(doc_ids) == 1 and not duplicates:
        return RedirectResponse(f"/review/{doc_ids[0][0]}?wait=1", status_code=303)

    query = "uploaded=1"
    if duplicates:
        query += f"&duplicates={len(duplicates)}"
        # Route user to the first duplicate so they can see which doc matched
        if not doc_ids:
            return RedirectResponse(f"/review/{duplicates[0]['existing_id']}", status_code=303)
    return RedirectResponse(f"/documents?{query}", status_code=303)


# ─── Two-step workflow: stage images, then process ─────────────

@router.post("/upload/stage")
async def stage_file(
    request: Request,
    files: list[UploadFile] = File(...),
):
    """Save uploaded images without triggering AI extraction."""
    staged = []
    duplicates = []
    for upload in files:
        suffix = Path(upload.filename).suffix.lower()
        if suffix not in ALLOWED_EXTENSIONS:
            continue

        file_bytes = await upload.read()

        if suffix in ALLOWED_PDF_EXTS:
            pages = pdf_to_images(file_bytes, upload.filename)
            for page_info in pages:
                page_path = Path(UPLOAD_DIR) / page_info["image_path"]
                page_hash = _hash_file(page_path)
                with get_db() as conn:
                    dup = _find_duplicate(conn, page_hash)
                    if dup:
                        try:
                            page_path.unlink(missing_ok=True)
                        except OSError:
                            pass
                        duplicates.append({
                            "name": f"{upload.filename} (p{page_info['page_number']})",
                            "existing_id": dup["id"],
                            "status": dup["status"],
                        })
                        continue
                    cursor = conn.execute(
                        """INSERT INTO documents
                           (image_path, image_hash, status, pdf_group_id, page_number)
                           VALUES (?, ?, 'staged', ?, ?)""",
                        (
                            page_info["image_path"],
                            page_hash,
                            page_info["pdf_group_id"],
                            page_info["page_number"],
                        ),
                    )
                    staged.append({
                        "id": cursor.lastrowid,
                        "image_path": page_info["image_path"],
                        "name": f"{upload.filename} (p{page_info['page_number']})",
                    })
        else:
            image_hash = _hash_bytes(file_bytes)
            with get_db() as conn:
                dup = _find_duplicate(conn, image_hash)
                if dup:
                    duplicates.append({
                        "name": upload.filename,
                        "existing_id": dup["id"],
                        "status": dup["status"],
                    })
                    continue
                rel_path = _save_image(file_bytes, upload.filename)
                cursor = conn.execute(
                    "INSERT INTO documents (image_path, image_hash, status) VALUES (?, ?, 'staged')",
                    (rel_path, image_hash),
                )
                staged.append({
                    "id": cursor.lastrowid,
                    "image_path": rel_path,
                    "name": upload.filename,
                })

    return JSONResponse({"staged": staged, "duplicates": duplicates})


@router.post("/upload/process-staged")
async def process_staged(
    request: Request,
    doc_ids: str = Form(...),
    provider: str = Form(""),
):
    """Trigger AI extraction for previously staged documents."""
    if not provider:
        provider = get_default_provider()

    ids = [int(x) for x in doc_ids.split(",") if x.strip().isdigit()]

    with get_db() as conn:
        rows = conn.execute(
            f"SELECT id, image_path FROM documents WHERE id IN ({','.join('?' * len(ids))}) AND status='staged'",
            ids,
        ).fetchall()
        for row in rows:
            conn.execute(
                "UPDATE documents SET status='pending', provider=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (provider, row["id"]),
            )

    for row in rows:
        asyncio.create_task(_extract_and_save(row["id"], row["image_path"], provider))

    if len(rows) == 1:
        return RedirectResponse(f"/review/{rows[0]['id']}?wait=1", status_code=303)
    return RedirectResponse("/documents?uploaded=1", status_code=303)


@router.delete("/upload/staged/{doc_id}")
async def remove_staged(doc_id: int):
    """Remove a single staged document before processing."""
    with get_db() as conn:
        row = conn.execute("SELECT image_path FROM documents WHERE id=? AND status='staged'", (doc_id,)).fetchone()
        if row:
            # Delete the file
            file_path = Path(UPLOAD_DIR) / row["image_path"]
            if file_path.exists():
                file_path.unlink()
            conn.execute("DELETE FROM documents WHERE id=?", (doc_id,))
            return JSONResponse({"ok": True})
    return JSONResponse({"ok": False}, status_code=404)
