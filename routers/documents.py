import os
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from config import UPLOAD_DIR
from database.connection import get_db
from services.thumbnails import get_or_create_thumbnail

router = APIRouter()
templates = Jinja2Templates(directory="templates")


@router.get("/thumbs/{rel_path:path}")
async def serve_thumbnail(rel_path: str, size: int = 320):
    """Serve a cached JPEG thumbnail for an uploaded image.
    Falls back to the original file if thumbnail generation fails."""
    # Clamp size to sensible values to prevent DoS
    if size not in (160, 240, 320, 480, 640):
        size = 320

    thumb = get_or_create_thumbnail(rel_path, size=size)
    if thumb is None:
        # Fallback: serve original if it exists
        base = Path(UPLOAD_DIR).resolve()
        full = (base / rel_path).resolve()
        try:
            full.relative_to(base)
        except ValueError:
            return JSONResponse({"error": "not found"}, status_code=404)
        if not full.is_file():
            return JSONResponse({"error": "not found"}, status_code=404)
        return FileResponse(
            full,
            headers={"Cache-Control": "public, max-age=604800"},
        )

    return FileResponse(
        thumb,
        media_type="image/jpeg",
        headers={"Cache-Control": "public, max-age=604800, immutable"},
    )

_DOC_LIST_COLUMNS = (
    "d.id, d.person_id, d.image_path, d.request_number, d.status, "
    "d.duplicate_of, d.extraction_error, d.created_at"
)

PAGE_SIZE = 50


@router.get("/documents")
async def document_queue(
    request: Request,
    status: str = "",
    uploaded: int = 0,
    duplicates: int = 0,
    page: int = 1,
):
    if page < 1:
        page = 1
    offset = (page - 1) * PAGE_SIZE

    with get_db() as conn:
        if status:
            rows = conn.execute(
                f"""SELECT {_DOC_LIST_COLUMNS}, p.first_name, p.family_name
                   FROM documents d
                   LEFT JOIN persons p ON p.id = d.person_id
                   WHERE d.status=?
                   ORDER BY d.id DESC
                   LIMIT ? OFFSET ?""",
                (status, PAGE_SIZE, offset),
            ).fetchall()
            total_filtered = conn.execute(
                "SELECT COUNT(*) AS n FROM documents WHERE status=?", (status,)
            ).fetchone()["n"]
        else:
            rows = conn.execute(
                f"""SELECT {_DOC_LIST_COLUMNS}, p.first_name, p.family_name
                   FROM documents d
                   LEFT JOIN persons p ON p.id = d.person_id
                   ORDER BY d.id DESC
                   LIMIT ? OFFSET ?""",
                (PAGE_SIZE, offset),
            ).fetchall()
            total_filtered = conn.execute(
                "SELECT COUNT(*) AS n FROM documents"
            ).fetchone()["n"]

        stats = conn.execute(
            """SELECT
               COUNT(*) AS total,
               SUM(CASE WHEN status='confirmed' THEN 1 ELSE 0 END) AS confirmed,
               SUM(CASE WHEN status='extracted' THEN 1 ELSE 0 END) AS pending_review,
               SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) AS processing,
               SUM(CASE WHEN status='error' THEN 1 ELSE 0 END) AS errors,
               SUM(CASE WHEN duplicate_of IS NOT NULL THEN 1 ELSE 0 END) AS duplicates
            FROM documents WHERE status != 'staged'"""
        ).fetchone()

    total_pages = max(1, (total_filtered + PAGE_SIZE - 1) // PAGE_SIZE)

    return templates.TemplateResponse(
        request,
        "documents.html",
        {
            "documents": [dict(r) for r in rows],
            "stats": dict(stats) if stats else {},
            "current_status": status,
            "uploaded": uploaded,
            "duplicates_skipped": duplicates,
            "page": page,
            "total_pages": total_pages,
            "page_size": PAGE_SIZE,
            "total_filtered": total_filtered,
        },
    )


@router.delete("/documents/{doc_id}")
async def delete_document(doc_id: int):
    """Delete a document and its properties. Also removes the image file from disk."""
    with get_db() as conn:
        doc = conn.execute(
            "SELECT image_path, person_id FROM documents WHERE id=?", (doc_id,)
        ).fetchone()
        if not doc:
            return JSONResponse({"error": "not found"}, status_code=404)

        # Delete properties for this document
        conn.execute("DELETE FROM properties WHERE document_id=?", (doc_id,))

        # Delete the document record
        conn.execute("DELETE FROM documents WHERE id=?", (doc_id,))

        # If the person has no remaining documents, remove them too
        if doc["person_id"]:
            remaining = conn.execute(
                "SELECT COUNT(*) AS n FROM documents WHERE person_id=?",
                (doc["person_id"],),
            ).fetchone()["n"]
            if remaining == 0:
                conn.execute("DELETE FROM properties WHERE person_id=?", (doc["person_id"],))
                conn.execute("DELETE FROM persons WHERE id=?", (doc["person_id"],))

    # Remove the image file from disk
    image_file = Path(UPLOAD_DIR) / doc["image_path"]
    try:
        image_file.unlink(missing_ok=True)
    except OSError:
        pass

    return JSONResponse({"ok": True})


@router.post("/documents/{doc_id}/retry")
async def retry_document(doc_id: int):
    """Re-run AI extraction on an errored or extracted document."""
    import asyncio
    from services.extractor import get_default_provider
    from routers.upload import _extract_and_save

    with get_db() as conn:
        doc = conn.execute(
            "SELECT id, image_path, provider, status FROM documents WHERE id=?",
            (doc_id,),
        ).fetchone()
        if not doc:
            return JSONResponse({"error": "not found"}, status_code=404)

        # Clear previous extraction data and requeue
        conn.execute("DELETE FROM properties WHERE document_id=?", (doc_id,))
        conn.execute(
            """UPDATE documents SET status='pending',
                   extraction_error=NULL,
                   raw_extraction_json=NULL,
                   updated_at=CURRENT_TIMESTAMP
               WHERE id=?""",
            (doc_id,),
        )

    provider = doc["provider"] or get_default_provider()
    asyncio.create_task(_extract_and_save(doc_id, doc["image_path"], provider))
    return JSONResponse({"ok": True, "status": "pending"})


@router.post("/documents/retry-errors")
async def retry_all_errors():
    """Re-run extraction for every document currently in error state."""
    import asyncio
    from services.extractor import get_default_provider
    from routers.upload import _extract_and_save

    default_provider = get_default_provider()
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, image_path, provider FROM documents WHERE status='error'"
        ).fetchall()
        ids = [r["id"] for r in rows]
        if ids:
            conn.execute("DELETE FROM properties WHERE document_id IN (" + ",".join("?" * len(ids)) + ")", ids)
            conn.execute(
                "UPDATE documents SET status='pending', extraction_error=NULL, raw_extraction_json=NULL, updated_at=CURRENT_TIMESTAMP WHERE id IN (" + ",".join("?" * len(ids)) + ")",
                ids,
            )

    for row in rows:
        provider = row["provider"] or default_provider
        asyncio.create_task(_extract_and_save(row["id"], row["image_path"], provider))

    return JSONResponse({"ok": True, "retried": len(rows)})


@router.post("/documents/scan-duplicates")
async def scan_duplicates():
    """
    Backfill image_hash for existing documents and flag duplicates.
    A doc is marked duplicate_of the earliest document (lowest id) that shares
    either the exact image hash OR the same request_number + search_scope + page_info.
    """
    import hashlib

    def _hash_file(path: Path) -> str | None:
        try:
            h = hashlib.sha256()
            with open(path, "rb") as f:
                for chunk in iter(lambda: f.read(1 << 20), b""):
                    h.update(chunk)
            return h.hexdigest()
        except OSError:
            return None

    hashed = 0
    flagged_by_hash = 0
    flagged_by_request = 0

    with get_db() as conn:
        # 1. Hash any document missing image_hash
        rows = conn.execute(
            "SELECT id, image_path FROM documents WHERE image_hash IS NULL OR image_hash=''"
        ).fetchall()
        for row in rows:
            full_path = Path(UPLOAD_DIR) / row["image_path"]
            digest = _hash_file(full_path)
            if digest:
                conn.execute(
                    "UPDATE documents SET image_hash=? WHERE id=?",
                    (digest, row["id"]),
                )
                hashed += 1

        # 2. Flag duplicates by image_hash (keep earliest)
        hash_groups = conn.execute(
            """SELECT image_hash, MIN(id) AS keeper, COUNT(*) AS n
               FROM documents
               WHERE image_hash IS NOT NULL AND image_hash != ''
                 AND status != 'staged'
               GROUP BY image_hash
               HAVING n > 1"""
        ).fetchall()
        for g in hash_groups:
            result = conn.execute(
                """UPDATE documents SET duplicate_of=?
                   WHERE image_hash=? AND id != ? AND status != 'staged'""",
                (g["keeper"], g["image_hash"], g["keeper"]),
            )
            flagged_by_hash += result.rowcount or 0

        # 3. Flag duplicates by request_number + search_scope + page_info (keep earliest)
        logical_groups = conn.execute(
            """SELECT TRIM(request_number) AS rn,
                      COALESCE(TRIM(search_scope),'') AS sc,
                      COALESCE(TRIM(page_info),'') AS pi,
                      MIN(id) AS keeper,
                      COUNT(*) AS n
               FROM documents
               WHERE status IN ('extracted','confirmed')
                 AND request_number IS NOT NULL AND TRIM(request_number) != ''
               GROUP BY rn, sc, pi
               HAVING n > 1"""
        ).fetchall()
        for g in logical_groups:
            result = conn.execute(
                """UPDATE documents SET duplicate_of=?
                   WHERE TRIM(request_number)=?
                     AND COALESCE(TRIM(search_scope),'')=?
                     AND COALESCE(TRIM(page_info),'')=?
                     AND id != ?
                     AND duplicate_of IS NULL
                     AND status IN ('extracted','confirmed')""",
                (g["keeper"], g["rn"], g["sc"], g["pi"], g["keeper"]),
            )
            flagged_by_request += result.rowcount or 0

        total_dupes = conn.execute(
            "SELECT COUNT(*) AS n FROM documents WHERE duplicate_of IS NOT NULL"
        ).fetchone()["n"]

    return JSONResponse({
        "ok": True,
        "hashed": hashed,
        "flagged_by_hash": flagged_by_hash,
        "flagged_by_request": flagged_by_request,
        "total_duplicates": total_dupes,
    })


@router.get("/documents/duplicates")
async def duplicates_view(request: Request):
    """List all documents flagged as duplicates alongside their originals."""
    with get_db() as conn:
        rows = conn.execute(
            """SELECT d.*, p.first_name, p.family_name,
                      o.request_number AS orig_request_number,
                      o.created_at AS orig_created_at
               FROM documents d
               LEFT JOIN persons p ON p.id = d.person_id
               LEFT JOIN documents o ON o.id = d.duplicate_of
               WHERE d.duplicate_of IS NOT NULL
               ORDER BY d.duplicate_of, d.id"""
        ).fetchall()
    return templates.TemplateResponse(
        request,
        "duplicates.html",
        {"documents": [dict(r) for r in rows]},
    )


@router.post("/documents/delete-duplicates")
async def delete_all_duplicates():
    """Delete every document flagged as duplicate_of another doc."""
    deleted = 0
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, image_path FROM documents WHERE duplicate_of IS NOT NULL"
        ).fetchall()
        for row in rows:
            conn.execute("DELETE FROM properties WHERE document_id=?", (row["id"],))
            conn.execute("DELETE FROM documents WHERE id=?", (row["id"],))
            # Remove file from disk
            try:
                (Path(UPLOAD_DIR) / row["image_path"]).unlink(missing_ok=True)
            except OSError:
                pass
            deleted += 1
    return JSONResponse({"ok": True, "deleted": deleted})
