import os
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from fastapi.templating import Jinja2Templates

from config import UPLOAD_DIR
from database.connection import get_db

router = APIRouter()
templates = Jinja2Templates(directory="templates")


@router.get("/documents")
async def document_queue(request: Request, status: str = "", uploaded: int = 0):
    with get_db() as conn:
        if status:
            rows = conn.execute(
                """SELECT d.*, p.first_name, p.family_name
                   FROM documents d
                   LEFT JOIN persons p ON p.id = d.person_id
                   WHERE d.status=?
                   ORDER BY d.id DESC""",
                (status,),
            ).fetchall()
        else:
            rows = conn.execute(
                """SELECT d.*, p.first_name, p.family_name
                   FROM documents d
                   LEFT JOIN persons p ON p.id = d.person_id
                   ORDER BY d.id DESC"""
            ).fetchall()

        stats = conn.execute(
            """SELECT
               COUNT(*) AS total,
               SUM(CASE WHEN status='confirmed' THEN 1 ELSE 0 END) AS confirmed,
               SUM(CASE WHEN status='extracted' THEN 1 ELSE 0 END) AS pending_review,
               SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) AS processing,
               SUM(CASE WHEN status='error' THEN 1 ELSE 0 END) AS errors
            FROM documents WHERE status != 'staged'"""
        ).fetchone()

    return templates.TemplateResponse(
        request,
        "documents.html",
        {
            "documents": [dict(r) for r in rows],
            "stats": dict(stats) if stats else {},
            "current_status": status,
            "uploaded": uploaded,
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
