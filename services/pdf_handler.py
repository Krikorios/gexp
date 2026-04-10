"""Convert PDF files to per-page images using PyMuPDF (no poppler dependency)."""

import uuid
from datetime import date
from pathlib import Path

from config import UPLOAD_DIR


def pdf_to_images(pdf_bytes: bytes, original_name: str) -> list[dict]:
    """
    Convert a PDF to individual page images.
    Returns list of dicts: [{"image_path": "relative/path.png", "page_number": 1}, ...]
    """
    import fitz  # PyMuPDF

    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    group_id = uuid.uuid4().hex
    today = date.today().isoformat()
    dest_dir = Path(UPLOAD_DIR) / today
    dest_dir.mkdir(parents=True, exist_ok=True)

    pages = []
    for page_num in range(len(doc)):
        page = doc[page_num]
        # Render at 200 DPI for good OCR quality
        pix = page.get_pixmap(dpi=200)

        filename = f"{group_id}_p{page_num + 1}.png"
        dest = dest_dir / filename
        pix.save(str(dest))

        pages.append({
            "image_path": str(Path(today) / filename),
            "page_number": page_num + 1,
            "pdf_group_id": group_id,
        })

    doc.close()
    return pages
