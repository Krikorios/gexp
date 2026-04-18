"""On-demand thumbnail generation with disk caching.

Thumbnails are stored under UPLOAD_DIR/.thumbs/<size>/<original relative path>.jpg
and generated lazily the first time they are requested. This keeps the
documents list fast even with hundreds of full-resolution uploads.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

from PIL import Image, ImageOps

from config import UPLOAD_DIR

THUMB_ROOT = Path(UPLOAD_DIR) / ".thumbs"
DEFAULT_SIZE = 320  # px on the longest edge
JPEG_QUALITY = 78


def _safe_rel(rel_path: str) -> Optional[Path]:
    """Resolve rel_path against UPLOAD_DIR, rejecting traversal."""
    base = Path(UPLOAD_DIR).resolve()
    target = (base / rel_path).resolve()
    try:
        target.relative_to(base)
    except ValueError:
        return None
    return target


def get_or_create_thumbnail(rel_path: str, size: int = DEFAULT_SIZE) -> Optional[Path]:
    """Return a path to a cached JPEG thumbnail for the given upload,
    creating it if needed. Returns None if the source doesn't exist."""
    source = _safe_rel(rel_path)
    if source is None or not source.is_file():
        return None

    thumb_dir = THUMB_ROOT / str(size)
    thumb_path = thumb_dir / (rel_path + ".jpg")

    # Regenerate if missing or source is newer
    if thumb_path.is_file():
        try:
            if thumb_path.stat().st_mtime >= source.stat().st_mtime:
                return thumb_path
        except OSError:
            pass

    thumb_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with Image.open(source) as im:
            im = ImageOps.exif_transpose(im)
            im.thumbnail((size, size), Image.LANCZOS)
            if im.mode not in ("RGB", "L"):
                im = im.convert("RGB")
            im.save(thumb_path, "JPEG", quality=JPEG_QUALITY, optimize=True)
    except (OSError, Image.UnidentifiedImageError):
        return None

    return thumb_path
