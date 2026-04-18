import hashlib
import hmac
import secrets
from datetime import datetime, timedelta

SESSION_TTL_SECONDS = 86400  # 24h

def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    hashed = hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'), salt.encode('utf-8'), 100000).hex()
    return f"{salt}${hashed}"

def verify_password(stored_password: str, provided_password: str) -> bool:
    try:
        salt, stored_hash = stored_password.split('$')
        hashed = hashlib.pbkdf2_hmac('sha256', provided_password.encode('utf-8'), salt.encode('utf-8'), 100000).hex()
        return hmac.compare_digest(hashed, stored_hash)
    except Exception:
        return False

def get_user_by_username(username: str):
    from database.connection import get_db
    with get_db() as conn:
        cursor = conn.execute("SELECT * FROM users WHERE username = ?", (username,))
        return cursor.fetchone()

def create_user(username: str, password: str):
    from database.connection import get_db
    with get_db() as conn:
        hashed = hash_password(password)
        conn.execute("INSERT INTO users (username, password_hash) VALUES (?, ?)", (username, hashed))

def delete_user(user_id: int):
    from database.connection import get_db
    with get_db() as conn:
        conn.execute("DELETE FROM users WHERE id = ?", (user_id,))

def get_all_users():
    from database.connection import get_db
    with get_db() as conn:
        cursor = conn.execute("SELECT id, username, created_at FROM users ORDER BY created_at DESC")
        return cursor.fetchall()


# ─── Sessions (DB-backed) ─────────────────────────────────────────

def create_session(user_id: int, username: str, ttl_seconds: int = SESSION_TTL_SECONDS) -> str:
    from database.connection import get_db
    session_id = secrets.token_urlsafe(32)
    expires_at = (datetime.utcnow() + timedelta(seconds=ttl_seconds)).strftime("%Y-%m-%d %H:%M:%S")
    with get_db() as conn:
        conn.execute(
            "INSERT INTO sessions (id, user_id, username, expires_at) VALUES (?, ?, ?, ?)",
            (session_id, user_id, username, expires_at),
        )
    return session_id


def get_session(session_id: str):
    """Return dict with user_id/username if session is valid and unexpired, else None."""
    if not session_id:
        return None
    from database.connection import get_db
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    with get_db() as conn:
        row = conn.execute(
            "SELECT id, user_id, username, expires_at FROM sessions WHERE id = ? AND expires_at > ?",
            (session_id, now),
        ).fetchone()
    return dict(row) if row else None


def delete_session(session_id: str) -> None:
    if not session_id:
        return
    from database.connection import get_db
    with get_db() as conn:
        conn.execute("DELETE FROM sessions WHERE id = ?", (session_id,))


def cleanup_expired_sessions() -> int:
    from database.connection import get_db
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    with get_db() as conn:
        cur = conn.execute("DELETE FROM sessions WHERE expires_at <= ?", (now,))
        return cur.rowcount or 0

