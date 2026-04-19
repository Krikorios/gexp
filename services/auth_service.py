import hashlib
import hmac
import secrets
from datetime import datetime, timedelta

SESSION_TTL_SECONDS = 86400  # 24h
PBKDF2_ITERATIONS = 600_000  # OWASP 2023+ guidance for PBKDF2-HMAC-SHA256

# Login rate-limit tuning
LOGIN_WINDOW_SECONDS = 900   # 15 min sliding window
LOGIN_MAX_FAILS_USER = 5     # per username
LOGIN_MAX_FAILS_IP = 20      # per IP (higher because NATs)

def hash_password(password: str, iterations: int = PBKDF2_ITERATIONS) -> str:
    salt = secrets.token_hex(16)
    hashed = hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'), salt.encode('utf-8'), iterations).hex()
    return f"pbkdf2${iterations}${salt}${hashed}"

def verify_password(stored_password: str, provided_password: str) -> bool:
    """Supports both the legacy 'salt$hash' (100k iter) format and the new
    'pbkdf2$<iter>$salt$hash' format."""
    try:
        parts = stored_password.split('$')
        if len(parts) == 4 and parts[0] == 'pbkdf2':
            iterations = int(parts[1])
            salt = parts[2]
            stored_hash = parts[3]
        elif len(parts) == 2:
            iterations = 100_000  # legacy
            salt, stored_hash = parts
        else:
            return False
        hashed = hashlib.pbkdf2_hmac('sha256', provided_password.encode('utf-8'), salt.encode('utf-8'), iterations).hex()
        return hmac.compare_digest(hashed, stored_hash)
    except Exception:
        return False


def needs_rehash(stored_password: str) -> bool:
    """Return True if the stored hash uses weaker params than current defaults."""
    try:
        parts = stored_password.split('$')
        if len(parts) == 4 and parts[0] == 'pbkdf2':
            return int(parts[1]) < PBKDF2_ITERATIONS
        return True  # legacy format → rehash
    except Exception:
        return True


def get_user_by_username(username: str):
    from database.connection import get_db
    with get_db() as conn:
        cursor = conn.execute("SELECT * FROM users WHERE username = ?", (username,))
        return cursor.fetchone()

def create_user(username: str, password: str, role: str = "user"):
    from database.connection import get_db
    if role not in {"admin", "user"}:
        role = "user"
    with get_db() as conn:
        hashed = hash_password(password)
        conn.execute(
            "INSERT INTO users (username, password_hash, role) VALUES (?, ?, ?)",
            (username, hashed, role),
        )

def delete_user(user_id: int):
    from database.connection import get_db
    with get_db() as conn:
        conn.execute("DELETE FROM users WHERE id = ?", (user_id,))
        conn.execute("DELETE FROM sessions WHERE user_id = ?", (user_id,))


def update_user_password(user_id: int, new_password: str) -> None:
    from database.connection import get_db
    with get_db() as conn:
        conn.execute(
            "UPDATE users SET password_hash=? WHERE id=?",
            (hash_password(new_password), user_id),
        )
        # Rotate any active sessions for this user.
        conn.execute("DELETE FROM sessions WHERE user_id=?", (user_id,))


def set_user_role(user_id: int, role: str) -> None:
    from database.connection import get_db
    if role not in {"admin", "user"}:
        raise ValueError("invalid role")
    with get_db() as conn:
        conn.execute("UPDATE users SET role=? WHERE id=?", (role, user_id))


def count_admins() -> int:
    from database.connection import get_db
    with get_db() as conn:
        row = conn.execute("SELECT COUNT(*) AS n FROM users WHERE role='admin'").fetchone()
        return int(row["n"]) if row else 0


def get_all_users():
    from database.connection import get_db
    with get_db() as conn:
        cursor = conn.execute(
            "SELECT id, username, role, created_at FROM users ORDER BY created_at DESC"
        )
        return cursor.fetchall()


# ─── Login rate-limiting ──────────────────────────────────────────

def record_login_attempt(username: str, ip: str, success: bool) -> None:
    from database.connection import get_db
    with get_db() as conn:
        conn.execute(
            "INSERT INTO login_attempts (username, ip, success) VALUES (?, ?, ?)",
            (username, ip, 1 if success else 0),
        )


def is_login_blocked(username: str, ip: str) -> bool:
    """Return True if too many recent failures for this username OR this IP."""
    from database.connection import get_db
    cutoff = (datetime.utcnow() - timedelta(seconds=LOGIN_WINDOW_SECONDS)).strftime("%Y-%m-%d %H:%M:%S")
    with get_db() as conn:
        by_user = conn.execute(
            "SELECT COUNT(*) AS n FROM login_attempts WHERE username=? AND success=0 AND created_at > ?",
            (username, cutoff),
        ).fetchone()["n"]
        if by_user >= LOGIN_MAX_FAILS_USER:
            return True
        by_ip = conn.execute(
            "SELECT COUNT(*) AS n FROM login_attempts WHERE ip=? AND success=0 AND created_at > ?",
            (ip, cutoff),
        ).fetchone()["n"]
        return by_ip >= LOGIN_MAX_FAILS_IP


def cleanup_old_login_attempts() -> int:
    from database.connection import get_db
    cutoff = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
    with get_db() as conn:
        cur = conn.execute("DELETE FROM login_attempts WHERE created_at < ?", (cutoff,))
        return cur.rowcount or 0


# ─── Sessions (DB-backed) ─────────────────────────────────────────

def create_session(user_id: int, username: str, role: str = "user", ttl_seconds: int = SESSION_TTL_SECONDS) -> str:
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
    """Return dict with user_id/username/role if valid and unexpired, else None."""
    if not session_id:
        return None
    from database.connection import get_db
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    with get_db() as conn:
        row = conn.execute(
            """SELECT s.id, s.user_id, s.username, s.expires_at, u.role
               FROM sessions s
               LEFT JOIN users u ON u.id = s.user_id
               WHERE s.id = ? AND s.expires_at > ?""",
            (session_id, now),
        ).fetchone()
    if not row:
        return None
    d = dict(row)
    d["role"] = d.get("role") or "user"
    return d


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

