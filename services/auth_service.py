import sqlite3
import hashlib
import secrets

def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    hashed = hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'), salt.encode('utf-8'), 100000).hex()
    return f"{salt}${hashed}"

def verify_password(stored_password: str, provided_password: str) -> bool:
    try:
        salt, stored_hash = stored_password.split('$')
        hashed = hashlib.pbkdf2_hmac('sha256', provided_password.encode('utf-8'), salt.encode('utf-8'), 100000).hex()
        return hashed == stored_hash
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

