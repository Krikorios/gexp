from database.connection import get_db


def _migrate(conn):
    """Add columns that may not exist in older databases."""
    # users.role migration
    cursor = conn.execute("PRAGMA table_info(users)")
    user_cols = {row[1] for row in cursor.fetchall()}
    if "role" not in user_cols:
        conn.execute("ALTER TABLE users ADD COLUMN role TEXT NOT NULL DEFAULT 'user'")
        # Promote the first-created user to admin so the app remains usable.
        first = conn.execute("SELECT id FROM users ORDER BY id LIMIT 1").fetchone()
        if first:
            conn.execute("UPDATE users SET role='admin' WHERE id=?", (first[0],))

    cursor = conn.execute("PRAGMA table_info(documents)")
    existing = {row[1] for row in cursor.fetchall()}
    migrations = [
        ("provider", "TEXT"),
        ("pdf_group_id", "TEXT"),
        ("page_number", "INTEGER"),
        ("page_info", "TEXT"),
        ("search_scope", "TEXT"),
        ("request_purpose", "TEXT"),
        ("data_valid_until", "TEXT"),
        ("registry_office", "TEXT"),
        ("owns_properties", "BOOLEAN"),
        ("declared_property_count", "INTEGER"),
        ("image_hash", "TEXT"),
        ("duplicate_of", "INTEGER"),
        ("duplicate_dismissed", "INTEGER DEFAULT 0"),
    ]
    for col, col_type in migrations:
        if col not in existing:
            conn.execute(f"ALTER TABLE documents ADD COLUMN {col} {col_type}")

    conn.execute("CREATE INDEX IF NOT EXISTS idx_documents_image_hash ON documents(image_hash)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_documents_request_number ON documents(request_number)")

    cursor = conn.execute("PRAGMA table_info(properties)")
    existing_prop = {row[1] for row in cursor.fetchall()}
    migrations_prop = [
        ("party_name", "TEXT"),
        ("qaza", "TEXT"),
    ]
    for col, col_type in migrations_prop:
        if col not in existing_prop:
            conn.execute(f"ALTER TABLE properties ADD COLUMN {col} {col_type}")

def create_tables():
    with get_db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'user',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS login_attempts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT,
                ip TEXT,
                success INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_login_attempts_username ON login_attempts(username, created_at);
            CREATE INDEX IF NOT EXISTS idx_login_attempts_ip ON login_attempts(ip, created_at);

            CREATE TABLE IF NOT EXISTS sessions (
                id TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                username TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_sessions_expires_at ON sessions(expires_at);
            CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions(user_id);

            CREATE TABLE IF NOT EXISTS persons (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                first_name TEXT NOT NULL,
                father_name TEXT,
                mother_name TEXT,
                family_name TEXT,
                family_origin TEXT,
                nationality TEXT,
                birth_date TEXT,
                registry_number TEXT UNIQUE,
                registry_place TEXT,
                first_name_norm TEXT,
                family_name_norm TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS documents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                person_id INTEGER REFERENCES persons(id),
                image_path TEXT NOT NULL,
                request_number TEXT,
                request_date TEXT,
                applicant_name_raw TEXT,
                request_purpose TEXT,
                data_valid_until TEXT,
                registry_office TEXT,
                owns_properties BOOLEAN,
                declared_property_count INTEGER,
                provider TEXT,
                pdf_group_id TEXT,
                page_number INTEGER,
                page_info TEXT,
                search_scope TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                raw_extraction_json TEXT,
                extraction_error TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS properties (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                document_id INTEGER NOT NULL REFERENCES documents(id),
                person_id INTEGER REFERENCES persons(id),
                row_order INTEGER DEFAULT 0,
                area_name TEXT,
                party_name TEXT,
                property_number TEXT,
                section TEXT,
                block TEXT,
                real_estate_district TEXT,
                space TEXT,
                qaza TEXT,
                num_shares TEXT,
                ownership_type TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_persons_first_name_norm ON persons(first_name_norm);
            CREATE INDEX IF NOT EXISTS idx_persons_family_name_norm ON persons(family_name_norm);
            CREATE INDEX IF NOT EXISTS idx_persons_registry_number ON persons(registry_number);
            CREATE INDEX IF NOT EXISTS idx_documents_person_id ON documents(person_id);
            CREATE INDEX IF NOT EXISTS idx_documents_status ON documents(status);
            CREATE INDEX IF NOT EXISTS idx_properties_person_id ON properties(person_id);
            CREATE INDEX IF NOT EXISTS idx_properties_property_number ON properties(property_number);
            CREATE INDEX IF NOT EXISTS idx_properties_district ON properties(real_estate_district);
            CREATE INDEX IF NOT EXISTS idx_properties_block ON properties(block);
        """)
        _migrate(conn)
