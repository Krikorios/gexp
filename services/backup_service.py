import os
import sqlite3
from datetime import datetime
from config import DATABASE_PATH
from database.connection import get_db

def create_backup() -> str:
    """Create a backup of the database to a backup file."""
    db_file = DATABASE_PATH
    if not os.path.exists(db_file):
        raise FileNotFoundError("Database file does not exist.")
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = f"{db_file}.{timestamp}.bak"
    
    # Safe SQLite backup
    with get_db() as conn:
        bck = sqlite3.connect(backup_file)
        with bck:
            conn.backup(bck)
        bck.close()
        
    return backup_file

