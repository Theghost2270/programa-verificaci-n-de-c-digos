import sqlite3

DB_NAME = "control.db"

def get_connection():
    return sqlite3.connect(DB_NAME)

def init_db(reset=False):
    conn = get_connection()
    try:
        cur = conn.cursor()

        if reset:
            cur.execute("DROP TABLE IF EXISTS pages")
            cur.execute("DROP TABLE IF EXISTS meta")

        cur.execute("""
        CREATE TABLE IF NOT EXISTS pages (
            page_number INTEGER,
            code TEXT UNIQUE,
            scanned INTEGER DEFAULT 0
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """)
        conn.commit()
    finally:
        conn.close()
