import sqlite3
from control.config import DB_PATH, ensure_dirs


def get_connection():
    ensure_dirs()
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def _ensure_pages_schema(cur):
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='pages'")
    has_pages = cur.fetchone() is not None

    if not has_pages:
        cur.execute(
            """
            CREATE TABLE pages (
                page_number INTEGER,
                code TEXT,
                scanned INTEGER DEFAULT 0,
                pdf_id INTEGER NOT NULL,
                UNIQUE(pdf_id, code),
                FOREIGN KEY (pdf_id) REFERENCES pdf_files(id)
            )
            """
        )
        return

    cur.execute("PRAGMA table_info(pages)")
    columns = {row[1] for row in cur.fetchall()}
    if "pdf_id" in columns:
        return

    cur.execute(
        """
        INSERT OR IGNORE INTO pdf_files (file_name, file_path, signature)
        VALUES ('legacy_migrated', 'legacy://migrated', 'legacy')
        """
    )
    cur.execute(
        "SELECT id FROM pdf_files WHERE file_path = 'legacy://migrated'"
    )
    legacy_pdf_id = cur.fetchone()[0]

    cur.execute(
        """
        CREATE TABLE pages_new (
            page_number INTEGER,
            code TEXT,
            scanned INTEGER DEFAULT 0,
            pdf_id INTEGER NOT NULL,
            UNIQUE(pdf_id, code),
            FOREIGN KEY (pdf_id) REFERENCES pdf_files(id)
        )
        """
    )
    cur.execute(
        """
        INSERT INTO pages_new (page_number, code, scanned, pdf_id)
        SELECT page_number, code, scanned, ?
        FROM pages
        """,
        (legacy_pdf_id,),
    )
    cur.execute("DROP TABLE pages")
    cur.execute("ALTER TABLE pages_new RENAME TO pages")


def init_db(reset=False):
    conn = get_connection()
    try:
        cur = conn.cursor()

        if reset:
            cur.execute("DROP TABLE IF EXISTS pages")
            cur.execute("DROP TABLE IF EXISTS pdf_files")
            cur.execute("DROP TABLE IF EXISTS meta")
            cur.execute("DROP TABLE IF EXISTS events")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS pdf_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_name TEXT NOT NULL,
                file_path TEXT UNIQUE NOT NULL,
                signature TEXT NOT NULL,
                loaded_at TEXT DEFAULT (datetime('now'))
            )
            """
        )
        _ensure_pages_schema(cur)
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_pages_code ON pages(code)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_pages_pdf_page ON pages(pdf_id, page_number)"
        )
        cur.execute("""
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT DEFAULT (datetime('now')),
            event_type TEXT NOT NULL,
            code TEXT,
            page_number INTEGER,
            details TEXT
        )
        """)
        conn.commit()
    finally:
        conn.close()
