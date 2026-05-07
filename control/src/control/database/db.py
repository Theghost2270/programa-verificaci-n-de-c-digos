import sqlite3
from control.config import DB_PATH, ensure_dirs


def get_connection():
    ensure_dirs()
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def _create_pages_table(cur, table_name="pages"):
    cur.execute(
        f"""
        CREATE TABLE {table_name} (
            page_number INTEGER NOT NULL CHECK(page_number >= 1),
            code TEXT NOT NULL CHECK(length(trim(code)) > 0),
            scanned INTEGER NOT NULL DEFAULT 0 CHECK(scanned IN (0, 1)),
            pdf_id INTEGER NOT NULL,
            UNIQUE(pdf_id, code),
            FOREIGN KEY (pdf_id) REFERENCES pdf_files(id)
        )
        """
    )


def _table_sql(cur, table_name):
    cur.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    )
    row = cur.fetchone()
    return row[0] if row and row[0] else ""


def _normalize_pages_constraints(cur):
    sql = _table_sql(cur, "pages").upper()
    required_tokens = (
        "CHECK(PAGE_NUMBER >= 1)",
        "CHECK(LENGTH(TRIM(CODE)) > 0)",
        "CHECK(SCANNED IN (0, 1))",
        "UNIQUE(PDF_ID, CODE)",
    )
    if all(token in sql for token in required_tokens):
        return

    _create_pages_table(cur, "pages_new")
    cur.execute(
        """
        INSERT INTO pages_new (page_number, code, scanned, pdf_id)
        SELECT
            COALESCE(MIN(CASE WHEN page_number >= 1 THEN page_number END), 1),
            UPPER(TRIM(code)),
            MAX(CASE WHEN scanned = 1 THEN 1 ELSE 0 END),
            pdf_id
        FROM pages
        WHERE pdf_id IS NOT NULL AND code IS NOT NULL AND TRIM(code) <> ''
        GROUP BY pdf_id, UPPER(TRIM(code))
        """
    )
    cur.execute("DROP TABLE pages")
    cur.execute("ALTER TABLE pages_new RENAME TO pages")


def _ensure_pages_schema(cur):
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='pages'")
    has_pages = cur.fetchone() is not None

    if not has_pages:
        _create_pages_table(cur)
        return

    cur.execute("PRAGMA table_info(pages)")
    columns = {row[1] for row in cur.fetchall()}
    if "pdf_id" in columns:
        _normalize_pages_constraints(cur)
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

    _create_pages_table(cur, "pages_new")
    cur.execute(
        """
        INSERT INTO pages_new (page_number, code, scanned, pdf_id)
        SELECT
            CASE WHEN page_number >= 1 THEN page_number ELSE 1 END,
            UPPER(TRIM(code)),
            CASE WHEN scanned = 1 THEN 1 ELSE 0 END,
            ?
        FROM pages
        WHERE code IS NOT NULL AND TRIM(code) <> ''
        """,
        (legacy_pdf_id,),
    )
    cur.execute("DROP TABLE pages")
    cur.execute("ALTER TABLE pages_new RENAME TO pages")


def _ensure_duplicates_schema(cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS code_duplicates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            detected_at TEXT DEFAULT (datetime('now')),
            code TEXT NOT NULL CHECK(length(trim(code)) > 0),
            duplicate_kind TEXT NOT NULL
                CHECK(duplicate_kind IN ('same_pdf', 'cross_pdf')),
            new_pdf_id INTEGER NOT NULL,
            new_page_number INTEGER NOT NULL CHECK(new_page_number >= 1),
            existing_pdf_id INTEGER NOT NULL,
            existing_page_number INTEGER NOT NULL CHECK(existing_page_number >= 1),
            FOREIGN KEY (new_pdf_id) REFERENCES pdf_files(id),
            FOREIGN KEY (existing_pdf_id) REFERENCES pdf_files(id)
        )
        """
    )


def init_db(reset=False):
    conn = get_connection()
    try:
        cur = conn.cursor()

        if reset:
            cur.execute("DROP TABLE IF EXISTS code_duplicates")
            cur.execute("DROP TABLE IF EXISTS pages")
            cur.execute("DROP TABLE IF EXISTS pdf_files")
            cur.execute("DROP TABLE IF EXISTS meta")
            cur.execute("DROP TABLE IF EXISTS events")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS pdf_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_name TEXT NOT NULL CHECK(length(trim(file_name)) > 0),
                file_path TEXT UNIQUE NOT NULL,
                signature TEXT NOT NULL CHECK(length(trim(signature)) > 0),
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
        _ensure_duplicates_schema(cur)
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_dup_code ON code_duplicates(code)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_dup_new_loc "
            "ON code_duplicates(new_pdf_id, new_page_number)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_dup_existing_loc "
            "ON code_duplicates(existing_pdf_id, existing_page_number)"
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
            event_type TEXT NOT NULL CHECK(length(trim(event_type)) > 0),
            code TEXT,
            page_number INTEGER CHECK(page_number IS NULL OR page_number >= 1),
            details TEXT
        )
        """)
        cur.execute("PRAGMA foreign_key_check")
        broken_rows = cur.fetchall()
        if broken_rows:
            raise sqlite3.IntegrityError(
                f"Integridad referencial invalida: {len(broken_rows)} fila(s)"
            )
        conn.commit()
    finally:
        conn.close()
