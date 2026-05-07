import hashlib
import json
import re
import sqlite3
from pathlib import Path

from control.database.db import get_connection

CODE_PATTERN = re.compile(r"\b[A-Z0-9][A-Z0-9\-]{5,}\b")


def _file_signature(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _normalize_code(raw):
    code = raw.strip().upper()
    code = code.strip("-")
    if len(code) < 6:
        return None
    if not any(char.isdigit() for char in code):
        return None
    return code


def _extract_codes(text):
    if not text:
        return []

    seen = set()
    codes = []
    for token in CODE_PATTERN.findall(text.upper()):
        code = _normalize_code(token)
        if not code or code in seen:
            continue
        seen.add(code)
        codes.append(code)
    return codes


def _log_extract_summary(cur, summary):
    cur.execute(
        "INSERT INTO events (event_type, details) VALUES (?, ?)",
        ("extract_summary", json.dumps(summary)),
    )


def _clear_duplicate_rows(cur, pdf_id):
    cur.execute(
        "DELETE FROM code_duplicates WHERE new_pdf_id = ? OR existing_pdf_id = ?",
        (pdf_id, pdf_id),
    )


def _register_duplicate(
    cur,
    code,
    duplicate_kind,
    new_pdf_id,
    new_page_number,
    existing_pdf_id,
    existing_page_number,
):
    cur.execute(
        """
        INSERT INTO code_duplicates (
            code,
            duplicate_kind,
            new_pdf_id,
            new_page_number,
            existing_pdf_id,
            existing_page_number
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            code,
            duplicate_kind,
            new_pdf_id,
            new_page_number,
            existing_pdf_id,
            existing_page_number,
        ),
    )


def list_loaded_pdfs():
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT f.file_name, f.file_path, COUNT(p.code) AS codes
            FROM pdf_files f
            LEFT JOIN pages p ON p.pdf_id = f.id
            GROUP BY f.id, f.file_name, f.file_path
            ORDER BY f.loaded_at DESC, f.id DESC
            """
        )
        return cur.fetchall()
    finally:
        conn.close()


def extract_pdf(pdf_path, progress_callback=None):
    path = Path(pdf_path).expanduser().resolve()
    if not path.exists():
        raise FileNotFoundError(f"No existe el PDF: {path}")
    if path.suffix.lower() != ".pdf":
        raise ValueError(f"Archivo no soportado: {path}")

    signature = _file_signature(path)
    path_text = str(path)

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, signature FROM pdf_files WHERE file_path = ?",
            (path_text,),
        )
        existing = cur.fetchone()

        if existing and existing[1] == signature:
            return False

        if existing:
            pdf_id = existing[0]
            cur.execute("DELETE FROM pages WHERE pdf_id = ?", (pdf_id,))
            _clear_duplicate_rows(cur, pdf_id)
            cur.execute(
                """
                UPDATE pdf_files
                SET file_name = ?, signature = ?, loaded_at = datetime('now')
                WHERE id = ?
                """,
                (path.name, signature, pdf_id),
            )
        else:
            cur.execute(
                "INSERT INTO pdf_files (file_name, file_path, signature) VALUES (?, ?, ?)",
                (path.name, path_text, signature),
            )
            pdf_id = cur.lastrowid

        # Import lazily so the program can still start even if dependency
        # installation is pending and there are cached PDFs.
        import pdfplumber

        inserted = 0
        duplicates = 0
        duplicates_same_pdf = 0
        duplicates_cross_pdf = 0
        codes_found = 0
        pages_processed = 0
        start_page = None
        end_page = None

        with pdfplumber.open(path_text) as pdf:
            total_pages = len(pdf.pages)

            for page_index, page in enumerate(pdf.pages, start=1):
                if progress_callback is not None:
                    progress_callback(path_text, page_index, total_pages)

                text = page.extract_text() or ""
                codes = _extract_codes(text)
                pages_processed += 1

                if not codes:
                    continue

                if start_page is None:
                    start_page = page_index
                end_page = page_index

                for code in codes:
                    codes_found += 1
                    cur.execute(
                        """
                        SELECT pdf_id, page_number
                        FROM pages
                        WHERE code = ?
                        ORDER BY pdf_id, page_number
                        """,
                        (code,),
                    )
                    previous_rows = cur.fetchall()
                    has_same_pdf_duplicate = False
                    for previous_pdf_id, previous_page in previous_rows:
                        duplicate_kind = (
                            "same_pdf" if previous_pdf_id == pdf_id else "cross_pdf"
                        )
                        _register_duplicate(
                            cur,
                            code=code,
                            duplicate_kind=duplicate_kind,
                            new_pdf_id=pdf_id,
                            new_page_number=page_index,
                            existing_pdf_id=previous_pdf_id,
                            existing_page_number=previous_page,
                        )
                        duplicates += 1
                        if duplicate_kind == "same_pdf":
                            has_same_pdf_duplicate = True
                            duplicates_same_pdf += 1
                        else:
                            duplicates_cross_pdf += 1

                    try:
                        cur.execute(
                            """
                            INSERT INTO pages (page_number, code, scanned, pdf_id)
                            VALUES (?, ?, 0, ?)
                            """,
                            (page_index, code, pdf_id),
                        )
                        inserted += 1
                    except sqlite3.IntegrityError:
                        if not has_same_pdf_duplicate:
                            cur.execute(
                                """
                                SELECT page_number
                                FROM pages
                                WHERE pdf_id = ? AND code = ?
                                ORDER BY page_number
                                LIMIT 1
                                """,
                                (pdf_id, code),
                            )
                            existing_same_pdf = cur.fetchone()
                            if existing_same_pdf is not None:
                                _register_duplicate(
                                    cur,
                                    code=code,
                                    duplicate_kind="same_pdf",
                                    new_pdf_id=pdf_id,
                                    new_page_number=page_index,
                                    existing_pdf_id=pdf_id,
                                    existing_page_number=existing_same_pdf[0],
                                )
                                duplicates += 1
                                duplicates_same_pdf += 1

        summary = {
            "pdf": path.name,
            "start_page": start_page,
            "end_page": end_page,
            "total_pages": total_pages,
            "pages_processed": pages_processed,
            "codes_found": codes_found,
            "inserted": inserted,
            "duplicates": duplicates,
            "duplicates_same_pdf": duplicates_same_pdf,
            "duplicates_cross_pdf": duplicates_cross_pdf,
        }
        _log_extract_summary(cur, summary)
        conn.commit()
        return True
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
