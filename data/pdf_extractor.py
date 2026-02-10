import json
import os
import pdfplumber
import re

from control.database.db import get_connection

CODE_REGEX = re.compile(r"\b[A-Z0-9]{6,20}\b")


def _get_pdf_signature(pdf_path):
    stat = os.stat(pdf_path)
    return {
        "path": os.path.abspath(pdf_path),
        "size": stat.st_size,
        "mtime": stat.st_mtime,
    }


def _get_cached_signature(cur):
    row = cur.execute(
        "SELECT value FROM meta WHERE key = 'pdf_signature'"
    ).fetchone()
    if not row:
        return None
    try:
        return json.loads(row[0])
    except json.JSONDecodeError:
        return None


def _set_cached_signature(cur, signature):
    cur.execute(
        "INSERT OR REPLACE INTO meta (key, value) VALUES ('pdf_signature', ?)",
        (json.dumps(signature),)
    )


def extract_pdf(pdf_path, start_page=1, end_page=None, use_cache=True):
    conn = get_connection()
    try:
        cur = conn.cursor()

        signature = _get_pdf_signature(pdf_path)
        cached_signature = _get_cached_signature(cur)
        if (
            use_cache
            and cached_signature == signature
            and start_page == 1
            and end_page is None
        ):
            cur.execute("SELECT 1 FROM pages LIMIT 1")
            if cur.fetchone():
                return False

        if cached_signature != signature:
            cur.execute("DELETE FROM pages")
            _set_cached_signature(cur, signature)

        with pdfplumber.open(pdf_path) as pdf:
            total_pages = len(pdf.pages)
            start_page = max(1, int(start_page))
            if end_page is None:
                end_page = total_pages
            else:
                end_page = min(int(end_page), total_pages)
            if start_page > end_page:
                return

            pages = pdf.pages[start_page - 1:end_page]
            for page_number, page in enumerate(pages, start=start_page):
                text = page.extract_text()
                if not text:
                    continue

                matches = CODE_REGEX.findall(text)

                values = [(page_number, code.upper()) for code in set(matches)]
                if not values:
                    continue

                cur.executemany(
                    "INSERT OR IGNORE INTO pages (page_number, code) VALUES (?, ?)",
                    values
                )

        conn.commit()
        return True
    finally:
        conn.close()
