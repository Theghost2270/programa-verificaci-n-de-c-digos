import html
import json
import sqlite3
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import quote
from urllib.parse import parse_qs, urlparse

from control.config import DB_PATH, ensure_dirs
from control.reporting import render_audit_csv_text

WEB_DIR = Path(__file__).resolve().parent / "web"
INDEX_HTML = WEB_DIR / "index.html"


def _connect():
    ensure_dirs()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _read_frontend():
    return INDEX_HTML.read_text(encoding="utf-8")


def _safe_query(cur, sql, params=()):
    try:
        cur.execute(sql, params)
        return cur.fetchall()
    except sqlite3.OperationalError:
        return []


def _extract_file_name(details_text):
    if not details_text:
        return None
    try:
        details = json.loads(details_text)
    except Exception:
        return None
    return details.get("file_name")


def _dashboard_payload():
    with _connect() as conn:
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM pdf_files")
        total_pdfs = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM pages")
        total_codes = cur.fetchone()[0]

        cur.execute("SELECT COUNT(DISTINCT pdf_id || ':' || page_number) FROM pages")
        total_pages = cur.fetchone()[0]

        cur.execute(
            "SELECT COUNT(DISTINCT pdf_id || ':' || page_number) FROM pages WHERE scanned = 1"
        )
        scanned_pages = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM code_duplicates")
        total_duplicates = cur.fetchone()[0]

        duplicate_count_rows = _safe_query(
            cur,
            "SELECT duplicate_kind, COUNT(*) AS total "
            "FROM code_duplicates GROUP BY duplicate_kind ORDER BY duplicate_kind",
        )
        duplicate_counts = {
            row["duplicate_kind"]: row["total"] for row in duplicate_count_rows
        }

        loaded_rows = _safe_query(
            cur,
            """
            SELECT
                f.id,
                f.file_name,
                f.file_path,
                COUNT(p.code) AS codes,
                COUNT(DISTINCT p.page_number) AS pages
            FROM pdf_files f
            LEFT JOIN pages p ON p.pdf_id = f.id
            GROUP BY f.id, f.file_name, f.file_path
            ORDER BY f.loaded_at DESC, f.id DESC
            """,
        )
        loaded_pdfs = [dict(row) for row in loaded_rows]

        duplicate_page_rows = _safe_query(
            cur,
            """
            SELECT pdf_id, page_number, COUNT(*) AS total FROM (
                SELECT new_pdf_id AS pdf_id, new_page_number AS page_number FROM code_duplicates
                UNION ALL
                SELECT existing_pdf_id AS pdf_id, existing_page_number AS page_number FROM code_duplicates
            )
            GROUP BY pdf_id, page_number
            """,
        )
        duplicate_pages = {
            (row["pdf_id"], row["page_number"]): row["total"]
            for row in duplicate_page_rows
        }

        page_rows = _safe_query(
            cur,
            """
            SELECT
                p.pdf_id,
                f.file_name,
                p.page_number,
                MIN(p.scanned) AS scanned,
                COUNT(*) AS codes
            FROM pages p
            JOIN pdf_files f ON f.id = p.pdf_id
            GROUP BY p.pdf_id, f.file_name, p.page_number
            ORDER BY f.file_name, p.page_number
            """,
        )
        pages = []
        for row in page_rows:
            key = (row["pdf_id"], row["page_number"])
            dup_total = duplicate_pages.get(key, 0)
            pages.append(
                {
                    "pdf_id": row["pdf_id"],
                    "file_name": row["file_name"],
                    "page_number": row["page_number"],
                    "codes": row["codes"],
                    "scanned": bool(row["scanned"]),
                    "duplicate_count": dup_total,
                    "status": "duplicado"
                    if dup_total
                    else ("escaneada" if row["scanned"] else "pendiente"),
                }
            )

        extract_rows = _safe_query(
            cur,
            "SELECT ts, details FROM events WHERE event_type = 'extract_summary' "
            "ORDER BY id DESC LIMIT 1",
        )
        latest_extract = None
        if extract_rows:
            latest_extract = {"ts": extract_rows[0]["ts"]}
            try:
                latest_extract.update(json.loads(extract_rows[0]["details"] or "{}"))
            except Exception:
                latest_extract["details"] = extract_rows[0]["details"]

        event_rows = _safe_query(
            cur,
            "SELECT ts, event_type, code, page_number, details "
            "FROM events ORDER BY id DESC LIMIT 80",
        )
        recent_events = []
        for row in event_rows:
            recent_events.append(
                {
                    "ts": row["ts"],
                    "event_type": row["event_type"],
                    "code": row["code"],
                    "page_number": row["page_number"],
                    "file_name": _extract_file_name(row["details"]),
                    "details": row["details"],
                }
            )

    return {
        "summary": {
            "total_pdfs": total_pdfs,
            "total_codes": total_codes,
            "total_pages": total_pages,
            "scanned_pages": scanned_pages,
            "pending_pages": max(0, total_pages - scanned_pages),
            "total_duplicates": total_duplicates,
            "duplicate_counts": duplicate_counts,
        },
        "loaded_pdfs": loaded_pdfs,
        "pages": pages,
        "latest_extract": latest_extract,
        "recent_events": recent_events,
    }


def _lookup_code_payload(code):
    normalized = code.strip().upper()
    if not normalized:
        return {"status": "empty", "code": ""}

    with _connect() as conn:
        cur = conn.cursor()
        rows = _safe_query(
            cur,
            """
            SELECT p.pdf_id, p.page_number, p.scanned, f.file_name
            FROM pages p
            JOIN pdf_files f ON f.id = p.pdf_id
            WHERE p.code = ?
            ORDER BY f.file_name, p.page_number
            """,
            (normalized,),
        )

    if not rows:
        return {"status": "not_found", "code": normalized}

    matches = [
        {
            "pdf_id": row["pdf_id"],
            "page_number": row["page_number"],
            "scanned": bool(row["scanned"]),
            "file_name": row["file_name"],
        }
        for row in rows
    ]
    return {
        "status": "ok" if len(matches) == 1 else "ambiguous",
        "code": normalized,
        "matches": matches,
    }


def _pdf_file_response(pdf_id):
    with _connect() as conn:
        cur = conn.cursor()
        rows = _safe_query(
            cur,
            "SELECT file_name, file_path FROM pdf_files WHERE id = ?",
            (pdf_id,),
        )
    if not rows:
        return None

    row = rows[0]
    path = Path(row["file_path"])
    if not path.exists() or not path.is_file():
        return {
            "status": 404,
            "content": b"PDF no encontrado",
            "content_type": "text/plain; charset=utf-8",
            "headers": None,
        }

    filename = row["file_name"] or path.name
    return {
        "status": 200,
        "content": path.read_bytes(),
        "content_type": "application/pdf",
        "headers": {
            "Content-Disposition": f"attachment; filename*=UTF-8''{quote(filename)}"
        },
    }


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        ensure_dirs()
        parsed = urlparse(self.path)

        if parsed.path == "/":
            if not INDEX_HTML.exists():
                self._send_text("<h2>No se encontro el frontend</h2>", status=500)
                return
            self._send_bytes(_read_frontend().encode("utf-8"), "text/html; charset=utf-8")
            return

        if parsed.path == "/api/dashboard":
            self._send_json(_dashboard_payload())
            return

        if parsed.path == "/api/code":
            params = parse_qs(parsed.query)
            code = params.get("value", [""])[0]
            self._send_json(_lookup_code_payload(code))
            return

        if parsed.path == "/pdf-file":
            params = parse_qs(parsed.query)
            pdf_id = params.get("id", [""])[0]
            if not str(pdf_id).isdigit():
                self._send_text("PDF invalido", status=400, content_type="text/plain; charset=utf-8")
                return
            response = _pdf_file_response(int(pdf_id))
            if response is None:
                self._send_text("PDF no encontrado", status=404, content_type="text/plain; charset=utf-8")
                return
            self._send_bytes(
                response["content"],
                response["content_type"],
                status=response["status"],
                headers=response["headers"],
            )
            return

        if parsed.path == "/report.csv":
            csv_text = render_audit_csv_text()
            headers = {
                "Content-Disposition": 'attachment; filename="auditoria.csv"'
            }
            self._send_bytes(
                csv_text.encode("utf-8"),
                "text/csv; charset=utf-8",
                headers=headers,
            )
            return

        if parsed.path.startswith("/pdf/"):
            parts = parsed.path.split("/")
            if (
                len(parts) >= 5
                and parts[2].isdigit()
                and parts[3] == "page"
                and parts[4].isdigit()
            ):
                pdf_id = int(parts[2])
                page_number = int(parts[4])
                self._send_text(
                    self._page_detail(pdf_id, page_number),
                    content_type="text/html; charset=utf-8",
                )
                return

        self._send_text("<h2>404</h2>", status=404)

    def _page_detail(self, pdf_id, page_number):
        with _connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT file_name FROM pdf_files WHERE id = ?", (pdf_id,))
            row = cur.fetchone()
            file_name = row["file_name"] if row else f"PDF {pdf_id}"
            cur.execute(
                "SELECT code, scanned FROM pages WHERE pdf_id = ? AND page_number = ? ORDER BY code",
                (pdf_id, page_number),
            )
            rows = cur.fetchall()

        parts = [
            "<!doctype html><html lang='es'><head><meta charset='utf-8'>",
            "<meta name='viewport' content='width=device-width, initial-scale=1'>",
            f"<title>{html.escape(file_name)} - Pagina {page_number}</title>",
            "<style>body{font-family:Segoe UI,Arial,sans-serif;margin:24px;color:#202b36}"
            "table{border-collapse:collapse;width:100%}th,td{border:1px solid #d5dde5;padding:8px;text-align:left}"
            "th{background:#f5f7fa}a{color:#0b67c2;text-decoration:none}</style></head><body>",
            "<p><a href='/'>Volver</a></p>",
            f"<h2>{html.escape(file_name)} - Pagina {page_number}</h2>",
            "<table><thead><tr><th>Codigo</th><th>Estado</th></tr></thead><tbody>",
        ]
        for row in rows:
            parts.append(
                "<tr>"
                f"<td>{html.escape(row['code'])}</td>"
                f"<td>{'Escaneado' if row['scanned'] else 'Pendiente'}</td>"
                "</tr>"
            )
        parts.append("</tbody></table></body></html>")
        return "".join(parts)

    def _send_json(self, payload, status=200):
        self._send_bytes(
            json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            "application/json; charset=utf-8",
            status=status,
        )

    def _send_text(self, content, status=200, content_type="text/html; charset=utf-8"):
        self._send_bytes(content.encode("utf-8"), content_type, status=status)

    def _send_bytes(self, content, content_type, status=200, headers=None):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        if headers:
            for key, value in headers.items():
                self.send_header(key, value)
        self.end_headers()
        self.wfile.write(content)


def main():
    server = HTTPServer(("127.0.0.1", 8000), Handler)
    print("Servidor en http://127.0.0.1:8000")
    server.serve_forever()


if __name__ == "__main__":
    main()
