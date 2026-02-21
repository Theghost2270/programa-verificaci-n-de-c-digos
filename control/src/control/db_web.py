import html
import json
import sqlite3
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

from control.config import DB_PATH, ensure_dirs


def _connect():
    ensure_dirs()
    return sqlite3.connect(DB_PATH)


def _render_page(title, body):
    return f"""<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(title)}</title>
  <style>
    body {{
      font-family: Arial, sans-serif;
      margin: 24px;
      color: #222;
    }}
    .top {{
      display: flex;
      gap: 12px;
      align-items: center;
      flex-wrap: wrap;
      margin-bottom: 16px;
    }}
    table {{
      border-collapse: collapse;
      width: 100%;
      margin-top: 8px;
    }}
    th, td {{
      border: 1px solid #ddd;
      padding: 8px;
      text-align: left;
    }}
    th {{
      background: #f3f3f3;
    }}
    .badge {{
      display: inline-block;
      padding: 2px 8px;
      border-radius: 8px;
      font-size: 12px;
    }}
    .ok {{ background: #d9f7d6; }}
    .pend {{ background: #ffe9c7; }}
    a {{ color: #0b4dbb; text-decoration: none; }}
    a:hover {{ text-decoration: underline; }}
  </style>
</head>
<body>
{body}
</body>
</html>"""


def _summary_table(status):
    where = ""
    if status == "pendientes":
        where = "HAVING MIN(scanned) = 0"
    elif status == "escaneadas":
        where = "HAVING MIN(scanned) = 1"

    with _connect() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT p.pdf_id, f.file_name, p.page_number, MIN(p.scanned) as scanned, COUNT(*) as codes "
            "FROM pages p JOIN pdf_files f ON f.id = p.pdf_id "
            "GROUP BY p.pdf_id, p.page_number "
            f"{where} "
            "ORDER BY f.file_name, p.page_number"
        )
        rows = cur.fetchall()

        cur.execute("SELECT COUNT(DISTINCT pdf_id || ':' || page_number) FROM pages")
        total_pages = cur.fetchone()[0]
        cur.execute(
            "SELECT COUNT(DISTINCT pdf_id || ':' || page_number) FROM pages WHERE scanned = 1"
        )
        scanned_pages = cur.fetchone()[0]

    parts = [
        "<div class='top'>",
        f"<strong>Paginas:</strong> {total_pages}",
        f"<strong>Escaneadas:</strong> {scanned_pages}",
        "<span>|</span>",
        "<a href='/?status=todas'>Todas</a>",
        "<a href='/?status=pendientes'>Pendientes</a>",
        "<a href='/?status=escaneadas'>Escaneadas</a>",
        "<a href='/report'>Reporte</a>",
        "</div>",
        "<table>",
        "<thead><tr><th>Archivo</th><th>Pagina</th><th>Estado</th><th>Codigos</th></tr></thead>",
        "<tbody>",
    ]

    for pdf_id, file_name, page_number, scanned, codes in rows:
        badge = "ok" if scanned else "pend"
        label = "Escaneada" if scanned else "Pendiente"
        parts.append(
            "<tr>"
            f"<td>{html.escape(file_name)}</td>"
            f"<td><a href='/pdf/{pdf_id}/page/{page_number}'>{page_number}</a></td>"
            f"<td><span class='badge {badge}'>{label}</span></td>"
            f"<td>{codes}</td>"
            "</tr>"
        )

    parts.extend(["</tbody>", "</table>"])
    return "\n".join(parts)


def _safe_query(cur, sql, params=()):
    try:
        cur.execute(sql, params)
        return cur.fetchall()
    except sqlite3.OperationalError:
        return None


def _latest_extract(cur):
    rows = _safe_query(
        cur,
        "SELECT ts, details FROM events "
        "WHERE event_type = 'extract_summary' "
        "ORDER BY id DESC LIMIT 1"
    )
    if not rows:
        return None
    return rows[0]


def _event_counts(cur):
    rows = _safe_query(
        cur,
        "SELECT event_type, COUNT(*) FROM events "
        "WHERE event_type LIKE 'scan_%' "
        "GROUP BY event_type ORDER BY event_type"
    )
    return rows or []


def _recent_events(cur, event_type, limit=50):
    rows = _safe_query(
        cur,
        "SELECT ts, code, page_number, details FROM events "
        "WHERE event_type = ? ORDER BY id DESC LIMIT ?",
        (event_type, limit)
    )
    return rows or []


def _render_report():
    with _connect() as conn:
        cur = conn.cursor()
        extract_row = _latest_extract(cur)
        counts = _event_counts(cur)
        missing_events = _recent_events(cur, "scan_error_missing_pages", 20)
        other_lot = _recent_events(cur, "scan_error_other_lot", 20)
        not_found = _recent_events(cur, "scan_error_not_found", 20)
        duplicate_resolution = _recent_events(
            cur, "scan_resolution_already_scanned", 20
        )
        other_lot_resolution = _recent_events(cur, "scan_resolution_other_lot", 20)

    parts = [
        "<div class='top'>",
        "<a href='/'><- Volver</a>",
        "<strong>Reporte</strong>",
        "</div>",
    ]

    if extract_row:
        ts, details = extract_row
        try:
            info = json.loads(details)
        except Exception:
            info = {}
        parts.append("<h3>Ultima extraccion</h3>")
        parts.append("<table><tbody>")
        parts.append(f"<tr><td>Fecha</td><td>{html.escape(ts)}</td></tr>")
        for key in (
            "pdf",
            "start_page",
            "end_page",
            "total_pages",
            "pages_processed",
            "codes_found",
            "inserted",
            "duplicates",
        ):
            if key in info:
                parts.append(
                    f"<tr><td>{html.escape(key)}</td>"
                    f"<td>{html.escape(str(info[key]))}</td></tr>"
                )
        parts.append("</tbody></table>")
    else:
        parts.append("<p>No hay resumen de extraccion.</p>")

    parts.append("<h3>Eventos de escaneo</h3>")
    if counts:
        parts.append("<table><thead><tr><th>Tipo</th><th>Conteo</th></tr></thead><tbody>")
        for event_type, count in counts:
            parts.append(
                f"<tr><td>{html.escape(event_type)}</td><td>{count}</td></tr>"
            )
        parts.append("</tbody></table>")
    else:
        parts.append("<p>No hay eventos de escaneo.</p>")

    def render_event_table(title, rows):
        parts.append(f"<h4>{html.escape(title)}</h4>")
        if not rows:
            parts.append("<p>Sin eventos.</p>")
            return
        parts.append("<table><thead><tr><th>Fecha</th><th>Codigo</th><th>Pagina</th><th>Detalles</th></tr></thead><tbody>")
        for ts, code, page_number, details in rows:
            parts.append(
                "<tr>"
                f"<td>{html.escape(ts)}</td>"
                f"<td>{html.escape(code or '')}</td>"
                f"<td>{html.escape(str(page_number) if page_number is not None else '')}</td>"
                f"<td>{html.escape(details or '')}</td>"
                "</tr>"
            )
        parts.append("</tbody></table>")

    render_event_table("Hojas saltadas", missing_events)
    render_event_table("Hojas de otro lote", other_lot)
    render_event_table("Codigo no encontrado", not_found)
    render_event_table("Clasificacion de duplicadas", duplicate_resolution)
    render_event_table("Clasificacion de otro lote", other_lot_resolution)

    return "\n".join(parts)


def _page_detail(pdf_id, page_number):
    with _connect() as conn:
        cur = conn.cursor()
        cur.execute("SELECT file_name FROM pdf_files WHERE id = ?", (pdf_id,))
        row = cur.fetchone()
        file_name = row[0] if row else f"PDF {pdf_id}"
        cur.execute(
            "SELECT code, scanned FROM pages WHERE pdf_id = ? AND page_number = ? ORDER BY code",
            (pdf_id, page_number),
        )
        rows = cur.fetchall()

    parts = [
        "<div class='top'>",
        "<a href='/'><- Volver</a>",
        f"<strong>{html.escape(file_name)} - Pagina {page_number}</strong>",
        "</div>",
        "<table>",
        "<thead><tr><th>Codigo</th><th>Estado</th></tr></thead>",
        "<tbody>",
    ]

    for code, scanned in rows:
        badge = "ok" if scanned else "pend"
        label = "Escaneado" if scanned else "Pendiente"
        parts.append(
            "<tr>"
            f"<td>{html.escape(code)}</td>"
            f"<td><span class='badge {badge}'>{label}</span></td>"
            "</tr>"
        )

    parts.extend(["</tbody>", "</table>"])
    return "\n".join(parts)


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        ensure_dirs()
        if not DB_PATH.exists():
            body = "<h2>No se encontro la base de datos</h2>"
            html_text = _render_page("DB Viewer", body)
            self._send(html_text, 404)
            return

        parsed = urlparse(self.path)
        if parsed.path == "/":
            params = parse_qs(parsed.query)
            status = params.get("status", ["todas"])[0]
            body = _summary_table(status)
            html_text = _render_page("Control DB", body)
            self._send(html_text)
            return

        if parsed.path == "/report":
            body = _render_report()
            html_text = _render_page("Reporte", body)
            self._send(html_text)
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
                body = _page_detail(pdf_id, page_number)
                html_text = _render_page(f"PDF {pdf_id} Pagina {page_number}", body)
                self._send(html_text)
                return

        self._send(_render_page("No encontrado", "<h2>404</h2>"), 404)

    def _send(self, content, status=200):
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(content.encode("utf-8"))


def main():
    server = HTTPServer(("127.0.0.1", 8000), Handler)
    print("Servidor en http://127.0.0.1:8000")
    server.serve_forever()


if __name__ == "__main__":
    main()
