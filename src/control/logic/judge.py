import json

from control.database.db import get_connection

MAX_MISSING_PAGES = 10
START_PAGE_BY_PDF = {}
VALID_RESOLUTIONS = {"falso_duplicado", "hoja_descartada", "otro"}


def set_page_range(start_page=None):
    START_PAGE_BY_PDF.clear()
    if start_page is None or start_page == "":
        return
    # Kept for compatibility: applies to the first PDF scanned.
    START_PAGE_BY_PDF["_default"] = max(1, int(start_page))


def reset_start_page():
    START_PAGE_BY_PDF.clear()


def get_start_page():
    return dict(START_PAGE_BY_PDF)


def reset_scans():
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("UPDATE pages SET scanned = 0")
        _log_event(cur, "reset_scans")
        conn.commit()
        START_PAGE_BY_PDF.clear()
    finally:
        conn.close()


def _log_event(cur, event_type, code=None, page_number=None, details=None):
    cur.execute(
        "INSERT INTO events (event_type, code, page_number, details) "
        "VALUES (?, ?, ?, ?)",
        (
            event_type,
            code,
            page_number,
            json.dumps(details) if details is not None else None,
        ),
    )


def _result(
    status,
    message,
    error_type=None,
    code=None,
    page_number=None,
    file_name=None,
    pdf_id=None,
):
    return {
        "status": status,
        "message": message,
        "error_type": error_type,
        "code": code,
        "page_number": page_number,
        "file_name": file_name,
        "pdf_id": pdf_id,
    }


def classify_scan_error(
    error_type, resolution, code=None, page_number=None, file_name=None, note=None
):
    if resolution not in VALID_RESOLUTIONS:
        raise ValueError(f"Resolucion invalida: {resolution}")

    if error_type == "already_scanned":
        event_type = "scan_resolution_already_scanned"
    elif error_type == "other_lot":
        event_type = "scan_resolution_other_lot"
    else:
        raise ValueError(f"Tipo de error no soportado: {error_type}")

    details = {"resolution": resolution}
    if file_name:
        details["file_name"] = file_name
    if note:
        details["note"] = note

    conn = get_connection()
    try:
        cur = conn.cursor()
        _log_event(
            cur,
            event_type,
            code=code,
            page_number=page_number,
            details=details,
        )
        conn.commit()
    finally:
        conn.close()


def _resolve_start_page(pdf_id, page_number):
    start_page = START_PAGE_BY_PDF.get(pdf_id)
    if start_page is None:
        default_start = START_PAGE_BY_PDF.pop("_default", None)
        start_page = default_start if default_start is not None else page_number
        START_PAGE_BY_PDF[pdf_id] = start_page
    return start_page


def process_scan(scanned_code):
    conn = get_connection()
    try:
        cur = conn.cursor()
        scanned_code = scanned_code.upper()

        cur.execute(
            """
            SELECT p.pdf_id, p.page_number, p.scanned, f.file_name
            FROM pages p
            JOIN pdf_files f ON f.id = p.pdf_id
            WHERE p.code = ?
            ORDER BY f.file_name, p.page_number
            """,
            (scanned_code,),
        )
        rows = cur.fetchall()

        if not rows:
            _log_event(cur, "scan_error_not_found", code=scanned_code)
            conn.commit()
            return _result("ERROR", "Codigo no existe en PDFs cargados", code=scanned_code)

        pdf_ids = {row[0] for row in rows}
        if len(pdf_ids) > 1:
            files = sorted({row[3] for row in rows})
            _log_event(
                cur,
                "scan_error_ambiguous_code",
                code=scanned_code,
                details={"files": files},
            )
            conn.commit()
            return _result(
                "ERROR",
                f"Codigo en multiples PDFs: {', '.join(files)}",
                error_type="ambiguous_code",
                code=scanned_code,
            )

        pdf_id, page_number, scanned, file_name = rows[0]

        if scanned:
            _log_event(
                cur,
                "scan_error_already_scanned",
                code=scanned_code,
                page_number=page_number,
                details={"file_name": file_name, "pdf_id": pdf_id},
            )
            conn.commit()
            return _result(
                "ERROR",
                f"Hoja {page_number} ya fue revisada ({file_name})",
                error_type="already_scanned",
                code=scanned_code,
                page_number=page_number,
                file_name=file_name,
                pdf_id=pdf_id,
            )

        start_page = _resolve_start_page(pdf_id, page_number)
        if page_number < start_page:
            _log_event(
                cur,
                "scan_error_other_lot",
                code=scanned_code,
                page_number=page_number,
                details={
                    "start_page": start_page,
                    "file_name": file_name,
                    "pdf_id": pdf_id,
                },
            )
            conn.commit()
            return _result(
                "ERROR",
                f"Hoja {page_number} es de un lote anterior en {file_name} (inicio {start_page})",
                error_type="other_lot",
                code=scanned_code,
                page_number=page_number,
                file_name=file_name,
                pdf_id=pdf_id,
            )

        cur.execute(
            """
            SELECT DISTINCT page_number
            FROM pages
            WHERE pdf_id = ? AND scanned = 0 AND page_number < ? AND page_number >= ?
            ORDER BY page_number
            """,
            (pdf_id, page_number, start_page),
        )
        missing_pages = [row[0] for row in cur.fetchall()]

        if missing_pages:
            shown = missing_pages[:MAX_MISSING_PAGES]
            missing_str = ", ".join(str(p) for p in shown)
            extra = len(missing_pages) - len(shown)
            _log_event(
                cur,
                "scan_error_missing_pages",
                code=scanned_code,
                page_number=page_number,
                details={
                    "missing_pages": missing_pages,
                    "file_name": file_name,
                    "pdf_id": pdf_id,
                },
            )
            conn.commit()
            if extra > 0:
                return _result(
                    "ERROR",
                    f"Faltan hojas anteriores en {file_name}: {missing_str} (+{extra} mas)",
                    code=scanned_code,
                    page_number=page_number,
                    file_name=file_name,
                    pdf_id=pdf_id,
                )
            return _result(
                "ERROR",
                f"Faltan hojas anteriores en {file_name}: {missing_str}",
                code=scanned_code,
                page_number=page_number,
                file_name=file_name,
                pdf_id=pdf_id,
            )

        cur.execute(
            "UPDATE pages SET scanned = 1 WHERE pdf_id = ? AND page_number = ?",
            (pdf_id, page_number),
        )
        _log_event(
            cur,
            "scan_ok",
            code=scanned_code,
            page_number=page_number,
            details={"file_name": file_name, "pdf_id": pdf_id},
        )
        conn.commit()

        return _result(
            "OK",
            f"Hoja {page_number} verificada correctamente ({file_name})",
            code=scanned_code,
            page_number=page_number,
            file_name=file_name,
            pdf_id=pdf_id,
        )
    finally:
        conn.close()
