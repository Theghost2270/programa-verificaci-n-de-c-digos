from control.database.db import get_connection

MAX_MISSING_PAGES = 10
START_PAGE = None
END_PAGE = None


def set_page_range(start_page=1, end_page=None):
    global START_PAGE, END_PAGE
    if start_page is None or start_page == "":
        START_PAGE = None
    else:
        START_PAGE = max(1, int(start_page))
    END_PAGE = int(end_page) if end_page not in (None, "") else None


def process_scan(scanned_code):
    global START_PAGE
    conn = get_connection()
    try:
        cur = conn.cursor()
        scanned_code = scanned_code.upper()

        cur.execute(
            "SELECT page_number, scanned FROM pages WHERE code = ?",
            (scanned_code,)
        )
        row = cur.fetchone()

        if not row:
            return "ERROR", "Codigo no existe en el PDF"

        page_number, scanned = row

        if scanned:
            return "ERROR", f"Hoja {page_number} ya fue revisada"

        if START_PAGE is None:
            START_PAGE = page_number
        elif page_number < START_PAGE:
            return "ERROR", (
                f"Hoja {page_number} es de un lote anterior "
                f"(inicio en {START_PAGE})"
            )

        cur.execute(
            "SELECT DISTINCT page_number FROM pages "
            "WHERE scanned = 0 AND page_number < ? AND page_number >= ? "
            "ORDER BY page_number",
            (page_number, START_PAGE)
        )
        missing_pages = [row[0] for row in cur.fetchall()]

        if missing_pages:
            shown = missing_pages[:MAX_MISSING_PAGES]
            missing_str = ", ".join(str(p) for p in shown)
            extra = len(missing_pages) - len(shown)
            if extra > 0:
                return "ERROR", (
                    f"Faltan hojas anteriores: {missing_str} "
                    f"(+{extra} mas)"
                )
            return "ERROR", f"Faltan hojas anteriores: {missing_str}"

        cur.execute(
            "UPDATE pages SET scanned = 1 WHERE page_number = ?",
            (page_number,)
        )
        conn.commit()

        return "OK", f"Hoja {page_number} verificada correctamente"

    finally:
        conn.close()
