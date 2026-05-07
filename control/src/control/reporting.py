import csv
import io
import json
from datetime import datetime
from pathlib import Path

from control.config import DATA_DIR
from control.database.db import get_connection


def _default_report_path():
    reports_dir = DATA_DIR / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    return reports_dir / f"audit-{stamp}.csv"


def _load_summary(cur):
    cur.execute("SELECT COUNT(*) FROM pdf_files")
    loaded_pdfs = cur.fetchone()[0]

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

    cur.execute("SELECT COUNT(*) FROM events")
    total_events = cur.fetchone()[0]

    return {
        "loaded_pdfs": loaded_pdfs,
        "total_codes": total_codes,
        "total_pages": total_pages,
        "scanned_pages": scanned_pages,
        "pending_pages": max(0, total_pages - scanned_pages),
        "total_duplicates": total_duplicates,
        "total_events": total_events,
    }


def _append_summary_rows(rows, summary):
    rows.append(
        {
            "section": "summary",
            "kind": "totals",
            "ts": datetime.now().isoformat(timespec="seconds"),
            "loaded_pdfs": summary["loaded_pdfs"],
            "total_codes": summary["total_codes"],
            "total_pages": summary["total_pages"],
            "scanned_pages": summary["scanned_pages"],
            "pending_pages": summary["pending_pages"],
            "total_duplicates": summary["total_duplicates"],
            "total_events": summary["total_events"],
        }
    )


def _append_loaded_pdfs_rows(cur, rows):
    cur.execute(
        """
        SELECT f.file_name, f.file_path, COUNT(p.code) AS codes
        FROM pdf_files f
        LEFT JOIN pages p ON p.pdf_id = f.id
        GROUP BY f.id, f.file_name, f.file_path
        ORDER BY f.loaded_at DESC, f.id DESC
        """
    )
    for file_name, file_path, codes in cur.fetchall():
        rows.append(
            {
                "section": "loaded_pdf",
                "kind": "pdf_cache",
                "file_name": file_name,
                "file_path": file_path,
                "codes": codes,
            }
        )


def _append_events_rows(cur, rows):
    cur.execute(
        "SELECT ts, event_type, code, page_number, details FROM events ORDER BY id DESC"
    )
    for ts, event_type, code, page_number, details in cur.fetchall():
        rows.append(
            {
                "section": "event",
                "kind": event_type,
                "ts": ts,
                "code": code,
                "page_number": page_number,
                "details": details,
            }
        )


def _append_duplicates_rows(cur, rows):
    cur.execute(
        """
        SELECT d.detected_at, d.code, d.duplicate_kind,
               n.file_name, d.new_page_number,
               e.file_name, d.existing_page_number
        FROM code_duplicates d
        LEFT JOIN pdf_files n ON n.id = d.new_pdf_id
        LEFT JOIN pdf_files e ON e.id = d.existing_pdf_id
        ORDER BY d.id DESC
        """
    )
    for (
        detected_at,
        code,
        duplicate_kind,
        new_file_name,
        new_page_number,
        existing_file_name,
        existing_page_number,
    ) in cur.fetchall():
        rows.append(
            {
                "section": "duplicate",
                "kind": duplicate_kind,
                "ts": detected_at,
                "code": code,
                "new_file_name": new_file_name,
                "new_page_number": new_page_number,
                "existing_file_name": existing_file_name,
                "existing_page_number": existing_page_number,
            }
        )


def _append_extract_summary_rows(cur, rows):
    cur.execute(
        "SELECT ts, details FROM events WHERE event_type = 'extract_summary' ORDER BY id DESC"
    )
    for ts, details in cur.fetchall():
        parsed = {}
        if details:
            try:
                parsed = json.loads(details)
            except Exception:
                parsed = {"raw_details": details}
        rows.append(
            {
                "section": "extract_summary",
                "kind": "extract_summary",
                "ts": ts,
                "details": json.dumps(parsed, ensure_ascii=False),
            }
        )


def build_audit_rows():
    rows = []
    with get_connection() as conn:
        cur = conn.cursor()
        summary = _load_summary(cur)
        _append_summary_rows(rows, summary)
        _append_loaded_pdfs_rows(cur, rows)
        _append_extract_summary_rows(cur, rows)
        _append_events_rows(cur, rows)
        _append_duplicates_rows(cur, rows)
    return rows


def _fieldnames():
    return [
        "section",
        "kind",
        "ts",
        "code",
        "page_number",
        "file_name",
        "file_path",
        "codes",
        "details",
        "new_file_name",
        "new_page_number",
        "existing_file_name",
        "existing_page_number",
        "loaded_pdfs",
        "total_codes",
        "total_pages",
        "scanned_pages",
        "pending_pages",
        "total_duplicates",
        "total_events",
    ]


def render_audit_csv_text():
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=_fieldnames(), extrasaction="ignore")
    writer.writeheader()
    writer.writerows(build_audit_rows())
    return buffer.getvalue()


def export_audit_csv(output_path=None):
    target = Path(output_path).expanduser().resolve() if output_path else _default_report_path()
    target.parent.mkdir(parents=True, exist_ok=True)

    with target.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=_fieldnames(), extrasaction="ignore")
        writer.writeheader()
        writer.writerows(build_audit_rows())

    return target
