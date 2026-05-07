import argparse
import sqlite3
import sys
from pathlib import Path

from control.database.db import init_db
from control.data.pdf_extractor import extract_pdf, list_loaded_pdfs
from control.config import PDF_DIR, ensure_dirs
from control.reporting import export_audit_csv
from control.ui.console import run_console

SEQUENCE_MODE = "secuencia"
VERIFICATION_MODE = "verificacion"


def _build_parser():
    parser = argparse.ArgumentParser(prog="control")
    subparsers = parser.add_subparsers(dest="command")

    report_parser = subparsers.add_parser("report", help="Exporta reporte de auditoria")
    report_parser.add_argument(
        "--csv",
        action="store_true",
        help="Exporta reporte en CSV",
    )
    report_parser.add_argument(
        "--output",
        help="Ruta de salida del CSV (opcional)",
    )
    return parser


def _run_report_command(args):
    if not args.csv:
        print("Usa: control report --csv [--output ruta.csv]")
        return 1
    output_path = export_audit_csv(output_path=args.output)
    print(f"Reporte CSV generado: {output_path}")
    return 0


def _show_loaded_cache():
    loaded = list_loaded_pdfs()
    if not loaded:
        print("Cache cargada: 0 PDFs")
        return
    print(f"Cache cargada: {len(loaded)} PDFs")
    for idx, row in enumerate(loaded[:5], start=1):
        file_name, _file_path, codes = row
        print(f"  {idx}. {file_name} ({codes} codigos)")
    if len(loaded) > 5:
        print(f"  ... y {len(loaded) - 5} mas")


def _choose_pdfs():
    files = [p for p in PDF_DIR.iterdir() if p.suffix.lower() == ".pdf"]
    files.sort(key=lambda p: p.name.lower())

    if not files:
        print(f"No hay PDFs en la carpeta '{PDF_DIR}'")
        return []

    if len(files) == 1:
        print(f"Usando PDF: {files[0].name}")
        return [str(files[0])]

    print("PDFs disponibles:")
    print("0. Cargar todos")
    for idx, path in enumerate(files, start=1):
        print(f"{idx}. {path.name}")
    print("Seleccion multiple: 1,3,5 o rangos 2-6")

    def parse_selection(raw):
        text = raw.strip().lower()
        if not text:
            return None
        if text in {"0", "all", "todos"}:
            return list(range(1, len(files) + 1))

        selected = set()
        tokens = [token.strip() for token in text.split(",") if token.strip()]
        if not tokens:
            return None

        for token in tokens:
            if "-" in token:
                parts = token.split("-", 1)
                if len(parts) != 2 or not parts[0].isdigit() or not parts[1].isdigit():
                    return None
                start = int(parts[0])
                end = int(parts[1])
                if start > end or start < 1 or end > len(files):
                    return None
                selected.update(range(start, end + 1))
            else:
                if not token.isdigit():
                    return None
                index = int(token)
                if index < 1 or index > len(files):
                    return None
                selected.add(index)

        return sorted(selected)

    while True:
        choice = input("Selecciona PDF(s): ").strip()
        selected = parse_selection(choice)
        if selected is None:
            print("Opcion invalida")
            continue
        return [str(files[index - 1]) for index in selected]


def _print_progress(pdf_path, processed, total):
    name = Path(pdf_path).name
    print(f"Indexando {name}: pagina {processed}/{total}")


def _choose_mode():
    print("Modo de trabajo:")
    print("1. Revisar secuencia")
    print("2. Verificacion")
    while True:
        choice = input("Selecciona modo (1-2): ").strip()
        if choice == "1":
            return SEQUENCE_MODE
        if choice == "2":
            return VERIFICATION_MODE
        print("Opcion invalida")


def main():
    parser = _build_parser()
    args = parser.parse_args(sys.argv[1:])

    ensure_dirs()
    try:
        init_db()
    except sqlite3.OperationalError:
        print("Base de datos bloqueada. Cierra otras instancias y vuelve a intentar.")
        return 1
    except sqlite3.IntegrityError as exc:
        print(
            "Se detecto un problema de integridad en la base de datos. "
            "Genera respaldo y revisa la consistencia antes de continuar."
        )
        print(f"Detalle: {exc}")
        return 1

    if args.command == "report":
        return _run_report_command(args)

    _show_loaded_cache()

    pdf_paths = _choose_pdfs()
    if not pdf_paths:
        print("Continuando solo con lo que ya esta en cache")
    else:
        indexed = 0
        reused = 0
        for pdf_path in pdf_paths:
            try:
                extracted = extract_pdf(
                    pdf_path,
                    progress_callback=_print_progress,
                )
            except sqlite3.OperationalError:
                print(
                    "No se pudo indexar por bloqueo de base de datos. "
                    "Cierra otras instancias y vuelve a intentar."
                )
                return
            if extracted:
                indexed += 1
            else:
                reused += 1
        print(f"PDFs indexados: {indexed} | cache reutilizada: {reused}")

    loaded = list_loaded_pdfs()
    if not loaded:
        print("No hay PDFs cargados para escanear")
        return 0

    mode = _choose_mode()
    print(f"Escaneo activo sobre {len(loaded)} PDF(s) cargado(s)")
    run_console(mode=mode)
    return 0


if __name__ == "__main__":
    sys.exit(main())
