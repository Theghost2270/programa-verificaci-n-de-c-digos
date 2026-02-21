import sqlite3
from pathlib import Path

from control.database.db import init_db
from control.data.pdf_extractor import extract_pdf, list_loaded_pdfs
from control.config import PDF_DIR, ensure_dirs
from control.ui.console import run_console

SEQUENCE_MODE = "secuencia"
VERIFICATION_MODE = "verificacion"


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

    while True:
        choice = input("Selecciona un PDF (numero): ").strip()
        if not choice:
            continue
        if choice == "0":
            return [str(p) for p in files]
        if choice.isdigit():
            index = int(choice)
            if 1 <= index <= len(files):
                return [str(files[index - 1])]
        print("Opcion invalida")


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
    ensure_dirs()
    try:
        init_db()
    except sqlite3.OperationalError:
        print("Base de datos bloqueada. Cierra otras instancias y vuelve a intentar.")
        return
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
        return

    mode = _choose_mode()
    print(f"Escaneo activo sobre {len(loaded)} PDF(s) cargado(s)")
    run_console(mode=mode)


if __name__ == "__main__":
    main()
