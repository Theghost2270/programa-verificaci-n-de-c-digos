import os

from control.database.db import init_db
from control.data.pdf_extractor import extract_pdf
from control.ui.console import run_console

PDF_DIR = "pdfs"

def _choose_pdf():
    files = [
        name for name in os.listdir(PDF_DIR)
        if name.lower().endswith(".pdf")
    ]
    files.sort()

    if not files:
        print(f"No hay PDFs en la carpeta '{PDF_DIR}'")
        return None

    if len(files) == 1:
        print(f"Usando PDF: {files[0]}")
        return os.path.join(PDF_DIR, files[0])

    print("PDFs disponibles:")
    for idx, name in enumerate(files, start=1):
        print(f"{idx}. {name}")

    while True:
        choice = input("Selecciona un PDF (numero): ").strip()
        if not choice:
            continue
        if choice.isdigit():
            index = int(choice)
            if 1 <= index <= len(files):
                return os.path.join(PDF_DIR, files[index - 1])
        print("Opcion invalida")


def main():
    init_db()

    os.makedirs(PDF_DIR, exist_ok=True)

    pdf_path = _choose_pdf()
    if not pdf_path:
        return

    extracted = extract_pdf(pdf_path)
    if extracted:
        print("PDF indexado")
    else:
        print("PDF ya indexado, usando cache")
    run_console()

if __name__ == "__main__":
    main()
