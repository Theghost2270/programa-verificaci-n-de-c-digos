from pathlib import Path
import shutil

ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT_DIR / "data"
DB_PATH = DATA_DIR / "control.db"
PDF_DIR = DATA_DIR / "pdfs"

LEGACY_DB_PATH = ROOT_DIR / "control.db"
LEGACY_PDF_DIR = ROOT_DIR / "pdfs"


def ensure_dirs():
    DATA_DIR.mkdir(exist_ok=True)
    PDF_DIR.mkdir(exist_ok=True)

    if LEGACY_DB_PATH.exists() and not DB_PATH.exists():
        shutil.move(str(LEGACY_DB_PATH), str(DB_PATH))

    if LEGACY_PDF_DIR.exists() and LEGACY_PDF_DIR.is_dir():
        for item in LEGACY_PDF_DIR.iterdir():
            target = PDF_DIR / item.name
            if not target.exists():
                shutil.move(str(item), str(target))
