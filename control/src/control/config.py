import os
import shutil
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
APP_NAME = "Control"


def _app_data_root():
    override = os.environ.get("CONTROL_DATA_DIR")
    if override:
        return Path(override).expanduser().resolve()

    if getattr(sys, "frozen", False):
        local_app_data = Path(os.environ.get("LOCALAPPDATA", Path.home()))
        return local_app_data / APP_NAME

    return ROOT_DIR / "data"


DATA_DIR = _app_data_root()
DB_PATH = DATA_DIR / "control.db"
PDF_DIR = DATA_DIR / "pdfs"


def _legacy_paths():
    paths = []
    # Legacy layout in source tree
    paths.append((ROOT_DIR / "control.db", ROOT_DIR / "pdfs"))

    # Legacy layout near bundled executable
    if getattr(sys, "frozen", False):
        exe_dir = Path(sys.executable).resolve().parent
        paths.append((exe_dir / "control.db", exe_dir / "pdfs"))

    # Legacy layout in current working directory
    cwd = Path.cwd()
    paths.append((cwd / "control.db", cwd / "pdfs"))
    return paths


def ensure_dirs():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    PDF_DIR.mkdir(parents=True, exist_ok=True)

    for legacy_db_path, legacy_pdf_dir in _legacy_paths():
        if legacy_db_path.exists() and not DB_PATH.exists():
            shutil.move(str(legacy_db_path), str(DB_PATH))

        if legacy_pdf_dir.exists() and legacy_pdf_dir.is_dir():
            for item in legacy_pdf_dir.iterdir():
                target = PDF_DIR / item.name
                if not target.exists():
                    shutil.move(str(item), str(target))
