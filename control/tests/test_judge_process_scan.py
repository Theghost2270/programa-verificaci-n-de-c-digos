import importlib


def _reload_modules():
    import control.config as config
    import control.database.db as db
    import control.logic.judge as judge

    importlib.reload(config)
    importlib.reload(db)
    importlib.reload(judge)
    return db, judge


def _seed_pdf(cur, file_name, file_path, signature):
    cur.execute(
        "INSERT INTO pdf_files (file_name, file_path, signature) VALUES (?, ?, ?)",
        (file_name, file_path, signature),
    )
    return cur.lastrowid


def _seed_page(cur, page_number, code, pdf_id, scanned=0):
    cur.execute(
        "INSERT INTO pages (page_number, code, scanned, pdf_id) VALUES (?, ?, ?, ?)",
        (page_number, code, scanned, pdf_id),
    )


def test_process_scan_ok_marks_page_scanned(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTROL_DATA_DIR", str(tmp_path))
    db, judge = _reload_modules()
    db.init_db(reset=True)

    conn = db.get_connection()
    try:
        cur = conn.cursor()
        pdf_id = _seed_pdf(cur, "a.pdf", "C:/a.pdf", "sig-a")
        _seed_page(cur, 1, "ABC123", pdf_id, scanned=0)
        conn.commit()
    finally:
        conn.close()

    result = judge.process_scan("abc123", mode="verificacion")

    assert result["status"] == "OK"
    assert result["page_number"] == 1
    assert result["file_name"] == "a.pdf"

    conn = db.get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT scanned FROM pages WHERE pdf_id = ? AND code = ?", (pdf_id, "ABC123"))
        assert cur.fetchone()[0] == 1
    finally:
        conn.close()


def test_process_scan_not_found(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTROL_DATA_DIR", str(tmp_path))
    db, judge = _reload_modules()
    db.init_db(reset=True)

    result = judge.process_scan("NOEXISTE", mode="verificacion")
    assert result["status"] == "ERROR"
    assert result["message"] == "Codigo no existe en PDFs cargados"


def test_process_scan_ambiguous_code_across_pdfs(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTROL_DATA_DIR", str(tmp_path))
    db, judge = _reload_modules()
    db.init_db(reset=True)

    conn = db.get_connection()
    try:
        cur = conn.cursor()
        pdf1 = _seed_pdf(cur, "a.pdf", "C:/a.pdf", "sig-a")
        pdf2 = _seed_pdf(cur, "b.pdf", "C:/b.pdf", "sig-b")
        _seed_page(cur, 1, "DUP111", pdf1, scanned=0)
        _seed_page(cur, 2, "DUP111", pdf2, scanned=0)
        conn.commit()
    finally:
        conn.close()

    result = judge.process_scan("dup111", mode="verificacion")
    assert result["status"] == "ERROR"
    assert result["error_type"] == "ambiguous_code"
    assert "multiples PDFs" in result["message"]


def test_process_scan_already_scanned(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTROL_DATA_DIR", str(tmp_path))
    db, judge = _reload_modules()
    db.init_db(reset=True)

    conn = db.get_connection()
    try:
        cur = conn.cursor()
        pdf_id = _seed_pdf(cur, "a.pdf", "C:/a.pdf", "sig-a")
        _seed_page(cur, 3, "DONE33", pdf_id, scanned=1)
        conn.commit()
    finally:
        conn.close()

    result = judge.process_scan("DONE33", mode="verificacion")
    assert result["status"] == "ERROR"
    assert result["error_type"] == "already_scanned"
    assert "ya fue revisada" in result["message"]


def test_process_scan_other_lot(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTROL_DATA_DIR", str(tmp_path))
    db, judge = _reload_modules()
    db.init_db(reset=True)
    judge.reset_start_page()

    conn = db.get_connection()
    try:
        cur = conn.cursor()
        pdf_id = _seed_pdf(cur, "lote.pdf", "C:/lote.pdf", "sig-l")
        _seed_page(cur, 5, "START05", pdf_id, scanned=0)
        _seed_page(cur, 4, "PREV04", pdf_id, scanned=0)
        conn.commit()
    finally:
        conn.close()

    first = judge.process_scan("START05", mode="verificacion")
    assert first["status"] == "OK"

    second = judge.process_scan("PREV04", mode="verificacion")
    assert second["status"] == "ERROR"
    assert second["error_type"] == "other_lot"
    assert "lote anterior" in second["message"]


def test_process_scan_missing_previous_pages(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTROL_DATA_DIR", str(tmp_path))
    db, judge = _reload_modules()
    db.init_db(reset=True)
    judge.set_page_range(1)

    conn = db.get_connection()
    try:
        cur = conn.cursor()
        pdf_id = _seed_pdf(cur, "seq.pdf", "C:/seq.pdf", "sig-s")
        _seed_page(cur, 1, "P001AA", pdf_id, scanned=0)
        _seed_page(cur, 2, "P002AA", pdf_id, scanned=0)
        _seed_page(cur, 3, "P003AA", pdf_id, scanned=0)
        conn.commit()
    finally:
        conn.close()

    result = judge.process_scan("P003AA", mode="verificacion")
    assert result["status"] == "ERROR"
    assert result["error_type"] is None
    assert "Faltan hojas anteriores" in result["message"]
    assert "1, 2" in result["message"]


def test_process_scan_sequence_mode_ignores_start_page_rule(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTROL_DATA_DIR", str(tmp_path))
    db, judge = _reload_modules()
    db.init_db(reset=True)
    # Simula inicio manual de lote en otra pagina; en secuencia no debe aplicar.
    judge.set_page_range(5)

    conn = db.get_connection()
    try:
        cur = conn.cursor()
        pdf_id = _seed_pdf(cur, "seq2.pdf", "C:/seq2.pdf", "sig-s2")
        _seed_page(cur, 4, "P004BB", pdf_id, scanned=0)
        _seed_page(cur, 5, "P005BB", pdf_id, scanned=0)
        conn.commit()
    finally:
        conn.close()

    result = judge.process_scan("P004BB", mode="secuencia")
    assert result["status"] == "OK"
    assert result["error_type"] is None
