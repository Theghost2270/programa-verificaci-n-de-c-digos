from control.logic.judge import process_scan, set_page_range

def run_console(start_page=None, end_page=None):
    set_page_range(start_page, end_page)
    print("=== CONTROL DE HOJAS ===")
    print("Escanea un codigo o escribe 'exit'")

    while True:
        code = input("> ").strip()

        if code.lower() == "exit":
            break

        status, message = process_scan(code)
        print(f"[{status}] {message}")
