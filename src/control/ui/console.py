import os
import struct
import math
import subprocess

try:
    import winsound
except ImportError:
    winsound = None

from control.logic.judge import (
    classify_scan_error,
    process_scan,
    set_page_range,
    reset_start_page,
    get_start_page,
    reset_scans,
)

RESOLUTION_OPTIONS = {
    "1": "falso_duplicado",
    "2": "hoja_descartada",
    "3": "otro",
}


def _build_tone_wav(frequency=1500, duration_ms=280, volume=0.95, sample_rate=44100):
    samples = int(sample_rate * (duration_ms / 1000.0))
    data = bytearray()
    amplitude = int(32767 * volume)
    for i in range(samples):
        t = i / sample_rate
        sample = int(amplitude * math.sin(2.0 * math.pi * frequency * t))
        data += struct.pack("<h", sample)

    byte_rate = sample_rate * 2
    block_align = 2
    subchunk2_size = len(data)
    chunk_size = 36 + subchunk2_size
    header = struct.pack(
        "<4sI4s4sIHHIIHH4sI",
        b"RIFF",
        chunk_size,
        b"WAVE",
        b"fmt ",
        16,
        1,
        1,
        sample_rate,
        byte_rate,
        block_align,
        16,
        b"data",
        subchunk2_size,
    )
    return header + data


def _beep_error():
    if winsound is not None:
        try:
            tone_1 = _build_tone_wav(frequency=1500, duration_ms=280, volume=0.95)
            tone_2 = _build_tone_wav(frequency=1800, duration_ms=220, volume=0.95)
            winsound.PlaySound(tone_1, winsound.SND_MEMORY)
            winsound.PlaySound(tone_2, winsound.SND_MEMORY)
            return
        except RuntimeError:
            pass

        try:
            winsound.Beep(1600, 280)
            winsound.Beep(2000, 220)
            return
        except RuntimeError:
            pass

        for wav_name in ("Windows Critical Stop.wav", "Alarm01.wav", "Alarm02.wav"):
            wav_path = os.path.join(r"C:\Windows\Media", wav_name)
            if not os.path.exists(wav_path):
                continue
            try:
                winsound.PlaySound(
                    wav_path, winsound.SND_FILENAME | winsound.SND_ASYNC
                )
                return
            except RuntimeError:
                pass

    try:
        subprocess.run(
            [
                "powershell",
                "-NoProfile",
                "-Command",
                "(New-Object -ComObject SAPI.SpVoice).Speak('Error') | Out-Null",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
            timeout=2,
        )
        return
    except Exception:
        pass

    print("\a", end="", flush=True)


def _ask_resolution(result):
    if result["error_type"] not in {"already_scanned", "other_lot"}:
        return

    print("Clasifica este caso:")
    print("1. falso duplicado")
    print("2. hoja descartada")
    print("3. otro")
    choice = input("Opcion (1-3, Enter para omitir): ").strip()
    if not choice:
        return

    resolution = RESOLUTION_OPTIONS.get(choice)
    if not resolution:
        print("Opcion invalida, se omite clasificacion")
        return

    note = None
    if resolution == "otro":
        note = input("Describe el caso: ").strip() or None

    classify_scan_error(
        result["error_type"],
        resolution,
        code=result.get("code"),
        page_number=result.get("page_number"),
        file_name=result.get("file_name"),
        note=note,
    )
    print("Clasificacion guardada")


def run_console(start_page=None):
    set_page_range(start_page)
    print("=== CONTROL DE HOJAS ===")
    print("Escanea un codigo o escribe 'exit'")
    print("Comandos: 'reset' para reiniciar el inicio del lote")
    print("Comandos: 'reset-scan' para limpiar hojas escaneadas")
    print("Comandos: 'status' para ver el inicio actual")
    print("Comandos: 'beep' para probar sonido")
    while True:
        code = input("> ").strip()

        if code.lower() == "exit":
            break
        if code.lower() == "reset":
            reset_start_page()
            print("Inicio del lote reiniciado")
            continue
        if code.lower() == "reset-scan":
            reset_scans()
            print("Se limpiaron las hojas escaneadas")
            continue
        if code.lower() == "status":
            current = get_start_page()
            if current is None:
                print("Inicio actual: (sin definir)")
            elif isinstance(current, dict):
                if not current:
                    print("Inicio actual: (sin definir)")
                else:
                    print(f"Inicio actual por PDF: {current}")
            else:
                print(f"Inicio actual: {current}")
            continue
        if code.lower() == "beep":
            _beep_error()
            print("Beep enviado")
            continue
        result = process_scan(code)
        print(f"[{result['status']}] {result['message']}")
        if result["status"] == "ERROR":
            _beep_error()
            _ask_resolution(result)
