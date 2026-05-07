# Control

Estructura recomendada para escalar y evitar problemas de imports.

## Setup

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -e .
```

## Ejecutar

```powershell
control
```

O:

```powershell
python -m control.main
```

## Ver la base (web)

```powershell
control-db
```

Luego abre:

```
http://127.0.0.1:8000
```

## Reporte CSV (auditoria)

```powershell
control report --csv
```

Opcionalmente puedes indicar salida:

```powershell
control report --csv --output C:\temp\auditoria.csv
```

## Datos

- PDFs: `data/pdfs/`
- Base de datos: `data/control.db`

El programa migra automaticamente si encuentra `control.db` o `pdfs/` en la raiz.

## Distribucion para otras PCs (sin Python)

1. Generar ejecutables:

```powershell
cd control
.\scripts\build.ps1 -PythonExe "py -3.14"
```

Resultado:

- `dist/control.exe`
- `dist/control-db.exe`

2. Crear instalador (Inno Setup):

- Abrir `installer/Control.iss` en Inno Setup Compiler.
- Compilar.
- Instalador generado en `installer/output/Control-Setup.exe`.

### Ruta de datos en app instalada

En modo instalado (exe), la app guarda datos en:

- `%LOCALAPPDATA%\Control\control.db`
- `%LOCALAPPDATA%\Control\pdfs\`

Puedes forzar otra ruta con:

```powershell
$env:CONTROL_DATA_DIR="D:\ControlData"
```
