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

## Datos

- PDFs: `data/pdfs/`
- Base de datos: `data/control.db`

El programa migra automaticamente si encuentra `control.db` o `pdfs/` en la raiz.
