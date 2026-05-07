param(
    [string]$PythonExe = "py -3",
    [switch]$Clean
)

$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

function Invoke-Step {
    param([string]$Command)
    Invoke-Expression $Command
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed ($LASTEXITCODE): $Command"
    }
}

if ($Clean) {
    Remove-Item -Recurse -Force -ErrorAction SilentlyContinue "$projectRoot\build"
    Remove-Item -Recurse -Force -ErrorAction SilentlyContinue "$projectRoot\dist"
}

Write-Host "Installing build dependencies..."
Invoke-Step "$PythonExe -m pip install -U pip pyinstaller"
Invoke-Step "$PythonExe -m pip install -e ."

Write-Host "Building control.exe..."
Invoke-Step "$PythonExe -m PyInstaller --noconfirm --clean --name control --onefile --console --paths src src\control\main.py"

Write-Host "Building control-db.exe..."
Invoke-Step "$PythonExe -m PyInstaller --noconfirm --clean --name control-db --onefile --console --paths src --add-data 'src\control\web;control\web' src\control\db_web.py"

Write-Host ""
Write-Host "Build complete:"
Write-Host "  $projectRoot\dist\control.exe"
Write-Host "  $projectRoot\dist\control-db.exe"
