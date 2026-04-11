@echo off
set PROJ_DIR=%~dp0
cd /d "%PROJ_DIR%"

if not exist "logs" mkdir logs

if exist ".venv\Scripts\activate.bat" (
    call .venv\Scripts\activate.bat
) else if exist "venv\Scripts\activate.bat" (
    call venv\Scripts\activate.bat
)

set PYTHONIOENCODING=utf-8
chcp 65001 > nul

echo [%date% %time%] Starting ydkball local daily update... >> logs\daily_local.log 2>&1

python backend\ingest\daily_update_local.py >> logs\daily_local.log 2>&1

echo [%date% %time%] Done. Exit code: %errorlevel% >> logs\daily_local.log 2>&1