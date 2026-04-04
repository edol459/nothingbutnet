@echo off
:: ydkball — Local Daily Update Launcher
:: ======================================
:: Run this via Windows Task Scheduler daily.
::
:: Task Scheduler setup:
::   Action:           Start a program
::   Program/script:   C:\path\to\nothingbutnet\run_daily_local.bat
::   Start in:         C:\path\to\nothingbutnet
::
:: Update PROJ_DIR below to match where your project lives.

set PROJ_DIR=%~dp0

cd /d "%PROJ_DIR%"

:: Activate virtual environment if present (.venv or venv)
if exist ".venv\Scripts\activate.bat" (
    call .venv\Scripts\activate.bat
) else if exist "venv\Scripts\activate.bat" (
    call venv\Scripts\activate.bat
)

echo [%date% %time%] Starting ydkball local daily update...

python backend\ingest\daily_update_local.py >> logs\daily_local.log 2>&1

echo [%date% %time%] Done. Exit code: %errorlevel%
