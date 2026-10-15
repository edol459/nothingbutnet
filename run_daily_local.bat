@echo off
REM ydkball - local daily update (Windows Task Scheduler)
REM
REM Launched by Task Scheduler. PROJ_DIR is this file's own folder, so moving
REM the repo does not break the job as long as the task points at this .bat.

set PROJ_DIR=%~dp0
cd /d "%PROJ_DIR%"

if not exist "logs" mkdir logs

if exist ".venv\Scripts\activate.bat" (
    call .venv\Scripts\activate.bat
) else if exist "venv\Scripts\activate.bat" (
    call venv\Scripts\activate.bat
) else (
    REM No virtualenv means system python, which probably lacks psycopg2 and
    REM dotenv - the pipeline then dies on import, before it can register the
    REM run, and the health report sees nothing at all. Say so in the log.
    echo [%date% %time%] WARNING: no .venv or venv found - falling back to system python >> logs\daily_local.log 2>&1
)

set PYTHONIOENCODING=utf-8
chcp 65001 > nul

echo [%date% %time%] Starting ydkball local daily update... >> logs\daily_local.log 2>&1

python backend\ingest\daily_update_local.py >> logs\daily_local.log 2>&1

REM Capture it on the very next line - any other command overwrites errorlevel.
set RC=%errorlevel%

echo [%date% %time%] Done. Exit code: %RC% >> logs\daily_local.log 2>&1

REM Hand the real code to Task Scheduler. Without this the .bat exits with the
REM echo above (always 0), so the task shows "success" however badly the run
REM failed - which is exactly how two days of missed runs went unnoticed.
exit /b %RC%
