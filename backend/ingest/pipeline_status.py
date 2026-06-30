"""
pipeline_status.py — record daily pipeline runs to the `pipeline_runs` table.
============================================================================

Gives the health check / admin dashboard a definitive answer to "did this
pipeline actually run today?" — instead of inferring it from data timestamps.
This is what catches the "Windows PC was asleep so nothing ran" case: if no
row appears for `local_daily` in the last 24h, the job simply didn't run.

Best-effort by design: every function swallows its own errors and prints a
warning, so instrumentation can NEVER break a data pipeline.

Usage (see daily_update.py for the full pattern):

    import pipeline_status
    run_id = pipeline_status.start_run("cloud_daily")
    ...
    pipeline_status.finish_run(run_id, "success", steps=[...], failed_steps=[])
"""

import os
import json
import socket
import psycopg2

# pipeline keys used across the codebase
CLOUD_DAILY = "cloud_daily"      # daily_update.py        (Railway cron)
LOCAL_DAILY = "local_daily"      # daily_update_local.py  (Windows Task Scheduler)
PUZZLE_GEN  = "puzzle_gen"       # generate_daily.py      (Railway cron)

_DDL = """
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id           SERIAL PRIMARY KEY,
    pipeline     TEXT NOT NULL,
    status       TEXT NOT NULL,            -- running | success | partial | failed
    host         TEXT,
    started_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at  TIMESTAMPTZ,
    duration_s   REAL,
    steps        JSONB,                    -- [{"label":..,"ok":bool,"skipped":bool}]
    failed_steps TEXT[],
    error        TEXT,
    created_at   TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_pipeline_started
    ON pipeline_runs (pipeline, started_at DESC);
"""


def _conn():
    return psycopg2.connect(os.getenv("DATABASE_URL"), connect_timeout=15)


def ensure_table(cur):
    cur.execute(_DDL)


def start_run(pipeline: str):
    """Insert a 'running' row and return its id (or None on failure)."""
    try:
        conn = _conn()
        cur = conn.cursor()
        ensure_table(cur)
        cur.execute(
            "INSERT INTO pipeline_runs (pipeline, status, host) "
            "VALUES (%s, 'running', %s) RETURNING id",
            (pipeline, socket.gethostname()))
        rid = cur.fetchone()[0]
        conn.commit()
        cur.close(); conn.close()
        return rid
    except Exception as e:
        print(f"[pipeline_status] start_run({pipeline}) failed: {e}", flush=True)
        return None


def finish_run(run_id, status: str, steps=None, failed_steps=None, error=None):
    """Mark a run finished with final status + per-step detail."""
    if run_id is None:
        return
    try:
        conn = _conn()
        cur = conn.cursor()
        cur.execute(
            """UPDATE pipeline_runs
                  SET status       = %s,
                      finished_at  = NOW(),
                      duration_s   = EXTRACT(EPOCH FROM (NOW() - started_at)),
                      steps        = %s,
                      failed_steps = %s,
                      error        = %s
                WHERE id = %s""",
            (status,
             json.dumps(steps) if steps is not None else None,
             list(failed_steps) if failed_steps else None,
             error, run_id))
        conn.commit()
        cur.close(); conn.close()
    except Exception as e:
        print(f"[pipeline_status] finish_run({run_id}) failed: {e}", flush=True)
