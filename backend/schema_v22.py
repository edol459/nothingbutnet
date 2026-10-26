"""
ydkball — Schema v22: resolvable content reports
=============================================================
python backend/schema_v22.py

`content_reports` has been written to since launch and never read: the only
references in server.py were the CREATE TABLE and the INSERT. Reports landed
in it and nobody could see them, while the app's confirmation dialog promised
"We'll review this content within 24 hours."

Resolving needs somewhere to record the outcome. Deleting the row would work
and lose the trail — which report prompted a removal is exactly what you want
later, and App Store guideline 1.2 expects a UGC app to act on reports, not
just collect them.

  resolved_at / resolved_by — who closed it and when; NULL means open.
  action                    — 'dismissed' or 'deleted', so a cleared queue
                              still says what was done.

Touches no hot table, so no lock dance is needed.

Safe to run multiple times.
"""
import os, sys
from dotenv import load_dotenv
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("DATABASE_URL not found."); sys.exit(1)

DDL = [
    "ALTER TABLE content_reports ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMPTZ",
    "ALTER TABLE content_reports ADD COLUMN IF NOT EXISTS resolved_by INTEGER",
    "ALTER TABLE content_reports ADD COLUMN IF NOT EXISTS action TEXT",
    # The queue is always read open-first, newest-first.
    """CREATE INDEX IF NOT EXISTS idx_content_reports_open
         ON content_reports (resolved_at, created_at DESC)""",
]


def run():
    print("Connecting to database...")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        for stmt in DDL:
            cur.execute(stmt)
        print("  content_reports ready")
        cur.execute("""SELECT COUNT(*) FILTER (WHERE resolved_at IS NULL) AS open,
                              COUNT(*) AS total FROM content_reports""")
        o, t = cur.fetchone()
        print(f"  reports: {t} total, {o} open")
        cur.close(); conn.close()
    except Exception as e:
        print(f"❌ {e}"); sys.exit(1)


if __name__ == "__main__":
    run()
