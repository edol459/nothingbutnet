"""
ydkball — Schema v5: Leverage-Filtered Lineup Columns
=============================================================
python backend/schema_v5.py

Adds leverage-filtered stat columns to team_lineups:
  - min_lev   — non-garbage-time minutes played
  - ortg_lev  — offensive rating (non-garbage-time)
  - drtg_lev  — defensive rating (non-garbage-time)
  - net_lev   — net rating (non-garbage-time)

NULL means fetch_lineups_pbp.py hasn't been run yet for that team/season.
Safe to run multiple times (ADD COLUMN IF NOT EXISTS).
"""
import os, sys
from dotenv import load_dotenv
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("DATABASE_URL not found."); sys.exit(1)

SQL = """
ALTER TABLE team_lineups ADD COLUMN IF NOT EXISTS min_lev  REAL;
ALTER TABLE team_lineups ADD COLUMN IF NOT EXISTS ortg_lev REAL;
ALTER TABLE team_lineups ADD COLUMN IF NOT EXISTS drtg_lev REAL;
ALTER TABLE team_lineups ADD COLUMN IF NOT EXISTS net_lev  REAL;
"""


def run():
    print("Connecting to database...")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        print("Adding _lev columns to team_lineups...")
        cur.execute(SQL)
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema='public' AND table_name='team_lineups'
              AND column_name IN ('min_lev','ortg_lev','drtg_lev','net_lev')
            ORDER BY column_name
        """)
        cols = [r[0] for r in cur.fetchall()]
        print(f"Columns ready: {', '.join(cols)}")
        cur.close(); conn.close()
        print("Done.")
    except Exception as e:
        print(f"Error: {e}"); sys.exit(1)


if __name__ == "__main__":
    run()
