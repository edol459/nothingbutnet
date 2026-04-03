"""
ydkball — Schema v4: Lineup & Roster Tables
====================================================
python backend/schema_v4.py

Adds:
  - team_rosters  (players per team per season)
  - team_lineups  (5-man lineup on/off stats per team per season)

Safe to run multiple times (all CREATE TABLE IF NOT EXISTS).
"""
import os, sys
from dotenv import load_dotenv
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("❌ DATABASE_URL not found."); sys.exit(1)

SQL = """
-- ── Team Rosters ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS team_rosters (
    id          SERIAL PRIMARY KEY,
    team_abbr   TEXT    NOT NULL,
    season      TEXT    NOT NULL,
    player_id   TEXT    NOT NULL,
    player_name TEXT    NOT NULL,
    number      TEXT,
    position    TEXT,
    updated_at  TIMESTAMP DEFAULT NOW(),
    UNIQUE(team_abbr, season, player_id)
);

CREATE INDEX IF NOT EXISTS idx_rosters_team_season ON team_rosters(team_abbr, season);

-- ── Team Lineups ─────────────────────────────────────────────────────────────
-- Stores 5-man lineup advanced stats for on/off tool.
-- group_id matches the NBA API GROUP_ID format: "-pid1-pid2-pid3-pid4-pid5-"
-- player_ids is a TEXT[] array of the 5 player IDs for fast filtering.
CREATE TABLE IF NOT EXISTS team_lineups (
    id          SERIAL PRIMARY KEY,
    team_abbr   TEXT    NOT NULL,
    season      TEXT    NOT NULL,
    group_id    TEXT    NOT NULL,
    player_ids  TEXT[]  NOT NULL,
    min         REAL,
    ortg        REAL,
    drtg        REAL,
    net         REAL,
    gp          INTEGER,
    updated_at  TIMESTAMP DEFAULT NOW(),
    UNIQUE(team_abbr, season, group_id)
);

CREATE INDEX IF NOT EXISTS idx_lineups_team_season ON team_lineups(team_abbr, season);
"""


def run():
    print("Connecting to database...")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        print("Creating team_rosters and team_lineups tables...")
        cur.execute(SQL)
        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name IN ('team_rosters', 'team_lineups')
            ORDER BY table_name
        """)
        tables = [r[0] for r in cur.fetchall()]
        print("\nTables ready:")
        for t in tables:
            cur.execute("""
                SELECT COUNT(*) FROM information_schema.columns
                WHERE table_schema='public' AND table_name=%s
            """, (t,))
            print(f"   {t:<20} {cur.fetchone()[0]} columns")
        cur.close(); conn.close()
        print("\nDone. Existing tables untouched.")
    except Exception as e:
        print(f"Error: {e}"); sys.exit(1)


if __name__ == "__main__":
    run()
