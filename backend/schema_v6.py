"""
ydkball — Schema v6: Scheduled Games
=============================================================
python backend/schema_v6.py

Creates `scheduled_games`, the forward-looking schedule for both leagues.

Why a separate table instead of rows in `games`: `games` is a results archive —
every one of its rows is Final, and roughly a dozen of the ~30 `FROM games`
queries in server.py have no status filter. Inserting unplayed games there would
silently fold them into feeds, browse, team pages and review counts, failing as
wrong numbers rather than as errors. This table is additive: nothing existing
reads it.

Populated by backend/ingest/fetch_scheduled_games.py (cloud_daily). Rows here
are upserted every run, so tip-time flexes and status changes self-heal, and
playoff/Cup games appear as soon as the league schedules them.

`game_time_utc` is the field `games` has never had — it's what a tip-off
notification needs. NULL means the league hasn't set a time yet (TBD).

Safe to run multiple times (IF NOT EXISTS throughout).
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
CREATE TABLE IF NOT EXISTS scheduled_games (
    game_id        TEXT PRIMARY KEY,
    league         TEXT NOT NULL,
    season         TEXT NOT NULL,
    season_type    TEXT NOT NULL,
    game_date      DATE NOT NULL,
    game_time_utc  TIMESTAMPTZ,
    home_team_abbr TEXT NOT NULL,
    away_team_abbr TEXT NOT NULL,
    status         TEXT NOT NULL DEFAULT 'Scheduled',
    status_text    TEXT,
    arena_name     TEXT,
    arena_city     TEXT,
    game_label     TEXT,
    is_neutral     BOOLEAN NOT NULL DEFAULT FALSE,
    postponed      BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Calendar markers for a month, one league or both.
CREATE INDEX IF NOT EXISTS idx_sched_games_date
    ON scheduled_games (game_date);
CREATE INDEX IF NOT EXISTS idx_sched_games_league_date
    ON scheduled_games (league, game_date);

-- "every upcoming game for this team" — the watchlist team subscription.
-- Two indexes because a team is home in half its games and away in the other.
CREATE INDEX IF NOT EXISTS idx_sched_games_home
    ON scheduled_games (league, home_team_abbr, game_date);
CREATE INDEX IF NOT EXISTS idx_sched_games_away
    ON scheduled_games (league, away_team_abbr, game_date);
"""


def run():
    print("Connecting to database...")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        print("Creating scheduled_games + indexes...")
        cur.execute(SQL)
        cur.execute("SELECT COUNT(*) FROM scheduled_games")
        print(f"✅ scheduled_games ready — {cur.fetchone()[0]} row(s)")
        cur.close(); conn.close()
    except Exception as e:
        print(f"❌ {e}")
        sys.exit(1)


if __name__ == "__main__":
    run()
