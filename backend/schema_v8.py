"""
ydkball — Schema v8: Watchlist
=============================================================
python backend/schema_v8.py

One user-facing watchlist, stored as two tables.

`watchlist_teams` is a subscription: one row means "every game this team
plays". It deliberately does NOT expand into ~82 game rows, because the
schedule grows — playoff, play-in and NBA Cup knockout games do not exist on
the schedule published in autumn, and a bulk-add frozen in October would
silently exclude the games that matter most. A subscription picks them up the
moment the league schedules them.

`watchlist_games` holds the exceptions in both directions: an `add` row is a
one-off game the user picked, a `remove` row hides a single game that a team
subscription would otherwise include. Same shape as a calendar app deleting one
instance of a repeating event. The primary key is (user_id, game_id), so a game
is either added or removed for a user, never both, and toggling flips the row.

The resolved watchlist is therefore:
    (games of subscribed teams  UNION  explicit adds)  MINUS  explicit removes

Both tables reference scheduled_games by game_id but carry no foreign key to
it: the schedule is refreshed daily and a re-issued game id should not cascade
someone's watchlist away.

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

SQL = """
CREATE TABLE IF NOT EXISTS watchlist_teams (
    user_id    INTEGER     NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    league     TEXT        NOT NULL,
    team_abbr  TEXT        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, league, team_abbr)
);

CREATE TABLE IF NOT EXISTS watchlist_games (
    user_id    INTEGER     NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    game_id    TEXT        NOT NULL,
    action     TEXT        NOT NULL CHECK (action IN ('add', 'remove')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, game_id)
);

-- Resolving a watchlist reads one user's rows split by action.
CREATE INDEX IF NOT EXISTS idx_watchlist_games_user_action
    ON watchlist_games (user_id, action);
"""


def run():
    print("Connecting to database...")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        print("Creating watchlist_teams + watchlist_games...")
        cur.execute(SQL)
        cur.execute("SELECT COUNT(*) FROM watchlist_teams")
        teams = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM watchlist_games")
        games = cur.fetchone()[0]
        print(f"✅ watchlist ready — {teams} team subscription(s), {games} game row(s)")
        cur.close(); conn.close()
    except Exception as e:
        print(f"❌ {e}")
        sys.exit(1)


if __name__ == "__main__":
    run()
