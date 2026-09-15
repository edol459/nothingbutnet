"""
ydkball — Schema v18: must-watch games
=============================================================
python backend/schema_v18.py

Lets a user star a game already on their watchlist. A starred game sorts to the
top of its day and gets an accent, answering "which of tonight's five do I
actually care about".

Why its own table and not a column on `watchlist_games`:

`watchlist_games` records a DECISION about membership — action 'add' or
'remove'. Most games on a watchlist have no row there at all, because they
arrived through a team subscription. Storing a highlight as a column would mean
inserting an action='add' row just to star a Pacers game you never hand-picked,
and `explicitly_added` is exactly what separates "Your picks" from the 84 games a
subscription drags in. Starring a game would silently reclassify it.

A highlight is orthogonal to how the game got there, so it gets its own table.
Deleting the row is the only way to un-star; there is no 'remove' action to
mirror, because a highlight has no inherited state to override.

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

DDL = """
CREATE TABLE IF NOT EXISTS watchlist_highlights (
    user_id    INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    -- No FK to games: a scheduled game has no row there until it is played.
    game_id    TEXT    NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, game_id)
);
CREATE INDEX IF NOT EXISTS idx_wl_highlights_user
    ON watchlist_highlights (user_id);
"""


def run():
    print("Connecting to database...")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        cur.execute(DDL)
        print("  watchlist_highlights ready")
        cur.execute("SELECT COUNT(*) FROM watchlist_highlights")
        print(f"  rows: {cur.fetchone()[0]}")
        cur.close(); conn.close()
    except Exception as e:
        print(f"❌ {e}")
        sys.exit(1)


if __name__ == "__main__":
    run()
