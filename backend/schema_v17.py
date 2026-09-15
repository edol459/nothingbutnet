"""
ydkball — Schema v17: where the game was played
=============================================================
python backend/schema_v17.py

Adds venue columns to `games` so a finished game knows the building it happened
in, plus the crowd that was in it.

Why this exists:

Attendance has been recordable since the beginning (`game_reviews.attended`),
but a game row could not say WHERE you were. The arena lived only in
`scheduled_games`, which holds the current season's schedule feed — so of the
attended logs on record, barely any could resolve a venue, and every one from a
prior season resolved none. "I was at this game" pointed at nothing.

The data was never missing, only unstored: the CDN boxscore has carried
`arena.arenaName` / `arenaCity` all along, and `attendance` / `sellout` beside
them, for games going back seasons. `backfill_game_venues.py` fills these in.

Why on `games` and not a join to `scheduled_games`:

`scheduled_games` is a rolling schedule feed, not a historical record — a game
from 2024-25 was never in it and never will be. Attendance is permanent and the
venue is a fact about the game itself, so it belongs on the row that outlives the
schedule.

`attendance` and `sellout` are the game's real crowd figure, not a user count.
They are here because they come free in the same payload and are exactly the
detail a ticket stub wants to print.

All four are NULLable with no default: a game we could not fetch must read as
"unknown", never as an invented zero. A 0 in `attendance` would be a claim that
nobody came.

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

COLUMNS = [
    ("arena_name", "TEXT"),
    ("arena_city", "TEXT"),
    # Real tickets sold, from the league feed. NULL = we never fetched it.
    ("attendance", "INTEGER"),
    ("sellout",    "BOOLEAN"),
]


def run():
    print("Connecting to database...")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        for col, ddl in COLUMNS:
            cur.execute("""SELECT 1 FROM information_schema.columns
                            WHERE table_name='games' AND column_name=%s""", (col,))
            if cur.fetchone():
                print(f"  games.{col} already present")
            else:
                cur.execute(f"ALTER TABLE games ADD COLUMN {col} {ddl}")
                print(f"  added games.{col}")

        # Attended games are the ones that matter first — they are what a ticketbook
        # renders, and the reason the venue is worth storing at all.
        cur.execute("""
            SELECT COUNT(*) FILTER (WHERE arena_name IS NOT NULL),
                   COUNT(*)
              FROM games
        """)
        filled, total = cur.fetchone()
        cur.execute("""
            SELECT COUNT(DISTINCT g.game_id) FILTER (WHERE g.arena_name IS NOT NULL),
                   COUNT(DISTINCT g.game_id)
              FROM games g
              JOIN game_reviews r ON r.game_id = g.game_id AND r.attended
        """)
        att_filled, att_total = cur.fetchone()
        print(f"\ngames with a venue: {filled} of {total}")
        print(f"attended games with a venue: {att_filled} of {att_total}")
        if att_filled < att_total:
            print("\nNext: python backend/ingest/backfill_game_venues.py --attended-first --apply")
        cur.close(); conn.close()
    except Exception as e:
        print(f"❌ {e}")
        sys.exit(1)


if __name__ == "__main__":
    run()
