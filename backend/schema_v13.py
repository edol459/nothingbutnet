"""
ydkball — Schema v13: Player of the Game picks
=============================================================
python backend/schema_v13.py

Replaces per-player letter grading with a single Player of the Game pick per
user per game.

Why: grading asked for up to 26 decisions per game. Of the 13 people who logged a
game while it existed, 5 tried it — but per-user volume halved month over month
(171 ratings from 5 users in July, 87 from 6 in August) while the user count
held. That taper is fatigue, and it matched what beta testers said out loud. One
pick keeps the idea — crowd-sourced player evaluation — at roughly a tenth of the
input cost, and unlike scattered grades it aggregates into something worth
publishing ("the room gave it to Clark").

This migration is ADDITIVE. `performance_reviews` and its rows are left exactly
as they are: letter grading never shipped past TestFlight, but the code and data
stay so it can be revived if demand appears.

The backfill converts each user's highest-graded player in a game into their pick
for that game, so the existing grading history becomes POTG history rather than
being orphaned.

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
CREATE TABLE IF NOT EXISTS potg_picks (
    user_id     INTEGER     NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    game_id     TEXT        NOT NULL,
    person_id   INTEGER     NOT NULL,
    -- Denormalised so a tally never needs a join across two leagues' player
    -- tables, the same reason favorite_players carries a name.
    player_name TEXT        NOT NULL,
    team_abbr   TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- One pick per game, enforced here rather than trusted to the endpoint.
    PRIMARY KEY (user_id, game_id)
);

-- Tally for one game.
CREATE INDEX IF NOT EXISTS idx_potg_game
    ON potg_picks (game_id);
-- A player's picks over time: profile counts and the monthly leaderboard.
CREATE INDEX IF NOT EXISTS idx_potg_person
    ON potg_picks (person_id, created_at DESC);
"""

# DISTINCT ON keeps the first row per (user, game) after the ORDER BY, so the
# highest grade wins; person_id breaks ties deterministically, which matters for
# a migration that may be re-run.
BACKFILL = """
INSERT INTO potg_picks (user_id, game_id, person_id, player_name, created_at)
SELECT DISTINCT ON (user_id, game_id)
       user_id, game_id, person_id,
       COALESCE(NULLIF(player_name, ''), 'Unknown'),
       created_at
FROM performance_reviews
WHERE rating IS NOT NULL
ORDER BY user_id, game_id, rating DESC, person_id
ON CONFLICT (user_id, game_id) DO NOTHING
"""

# performance_reviews.player_name was added by a later ALTER, so its earliest
# rows carry no name. Recover them from the identity tables rather than leaving
# "Unknown" on a profile — person_id was always written correctly.
REPAIR_NAMES = """
UPDATE potg_picks p SET player_name = COALESCE(
        (SELECT player_name FROM players WHERE player_id = p.person_id),
        (SELECT player_name FROM wnba_player_seasons WHERE player_id = p.person_id LIMIT 1),
        p.player_name)
 WHERE p.player_name = 'Unknown'
"""


def run():
    print("Connecting to database...")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        print("Creating potg_picks...")
        cur.execute(DDL)

        cur.execute("SELECT COUNT(*) FROM performance_reviews")
        before = cur.fetchone()[0]
        cur.execute("""SELECT COUNT(*) FROM (
                         SELECT DISTINCT user_id, game_id FROM performance_reviews
                         WHERE rating IS NOT NULL) t""")
        expected = cur.fetchone()[0]

        print("Backfilling picks from existing grades...")
        cur.execute(BACKFILL)
        print(f"  inserted {cur.rowcount} pick(s)")

        cur.execute(REPAIR_NAMES)
        if cur.rowcount:
            print(f"  recovered {cur.rowcount} missing player name(s)")

        cur.execute("SELECT COUNT(*), COUNT(DISTINCT user_id) FROM potg_picks")
        rows, users = cur.fetchone()
        cur.execute("SELECT COUNT(*) FROM performance_reviews")
        after = cur.fetchone()[0]

        print(f"\npotg_picks: {rows} pick(s) across {users} user(s)"
              f"  (expected up to {expected} from grades)")
        print(f"performance_reviews: {before} -> {after} "
              f"{'✅ untouched' if before == after else '❌ CHANGED'}")
        cur.close(); conn.close()
    except Exception as e:
        print(f"❌ {e}")
        sys.exit(1)


if __name__ == "__main__":
    run()
