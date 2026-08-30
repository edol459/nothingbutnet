"""
ydkball — Schema v9: Awards ballots
=============================================================
python backend/schema_v9.py

An awards ballot is a `game_lists` row with list_type = 'awards'. Reusing that
table is deliberate: ballots inherit likes, comments, the feed, profile
placement and the public list page for free. What they do NOT inherit is the
shape of an item — every other list type is an ordered bag of one entity, while
a ballot is a fixed set of named slots holding at most one player each. Hence a
dedicated item table keyed by (list_id, award_code) rather than a sort_order.

Three columns move onto game_lists:

  `league` / `season` — a ballot is a prediction ABOUT something. Which league's
      awards, and which season's, are properties of the list itself, not of any
      item. Both stay NULL for the seven existing list types.

  `locked_at` — the deadline, set at creation from the season's first scheduled
      regular-season game. This is the whole integrity story: a prediction made
      after the season starts is worthless, and one that could be quietly edited
      in March is worse than worthless. NULL means "never locks", which is what
      every pre-existing list is.

`award_ballot_items` holds the picks. The primary key IS the rule — (list_id,
award_code) means one player per award, enforced by Postgres instead of by
application code, so an upsert is the natural way to change a pick.

`award_results` is the answer key: who actually won. Kept as its own table
rather than read live from `player_seasons.awards` because (a) WNBA seasons
have no awards column at all, so those rows can only ever be entered by hand,
and (b) grading should be a stable snapshot — a ballot graded in May shouldn't
silently re-grade itself if an upstream backfill rewrites history.

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
ALTER TABLE game_lists ADD COLUMN IF NOT EXISTS league    TEXT;
ALTER TABLE game_lists ADD COLUMN IF NOT EXISTS season    TEXT;
ALTER TABLE game_lists ADD COLUMN IF NOT EXISTS locked_at TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS award_ballot_items (
    list_id     INTEGER     NOT NULL REFERENCES game_lists(id) ON DELETE CASCADE,
    award_code  TEXT        NOT NULL,
    person_id   INTEGER,
    player_name TEXT        NOT NULL,
    team        TEXT,
    added_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (list_id, award_code)
);

CREATE TABLE IF NOT EXISTS award_results (
    league      TEXT        NOT NULL,
    season      TEXT        NOT NULL,
    award_code  TEXT        NOT NULL,
    person_id   INTEGER,
    player_name TEXT        NOT NULL,
    team        TEXT,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (league, season, award_code)
);

-- Grading a ballot looks up one league+season's whole answer key at once.
CREATE INDEX IF NOT EXISTS idx_award_results_season
    ON award_results (league, season);

-- "Every ballot for this season" powers the results leaderboard and the
-- lock sweep; both filter on type before anything else.
CREATE INDEX IF NOT EXISTS idx_game_lists_awards
    ON game_lists (league, season) WHERE list_type = 'awards';
"""


def main():
    conn = psycopg2.connect(DATABASE_URL)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute(SQL)
    print("schema v9 applied — awards ballots ready.")
    cur.close(); conn.close()


if __name__ == "__main__":
    main()
