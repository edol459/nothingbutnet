"""
ydkball — Schema v10: One ballot per season
=============================================================
python backend/schema_v10.py

Ballots pay Ball Knowledge XP for correct picks, and the moment a prediction is
worth something, being able to make ten of them is an exploit: fill one ballot
per MVP candidate and a correct pick is guaranteed. So "one ballot per user, per
league, per season" stops being a nicety and becomes a rule the database has to
enforce — application checks race, indexes don't.

Partial unique index rather than a table constraint because it only applies to
one list_type; every other kind of list stays as unlimited as it was. Same shape
as idx_allegiance_current in schema_v7.

Safe to run multiple times. If it fails with a uniqueness violation, some user
already has two ballots for one season — list them with the query in the error
path and merge or delete by hand before re-running.
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
CREATE UNIQUE INDEX IF NOT EXISTS idx_one_ballot_per_season
    ON game_lists (user_id, league, season)
    WHERE list_type = 'awards';
"""

DUPES = """
SELECT user_id, league, season, COUNT(*) AS n, array_agg(id) AS ids
FROM game_lists WHERE list_type = 'awards'
GROUP BY user_id, league, season HAVING COUNT(*) > 1
"""


def main():
    conn = psycopg2.connect(DATABASE_URL)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    try:
        cur.execute(SQL)
        print("schema v10 applied — one ballot per user/league/season.")
    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        cur.execute(DUPES)
        rows = cur.fetchall()
        print("Cannot create the index — these users hold more than one ballot:")
        for r in rows:
            print(f"  user {r[0]}  {r[1]} {r[2]}  ->  ids {r[4]}")
        print("Delete or merge the extras, then re-run.")
        sys.exit(1)
    finally:
        cur.close(); conn.close()


if __name__ == "__main__":
    main()
