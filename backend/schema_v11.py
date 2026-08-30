"""
ydkball — Schema v11: One allegiance per user
=============================================================
python backend/schema_v11.py

(Originally written as schema_v10; renumbered after v10 was taken by the awards
ballot index. Already applied to production — kept so a rebuild from scratch
lands on the same shape rather than the per-league index v7 created.)

v7 allowed one open allegiance per league, on the theory that a two-league fan
holds two identities on two offset season clocks. In practice that made the
product incoherent: onboarding asked for exactly one team per league while the
watchlist let you follow a dozen — so the "favourite team" question was really a
watchlist question wearing the wrong constraint.

The split is clean now. The watchlist is where "teams whose games I want" lives,
and allegiance is one team, full stop: the badge over your avatar and the streak
you're judged on.

What this does:
  1. Collapses any user holding more than one open allegiance down to one,
     keeping the EARLIEST — that's the longest-running streak, so nobody loses
     tenure to a migration.
  2. Replaces the (user_id, league) partial unique index from v7 with one on
     (user_id), so the database enforces the new rule rather than trusting the
     endpoints to.

`league` stays on the table: LA, NY and POR exist in both leagues, so a team
abbreviation alone doesn't identify a team.

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

# Close every open row except the oldest per user. ended_at is NOW() rather than
# backdated — the allegiance genuinely ended at migration time.
COLLAPSE = """
UPDATE team_allegiance a
   SET ended_at = NOW()
 WHERE a.ended_at IS NULL
   AND a.id <> (
       SELECT b.id FROM team_allegiance b
        WHERE b.user_id = a.user_id AND b.ended_at IS NULL
        ORDER BY b.started_at ASC, b.id ASC
        LIMIT 1
   )
"""

REINDEX = """
DROP INDEX IF EXISTS idx_allegiance_current;
CREATE UNIQUE INDEX IF NOT EXISTS idx_allegiance_current
    ON team_allegiance (user_id) WHERE ended_at IS NULL;
"""


def run():
    print("Connecting to database...")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        cur.execute("""SELECT COUNT(*) FROM (
            SELECT user_id FROM team_allegiance WHERE ended_at IS NULL
            GROUP BY user_id HAVING COUNT(*) > 1) t""")
        print(f"users holding more than one allegiance: {cur.fetchone()[0]}")

        cur.execute(COLLAPSE)
        print(f"  closed {cur.rowcount} extra allegiance row(s)")

        print("Swapping the unique index to one-per-user...")
        cur.execute(REINDEX)

        cur.execute("""SELECT COUNT(*), COUNT(DISTINCT user_id)
                       FROM team_allegiance WHERE ended_at IS NULL""")
        rows, users = cur.fetchone()
        ok = "1:1 ✅" if rows == users else "MISMATCH ❌"
        print(f"{rows} open allegiance(s) across {users} user(s) — {ok}")
        cur.close(); conn.close()
    except Exception as e:
        print(f"❌ {e}")
        sys.exit(1)


if __name__ == "__main__":
    run()
