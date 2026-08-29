"""
ydkball — Schema v7: Onboarding flag + team allegiance
=============================================================
python backend/schema_v7.py

Two things, both prerequisites for iOS first-run onboarding.

1. `users.onboarded_at` — NULL means the user has never completed onboarding.
   Server-side on purpose: @AppStorage is device-local, so a flag kept there
   re-runs onboarding on every reinstall, never fires on a second device, and
   never fires at all for someone who signed up on the web. A nullable
   timestamp is a superset of the boolean — same logic, plus when it happened.

2. `team_allegiance` — one team per league, with history. Replaces the single
   `users.favorite_team` scalar, which can only hold one team and overwrites in
   place, leaving no record of when it was set or what it was before. The open
   row (ended_at IS NULL) is the current allegiance; closed rows are the
   receipts that make a loyalty streak possible.

Team abbreviations are stored canonical (games-table form) with `league` as the
discriminator — NOT the "WNBA_" prefix `users.favorite_team` uses. That column
keeps its existing format; server.py syncs it from this table.

Backfill: every user with a favorite_team gets an open allegiance row seeded at
migration time. Real start dates are unrecoverable (the column overwrites in
place), so an early adopter and a user who signed up yesterday both begin at
zero — a deliberate call, since backdating from created_at would invent history
that isn't there.

Safe to run multiple times — the backfill skips users who already have a row.
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
ALTER TABLE users ADD COLUMN IF NOT EXISTS onboarded_at TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS team_allegiance (
    id         SERIAL      PRIMARY KEY,
    user_id    INTEGER     NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    league     TEXT        NOT NULL,
    team_abbr  TEXT        NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ended_at   TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- At most one current allegiance per league. A partial unique index is what
-- makes "switch teams" safe: closing the old row is required before opening a
-- new one, so a double-write can't leave two live allegiances behind.
CREATE UNIQUE INDEX IF NOT EXISTS idx_allegiance_current
    ON team_allegiance (user_id, league) WHERE ended_at IS NULL;

-- Profile reads pull the whole history for one user at a time.
CREATE INDEX IF NOT EXISTS idx_allegiance_user
    ON team_allegiance (user_id, league, started_at DESC);
"""

# users.favorite_team stores WNBA teams with a "WNBA_" prefix to keep them from
# colliding with NBA abbreviations (LA / NY / POR exist in both leagues).
BACKFILL = """
INSERT INTO team_allegiance (user_id, league, team_abbr, started_at)
SELECT u.id,
       CASE WHEN u.favorite_team LIKE 'WNBA\\_%' THEN 'wnba' ELSE 'nba' END,
       CASE WHEN u.favorite_team LIKE 'WNBA\\_%'
            THEN SUBSTRING(u.favorite_team FROM 6)
            ELSE u.favorite_team END,
       NOW()
FROM users u
WHERE u.favorite_team IS NOT NULL
  AND u.favorite_team <> ''
  AND NOT EXISTS (
      SELECT 1 FROM team_allegiance a
      WHERE a.user_id = u.id AND a.ended_at IS NULL
        AND a.league = CASE WHEN u.favorite_team LIKE 'WNBA\\_%' THEN 'wnba' ELSE 'nba' END
  )
"""


def run():
    print("Connecting to database...")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        print("Adding users.onboarded_at + team_allegiance...")
        cur.execute(DDL)

        print("Backfilling allegiance from users.favorite_team...")
        cur.execute(BACKFILL)
        print(f"  seeded {cur.rowcount} allegiance row(s)")

        cur.execute("""SELECT league, COUNT(*) FROM team_allegiance
                       WHERE ended_at IS NULL GROUP BY league ORDER BY league""")
        for league, n in cur.fetchall():
            print(f"  {league}: {n} current")

        cur.execute("SELECT COUNT(*) FROM users WHERE onboarded_at IS NULL")
        print(f"✅ done — {cur.fetchone()[0]} user(s) will see onboarding on next launch")
        cur.close(); conn.close()
    except Exception as e:
        print(f"❌ {e}")
        sys.exit(1)


if __name__ == "__main__":
    run()
