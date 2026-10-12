"""
ydkball — Schema v19: gifted Pro
=============================================================
python backend/schema_v19.py

Lets an admin grant Pro for a fixed stretch without a purchase.

Why `is_pro` stays the one source of truth:

Twelve places decide whether someone is Pro — /auth/me, the mobile-token lookup,
the review-length gate, the daily-game gate, profile payloads, feed author badges.
Every one reads `users.is_pro`. Computing an effective value per call site would
mean touching all twelve and leaving a grant half-honoured wherever one was
missed. So a gift simply SETS `is_pro`, and `pro_until` records when to take it
back. One column still answers the question everywhere.

`pro_gifts` is the audit trail — who granted what, to whom, and when. A boolean
that silently changed with no record of why is the thing you regret during a
billing dispute.

Expiry is swept daily rather than computed on read. A gift is not a payment, so a
few hours of overrun costs nothing, and the sweep is where the awkward case gets
handled: a user who was gifted AND later subscribed must not be revoked when the
gift lapses. See expire_pro_gifts.py.

Safe to run multiple times.
"""
import os, sys
import time as _time
from dotenv import load_dotenv
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("DATABASE_URL not found."); sys.exit(1)

DDL_COLUMN = "ALTER TABLE users ADD COLUMN IF NOT EXISTS pro_until TIMESTAMPTZ"

DDL_TABLE = """
CREATE TABLE IF NOT EXISTS pro_gifts (
    id          SERIAL PRIMARY KEY,
    user_id     INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    -- Who granted it. NOT a foreign key on purpose: the record of a gift should
    -- outlive the admin account that issued it.
    granted_by  INTEGER,
    months      INTEGER NOT NULL,
    reason      TEXT,
    granted_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at  TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_pro_gifts_user ON pro_gifts (user_id, granted_at DESC);
"""


def run():
    print("Connecting to database...")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        # The new table first: nothing references `users` yet, so it cannot queue.
        cur.execute(DDL_TABLE)
        print("  pro_gifts ready")

        # ALTER TABLE users needs ACCESS EXCLUSIVE. On a live database that waits behind
        # any open transaction — and crucially, everything that arrives AFTER it queues
        # behind the waiting ALTER, so an unbounded wait turns one blocked migration into
        # a stalled table. lock_timeout makes it fail fast and harmlessly instead.
        #
        # This is not hypothetical: run without it on 2026-10-12, this statement sat
        # behind an idle-in-transaction backend and two admin queries piled up behind it.
        for attempt in range(1, 7):
            try:
                cur.execute("SET lock_timeout = '3s'")
                cur.execute(DDL_COLUMN)
                print(f"  users.pro_until ready (attempt {attempt})")
                break
            except psycopg2.errors.LockNotAvailable:
                conn.rollback()
                print(f"  attempt {attempt}: users is locked, retrying in 5s…")
                _time.sleep(5)
        else:
            print("\n❌ Could not get a lock on `users` after 6 tries.")
            print("   Something is holding a transaction open. Check:")
            print("     SELECT pid, state, query FROM pg_stat_activity")
            print("      WHERE state LIKE 'idle in transaction%';")
            sys.exit(1)
        cur.execute("SET lock_timeout = 0")
        cur.execute("SELECT COUNT(*) FROM pro_gifts"); print(f"  gifts on record: {cur.fetchone()[0]}")
        cur.execute("SELECT COUNT(*) FROM users WHERE pro_until IS NOT NULL")
        print(f"  users with a live or lapsed gift: {cur.fetchone()[0]}")
        cur.close(); conn.close()
    except Exception as e:
        print(f"❌ {e}"); sys.exit(1)


if __name__ == "__main__":
    run()
