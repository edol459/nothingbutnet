"""
ydkball — Schema v20: email opt-out
=============================================================
python backend/schema_v20.py

Records who has unsubscribed from product email.

Why a column and not a preferences table:
    The `notify_*` columns on `users` already hold push preferences, so email
    opt-out belongs beside them rather than in a table of its own. One boolean
    answers the only question the sender asks.

Why `email_opt_out` defaults FALSE rather than requiring opt-IN:
    US CAN-SPAM is opt-out for mail to your own users. If ydkball ever mails
    people in the EU/UK this is not sufficient on its own — that needs consent
    or soft opt-in, which is a product decision, not a schema one.

    The honest reading of this column: "has this person told us to stop".
    It is NOT evidence that anyone said yes.

`email_opt_out_at` exists because a bare boolean cannot answer "when did they
ask", which is the first question in any complaint.

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

DDL = [
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS email_opt_out BOOLEAN NOT NULL DEFAULT FALSE",
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS email_opt_out_at TIMESTAMPTZ",
]


def run():
    print("Connecting to database...")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        # ALTER TABLE users takes ACCESS EXCLUSIVE and waits behind any open
        # transaction — and everything arriving after it queues behind the wait,
        # so an unbounded attempt turns one blocked migration into a stalled
        # table. Learned the hard way on 2026-10-12. Fail fast instead.
        for stmt in DDL:
            col = stmt.split("IF NOT EXISTS ")[1].split()[0]
            for attempt in range(1, 7):
                try:
                    cur.execute("SET lock_timeout = '3s'")
                    cur.execute(stmt)
                    print(f"  users.{col} ready (attempt {attempt})")
                    break
                except psycopg2.errors.LockNotAvailable:
                    conn.rollback()
                    print(f"  attempt {attempt}: users is locked, retrying in 5s…")
                    _time.sleep(5)
            else:
                print(f"\n❌ Could not get a lock on `users` for {col} after 6 tries.")
                print("   Something is holding a transaction open. Check:")
                print("     SELECT pid, state, query FROM pg_stat_activity")
                print("      WHERE state LIKE 'idle in transaction%';")
                sys.exit(1)

        cur.execute("SET lock_timeout = 0")
        cur.execute("SELECT COUNT(*) FROM users WHERE email_opt_out")
        print(f"  users currently opted out: {cur.fetchone()[0]}")
        cur.close(); conn.close()
    except Exception as e:
        print(f"❌ {e}"); sys.exit(1)


if __name__ == "__main__":
    run()
