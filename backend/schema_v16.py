"""
ydkball — Schema v16: a digest time you choose
=============================================================
python backend/schema_v16.py

Adds a user-settable delivery time for the daily watchlist digest — the "alarm
clock" — plus the timezone it has to be interpreted in.

Why two columns and not one:

Storing the time as an offset from UTC would be wrong twice a year. A user who
picks 8:00 AM in New York means 12:00 UTC in winter and 11:00 UTC in summer, and
a stored UTC value silently drifts an hour every DST transition. Storing the
LOCAL wall-clock time next to an IANA zone name lets the sender convert at send
time, which is the only representation that stays correct across those changes.

`timezone` is refreshed by the app whenever it registers a device token, so a
user who moves gets their digest at 8 AM in the new place without touching a
setting. NULL falls back to America/New_York rather than skipping the send — the
audience is US basketball, and a digest an hour off beats no digest at all.

Delivery granularity is whatever the send cron runs at (every 15 minutes). The
sender fires at the first run at or after the chosen time, so a digest is never
early and at most one interval late. That is a deliberate trade: never waking
someone before they asked matters more than exactness.

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
    # Local wall-clock time. TIME, not TIMESTAMP: it is a time of day, not a moment.
    ("notify_digest_at", "TIME NOT NULL DEFAULT '08:00'"),
    ("timezone",         "TEXT"),
]


def run():
    print("Connecting to database...")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        for col, ddl in COLUMNS:
            cur.execute("""SELECT 1 FROM information_schema.columns
                            WHERE table_name='users' AND column_name=%s""", (col,))
            if cur.fetchone():
                print(f"  users.{col} already present")
            else:
                cur.execute(f"ALTER TABLE users ADD COLUMN {col} {ddl}")
                print(f"  added users.{col}")

        # Proves the conversion the sender will rely on actually works in this database.
        cur.execute("""
            SELECT COUNT(*),
                   COUNT(timezone),
                   MIN(notify_digest_at)::text,
                   (NOW() AT TIME ZONE COALESCE(MAX(timezone), 'America/New_York'))::time::text
            FROM users
        """)
        total, with_tz, t, local_now = cur.fetchone()
        print(f"\nusers: {total} | with a timezone: {with_tz} | default digest time: {t}")
        print(f"timezone conversion works — local now resolves to {local_now}")
        cur.close(); conn.close()
    except Exception as e:
        print(f"❌ {e}")
        sys.exit(1)


if __name__ == "__main__":
    run()
