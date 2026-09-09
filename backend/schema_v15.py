"""
ydkball — Schema v15: push notification plumbing
=============================================================
python backend/schema_v15.py

Three tables' worth of groundwork for watchlist notifications. Nothing here sends
anything; this is the state a sender needs.

Why the pieces are shaped this way:

`device_tokens` is keyed on the TOKEN, not the user. A device token identifies a
device, not a person — sign out on a phone and sign in as someone else and the
same token must move to the new user, not duplicate. One user can hold several
(phone + iPad); one token belongs to exactly one user at a time.

`environment` exists because APNs sandbox and production are different hosts with
different tokens. A build signed for development gets a sandbox token, and sending
it to the production host fails with BadDeviceToken. Storing which one it came
from is what stops that being a mystery.

`notification_sends` is a dedupe ledger and its PRIMARY KEY *is* the "never send
the same thing twice" rule — the same guarantee `potg_picks` gets from its key.
A crashed or re-run send job cannot double-notify, which matters more here than
almost anywhere else: a duplicate row is invisible, a duplicate push is not.

Preferences live on `users` as three booleans rather than a table, because they
are a fixed, small set read on every send. Defaults are deliberate:

  notify_final  = TRUE   one per watched game, when there is something to act on
  notify_tipoff = FALSE  opt-in; fires before every watched game
  notify_digest = FALSE  opt-in; one a day

Following three teams with everything on is ~7 pushes a day, and the usual
response to that is to disable notifications for the app entirely — which costs
the channel permanently. Off-by-default is recoverable; a disabled channel isn't.

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
CREATE TABLE IF NOT EXISTS device_tokens (
    token       TEXT        PRIMARY KEY,
    user_id     INTEGER     NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    platform    TEXT        NOT NULL DEFAULT 'ios',
    -- 'sandbox' (development builds) or 'production'. Sending a sandbox token to
    -- the production host is rejected, so the sender picks the host from this.
    environment TEXT        NOT NULL DEFAULT 'production',
    -- Cleared on a successful send, set when APNs reports the token is dead.
    invalid_at  TIMESTAMPTZ,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_device_tokens_user
    ON device_tokens (user_id) WHERE invalid_at IS NULL;

CREATE TABLE IF NOT EXISTS notification_sends (
    user_id   INTEGER     NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    kind      TEXT        NOT NULL,   -- 'final' | 'tipoff' | 'digest'
    reference TEXT        NOT NULL,   -- game_id, or the date for a digest
    sent_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- This key IS the "never twice" rule. A re-run send job is a no-op, not a
    -- second push.
    PRIMARY KEY (user_id, kind, reference)
);
CREATE INDEX IF NOT EXISTS idx_notification_sends_recent
    ON notification_sends (sent_at DESC);
"""

PREFS = [
    ("notify_final",  "BOOLEAN NOT NULL DEFAULT TRUE"),
    ("notify_tipoff", "BOOLEAN NOT NULL DEFAULT FALSE"),
    ("notify_digest", "BOOLEAN NOT NULL DEFAULT FALSE"),
]


def run():
    print("Connecting to database...")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        print("Creating device_tokens + notification_sends...")
        cur.execute(DDL)

        for col, ddl in PREFS:
            cur.execute("""SELECT 1 FROM information_schema.columns
                            WHERE table_name='users' AND column_name=%s""", (col,))
            if cur.fetchone():
                print(f"  users.{col} already present")
            else:
                cur.execute(f"ALTER TABLE users ADD COLUMN {col} {ddl}")
                print(f"  added users.{col}")

        cur.execute("SELECT COUNT(*) FROM device_tokens")
        print(f"\ndevice_tokens: {cur.fetchone()[0]} row(s)")
        cur.execute("SELECT COUNT(*) FROM notification_sends")
        print(f"notification_sends: {cur.fetchone()[0]} row(s)")
        cur.execute("""SELECT COUNT(*) FILTER (WHERE notify_final),
                              COUNT(*) FILTER (WHERE notify_tipoff),
                              COUNT(*) FILTER (WHERE notify_digest),
                              COUNT(*) FROM users""")
        f, t, d, tot = cur.fetchone()
        print(f"prefs across {tot} users — final:{f} tipoff:{t} digest:{d}")
        cur.close(); conn.close()
    except Exception as e:
        print(f"❌ {e}")
        sys.exit(1)


if __name__ == "__main__":
    run()
