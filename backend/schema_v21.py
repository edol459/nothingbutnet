"""
ydkball — Schema v21: product email ledger
=============================================================
python backend/schema_v21.py

Records which product emails actually went out, to whom.

Why this table is the safety mechanism, not a log:
    The PRIMARY KEY (campaign, user_id) *is* the "never send twice" rule —
    the same shape as `notification_sends` (schema_v15). A send to 346 people
    is a loop of 346 API calls, and loops die halfway: a crash, a rate limit,
    a laptop closing. Without this, re-running to finish the job mails the
    first 200 people a second time. There is no recalling an email.

    So the sender INSERTs the row and only then sends. A duplicate insert
    raises, the recipient is skipped, and a resumed run picks up exactly where
    it stopped.

`provider_id` is Resend's message id, which is what you quote when asking why
something bounced. `status` carries 'sent' or 'failed:<reason>' so a failed
attempt is visible rather than looking identical to never-tried.

Touches no existing table, so unlike schema_v19/v20 this needs no lock dance.

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
CREATE TABLE IF NOT EXISTS email_sends (
    campaign    TEXT    NOT NULL,
    user_id     INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    email       TEXT    NOT NULL,
    status      TEXT    NOT NULL DEFAULT 'sent',
    provider_id TEXT,
    sent_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (campaign, user_id)
);
CREATE INDEX IF NOT EXISTS idx_email_sends_campaign
    ON email_sends (campaign, sent_at DESC);
"""


def run():
    print("Connecting to database...")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        cur.execute(DDL)
        print("  email_sends ready")
        cur.execute("SELECT campaign, COUNT(*) FROM email_sends GROUP BY 1 ORDER BY 1")
        rows = cur.fetchall()
        if rows:
            for camp, n in rows:
                print(f"    {camp}: {n} sent")
        else:
            print("    (no campaigns sent yet)")
        cur.close(); conn.close()
    except Exception as e:
        print(f"❌ {e}"); sys.exit(1)


if __name__ == "__main__":
    run()
