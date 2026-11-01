"""
ydkball — Schema v23: threaded replies
=============================================================
python backend/schema_v23.py

Lets a reply answer another reply, not just the review.

Why this is reversible by design:

    `parent_reply_id` is nullable and nothing requires it. Flattening the
    feature later means ignoring the column — the client stops nesting, every
    reply reads as a response to the review, and no data is lost or migrated.
    That matters, because the usage that prompted this is thin: 16 replies
    ever, and 14 of 15 reviews have exactly one. If conversation does get
    unwieldy, reverting is a client change, not a migration.

ON DELETE CASCADE: deleting a reply takes its answers with it. The
alternative is orphans that render as top-level replies answering something
nobody can see.

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

DDL = [
    """ALTER TABLE review_replies
         ADD COLUMN IF NOT EXISTS parent_reply_id INTEGER
         REFERENCES review_replies(id) ON DELETE CASCADE""",
    # The thread view asks "children of this reply, oldest first" on every render.
    """CREATE INDEX IF NOT EXISTS idx_review_replies_parent
         ON review_replies (parent_reply_id, created_at)""",
]


def run():
    print("Connecting to database...")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        for stmt in DDL:
            cur.execute(stmt)
        print("  review_replies.parent_reply_id ready")
        cur.execute("""SELECT COUNT(*) FILTER (WHERE parent_reply_id IS NOT NULL) AS nested,
                              COUNT(*) AS total FROM review_replies""")
        n, t = cur.fetchone()
        print(f"  replies: {t} total, {n} nested")
        cur.close(); conn.close()
    except Exception as e:
        print(f"❌ {e}"); sys.exit(1)


if __name__ == "__main__":
    run()
