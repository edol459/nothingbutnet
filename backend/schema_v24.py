"""
ydkball — Schema v24: comments on a player's performance
=============================================================
python backend/schema_v24.py

A comment section under one player's night: "Steph 41 PTS · GSW @ DEN".

Keyed on (game_id, person_id), NOT on a row in some other table
-----------------------------------------------------------------
A performance is not an entity anyone owns. `performance_reviews` was retired
in September 2026, and `potg_picks` records who someone VOTED for, not the
performance itself — so neither can be the parent. The card's identity is the
pair, exactly as `potg_picks` and `review_tags` are keyed, and a comment hangs
off that pair directly. No FK to `games` for the same reason the drafts table
has none: `games` holds finished games only, and a performance is worth talking
about while it is still happening.

Deliberately unowned, and therefore deliberately quiet
------------------------------------------------------
A top-level comment notifies NOBODY. There is no author of a performance to
tell, and inventing a recipient (everyone who logged the game, say) would make
a busy night spam people who never opened that player. This is a surface you
read, like a comment section, not a conversation someone is pulled back into.

A REPLY is different: `parent_id` names exactly one person to notify, so that
one does. `ON DELETE CASCADE` means deleting a comment takes its replies with
it rather than leaving answers to something nobody can see — same reasoning as
`review_replies.parent_reply_id`.

Reporting
---------
`content_reports.comment_id` is added alongside the existing `review_id`. This
is a new surface for public free text, and an app that hosts user content needs
a way to report it (App Store guideline 1.2); the existing queue in /admin picks
these up with no extra plumbing. Both columns are nullable — a report concerns
one or the other.

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
    """CREATE TABLE IF NOT EXISTS performance_comments (
           id         SERIAL PRIMARY KEY,
           game_id    TEXT    NOT NULL,
           person_id  INTEGER NOT NULL,
           user_id    INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
           parent_id  INTEGER REFERENCES performance_comments(id) ON DELETE CASCADE,
           body       TEXT    NOT NULL,
           created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
           updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
       )""",

    # The card's own query: "every comment on this performance, oldest first".
    """CREATE INDEX IF NOT EXISTS idx_perf_comments_card
         ON performance_comments (game_id, person_id, created_at)""",

    # "Replies to my comment", for the notification arm.
    """CREATE INDEX IF NOT EXISTS idx_perf_comments_parent
         ON performance_comments (parent_id, created_at)
      WHERE parent_id IS NOT NULL""",

    # A user's own comment history, for moderation and for deletion on account close.
    """CREATE INDEX IF NOT EXISTS idx_perf_comments_user
         ON performance_comments (user_id, created_at DESC)""",

    "ALTER TABLE content_reports ADD COLUMN IF NOT EXISTS comment_id INTEGER",
]


def run():
    print("Connecting to database...")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        for stmt in DDL:
            cur.execute(stmt)
        print("  performance_comments ready")
        print("  content_reports.comment_id ready")

        cur.execute("""SELECT COUNT(*) FILTER (WHERE parent_id IS NULL) AS top,
                              COUNT(*) FILTER (WHERE parent_id IS NOT NULL) AS replies,
                              COUNT(DISTINCT (game_id, person_id)) AS cards
                         FROM performance_comments""")
        t, r, cards = cur.fetchone()
        print(f"  comments: {t} top-level, {r} replies, across {cards} performance(s)")
        cur.close(); conn.close()
    except Exception as e:
        print(f"❌ {e}"); sys.exit(1)


if __name__ == "__main__":
    run()
