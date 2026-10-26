"""
ydkball — Schema v23: friend tags and custom tags on a log
=============================================================
python backend/schema_v23.py

Stat tags already live in `game_reviews.tags` as a JSONB array and stay exactly
where they are. Two new kinds of tag need a real table instead:

  friend  — "I watched this one with Dan", pointing at a real user
  custom  — free text the user types ("playoff push", "watched at the bar")

Why a table and not three more fields in that JSONB array:

  * Both new kinds exist to be FILTERED and COUNTED — "show me every game I
    watched with Dan", "which tag do I use most". That is an indexed lookup, and
    a JSONB array inside a column that is rewritten wholesale on every log edit
    is the wrong shape for it.
  * A friend tag has a second owner. It points at another user's id, needs a
    foreign key when that account is deleted, and is read from *their* side to
    build their notification. None of that works inside the tagger's JSONB blob.

Keyed on (user_id, game_id), NOT on review_id
---------------------------------------------
A log is not a review. 70 logs in the database today are a Player-of-the-Game
pick with no `game_reviews` row at all, so a review_id foreign key would make
those logs untaggable. It would also cascade-delete "who I watched this with"
whenever a rating is removed, which is not what deleting a rating means.
`potg_picks` is keyed the same way for the same reason.

The UNIQUE constraint IS the "no duplicate tag on one log" rule — tagging Dan
twice, or typing the same custom tag twice, is a no-op rather than two rows.

`label` is a snapshot, deliberately
-----------------------------------
For a friend tag, `label` holds their display name as it read at tag time and
`friend_user_id` carries the link. Paired with ON DELETE SET NULL, a deleted
account leaves your own diary entry still reading "with Dan" instead of the
memory silently disappearing from a game you watched years ago.

`label_norm` is the grouping key (lower, trimmed). Without it you get "Game 7"
and "game 7" as two separate tags inside a week.

Drafts
------
`game_log_drafts` gets the same two kinds so a tag survives an app restart
mid-compose. They go in their own columns rather than being folded into the
existing `tags` column, which would break the `_clean_tags` contract that both
the draft and the publish path share.

Touches no hot table. Safe to run multiple times.
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
    """CREATE TABLE IF NOT EXISTS review_tags (
           id             SERIAL PRIMARY KEY,
           user_id        INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
           game_id        TEXT    NOT NULL,
           kind           TEXT    NOT NULL CHECK (kind IN ('friend', 'custom')),
           label          TEXT    NOT NULL,
           label_norm     TEXT    NOT NULL,
           friend_user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
           created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
           UNIQUE (user_id, game_id, kind, label_norm)
       )""",

    # Reading one log's tags, and wiping them before a replace-all write.
    """CREATE INDEX IF NOT EXISTS idx_review_tags_log
         ON review_tags (user_id, game_id)""",

    # "Every game I tagged <label>" and the tag counts behind the filter chips.
    """CREATE INDEX IF NOT EXISTS idx_review_tags_user_label
         ON review_tags (user_id, label_norm)""",

    # The notification side: "who tagged ME, most recent first". Partial because
    # custom tags have no friend_user_id and never appear in that query.
    """CREATE INDEX IF NOT EXISTS idx_review_tags_friend
         ON review_tags (friend_user_id, created_at DESC) WHERE kind = 'friend'""",

    # Drafts: mid-compose tags, same two kinds.
    "ALTER TABLE game_log_drafts ADD COLUMN IF NOT EXISTS friend_tags JSONB DEFAULT '[]'::jsonb",
    "ALTER TABLE game_log_drafts ADD COLUMN IF NOT EXISTS custom_tags JSONB DEFAULT '[]'::jsonb",
]


def run():
    print("Connecting to database...")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        for stmt in DDL:
            cur.execute(stmt)
        print("  review_tags ready")
        print("  game_log_drafts.friend_tags / custom_tags ready")

        cur.execute("""SELECT COUNT(*) FILTER (WHERE kind = 'friend') AS friend,
                              COUNT(*) FILTER (WHERE kind = 'custom') AS custom,
                              COUNT(DISTINCT (user_id, game_id))      AS logs
                         FROM review_tags""")
        f, c, logs = cur.fetchone()
        print(f"  review_tags: {f} friend, {c} custom, across {logs} log(s)")
        cur.close(); conn.close()
    except Exception as e:
        print(f"❌ {e}"); sys.exit(1)


if __name__ == "__main__":
    run()
