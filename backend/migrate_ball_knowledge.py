"""
Ball Knowledge — one-time migration script.

1. Adds xp column to users (safe if already exists)
2. Creates xp_events table + indexes (safe if already exists)
3. Backfills 5 XP per existing review like (skips already-processed likes)
4. Recalculates users.xp from xp_events for all affected users

Run once: python backend/migrate_ball_knowledge.py
"""
import os, sys
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("DATABASE_URL not found."); sys.exit(1)

conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
cur  = conn.cursor()

print("Step 1: Adding xp column to users...")
cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS xp INTEGER NOT NULL DEFAULT 0")

print("Step 2: Creating xp_events table...")
cur.execute("""
    CREATE TABLE IF NOT EXISTS xp_events (
        id           SERIAL  PRIMARY KEY,
        user_id      INTEGER REFERENCES users(id) ON DELETE CASCADE,
        event_type   TEXT    NOT NULL,
        reference_id TEXT,
        xp_amount    INTEGER NOT NULL,
        created_at   TIMESTAMP DEFAULT NOW()
    )
""")
cur.execute("CREATE INDEX IF NOT EXISTS idx_xp_events_user ON xp_events(user_id)")
cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_xp_events_user_type_ref
    ON xp_events(user_id, event_type, reference_id)
""")

print("Step 3: Finding unprocessed likes to backfill...")
cur.execute("""
    SELECT rl.user_id AS liker_id, rl.review_id, gr.user_id AS author_id, rl.created_at
    FROM review_likes rl
    JOIN game_reviews gr ON gr.id = rl.review_id
    WHERE gr.user_id != rl.user_id
      AND NOT EXISTS (
          SELECT 1 FROM xp_events xe
          WHERE xe.user_id      = gr.user_id
            AND xe.event_type   = 'review_like'
            AND xe.reference_id = (rl.review_id::text || ':' || rl.user_id::text)
      )
""")
rows = cur.fetchall()
print(f"  Found {len(rows)} unprocessed likes.")

for row in rows:
    ref = f"{row['review_id']}:{row['liker_id']}"
    cur.execute(
        "INSERT INTO xp_events (user_id, event_type, reference_id, xp_amount, created_at) "
        "VALUES (%s, 'review_like', %s, 5, %s)",
        (row["author_id"], ref, row["created_at"])
    )

print("Step 4: Recalculating xp for affected users...")
cur.execute("""
    UPDATE users u
    SET xp = COALESCE((SELECT SUM(xp_amount) FROM xp_events WHERE user_id = u.id), 0)
    WHERE EXISTS (SELECT 1 FROM xp_events WHERE user_id = u.id)
""")

conn.commit()
cur.close(); conn.close()
print(f"Done — backfilled {len(rows)} like XP events.")
