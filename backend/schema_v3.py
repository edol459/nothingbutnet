"""
ydkball — Schema v3: Friendships
=========================================
python backend/schema_v3.py

Adds:
  - friendships table
  - users.display_name_set column (tracks whether user has chosen a name)
Safe to run multiple times.
"""
import os, sys
from dotenv import load_dotenv
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("❌ DATABASE_URL not found."); sys.exit(1)

SQL = """
-- Track whether user has explicitly set their display name
-- (used to trigger the first-login name prompt)
ALTER TABLE users ADD COLUMN IF NOT EXISTS display_name_set BOOLEAN DEFAULT FALSE;

-- For existing users who signed in before this column existed,
-- assume they haven't set a name yet (they'll be prompted once)
UPDATE users SET display_name_set = FALSE WHERE display_name_set IS NULL;

-- Friendships (bidirectional via two rows or status field)
-- status: 'pending' | 'accepted'
-- sender_id sent the request, receiver_id received it
CREATE TABLE IF NOT EXISTS friendships (
    id          SERIAL PRIMARY KEY,
    sender_id   INTEGER REFERENCES users(id) ON DELETE CASCADE,
    receiver_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    status      TEXT NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending', 'accepted')),
    created_at  TIMESTAMP DEFAULT NOW(),
    updated_at  TIMESTAMP DEFAULT NOW(),
    UNIQUE(sender_id, receiver_id)
);

CREATE INDEX IF NOT EXISTS idx_friends_sender   ON friendships(sender_id);
CREATE INDEX IF NOT EXISTS idx_friends_receiver ON friendships(receiver_id);
CREATE INDEX IF NOT EXISTS idx_friends_status   ON friendships(status);
"""

def run():
    print("⚠️  Connecting to database...")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        print("🏗️  Applying schema v3 (friendships + display_name_set)...")
        cur.execute(SQL)
        cur.close(); conn.close()
        print("✅ Done.")
    except Exception as e:
        print(f"❌ Error: {e}"); sys.exit(1)

if __name__ == "__main__":
    run()