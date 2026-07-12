"""
Backfill game_watches from existing ratings: every game a user has rated
(game review) or whose player performance they rated implies they watched it.
Uses MIN(created_at) so the watch date reflects when they first logged the game,
keeping the diary in correct chronological order. Idempotent.

Run once: python backend/migrate_backfill_watches.py
"""
import os, sys
from dotenv import load_dotenv
import psycopg2

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("DATABASE_URL not found."); sys.exit(1)

conn = psycopg2.connect(DATABASE_URL)
cur  = conn.cursor()

cur.execute("""
    INSERT INTO game_watches (user_id, game_id, created_at)
    SELECT user_id, game_id, MIN(created_at)
    FROM game_reviews
    GROUP BY user_id, game_id
    ON CONFLICT (user_id, game_id) DO NOTHING
""")
from_reviews = cur.rowcount

cur.execute("""
    INSERT INTO game_watches (user_id, game_id, created_at)
    SELECT user_id, game_id, MIN(created_at)
    FROM performance_reviews
    GROUP BY user_id, game_id
    ON CONFLICT (user_id, game_id) DO NOTHING
""")
from_perf = cur.rowcount

conn.commit()

cur.execute("SELECT COUNT(*) FROM game_watches")
total = cur.fetchone()[0]

cur.close(); conn.close()
print(f"Done — inserted {from_reviews} watches from game reviews, "
      f"{from_perf} from performance reviews. game_watches now has {total} rows.")
