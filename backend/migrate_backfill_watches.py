"""
Backfill game_watches from existing ratings: every game a user has rated
(game review) or whose player performance they rated implies they watched it.
Uses MIN(created_at) so the watch date reflects when they first logged the game,
keeping the diary in correct chronological order. Idempotent.

Run once: python backend/migrate_backfill_watches.py

Fails fast on lock contention (lock_timeout) so it never hangs indefinitely.
If it reports a lock timeout, something (usually the live app) holds a lock on
game_reviews / game_watches — retry in a moment, or see the blocker query below.
"""
import os, sys
from dotenv import load_dotenv
import psycopg2

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("DATABASE_URL not found."); sys.exit(1)

print("connecting…", flush=True)
conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
cur  = conn.cursor()
print("connected. setting timeouts…", flush=True)
cur.execute("SET lock_timeout = '15s'")
cur.execute("SET statement_timeout = '120s'")

try:
    print("backfilling from game_reviews…", flush=True)
    cur.execute("""
        INSERT INTO game_watches (user_id, game_id, created_at)
        SELECT user_id, game_id, MIN(created_at)
        FROM game_reviews
        GROUP BY user_id, game_id
        ON CONFLICT (user_id, game_id) DO NOTHING
    """)
    from_reviews = cur.rowcount
    print(f"  → {from_reviews} inserted", flush=True)

    print("backfilling from performance_reviews…", flush=True)
    cur.execute("""
        INSERT INTO game_watches (user_id, game_id, created_at)
        SELECT user_id, game_id, MIN(created_at)
        FROM performance_reviews
        GROUP BY user_id, game_id
        ON CONFLICT (user_id, game_id) DO NOTHING
    """)
    from_perf = cur.rowcount
    print(f"  → {from_perf} inserted", flush=True)

    conn.commit()

    cur.execute("SELECT COUNT(*) FROM game_watches")
    total = cur.fetchone()[0]
    print(f"Done — {from_reviews} from game reviews, {from_perf} from "
          f"performance reviews. game_watches now has {total} rows.", flush=True)

except psycopg2.errors.LockNotAvailable:
    conn.rollback()
    print("\nLOCK TIMEOUT — a lock on game_reviews/game_watches is held by "
          "another session (likely the live app). Nothing was changed. "
          "Wait a few seconds and re-run.\n"
          "To see the blocker, run in another psql session:\n"
          "  SELECT pid, state, wait_event, now()-query_start AS dur, query\n"
          "  FROM pg_stat_activity WHERE state <> 'idle' ORDER BY query_start;",
          flush=True)
    sys.exit(1)
finally:
    cur.close(); conn.close()
