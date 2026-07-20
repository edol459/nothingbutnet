"""
Duplicate each user's favorite games into an auto-created "Favorite Games" list.

NON-DESTRUCTIVE: this only READS the favorite_games table and WRITES new rows to
game_lists / game_list_items. It never deletes or edits favorite_games, so every
user keeps their existing up-to-4 favorite games exactly as they are — this just
also copies them into a normal games list they can see on their profile.

Idempotent: a user who already has a games list titled "Favorite Games" is
skipped, so re-running never creates duplicates.

Usage:
    python backend/migrate_favorite_games_to_list.py --dry-run   # preview only
    python backend/migrate_favorite_games_to_list.py             # apply
"""
import os, sys
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

LIST_TITLE = "Favorite Games"
LIST_DESC  = "My favorite games."

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("DATABASE_URL not found."); sys.exit(1)

dry_run = "--dry-run" in sys.argv

conn = psycopg2.connect(DATABASE_URL)
cur  = conn.cursor(cursor_factory=RealDictCursor)

# Users that actually have favorite games, with those games in slot order.
cur.execute("""
    SELECT user_id,
           array_agg(game_id ORDER BY position) AS game_ids
    FROM favorite_games
    GROUP BY user_id
    ORDER BY user_id
""")
rows = cur.fetchall()

users_processed = lists_created = items_copied = users_skipped = 0

for r in rows:
    user_id  = r["user_id"]
    game_ids = r["game_ids"]
    users_processed += 1

    # Idempotency: skip if this user already has a "Favorite Games" games list.
    cur.execute("""
        SELECT id FROM game_lists
        WHERE user_id = %s AND list_type = 'games' AND title = %s
        LIMIT 1
    """, (user_id, LIST_TITLE))
    if cur.fetchone():
        users_skipped += 1
        continue

    if dry_run:
        lists_created += 1
        items_copied  += len(game_ids)
        print(f"[dry-run] user {user_id}: would create '{LIST_TITLE}' with {len(game_ids)} game(s)")
        continue

    cur.execute("""
        INSERT INTO game_lists (user_id, title, description, is_public, is_ranked, list_type)
        VALUES (%s, %s, %s, TRUE, FALSE, 'games')
        RETURNING id
    """, (user_id, LIST_TITLE, LIST_DESC))
    list_id = cur.fetchone()["id"]
    lists_created += 1

    for pos, game_id in enumerate(game_ids, start=1):
        cur.execute("""
            INSERT INTO game_list_items (list_id, game_id, sort_order)
            VALUES (%s, %s, %s)
            ON CONFLICT (list_id, game_id) DO NOTHING
        """, (list_id, game_id, pos))
        items_copied += 1

if dry_run:
    conn.rollback()
    print(f"\n[dry-run] {users_processed} user(s) with favorites; "
          f"would create {lists_created} list(s), copy {items_copied} item(s); "
          f"{users_skipped} already had a 'Favorite Games' list.")
else:
    conn.commit()
    print(f"\nDone. Processed {users_processed} user(s): created {lists_created} list(s), "
          f"copied {items_copied} game(s); skipped {users_skipped} (already had one). "
          f"favorite_games left untouched.")

cur.close(); conn.close()
