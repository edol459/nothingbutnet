"""
ydkball — recover deleted game_reviews from a restored Railway snapshot
=============================================================
One-off recovery tool. Written 2026-09-09 after a destructive DELETE removed 57
of user 1's reviews. Safe to delete once the recovery is done.

    # 1. read from the RESTORED snapshot (never writes anything)
    #    user 1 is Ethan's personal account — the data that was lost. The
    #    user-83 test-account rule governs where new TEST writes go; it has
    #    nothing to do with which account this recovery reads.
    python backend/recover_reviews.py extract --url "<restored-db-url>" --user 1

    # 2. look at what it found
    python backend/recover_reviews.py show

    # 3. write them back into production (asks before doing it)
    python backend/recover_reviews.py restore

`extract` is read-only and talks to the restored volume. `restore` writes to
whatever DATABASE_URL points at, which is production — it prints every row and
requires typing the confirmation phrase first.

WHY IDs ARE NOT PRESERVED

The rows are re-inserted without their original `id`, letting the sequence assign
new ones. Preserving them would risk colliding with ids production has handed to
other users' reviews since. Nothing depends on the old values — `review_likes`
and `review_replies` that pointed at them were already removed by the cascade
when the rows were deleted, so there is nothing left to re-link.

`created_at` IS preserved, because that is the diary date. A recovered review
belongs on the night it was written, not today.
"""
import os, sys, json, argparse
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras

load_dotenv()

DUMP_PATH = os.path.join(os.path.dirname(__file__), "..",
                         "recovered_reviews.json")

# Explicit column list rather than SELECT *: the snapshot predates schema_v14
# (which made `rating` nullable) and later migrations, so its table shape differs
# from production's. These columns exist in both.
COLUMNS = ["user_id", "game_id", "rating", "review_text", "tags",
           "attended", "created_at", "updated_at"]


def extract(url: str, user_id: int) -> None:
    conn = psycopg2.connect(url)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(f"""SELECT {', '.join(COLUMNS)}
                      FROM game_reviews WHERE user_id = %s
                     ORDER BY created_at""", (user_id,))
    rows = [dict(r) for r in cur.fetchall()]
    for r in rows:
        r["created_at"] = str(r["created_at"])
        r["updated_at"] = str(r["updated_at"]) if r.get("updated_at") else None
        if r.get("tags") is not None and not isinstance(r["tags"], str):
            r["tags"] = json.dumps(r["tags"])
    cur.close(); conn.close()

    with open(DUMP_PATH, "w") as f:
        json.dump(rows, f, indent=2)
    print(f"extracted {len(rows)} review(s) for user {user_id}")
    if rows:
        print(f"  earliest: {rows[0]['created_at'][:10]}")
        print(f"  latest:   {rows[-1]['created_at'][:10]}")
        print(f"  with written text: {sum(1 for r in rows if (r.get('review_text') or '').strip())}")
    print(f"saved to {os.path.abspath(DUMP_PATH)}")


def show() -> None:
    rows = json.load(open(DUMP_PATH))
    print(f"{len(rows)} review(s) in {DUMP_PATH}\n")
    for r in rows:
        text = (r.get("review_text") or "").replace("\n", " ")
        text = (text[:60] + "…") if len(text) > 60 else text
        print(f"  {r['created_at'][:10]}  game {r['game_id']:>12}  "
              f"rating {str(r['rating']):>4}  {text}")


def restore() -> None:
    rows = json.load(open(DUMP_PATH))
    target = os.getenv("DATABASE_URL")
    print(f"about to insert {len(rows)} review(s) into PRODUCTION")
    print(f"  {target.split('@')[-1] if target else '?'}\n")
    show()
    print("\nThis is additive: ON CONFLICT (user_id, game_id) DO NOTHING, so a")
    print("review that already exists is left exactly as it is.")
    if input('\nType "restore my reviews" to proceed: ').strip() != "restore my reviews":
        print("aborted — nothing written."); return

    conn = psycopg2.connect(target)
    cur = conn.cursor()
    inserted = 0
    for r in rows:
        cur.execute("""
            INSERT INTO game_reviews (user_id, game_id, rating, review_text,
                                      tags, attended, created_at, updated_at)
            VALUES (%(user_id)s, %(game_id)s, %(rating)s, %(review_text)s,
                    %(tags)s, %(attended)s, %(created_at)s, %(updated_at)s)
            ON CONFLICT (user_id, game_id) DO NOTHING
        """, r)
        inserted += cur.rowcount

    # The counters were re-synced while the rows were missing, so they now
    # under-count. Recompute only the games touched. COUNT(rating), never
    # COUNT(*) — a note-only review must not drag the average down.
    game_ids = list({r["game_id"] for r in rows})
    cur.execute("""
        UPDATE games g
           SET review_count = (SELECT COUNT(rating) FROM game_reviews r
                                WHERE r.game_id = g.game_id),
               rating_sum   = (SELECT COALESCE(SUM(rating), 0) FROM game_reviews r
                                WHERE r.game_id = g.game_id)
         WHERE g.game_id = ANY(%s)
    """, (game_ids,))
    print(f"  recomputed counters on {cur.rowcount} game(s)")

    # A review implies a watch — the invariant submit_game_log maintains.
    cur.execute("""
        INSERT INTO game_watches (user_id, game_id)
        SELECT DISTINCT user_id, game_id FROM game_reviews WHERE user_id = %s
        ON CONFLICT DO NOTHING
    """, (rows[0]["user_id"],))
    print(f"  restored {cur.rowcount} game_watches row(s)")

    conn.commit()
    cur.execute("SELECT COUNT(*) FROM game_reviews WHERE user_id = %s",
                (rows[0]["user_id"],))
    print(f"\ninserted {inserted} review(s); that account now has {cur.fetchone()[0]}")
    cur.close(); conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("action", choices=["extract", "show", "restore"])
    ap.add_argument("--url", help="connection string for the RESTORED snapshot")
    ap.add_argument("--user", type=int, default=1, help="user id to recover")
    a = ap.parse_args()

    if a.action == "extract":
        if not a.url:
            print("--url is required (the restored snapshot, not production)")
            sys.exit(1)
        extract(a.url, a.user)
    elif a.action == "show":
        show()
    else:
        restore()
