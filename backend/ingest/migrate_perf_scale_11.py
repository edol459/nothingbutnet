"""
ydkball — migrate performance_reviews from the 10-point scale to the 11-point letter scale
==========================================================================================
python backend/ingest/migrate_perf_scale_11.py [--apply]

DO NOT RUN THIS UNTIL THE APP STORE UPDATE SHIPS.

Player performances are graded A+ … F. A full letter scale wants ELEVEN values (A, B and C
each with +/- is nine, plus D and F) but the column holds ten, so C- does not exist yet.
Adding it means shifting stored ratings — and every app build older than the letter-grade
release renders these as stars (rating/2), so shifting early makes every rating on those
builds jump half a star. Hence: ship the app update first, then run this.

The remap inserts C- at 3 and leaves the bottom of the scale alone:

    old 10 (A+) -> new 11 (A+)        old  4 (C+) -> new  5 (C+)
    old  9 (A)  -> new 10 (A)         old  3 (C)  -> new  4 (C)
    old  8 (A-) -> new  9 (A-)                       new  3 (C-) newly reachable
    old  7 (B+) -> new  8 (B+)        old  2 (D)  -> new  2 (D)   [unchanged]
    old  6 (B)  -> new  7 (B)         old  1 (F)  -> new  1 (F)   [unchanged]
    old  5 (B-) -> new  6 (B-)

i.e. `rating + 1` for everything >= 3. Every existing grade keeps its letter; D and F do not
move at all.

Once run, bump PERF_RATING_MAX to 11 in server.py and PerfGrade.maxRating to 11 in the iOS
app (and give the picker's C bucket its minus value) — the code intentionally still writes
the 10-point scale until then.

Runs read-only by default. Pass --apply to write.
Safe to run twice: it detects an already-migrated table and refuses rather than double-shifting.
"""

import os
import sys
import argparse

from dotenv import load_dotenv
import psycopg2
import psycopg2.extras

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

OLD_MAX = 10
NEW_MAX = 11


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="actually write (default is a dry run)")
    args = ap.parse_args()

    conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) AS n, MIN(rating) AS lo, MAX(rating) AS hi FROM performance_reviews")
    row = cur.fetchone()
    total, lo, hi = row["n"], row["lo"], row["hi"]
    print(f"performance_reviews: {total} rows, rating range {lo}–{hi}")

    if total == 0:
        print("Nothing to migrate (table is empty). The CHECK widening in _ensure_tables is enough.")
        conn.close()
        return

    # A rating of 11 can only exist post-migration, so it's the marker that we already ran.
    cur.execute("SELECT COUNT(*) AS n FROM performance_reviews WHERE rating > %s", (OLD_MAX,))
    already = cur.fetchone()["n"]
    if already:
        print(f"\nABORT: {already} row(s) already have rating > {OLD_MAX}. This table looks "
              f"migrated.\nRunning again would shift every grade a second time.")
        conn.close()
        sys.exit(1)

    cur.execute("""
        SELECT rating, COUNT(*) AS n FROM performance_reviews GROUP BY rating ORDER BY rating
    """)
    rows = cur.fetchall()
    print("\n  old -> new   rows")
    moved = 0
    for r in rows:
        old = r["rating"]
        new = old + 1 if old >= 3 else old
        if new != old:
            moved += r["n"]
        print(f"   {old:>3} -> {new:>3}   {r['n']}")
    print(f"\n{moved} of {total} rows would change value (D and F stay put).")

    if not args.apply:
        print("\nDRY RUN — nothing written. Re-run with --apply to migrate.")
        conn.close()
        return

    # Descending order matters: the UNIQUE constraint is on (user_id, game_id, person_id) so
    # there's no rating collision, but updating high-to-low also keeps the CHECK satisfied at
    # every intermediate step if the constraint has not been widened yet.
    cur.execute("""
        ALTER TABLE performance_reviews DROP CONSTRAINT IF EXISTS performance_reviews_rating_check
    """)
    cur.execute("""
        ALTER TABLE performance_reviews
        ADD CONSTRAINT performance_reviews_rating_check CHECK (rating >= 1 AND rating <= 11)
    """)
    cur.execute("UPDATE performance_reviews SET rating = rating + 1 WHERE rating >= 3")
    changed = cur.rowcount
    conn.commit()

    cur.execute("SELECT COUNT(*) AS n, MIN(rating) AS lo, MAX(rating) AS hi FROM performance_reviews")
    after = cur.fetchone()
    print(f"\nAPPLIED: {changed} rows updated. New range {after['lo']}–{after['hi']} "
          f"across {after['n']} rows.")
    conn.close()


if __name__ == "__main__":
    main()
