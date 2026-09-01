"""
ydkball — Schema v14: a note no longer needs a star rating
=============================================================
python backend/schema_v14.py

Makes `game_reviews.rating` nullable so a written note can stand on its own.

Why: after Player of the Game replaced letter grading, every field on the log
sheet became independently submittable — a rating alone, a pick alone, "I
watched this" alone — except the note, which the server rejected with "A written
review needs a game rating." That asymmetry wasn't a rule anyone chose; it fell
out of `rating INTEGER NOT NULL` on the only table review text has to live in.
Worse, once a pick alone became submittable, pick + note + no rating passed the
client's gate and 400'd at the server.

The CHECK (rating BETWEEN 1 AND 10) is deliberately left in place: a CHECK
passes on NULL, so it still rejects a 0 or an 11 while allowing "no rating".

The companion change is in server.py: every place that averaged ratings used
COUNT(*) as its denominator, which would now include ratingless rows and drag
every game's average down. Those are COUNT(rating) as of this migration — the
app already labels that number "N ratings".

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


def run():
    print("Connecting to database...")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        cur.execute("""
            SELECT is_nullable FROM information_schema.columns
             WHERE table_name = 'game_reviews' AND column_name = 'rating'
        """)
        row = cur.fetchone()
        if not row:
            print("game_reviews.rating not found — nothing to do."); sys.exit(1)

        if row[0] == "YES":
            print("game_reviews.rating is already nullable. ✅")
        else:
            print("Dropping NOT NULL on game_reviews.rating...")
            cur.execute("ALTER TABLE game_reviews ALTER COLUMN rating DROP NOT NULL")
            print("  done")

        # Nothing is backfilled: every existing row has a rating by construction.
        cur.execute("SELECT COUNT(*), COUNT(rating) FROM game_reviews")
        total, rated = cur.fetchone()
        print(f"\ngame_reviews: {total} row(s), {rated} rated, {total - rated} note-only")

        cur.execute("""
            SELECT COUNT(*) FROM games
             WHERE review_count IS DISTINCT FROM (
                 SELECT COUNT(rating) FROM game_reviews WHERE game_id = games.game_id)
        """)
        drift = cur.fetchone()[0]
        print(f"games.review_count rows disagreeing with COUNT(rating): {drift}"
              f"  {'✅' if drift == 0 else '— the daily backfill will settle these'}")
        cur.close(); conn.close()
    except Exception as e:
        print(f"❌ {e}")
        sys.exit(1)


if __name__ == "__main__":
    run()
