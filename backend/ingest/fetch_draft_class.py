"""
ydkball — Seed a draft class into `players`
============================================
    python backend/ingest/fetch_draft_class.py --year 2026
    python backend/ingest/fetch_draft_class.py --year 2026 --dry-run

Rookies are invisible to the rest of the app until they play: almost every
player lookup joins `player_seasons`, and a player drafted in June has no row
there until October. That is fine everywhere except one place — the Rookie of
the Year slot on an awards ballot, which is filled in the preseason and is about
players who by definition have no stats yet.

So this seeds the draft class into `players` from the NBA's own DraftHistory
feed: real PERSON_IDs, so a rookie who later plays merges with their own
gamelogs instead of forking into a duplicate.

Seeding `players` and nothing else is deliberate. It does not touch
`player_seasons`, so these rows stay out of every stats surface, both daily
games, and the ordinary list player search — all of which join through that
table. The awards picker is the only place they surface, which is the point.

Runs from a residential IP (stats.nba.com blocks Railway), so this is a local
job like daily_update_local.py. Once a year, after the draft. Safe to re-run.
"""
import argparse
import os
import sys
import time

from dotenv import load_dotenv
import psycopg2

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from nba_api.stats.endpoints import drafthistory

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("DATABASE_URL not found."); sys.exit(1)


def fetch(year, tries=4):
    for attempt in range(tries):
        try:
            df = drafthistory.DraftHistory(
                season_year_nullable=str(year), timeout=45).get_data_frames()[0]
            return df
        except Exception as e:
            if attempt == tries - 1:
                raise
            print(f"  retry {attempt + 1}: {e}")
            time.sleep(2 + attempt * 2)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", required=True, type=int, help="draft year, e.g. 2026")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    print(f"Fetching the {args.year} draft class…")
    df = fetch(args.year)
    if df is None or df.empty:
        print("No picks returned — has the draft happened yet?")
        sys.exit(1)

    picks = []
    for _, r in df.iterrows():
        pid = r.get("PERSON_ID")
        name = (r.get("PLAYER_NAME") or "").strip()
        if not pid or not name:
            continue
        picks.append((int(pid), name, r.get("ROUND_NUMBER"), r.get("OVERALL_PICK"),
                      r.get("TEAM_ABBREVIATION")))
    print(f"  {len(picks)} picks")
    for pid, name, rnd, overall, team in picks[:5]:
        print(f"    #{overall} {name} ({team})")
    if len(picks) > 5:
        print(f"    … and {len(picks) - 5} more")

    if args.dry_run:
        print("\n[dry run] nothing written.")
        return

    conn = psycopg2.connect(DATABASE_URL)
    inserted = updated = 0
    with conn.cursor() as cur:
        for pid, name, rnd, overall, team in picks:
            cur.execute("SELECT 1 FROM players WHERE player_id = %s", (pid,))
            existed = cur.fetchone() is not None
            # Only the draft fields are authoritative here. A player who already
            # has a row (a re-draft, or a re-run after they debut) keeps whatever
            # bio the roster sync filled in.
            cur.execute("""
                INSERT INTO players (player_id, player_name, draft_year, draft_round,
                                     draft_number, is_active, updated_at)
                VALUES (%s, %s, %s, %s, %s, TRUE, NOW())
                ON CONFLICT (player_id) DO UPDATE
                    SET draft_year   = EXCLUDED.draft_year,
                        draft_round  = EXCLUDED.draft_round,
                        draft_number = EXCLUDED.draft_number,
                        is_active    = TRUE,
                        updated_at   = NOW()
            """, (pid, name, args.year,
                  int(rnd) if rnd else None, int(overall) if overall else None))
            if existed:
                updated += 1
            else:
                inserted += 1
    conn.commit()
    conn.close()
    print(f"\n{inserted} new player(s), {updated} updated.")


if __name__ == "__main__":
    main()
