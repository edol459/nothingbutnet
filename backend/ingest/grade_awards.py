"""
ydkball — Record award winners automatically, once they exist
=============================================================
    python backend/ingest/grade_awards.py            # gated; usually a no-op
    python backend/ingest/grade_awards.py --dry-run
    python backend/ingest/grade_awards.py --season 2026-27 --force

Wraps the two commands that turn "the league announced" into "everyone's ballot
is graded", so nobody has to remember them in May:

    fetch_awards.py --refresh-season <season>    (fills player_seasons.awards)
    record_award_results.py --season <season>    (writes the answer key)

Safe to run daily because it gates itself. It does nothing unless there are
locked ballots for a season whose answer key is still incomplete — so it sleeps
all season, wakes for the couple of weeks around the announcements, and goes
quiet again the moment the winners are in.

The expensive half is the awards refresh: ~570 player lookups against
stats.nba.com, several minutes. That's why the gate matters, and why this lives
in daily_update_local.py — stats.nba.com blocks Railway, so it needs the
residential IP. The recorder alone would run anywhere.

Coach of the Year is never filled in here. No upstream feed reports it, so it
stays a hand-entered row:

    record_award_results.py --season 2026-27 --set COY="Name"

Its absence is also why the gate ignores COY when deciding whether a season is
"done" — otherwise this would re-run forever waiting for a winner that can only
arrive by hand.
"""
import argparse
import os
import subprocess
import sys

from dotenv import load_dotenv
import psycopg2

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("DATABASE_URL not found."); sys.exit(1)

# The awards this can actually resolve on its own. COY is excluded on purpose —
# see the module docstring.
AUTO_AWARDS = {
    "nba":  ["MVP", "ROTY", "DPOY", "6MOY", "MIP", "CPOY", "CHAMPION"],
    "wnba": ["CHAMPION"],          # WNBA player awards have no feed either
}


def seasons_needing_grading(conn):
    """(league, season) pairs with locked ballots and an incomplete answer key.

    Anchored on ballots rather than on the calendar: if nobody made a ballot,
    there is nothing to grade and no reason to spend the API calls.
    """
    out = []
    with conn.cursor() as cur:
        cur.execute("""
            SELECT gl.league, gl.season, COUNT(*) AS ballots
            FROM game_lists gl
            WHERE gl.list_type = 'awards'
              AND gl.locked_at IS NOT NULL AND gl.locked_at < NOW()
            GROUP BY gl.league, gl.season
            ORDER BY gl.season DESC
        """)
        for league, season, ballots in cur.fetchall():
            wanted = AUTO_AWARDS.get(league, [])
            if not wanted:
                continue
            cur.execute("""SELECT award_code FROM award_results
                           WHERE league = %s AND season = %s""", (league, season))
            have = {r[0] for r in cur.fetchall()}
            missing = [a for a in wanted if a not in have]
            if missing:
                out.append((league, season, ballots, missing))
    return out


def run(script, args, dry_run):
    cmd = [sys.executable, os.path.join(HERE, script)] + args
    print("  $ " + " ".join(cmd[1:]))
    if dry_run:
        return True
    result = subprocess.run(cmd)
    return result.returncode == 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", default=None, help="grade this season regardless of the gate")
    ap.add_argument("--league", default="nba", choices=["nba", "wnba"])
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--force", action="store_true",
                    help="with --season, skip the has-ballots / is-incomplete gate")
    args = ap.parse_args()

    conn = psycopg2.connect(DATABASE_URL)

    if args.season and args.force:
        todo = [(args.league, args.season, 0, ["(forced)"])]
    else:
        todo = seasons_needing_grading(conn)
        if args.season:
            todo = [t for t in todo if t[1] == args.season]

    if not todo:
        print("Nothing to grade — no locked ballots with a missing answer key.")
        conn.close()
        return

    for league, season, ballots, missing in todo:
        print(f"\n{league.upper()} {season}: {ballots} locked ballot(s), "
              f"missing {', '.join(missing)}")
        if league == "nba":
            # Only the NBA has a player-awards feed to refresh; the WNBA's
            # champion comes from the games table, which needs no fetch.
            if not run("fetch_awards.py", ["--refresh-season", season], args.dry_run):
                print("  awards refresh failed — leaving the answer key alone")
                continue
        run("record_award_results.py", ["--league", league, "--season", season], args.dry_run)

    conn.close()
    if args.dry_run:
        print("\n[dry run] nothing executed.")
    else:
        print("\nDone. Coach of the Year, if you want it, is still by hand:")
        for league, season, _, _ in todo:
            if league == "nba":
                print(f'  python backend/ingest/record_award_results.py '
                      f'--season {season} --set COY="Name"')


if __name__ == "__main__":
    main()
