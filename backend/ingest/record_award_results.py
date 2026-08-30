"""
ydkball — Record award winners (the answer key for awards ballots)
==================================================================
    python backend/ingest/record_award_results.py --season 2026-27
    python backend/ingest/record_award_results.py --season 2026-27 --dry-run
    python backend/ingest/record_award_results.py --league wnba --season 2026 \
           --set MVP="A'ja Wilson"

Ballots are graded against `award_results`, not against player_seasons.awards,
for two reasons. WNBA seasons carry no awards column at all, so those winners
can only ever be entered by hand. And grading should be a snapshot: a ballot
graded in May must not silently re-grade itself if an upstream backfill later
rewrites history.

NBA winners are lifted straight from `player_seasons.awards`, which
fetch_awards.py fills from the NBA's own PlayerAwards feed — so run
fetch_awards.py for the season first, then this. Award codes here are the same
labels that file writes, which is what makes the lookup a plain array match.

Run once a season, after the league announces. Safe to run repeatedly; a
re-run overwrites a winner only if the source now disagrees.
"""
import argparse
import os
import sys

from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("DATABASE_URL not found."); sys.exit(1)

# Codes must match _AWARD_TEMPLATES in server.py — those are the slots a ballot
# has, and a result for any other code would never be read.
NBA_AWARDS  = ["MVP", "ROTY", "DPOY", "6MOY", "MIP", "CPOY", "COY", "CHAMPION"]
WNBA_AWARDS = ["MVP", "ROTY", "DPOY", "6WOY", "MIP", "CHAMPION"]

# Lifted from player_seasons.awards. The rest of a ballot can't come from there:
# CHAMPION is derived from the schedule (below), and COY has no upstream feed at
# all — the NBA's awards endpoint is keyed to players, and a coach isn't one — so
# it's entered by hand like the WNBA winners.
FROM_PLAYER_AWARDS = ["MVP", "ROTY", "DPOY", "6MOY", "MIP", "CPOY"]


def champion(conn, league, season):
    """The winner of the season's last playoff game.

    No feed needed — the champion is a fact about a schedule we already store, so
    it's the one slot that grades itself the moment the Finals end.
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT home_team_abbr, away_team_abbr, home_score, away_score
            FROM games
            WHERE league = %s AND season = %s AND season_type = 'Playoffs'
              AND status = 'Final' AND home_score IS NOT NULL
            ORDER BY game_date DESC, game_id DESC
            LIMIT 1
        """, (league, season))
        g = cur.fetchone()
        if not g:
            print("  CHAMPION — no finished playoff game for that season yet")
            return None
        won = g["home_team_abbr"] if g["home_score"] > g["away_score"] else g["away_team_abbr"]
        print(f"  CHAMPION — {won}")
        # The abbr goes in `team`, which is what a team pick is graded against.
        return ("CHAMPION", None, won, won)


def nba_winners(conn, season):
    """Whoever's player_seasons row for that season lists the award."""
    out = []
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        for code in FROM_PLAYER_AWARDS:
            cur.execute("""
                SELECT ps.player_id, p.player_name, ps.team_abbr
                FROM player_seasons ps
                JOIN players p ON p.player_id = ps.player_id
                WHERE ps.season = %s AND ps.season_type = 'Regular Season'
                  AND ps.awards @> ARRAY[%s]::text[]
                LIMIT 2
            """, (season, code))
            rows = cur.fetchall()
            if not rows:
                print(f"  {code:5} — no winner in player_seasons yet")
                continue
            if len(rows) > 1:
                # Two winners means the upstream feed is mid-backfill or the
                # award was shared; either way a human should look before we
                # pick one arbitrarily.
                print(f"  {code:5} — SKIPPED, {len(rows)} players carry this award")
                continue
            r = rows[0]
            out.append((code, r["player_id"], r["player_name"], r["team_abbr"]))
            print(f"  {code:5} — {r['player_name']} ({r['team_abbr']})")
    return out


def write(conn, league, season, winners, dry_run=False):
    if dry_run:
        print(f"\n[dry run] {len(winners)} winner(s) not written.")
        return
    with conn.cursor() as cur:
        for code, pid, name, team in winners:
            cur.execute("""
                INSERT INTO award_results (league, season, award_code, person_id, player_name, team)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (league, season, award_code) DO UPDATE
                    SET person_id = EXCLUDED.person_id,
                        player_name = EXCLUDED.player_name,
                        team = EXCLUDED.team,
                        recorded_at = NOW()
            """, (league, season, code, pid, name, team))
    conn.commit()
    print(f"\nWrote {len(winners)} winner(s) for {league} {season}.")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--league", default="nba", choices=["nba", "wnba"])
    ap.add_argument("--season", required=True, help="e.g. 2026-27 (NBA) or 2026 (WNBA)")
    ap.add_argument("--set", action="append", default=[], metavar='CODE="Player Name"',
                    help="enter a winner by hand — the only path for the WNBA")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    valid = NBA_AWARDS if args.league == "nba" else WNBA_AWARDS
    conn = psycopg2.connect(DATABASE_URL)

    winners = []
    if args.set:
        for pair in args.set:
            code, _, name = pair.partition("=")
            code, name = code.strip(), name.strip()
            if code not in valid:
                print(f"unknown award '{code}' for {args.league} (expected one of {valid})")
                sys.exit(1)
            if not name:
                print(f"no player name given for {code}"); sys.exit(1)
            # No person_id on a hand-entered winner; server.py falls back to
            # comparing normalised names when either side lacks an id.
            winners.append((code, None, name, None))
            print(f"  {code:5} — {name} (manual)")
    elif args.league == "nba":
        print(f"Reading NBA {args.season} winners from player_seasons.awards…")
        winners = nba_winners(conn, args.season)
        champ = champion(conn, "nba", args.season)
        if champ:
            winners.append(champ)
        print('  COY      — no upstream feed; pass it with --set COY="Name"')
    else:
        # The champion is derivable even though the WNBA's player awards aren't.
        champ = champion(conn, "wnba", args.season)
        winners = [champ] if champ else []
        print("WNBA player awards have no upstream feed — pass them with --set, e.g.\n"
              '  --set MVP="A\'ja Wilson" --set DPOY="Alanna Smith"')
        if not winners:
            sys.exit(1)

    if not winners:
        print("Nothing to write."); sys.exit(1)
    write(conn, args.league, args.season, winners, args.dry_run)
    conn.close()


if __name__ == "__main__":
    main()
