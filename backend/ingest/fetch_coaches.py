"""
ydkball — Head coaches, for the Coach of the Year ballot slot
=============================================================
    python backend/ingest/fetch_coaches.py --season 2026-27
    python backend/ingest/fetch_coaches.py --season 2026-27 --dry-run

Coaches exist nowhere else in the app — separate id namespace, no stats, no
roster row — so the ballot's Coach of the Year slot has nothing to offer until
this runs. Source is CommonTeamRoster's second data frame, one call per team.

NBA only: nba_api has no WNBA coach feed, which is why the WNBA ballot carries
no Coach of the Year slot at all.

Runs from a residential IP (stats.nba.com blocks Railway), so this is a local
job like daily_update_local.py. Once before the season, and again after any
midseason firing you care about. Safe to re-run — coaches are upserted, and a
coach who changed teams updates in place.
"""
import argparse
import os
import sys
import time

from dotenv import load_dotenv
import psycopg2

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from nba_api.stats.endpoints import commonteamroster
from nba_api.stats.static import teams as static_teams
import season_util

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("DATABASE_URL not found."); sys.exit(1)


def fetch_team(team_id, season, tries=3):
    for attempt in range(tries):
        try:
            frames = commonteamroster.CommonTeamRoster(
                team_id=team_id, season=season, timeout=45).get_data_frames()
            return frames[1] if len(frames) > 1 else None
        except Exception as e:
            if attempt == tries - 1:
                print(f"    failed: {e}")
                return None
            time.sleep(2 + attempt * 2)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", default=None,
                    help="e.g. 2026-27; defaults to the upcoming season")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    # Membership, not stats — same rule as rosters.
    season = args.season or season_util.roster_season()

    nba_teams = static_teams.get_teams()
    print(f"Fetching head coaches for {season} across {len(nba_teams)} teams…")

    found = []
    for t in nba_teams:
        df = fetch_team(t["id"], season)
        if df is None or df.empty:
            continue
        # COACH_TYPE distinguishes the head coach from assistants and trainers;
        # IS_ASSISTANT is not a clean boolean in this feed, so don't trust it.
        heads = df[df["COACH_TYPE"].astype(str).str.strip().str.lower() == "head coach"]
        for _, r in heads.iterrows():
            cid, name = r.get("COACH_ID"), (r.get("COACH_NAME") or "").strip()
            if not cid or not name:
                continue
            found.append((int(cid), name, t["abbreviation"]))
            print(f"  {t['abbreviation']:4} {name}")
        time.sleep(0.4)          # be gentle; this is 30 sequential calls

    if not found:
        print("No coaches returned — is the season string right?")
        sys.exit(1)
    print(f"\n{len(found)} head coach(es).")
    if args.dry_run:
        print("[dry run] nothing written.")
        return

    conn = psycopg2.connect(DATABASE_URL)
    with conn.cursor() as cur:
        for cid, name, abbr in found:
            cur.execute("""
                INSERT INTO coaches (coach_id, season, league, coach_name, team_abbr, is_head, updated_at)
                VALUES (%s, %s, 'nba', %s, %s, TRUE, NOW())
                ON CONFLICT (coach_id, season) DO UPDATE
                    SET coach_name = EXCLUDED.coach_name,
                        team_abbr  = EXCLUDED.team_abbr,
                        is_head    = TRUE,
                        updated_at = NOW()
            """, (cid, season, name, abbr))
    conn.commit(); conn.close()
    print(f"Wrote {len(found)} coaches for {season}.")


if __name__ == "__main__":
    main()
