"""
ydkball — Fetch Lineup & Roster Data
============================================
python backend/fetch_lineups.py [--season 2025-26] [--team BOS]

Pulls 5-man lineup advanced stats + rosters from the NBA API
and upserts them into team_rosters and team_lineups.

By default fetches all 30 teams for the given season.
Pass --team to refresh a single team.
"""
import os, sys, time, math, argparse
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("DATABASE_URL not found."); sys.exit(1)

TEAM_IDS = {
    "ATL":1610612737,"BOS":1610612738,"BKN":1610612751,"CHA":1610612766,
    "CHI":1610612741,"CLE":1610612739,"DAL":1610612742,"DEN":1610612743,
    "DET":1610612765,"GSW":1610612744,"HOU":1610612745,"IND":1610612754,
    "LAC":1610612746,"LAL":1610612747,"MEM":1610612763,"MIA":1610612748,
    "MIL":1610612749,"MIN":1610612750,"NOP":1610612740,"NYK":1610612752,
    "OKC":1610612760,"ORL":1610612753,"PHI":1610612755,"PHX":1610612756,
    "POR":1610612757,"SAC":1610612758,"SAS":1610612759,"TOR":1610612761,
    "UTA":1610612762,"WAS":1610612764,
}


def safe(v):
    try:
        f = float(v)
        return None if math.isnan(f) or math.isinf(f) else round(f, 1)
    except Exception:
        return None


def fetch_team(cur, team_abbr, season):
    import pandas as pd
    from nba_api.stats.endpoints import LeagueDashLineups, CommonTeamRoster

    team_id = TEAM_IDS[team_abbr]
    print(f"  {team_abbr} roster...", end=" ", flush=True)

    try:
        r_df = CommonTeamRoster(team_id=team_id, season=season).get_data_frames()[0]
        time.sleep(0.8)
        for _, row in r_df.iterrows():
            cur.execute("""
                INSERT INTO team_rosters (team_abbr, season, player_id, player_name, number, position)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (team_abbr, season, player_id) DO UPDATE SET
                    player_name = EXCLUDED.player_name,
                    number      = EXCLUDED.number,
                    position    = EXCLUDED.position,
                    updated_at  = NOW()
            """, (
                team_abbr, season,
                str(int(row["PLAYER_ID"])),
                str(row["PLAYER"]),
                str(row.get("NUM", "") or ""),
                str(row.get("POSITION", "") or ""),
            ))
        print(f"{len(r_df)} players", end=" | ", flush=True)
    except Exception as e:
        print(f"roster error: {e}", end=" | ", flush=True)

    print("lineups...", end=" ", flush=True)
    ep = LeagueDashLineups(
        team_id_nullable=team_id,
        group_quantity=5,
        season=season,
        season_type_all_star="Regular Season",
        measure_type_detailed_defense="Advanced",
        per_mode_detailed="Totals",
        timeout=60,
    )
    time.sleep(0.8)
    df = ep.get_data_frames()[0]

    if df.empty:
        print("no lineups found")
        return

    count = 0
    for _, row in df.iterrows():
        group_id = str(row["GROUP_ID"])
        pids = [p for p in group_id.split("-") if p.strip()]
        mins = float(row["MIN"]) if pd.notna(row.get("MIN")) else 0.0
        cur.execute("""
            INSERT INTO team_lineups
                (team_abbr, season, group_id, player_ids, min, ortg, drtg, net, gp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (team_abbr, season, group_id) DO UPDATE SET
                player_ids = EXCLUDED.player_ids,
                min        = EXCLUDED.min,
                ortg       = EXCLUDED.ortg,
                drtg       = EXCLUDED.drtg,
                net        = EXCLUDED.net,
                gp         = EXCLUDED.gp,
                updated_at = NOW()
        """, (
            team_abbr, season, group_id, pids, mins,
            safe(row.get("OFF_RATING")),
            safe(row.get("DEF_RATING")),
            safe(row.get("NET_RATING")),
            int(row["GP"]) if pd.notna(row.get("GP")) else 0,
        ))
        count += 1

    print(f"{count} lineups")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", default="2025-26")
    parser.add_argument("--team",   default=None, help="Single team abbr, e.g. BOS")
    args = parser.parse_args()

    teams = [args.team.upper()] if args.team else list(TEAM_IDS.keys())
    unknown = [t for t in teams if t not in TEAM_IDS]
    if unknown:
        print(f"Unknown teams: {unknown}"); sys.exit(1)

    conn = psycopg2.connect(DATABASE_URL)
    cur  = conn.cursor()

    print(f"Fetching {len(teams)} team(s) for {args.season}...\n")
    for i, abbr in enumerate(teams, 1):
        print(f"[{i}/{len(teams)}] ", end="")
        fetch_team(cur, abbr, args.season)
        conn.commit()
        if i < len(teams):
            time.sleep(1.0)  # be polite between teams

    cur.close()
    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
