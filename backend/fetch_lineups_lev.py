"""
NothingButNet — Leverage-Filtered Lineup Stats (Game-Level Blowout Filter)
===========================================================================
python backend/fetch_lineups_lev.py [--season 2025-26] [--team BOS] [--threshold 15]

Computes leverage-filtered lineup stats by excluding entire games decided by
more than --threshold points (default 15). Much simpler and faster than the
PBP-based approach — one LeagueDashLineups call per close game instead of
two heavy API calls (GameRotation + PlayByPlayV3) per every game.

Game margins are read from the `games` DB table (populated by fetch_games.py).

Runtime: ~1–2 min per team, ~30–40 min for all 30 teams.
"""
import os, sys, time, math, argparse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
import psycopg2, psycopg2.extras
import pandas as pd

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


def safe_float(v):
    try:
        f = float(v)
        return None if (math.isnan(f) or math.isinf(f)) else f
    except Exception:
        return None


def fetch_game_lineups(team_id, season, date_str):
    """
    Fetch 5-man lineup stats for a single game date.
    Returns list of (frozenset_of_pids, min, ortg, drtg, net).
    """
    from nba_api.stats.endpoints import LeagueDashLineups
    ep = LeagueDashLineups(
        team_id_nullable=team_id,
        group_quantity=5,
        season=season,
        season_type_all_star="Regular Season",
        measure_type_detailed_defense="Advanced",
        per_mode_detailed="Totals",
        date_from_nullable=date_str,
        date_to_nullable=date_str,
        timeout=60,
    )
    df = ep.get_data_frames()[0]
    results = []
    for _, row in df.iterrows():
        pids  = frozenset(p for p in str(row["GROUP_ID"]).split("-") if p.strip())
        mins  = safe_float(row.get("MIN"))  or 0.0
        ortg  = safe_float(row.get("OFF_RATING"))
        drtg  = safe_float(row.get("DEF_RATING"))
        net   = safe_float(row.get("NET_RATING"))
        if mins > 0:
            results.append((pids, mins, ortg, drtg, net))
    return results


def fetch_team_lev(team_abbr, season, threshold):
    """Process one team: find close games, fetch lineup stats, update DB."""
    team_id = TEAM_IDS[team_abbr]

    conn = psycopg2.connect(DATABASE_URL)
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # ── Load existing lineup records ──────────────────────────────
    cur.execute("""
        SELECT id, player_ids FROM team_lineups
        WHERE team_abbr = %s AND season = %s
    """, (team_abbr, season))
    db_rows = cur.fetchall()
    if not db_rows:
        print(f"  [{team_abbr}] No lineups in DB. Run fetch_lineups.py first.")
        cur.close(); conn.close(); return

    lineup_lookup = {
        frozenset(str(p) for p in row["player_ids"]): row["id"]
        for row in db_rows
    }

    # ── Get close games from DB ───────────────────────────────────
    cur.execute("""
        SELECT game_date, home_team_abbr, away_team_abbr, home_score, away_score
        FROM games
        WHERE season = %s
          AND season_type = 'Regular Season'
          AND (home_team_abbr = %s OR away_team_abbr = %s)
          AND home_score IS NOT NULL AND away_score IS NOT NULL
          AND ABS(home_score - away_score) <= %s
        ORDER BY game_date
    """, (season, team_abbr, team_abbr, threshold))
    close_games = cur.fetchall()

    total_games_q = cur.execute("""
        SELECT COUNT(*) AS n FROM games
        WHERE season = %s AND season_type = 'Regular Season'
          AND (home_team_abbr = %s OR away_team_abbr = %s)
          AND home_score IS NOT NULL
    """, (season, team_abbr, team_abbr)) or cur.fetchone()
    cur.execute("""
        SELECT COUNT(*) AS n FROM games
        WHERE season = %s AND season_type = 'Regular Season'
          AND (home_team_abbr = %s OR away_team_abbr = %s)
          AND home_score IS NOT NULL
    """, (season, team_abbr, team_abbr))
    total_games = cur.fetchone()["n"]

    print(f"  [{team_abbr}] {len(close_games)}/{total_games} games within {threshold} pts",
          flush=True)

    if not close_games:
        cur.close(); conn.close(); return

    # ── Fetch lineup stats for each close game (2 threads) ────────
    weighted = defaultdict(lambda: {"min_sum": 0.0, "ortg_w": 0.0, "drtg_w": 0.0, "net_w": 0.0})

    def _fetch(game):
        date_str = game["game_date"].strftime("%m/%d/%Y")
        try:
            rows = fetch_game_lineups(team_id, season, date_str)
            time.sleep(0.6)
            return date_str, rows
        except Exception as e:
            time.sleep(0.6)
            return date_str, []

    completed = 0
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = {pool.submit(_fetch, g): g for g in close_games}
        for fut in as_completed(futures):
            date_str, rows = fut.result()
            completed += 1
            for pids, mins, ortg, drtg, net in rows:
                if ortg is not None and drtg is not None:
                    weighted[pids]["min_sum"] += mins
                    weighted[pids]["ortg_w"]  += ortg * mins
                    weighted[pids]["drtg_w"]  += drtg * mins
                    weighted[pids]["net_w"]   += (net or 0) * mins

    # ── Write _lev columns to DB ──────────────────────────────────
    updated = 0
    for pids, w in weighted.items():
        db_id = lineup_lookup.get(pids)
        if db_id is None:
            continue
        ms = w["min_sum"]
        if ms <= 0:
            continue
        ortg_lev = round(w["ortg_w"] / ms, 1)
        drtg_lev = round(w["drtg_w"] / ms, 1)
        net_lev  = round(w["net_w"]  / ms, 1)
        cur.execute("""
            UPDATE team_lineups
            SET min_lev=%s, ortg_lev=%s, drtg_lev=%s, net_lev=%s, updated_at=NOW()
            WHERE id=%s
        """, (round(ms, 1), ortg_lev, drtg_lev, net_lev, db_id))
        updated += 1

    conn.commit()
    cur.close(); conn.close()
    print(f"  [{team_abbr}] {updated} lineups updated.", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--season",    default="2025-26")
    parser.add_argument("--team",      default=None)
    parser.add_argument("--threshold", default=15, type=int,
                        help="Max final margin to count as a close game (default 15)")
    args = parser.parse_args()

    teams = [args.team.upper()] if args.team else list(TEAM_IDS.keys())
    unknown = [t for t in teams if t not in TEAM_IDS]
    if unknown:
        print(f"Unknown teams: {unknown}"); sys.exit(1)

    print(f"Leverage filter: games decided by ≤{args.threshold} pts")
    print(f"Processing {len(teams)} team(s) for {args.season}...\n")

    for i, abbr in enumerate(teams, 1):
        print(f"[{i}/{len(teams)}] {abbr}")
        try:
            fetch_team_lev(abbr, args.season, args.threshold)
        except Exception as e:
            print(f"  [{abbr}] error: {e}")
        print()
        if i < len(teams):
            time.sleep(0.5)

    print("Done.")


if __name__ == "__main__":
    main()
