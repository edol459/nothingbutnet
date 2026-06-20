"""
Backfill `fgm_25ft_pg` — made field goals from 25+ feet per game — into player_seasons.

Source: stats.nba.com LeagueDashPlayerShotLocations with distance_range="5ft Range",
summing the 25-29 / 30-34 / 35-39 / 40+ ft buckets (i.e. "deep" makes).  Available
for every season back to 1996-97.

    python backend/ingest/fetch_shot_distance.py --season 2025-26
    python backend/ingest/fetch_shot_distance.py --all          # 1996-97 → current
    python backend/ingest/fetch_shot_distance.py --all --since 2020-21

stats.nba.com is rate-limited, so we pace requests and retry on failure.
"""

import os
import time
import argparse

import psycopg2
from dotenv import load_dotenv
from nba_api.stats.endpoints import leaguedashplayershotlocations as L

load_dotenv()

SEASON_TYPE = "Regular Season"
DEEP_BUCKETS = {"25-29 ft.", "30-34 ft.", "35-39 ft.", "40+ ft."}


def all_seasons(since="1996-97"):
    start = int(since[:4])
    end = 2025  # 2025-26 is the latest; bump as seasons are added
    return [f"{y}-{str(y+1)[-2:]}" for y in range(start, end + 1)]


def fetch_25plus_per_game(season, tries=4):
    """Return {player_id: fgm_25ft_per_game} for a season."""
    for attempt in range(tries):
        try:
            r = L.LeagueDashPlayerShotLocations(
                season=season, season_type_all_star=SEASON_TYPE,
                distance_range="5ft Range", per_mode_detailed="PerGame", timeout=60)
            d = r.get_dict()["resultSets"]
            buckets = d["headers"][0]["columnNames"]   # 9 distance labels
            cols = d["headers"][1]["columnNames"]       # info cols then FGM,FGA,FG_PCT per bucket
            n_info = cols.index("FGM")                  # first FGM = start of bucket data
            pid_i = cols.index("PLAYER_ID")
            deep = [i for i, b in enumerate(buckets) if b in DEEP_BUCKETS]
            out = {}
            for row in d["rowSet"]:
                fgm25 = sum(row[n_info + b * 3] or 0 for b in deep)   # 3 = FGM/FGA/FG_PCT stride
                out[row[pid_i]] = round(float(fgm25), 3)
            return out
        except Exception as e:
            wait = 3 * (attempt + 1)
            print(f"  retry {attempt+1}/{tries} for {season} after error: {str(e)[:80]} (sleep {wait}s)")
            time.sleep(wait)
    print(f"  ✗ giving up on {season}")
    return {}


def ensure_column(conn):
    with conn.cursor() as cur:
        cur.execute("ALTER TABLE player_seasons ADD COLUMN IF NOT EXISTS fgm_25ft_pg REAL")
    conn.commit()


def backfill_season(conn, season):
    data = fetch_25plus_per_game(season)
    if not data:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """UPDATE player_seasons SET fgm_25ft_pg = %s, updated_at = NOW()
               WHERE player_id = %s AND season = %s AND season_type = %s""",
            [(v, pid, season, SEASON_TYPE) for pid, v in data.items()],
        )
        written = cur.rowcount
    conn.commit()
    print(f"  ✅ {season}: {written} rows updated ({len(data)} players fetched)")
    return written


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", type=str, default=None)
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--since", type=str, default="1996-97")
    ap.add_argument("--pause", type=float, default=1.5, help="seconds between seasons")
    args = ap.parse_args()

    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    ensure_column(conn)

    seasons = all_seasons(args.since) if args.all else [args.season or "2025-26"]
    print(f"Backfilling fgm_25ft_pg for {len(seasons)} season(s): {seasons[0]}…{seasons[-1]}")
    for s in seasons:
        backfill_season(conn, s)
        time.sleep(args.pause)
    conn.close()
    print("done.")


if __name__ == "__main__":
    main()
