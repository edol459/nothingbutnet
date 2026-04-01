"""
NothingButNet — Percentile Computation
========================================
python backend/ingest/compute_pctiles.py

For every numeric column in player_seasons, computes each qualifying
player's league-wide percentile rank and stores the full map in
player_pctiles as JSONB.

Qualifying players: min >= MIN_MINUTES_TOTAL.

Run after fetch_stats.py:
    python backend/ingest/compute_pctiles.py
    python backend/ingest/compute_pctiles.py --season 2023-24

The Builder tool reads from player_pctiles to rank players.
"""

import os
import sys
import json
import argparse
from datetime import datetime
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
SEASON       = os.getenv("NBA_SEASON", "2024-25")
SEASON_TYPE  = os.getenv("NBA_SEASON_TYPE", "Regular Season")

if not DATABASE_URL:
    print("❌ DATABASE_URL not set.")
    sys.exit(1)

parser = argparse.ArgumentParser()
parser.add_argument("--season",      default=SEASON)
parser.add_argument("--season-type", default=SEASON_TYPE)
parser.add_argument("--min-minutes", type=int, default=500)
args = parser.parse_args()

SEASON      = args.season
SEASON_TYPE = args.season_type
MIN_MINUTES = args.min_minutes

# Stats to skip (non-numeric / identifier columns)
SKIP_COLS = {
    "id", "player_id", "season", "season_type", "team_id",
    "team_abbr", "updated_at",
}

# Stats where LOWER is better — percentile is inverted so 100 = best
INVERT_COLS = {
    # Turnovers / fouls
    "tov", "pf",
    "bad_pass_tov", "lost_ball_tov", "drive_tov",

    # Defensive rating: lower points allowed per 100 possessions = better
    "def_rating",

    # Rim defense: lower opponent FG% allowed = better
    "def_rim_fg_pct",

    # Defensive playtype PPP: lower points allowed per possession = better
    "def_iso_ppp", "def_pnr_bh_ppp", "def_post_ppp", "def_spotup_ppp", "def_pnr_roll_ppp",

    # Isolation offense: lower turnover rate = better
    "iso_tov_pct",

    # Closest defender: fewer makes allowed = better (per tightness bucket)
    "cd_fgm_vt", "cd_fgm_tg", "cd_fgm_op", "cd_fgm_wo",
}


def compute_pctiles(values: list[tuple[int, float]]) -> dict[int, float]:
    """
    Given [(player_id, value), ...], return {player_id: percentile}.
    Percentile = fraction of players with a LOWER value × 100.
    Ties share the same percentile (averaged rank method).
    """
    sorted_vals = sorted(v for _, v in values)
    n = len(sorted_vals)
    result = {}
    for pid, val in values:
        # Count how many players have strictly lower value
        rank = sum(1 for v in sorted_vals if v < val)
        pct  = round((rank / (n - 1)) * 100, 2) if n > 1 else 50.0
        result[pid] = pct
    return result


def run():
    print(f"\n📊 NothingButNet — Computing Percentiles")
    print(f"   Season: {SEASON} {SEASON_TYPE}")
    print(f"   Min minutes: {MIN_MINUTES}")
    print("=" * 50)

    conn = psycopg2.connect(DATABASE_URL)
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Fetch qualifying players
    cur.execute("""
        SELECT * FROM player_seasons
        WHERE season = %s AND season_type = %s
          AND min >= %s
    """, (SEASON, SEASON_TYPE, MIN_MINUTES))
    rows = cur.fetchall()

    if not rows:
        print("❌ No players found. Run fetch_stats.py first.")
        sys.exit(1)

    print(f"   {len(rows)} qualifying players found.\n")

    # Get all numeric column names
    cur.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'player_seasons'
        ORDER BY ordinal_position
    """)
    all_cols = cur.fetchall()
    numeric_cols = [
        c["column_name"] for c in all_cols
        if c["data_type"] in ("real", "integer", "numeric", "double precision")
        and c["column_name"] not in SKIP_COLS
    ]

    print(f"   {len(numeric_cols)} numeric stats to rank.\n")

    upserted = 0
    skipped  = 0
    wcur = conn.cursor()

    # Stats stored as season totals — normalize by GP before ranking
    # so percentiles reflect per-game rate, not volume
    TOTAL_KEYS = {
        'drives', 'drive_fga', 'drive_fgm', 'drive_pts', 'drive_passes', 'drive_pf', 'drive_tov',
        'passes_made', 'passes_received',
        'ast_pts_created', 'potential_ast',
        'touches', 'paint_touches', 'elbow_touches',
        'pull_up_fga', 'pull_up_fgm', 'pull_up_fg3a',
        'cs_fga', 'cs_fgm', 'cs_fg3a',
        'contested_shots', 'contested_2pt', 'contested_3pt',
        'deflections',
        'def_rim_fga', 'def_rim_fgm',
        'screen_ast_pts',
        'cd_fga_vt', 'cd_fga_tg', 'cd_fga_op', 'cd_fga_wo',
        'cd_fgm_vt', 'cd_fgm_tg', 'cd_fgm_op', 'cd_fgm_wo',
        'iso_fga', 'pnr_bh_fga', 'transition_fga',
        'pts_paint',
    }

    for stat in numeric_cols:
        # Collect (player_id, value) pairs where value is non-null
        if stat in TOTAL_KEYS:
            # Divide by GP to get per-game rate for fair comparison
            pairs = [
                (int(r["player_id"]), float(r[stat]) / float(r["gp"]))
                for r in rows
                if r[stat] is not None and r.get("gp") and float(r["gp"]) > 0
            ]
        else:
            pairs = [
                (int(r["player_id"]), float(r[stat]))
                for r in rows
                if r[stat] is not None
            ]

        if len(pairs) < 5:
            skipped += 1
            continue

        pct_map = compute_pctiles(pairs)

        # Invert if lower = better
        if stat in INVERT_COLS:
            pct_map = {pid: round(100 - pct, 2) for pid, pct in pct_map.items()}

        wcur.execute("""
            INSERT INTO player_pctiles (season, season_type, stat_key, pctile_map, updated_at)
            VALUES (%s, %s, %s, %s, NOW())
            ON CONFLICT (season, season_type, stat_key) DO UPDATE SET
                pctile_map = EXCLUDED.pctile_map,
                updated_at = NOW()
        """, (SEASON, SEASON_TYPE, stat, json.dumps(pct_map)))

        upserted += 1

    conn.commit()
    wcur.close()
    cur.close()
    conn.close()

    print(f"✅ Done.")
    print(f"   {upserted} stat percentile maps stored.")
    print(f"   {skipped} stats skipped (too few players with data).")


if __name__ == "__main__":
    run()