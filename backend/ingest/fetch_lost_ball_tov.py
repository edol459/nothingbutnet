"""
The Impact Board — Lost Ball TOV from PBP
==========================================
python backend/ingest/fetch_lost_ball_tov.py

Fetches PlayByPlayV3 for every regular season game and counts
lost ball turnovers (strips, fumbles, travels while dribbling)
per player. Stores in player_seasons.lost_ball_tov.

bad_pass_tov is assumed to already be populated.
This script only adds the lost_ball_tov column.

Takes ~45-60 min (1230 games × 1.8s delay).
Safe to interrupt and re-run — tracks progress in a local file.
"""

import os
import sys
import time
import json
import argparse
from datetime import datetime
from collections import defaultdict
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')

parser = argparse.ArgumentParser()
parser.add_argument('--season',      default=os.getenv('NBA_SEASON', '2024-25'))
parser.add_argument('--season-type', default=os.getenv('NBA_SEASON_TYPE', 'Regular Season'))
args = parser.parse_args()

SEASON      = args.season
SEASON_TYPE = args.season_type

# Progress file lives next to this script regardless of invocation directory
season_slug   = SEASON.replace('-', '_')
PROGRESS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), f'lost_ball_progress_{season_slug}.json')
DELAY         = 1.8

if not DATABASE_URL:
    print("❌ DATABASE_URL not set.")
    sys.exit(1)

def try_import(name):
    try:
        import importlib
        mod = importlib.import_module("nba_api.stats.endpoints")
        return getattr(mod, name)
    except (ImportError, AttributeError):
        return None

LeagueGameFinder = try_import("LeagueGameFinder")
PlayByPlayV3     = try_import("PlayByPlayV3")

if not LeagueGameFinder or not PlayByPlayV3:
    print("❌ nba_api not available.")
    sys.exit(1)


def get_game_ids(season, season_type):
    print(f"Fetching game IDs for {season} {season_type}...")
    time.sleep(DELAY)
    try:
        gf = LeagueGameFinder(
            season_nullable=season,
            season_type_nullable=season_type,
            league_id_nullable="00",
        )
        df = gf.get_data_frames()[0]
        game_ids = sorted(df['GAME_ID'].unique().tolist())
        print(f"  ✅ {len(game_ids)} games found")
        return game_ids
    except Exception as e:
        print(f"  ❌ {e}")
        return []


def process_game(game_id):
    """Returns {player_id: lost_ball_count} or None on failure."""
    time.sleep(DELAY)
    try:
        pbp = PlayByPlayV3(game_id=game_id, timeout=10).get_data_frames()[0]
        mask = (
            (pbp['actionType'] == 'Turnover') &
            (pbp['subType'] == 'Lost Ball')
        )
        counts = defaultdict(int)
        for _, row in pbp[mask].iterrows():
            try:
                pid = int(float(row['personId']))
                if pid > 0:
                    counts[pid] += 1
            except:
                pass
        return dict(counts)
    except Exception as e:
        return None


def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    return {'processed_games': [], 'player_counts': {}}

def save_progress(progress):
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f)


def write_to_db(player_counts, season, season_type):
    conn = psycopg2.connect(DATABASE_URL)
    cur  = conn.cursor()

    cur.execute("""
        ALTER TABLE player_seasons
          ADD COLUMN IF NOT EXISTS lost_ball_tov REAL
    """)
    conn.commit()

    updated = 0
    for pid_str, count in player_counts.items():
        pid = int(pid_str)
        cur.execute("""
            UPDATE player_seasons
            SET lost_ball_tov = %s
            WHERE player_id = %s AND season = %s AND season_type = %s
        """, (count, pid, season, season_type))
        if cur.rowcount > 0:
            updated += 1

    conn.commit()

    cur.execute("""
        SELECT COUNT(*) FROM player_seasons
        WHERE lost_ball_tov IS NOT NULL
          AND season = %s AND season_type = %s
    """, (season, season_type))
    count_with_data = cur.fetchone()[0]

    cur.close()
    conn.close()
    print(f"  ✅ Updated {updated} players in DB")
    print(f"  ✅ {count_with_data} players now have lost_ball_tov data")


def spot_check():
    conn = psycopg2.connect(DATABASE_URL)
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    print(f"\n{'='*75}")
    print(f"Best ball security — lowest lost ball TOV/g (min 1000 min, G/GF only)")
    print(f"{'='*75}")
    cur.execute("""
        SELECT p.player_name, p.position_group, ps.ast, ps.tov,
               ps.bad_pass_tov, ps.lost_ball_tov, ps.gp,
               ROUND(CAST(ps.lost_ball_tov / NULLIF(ps.gp,0) AS NUMERIC), 2) AS lb_pg
        FROM player_seasons ps
        JOIN players p ON ps.player_id = p.player_id
        WHERE ps.season = %s AND ps.season_type = %s
          AND ps.lost_ball_tov IS NOT NULL
          AND ps.min >= 1000
          AND p.position_group IN ('G', 'GF')
        ORDER BY lb_pg ASC NULLS LAST
        LIMIT 20
    """, (SEASON, SEASON_TYPE))
    rows = cur.fetchall()
    print(f"{'Player':<25} {'POS':<4} {'AST':>5} {'TOV':>5} {'BP':>5} {'LB':>5} {'LB/G':>7}")
    print("─" * 75)
    for r in rows:
        print(f"  {r['player_name']:<23} {r['position_group'] or '':<4} "
              f"{r['ast'] or 0:>5.1f} {r['tov'] or 0:>5.1f} "
              f"{r['bad_pass_tov'] or 0:>5.0f} {r['lost_ball_tov'] or 0:>5.0f} "
              f"{r['lb_pg'] or 0:>7.2f}")

    print(f"\n{'='*75}")
    print(f"Worst ball security — highest lost ball TOV/g (min 1000 min, G/GF only)")
    print(f"{'='*75}")
    cur.execute("""
        SELECT p.player_name, p.position_group, ps.ast, ps.tov,
               ps.bad_pass_tov, ps.lost_ball_tov, ps.gp,
               ROUND(CAST(ps.lost_ball_tov / NULLIF(ps.gp,0) AS NUMERIC), 2) AS lb_pg
        FROM player_seasons ps
        JOIN players p ON ps.player_id = p.player_id
        WHERE ps.season = %s AND ps.season_type = %s
          AND ps.lost_ball_tov IS NOT NULL
          AND ps.min >= 1000
          AND p.position_group IN ('G', 'GF')
        ORDER BY lb_pg DESC NULLS LAST
        LIMIT 20
    """, (SEASON, SEASON_TYPE))
    rows = cur.fetchall()
    print(f"{'Player':<25} {'POS':<4} {'AST':>5} {'TOV':>5} {'BP':>5} {'LB':>5} {'LB/G':>7}")
    print("─" * 75)
    for r in rows:
        print(f"  {r['player_name']:<23} {r['position_group'] or '':<4} "
              f"{r['ast'] or 0:>5.1f} {r['tov'] or 0:>5.1f} "
              f"{r['bad_pass_tov'] or 0:>5.0f} {r['lost_ball_tov'] or 0:>5.0f} "
              f"{r['lb_pg'] or 0:>7.2f}")

    cur.close()
    conn.close()


def main():
    print(f"\nThe Impact Board — Lost Ball TOV Aggregation")
    print(f"Season: {SEASON} {SEASON_TYPE}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")

    progress = load_progress()
    processed = set(progress['processed_games'])
    player_counts = {int(k): v for k, v in progress['player_counts'].items()}

    all_game_ids = get_game_ids(SEASON, SEASON_TYPE)
    if not all_game_ids:
        print("❌ No games found. Exiting.")
        return

    remaining = [g for g in all_game_ids if g not in processed]
    print(f"\n{len(processed)} games already processed, {len(remaining)} remaining")
    print(f"Estimated time: {len(remaining) * DELAY / 60:.0f} min\n")

    if not remaining:
        print("✅ All games already processed — skipping to DB write")
    else:
        failed = []
        for i, game_id in enumerate(remaining):
            result = process_game(game_id)

            if result is None:
                failed.append(game_id)
                continue

            for pid, count in result.items():
                player_counts[pid] = player_counts.get(pid, 0) + count

            processed.add(game_id)

            if (i + 1) % 50 == 0:
                save_progress({
                    'processed_games': list(processed),
                    'player_counts':   {str(k): v for k, v in player_counts.items()}
                })
                total_lb = sum(player_counts.values())
                print(f"  [{i+1}/{len(remaining)}] {game_id} — "
                      f"{len(player_counts)} players, {total_lb} lost balls total")

        save_progress({
            'processed_games': list(processed),
            'player_counts':   {str(k): v for k, v in player_counts.items()}
        })

        if failed:
            print(f"\n⚠️  {len(failed)} games failed: {failed[:5]}")

    print(f"\nWriting to database...")
    print(f"  {len(player_counts)} players with lost ball data")
    write_to_db(
        {str(k): v for k, v in player_counts.items()},
        SEASON, SEASON_TYPE
    )

    print(f"\nRunning spot check...")
    spot_check()

    print(f"\n{'='*60}")
    print(f"✅ Done — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"Next step: python backend/ingest/compute_metrics.py")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()