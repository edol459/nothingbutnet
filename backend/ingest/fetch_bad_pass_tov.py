"""
The Impact Board — Bad Pass TOV from PBP
==========================================
python backend/ingest/fetch_bad_pass_tov.py

Fetches PlayByPlayV3 for every 2024-25 regular season game,
counts bad pass turnovers per player, stores in player_seasons.

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
PROGRESS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), f'bad_pass_progress_{season_slug}.json')
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


# ── Step 1: Get all game IDs ──────────────────────────────────
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
        # Each game appears twice (one row per team) — deduplicate
        game_ids = sorted(df['GAME_ID'].unique().tolist())
        print(f"  ✅ {len(game_ids)} games found")
        return game_ids
    except Exception as e:
        print(f"  ❌ {e}")
        return []


# ── Step 2: Process PBP for one game ─────────────────────────
def process_game(game_id):
    """
    Returns dict: {player_id: bad_pass_count}
    """
    time.sleep(DELAY)
    try:
        from nba_api.stats.endpoints import PlayByPlayV3 as PBP
        pbp = PBP(game_id=game_id, timeout=10).get_data_frames()[0]

        # Filter to bad pass turnovers
        mask = (
            (pbp['actionType'] == 'Turnover') &
            (pbp['subType'] == 'Bad Pass')
        )
        bad_passes = pbp[mask]

        counts = defaultdict(int)
        for _, row in bad_passes.iterrows():
            try:
                pid = int(float(row['personId']))
                if pid > 0:
                    counts[pid] += 1
            except:
                pass

        return dict(counts)
    except Exception as e:
        return None  # Signal failure


# ── Step 3: Accumulate and save ───────────────────────────────
def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    return {'processed_games': [], 'player_counts': {}}

def save_progress(progress):
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f)


# ── Step 4: Write to DB ───────────────────────────────────────
def write_to_db(player_counts, season, season_type):
    """
    Add bad_pass_tov column to player_seasons and update counts.
    """
    conn = psycopg2.connect(DATABASE_URL)
    cur  = conn.cursor()

    # Add column if it doesn't exist
    cur.execute("""
        ALTER TABLE player_seasons
        ADD COLUMN IF NOT EXISTS bad_pass_tov REAL
    """)
    conn.commit()

    # Update each player
    updated = 0
    for pid_str, count in player_counts.items():
        pid = int(pid_str)
        cur.execute("""
            UPDATE player_seasons
            SET bad_pass_tov = %s
            WHERE player_id = %s AND season = %s AND season_type = %s
        """, (count, pid, season, season_type))
        if cur.rowcount > 0:
            updated += 1

    conn.commit()

    # Verify
    cur.execute("""
        SELECT COUNT(*) FROM player_seasons
        WHERE bad_pass_tov IS NOT NULL
        AND season = %s AND season_type = %s
    """, (season, season_type))
    count_with_data = cur.fetchone()[0]

    cur.close()
    conn.close()

    print(f"  ✅ Updated {updated} players in DB")
    print(f"  ✅ {count_with_data} players now have bad_pass_tov data")


# ── Spot check ────────────────────────────────────────────────
def spot_check(player_counts):
    # Load player names from DB
    conn = psycopg2.connect(DATABASE_URL)
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT ps.player_id, p.player_name, ps.ast, ps.tov,
               ps.bad_pass_tov, ps.gp
        FROM player_seasons ps
        JOIN players p ON ps.player_id = p.player_id
        WHERE ps.season = %s AND ps.season_type = %s
          AND ps.bad_pass_tov IS NOT NULL
          AND ps.min >= 1000
        ORDER BY ps.bad_pass_tov ASC
        LIMIT 20
    """, (SEASON, SEASON_TYPE))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    print(f"\n{'='*70}")
    print(f"Players with LOWEST bad pass TOV per game (min 1000 min)")
    print(f"{'='*70}")
    print(f"{'Player':<25} {'GP':<5} {'AST':>5} {'TOV':>6} {'BP_TOV':>8} {'BP/G':>7}")
    print("─" * 70)
    for r in rows:
        bp_pg = (r['bad_pass_tov'] / r['gp']) if r['gp'] else 0
        print(f"  {r['player_name']:<23} {r['gp']:<5} "
              f"{r['ast'] or 0:>5.1f} {r['tov'] or 0:>6.1f} "
              f"{r['bad_pass_tov'] or 0:>8.0f} {bp_pg:>7.2f}")

    # Also show top passers by pot_ast / bad_pass_tov
    conn = psycopg2.connect(DATABASE_URL)
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT p.player_name, ps.ast, ps.tov, ps.bad_pass_tov, ps.gp,
               ps.potential_ast,
               ROUND(CAST(
                   CASE WHEN ps.bad_pass_tov > 0
                   THEN (ps.potential_ast / ps.gp) / (ps.bad_pass_tov / ps.gp)
                   ELSE NULL END
               AS NUMERIC), 2) AS pot_ast_per_bp_tov
        FROM player_seasons ps
        JOIN players p ON ps.player_id = p.player_id
        WHERE ps.season = %s AND ps.season_type = %s
          AND ps.bad_pass_tov IS NOT NULL AND ps.bad_pass_tov > 0
          AND ps.min >= 1000
        ORDER BY pot_ast_per_bp_tov DESC NULLS LAST
        LIMIT 20
    """, (SEASON, SEASON_TYPE))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    print(f"\n{'='*70}")
    print(f"Top 20 — Potential AST / Bad Pass TOV (min 1000 min)")
    print(f"{'='*70}")
    print(f"{'Player':<25} {'AST':>5} {'TOV':>6} {'BP_TOV':>8} {'PotAST/BPTOV':>14}")
    print("─" * 70)
    for r in rows:
        print(f"  {r['player_name']:<23} "
              f"{r['ast'] or 0:>5.1f} {r['tov'] or 0:>6.1f} "
              f"{r['bad_pass_tov'] or 0:>8.0f} "
              f"{r['pot_ast_per_bp_tov'] or 0:>14.2f}")


# ── Main ──────────────────────────────────────────────────────
def main():
    print(f"\nThe Impact Board — Bad Pass TOV Aggregation")
    print(f"Season: {SEASON} {SEASON_TYPE}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")

    # Load progress (allows resuming)
    progress = load_progress()
    processed = set(progress['processed_games'])
    player_counts = {int(k): v for k, v in progress['player_counts'].items()}

    # Get all game IDs
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
                if i % 50 == 0:
                    print(f"  [{i+1}/{len(remaining)}] {game_id} ❌ failed")
                continue

            # Accumulate counts
            for pid, count in result.items():
                player_counts[pid] = player_counts.get(pid, 0) + count

            processed.add(game_id)

            # Save progress every 50 games
            if (i + 1) % 50 == 0:
                save_progress({
                    'processed_games': list(processed),
                    'player_counts':   {str(k): v for k, v in player_counts.items()}
                })
                total_bp = sum(player_counts.values())
                print(f"  [{i+1}/{len(remaining)}] {game_id} — "
                      f"{len(player_counts)} players, {total_bp} bad passes total")

        # Final save
        save_progress({
            'processed_games': list(processed),
            'player_counts':   {str(k): v for k, v in player_counts.items()}
        })

        if failed:
            print(f"\n⚠️  {len(failed)} games failed: {failed[:10]}")

    # Write to DB
    print(f"\nWriting to database...")
    print(f"  {len(player_counts)} players with bad pass data")
    write_to_db(
        {str(k): v for k, v in player_counts.items()},
        SEASON, SEASON_TYPE
    )

    # Update compute_metrics to use new column
    print(f"\nRunning spot check...")
    spot_check(player_counts)

    print(f"\n{'='*60}")
    print(f"✅ Done — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"Next step: python backend/ingest/compute_metrics.py")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()