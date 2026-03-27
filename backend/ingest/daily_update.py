"""
The Impact Board — Daily Update Pipeline
==========================================
python backend/ingest/daily_update.py

Detects the current season from the DB, then runs:
  1. fetch_season.py        — re-fetch all season aggregate data
  2. fetch_new_pbp_stats.py — incremental PBP for new games only
  3. fetch_matchups.py      — opponent-adjusted matchup defensive metric
  4. fetch_nba_stats.py     — gravity, shot quality, leverage
  5. fetch_darko.py         — DARKO DPM
  6. fetch_lebron.py        — LEBRON
  7. fetch_net_pts.py       — Net Points
  8. compute_metrics.py     — recompute all derived metrics
"""

import os
import sys
import subprocess
from datetime import datetime
from dotenv import load_dotenv
import psycopg2

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')


def get_current_season():
    """Detect active season from DB — same logic as server.py."""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur  = conn.cursor()
        cur.execute("""
            SELECT season, season_type FROM player_seasons
            ORDER BY season DESC, season_type LIMIT 1
        """)
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row:
            return row[0], row[1]
    except Exception as e:
        print(f"⚠️  Could not detect season from DB: {e}")
    return os.getenv('NBA_SEASON', '2024-25'), os.getenv('NBA_SEASON_TYPE', 'Regular Season')


def run(script, label, extra_args=None):
    print(f"\n{'='*60}")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {label}")
    print(f"{'='*60}")

    cmd  = [sys.executable, script] + (extra_args or [])
    root = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )

    result = subprocess.run(cmd, cwd=root)

    if result.returncode != 0:
        print(f"\n❌ {label} failed (exit {result.returncode})")
        return False

    print(f"\n✅ {label} complete")
    return True


def main():
    season, season_type = get_current_season()

    print(f"\n{'='*60}")
    print(f"THE IMPACT BOARD — Daily Update")
    print(f"Season: {season} | {season_type}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")

    base = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        'ingest'
    )

    steps = [
        (
            os.path.join(base, 'fetch_season.py'),
            'Season data fetch',
            ['--season', season, '--season-type', season_type]
        ),
        (
            os.path.join(base, 'fetch_new_pbp_stats.py'),
            'Incremental PBP stats',
            ['--season', season, '--season-type', season_type]
        ),
        (
            os.path.join(base, 'fetch_matchups.py'),
            'Matchup defense (opponent-adjusted)',
            ['--season', season, '--season-type', season_type,
             '--min-poss', '20', '--min-def-poss', '300']
        ),
        (
            os.path.join(base, 'fetch_nba_stats.py'),
            'NBA Stats (gravity, shot quality, leverage)',
            ['--season', season, '--season-type', season_type]
        ),
        (
            os.path.join(base, 'fetch_darko.py'),
            'DARKO DPM fetch',
            ['--season', season, '--season-type', season_type]
        ),
        (
            os.path.join(base, 'fetch_lebron.py'),
            'LEBRON fetch',
            ['--season', season, '--season-type', season_type]
        ),
        (
            os.path.join(base, 'fetch_net_pts.py'),
            'Net Points fetch',
            ['--season', season, '--season-type', season_type]
        ),
        (
            os.path.join(base, 'compute_metrics.py'),
            'Metrics computation',
            ['--season', season, '--season-type', season_type]
        ),
    ]

    for script, label, args in steps:
        if not os.path.exists(script):
            print(f"⚠️  Skipping {label} — script not found")
            continue
        if not run(script, label, args):
            print(f"\n❌ Pipeline stopped at: {label}")
            sys.exit(1)

    print(f"\n{'='*60}")
    print(f"✅ Daily update complete")
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()