"""
NothingButNet — Daily Update Pipeline
==========================================
python backend/ingest/daily_update.py

Runs the full update pipeline in order:
  1.  fetch_players.py         — sync players table (names, positions, active status)
  2.  fetch_season.py          — re-fetch all season aggregate stats
  3.  fetch_new_pbp_stats.py   — incremental PBP for new games only
  4.  fetch_bad_pass_tov.py    — bad pass turnovers (separate endpoint)
  5.  fetch_lost_ball_tov.py   — lost ball turnovers (separate endpoint)
  6.  fetch_closest_defender.py — closest defender shot data
  7.  fetch_matchups.py        — opponent-adjusted matchup defensive metric
  8.  fetch_nba_stats.py       — gravity, shot quality, leverage
  9.  fetch_darko.py           — DARKO DPM (darko.app)
  10. fetch_lebron.py          — LEBRON + O/D-LEBRON + WAR (fanspo.com)
  11. fetch_net_pts.py         — Net Points per 100 (ESPN via S3)
  12. compute_metrics.py       — recompute all derived metrics
  13. compute_pctiles.py       — recompute percentiles for Builder
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
    """Detect active season from DB."""
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
    return os.getenv('NBA_SEASON', '2025-26'), os.getenv('NBA_SEASON_TYPE', 'Regular Season')


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
    print(f"NOTHINGBUTNET — Daily Update")
    print(f"Season: {season} | {season_type}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")

    base = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        'ingest'
    )

    season_args = ['--season', season, '--season-type', season_type]

    steps = [
        # ── Players ───────────────────────────────────────────
        (
            'fetch_players.py',
            'Players sync',
            ['--season', season],
        ),
        # ── NBA API stats ─────────────────────────────────────
        (
            'fetch_season.py',
            'Season aggregate stats',
            season_args,
        ),
        (
            'fetch_new_pbp_stats.py',
            'Incremental PBP stats',
            season_args,
        ),
        (
            'fetch_bad_pass_tov.py',
            'Bad pass turnovers',
            season_args,
        ),
        (
            'fetch_lost_ball_tov.py',
            'Lost ball turnovers',
            season_args,
        ),
        (
            'fetch_closest_defender.py',
            'Closest defender shots',
            season_args,
        ),
        (
            'fetch_matchups.py',
            'Matchup defense',
            season_args + ['--min-poss', '20', '--min-def-poss', '300'],
        ),
        (
            'fetch_nba_stats.py',
            'NBA Stats (gravity, shot quality, leverage)',
            season_args,
        ),
        # ── External metrics ──────────────────────────────────
        (
            'fetch_darko.py',
            'DARKO DPM',
            season_args,
        ),
        (
            'fetch_lebron.py',
            'LEBRON',
            season_args,
        ),
        (
            'fetch_net_pts.py',
            'Net Points per 100',
            season_args,
        ),
        # ── Compute ───────────────────────────────────────────
        (
            'compute_metrics.py',
            'Derived metrics',
            season_args,
        ),
        (
            'compute_pctiles.py',
            'Percentiles (Builder)',
            season_args,
        ),
    ]

    failed_steps = []
    for script_name, label, args in steps:
        path = os.path.join(base, script_name)
        if not os.path.exists(path):
            print(f"\n⚠️  Skipping '{label}' — {script_name} not found")
            continue
        if not run(path, label, args):
            failed_steps.append(label)
            # Compute steps are hard dependencies — stop if they fail
            if script_name in ('compute_metrics.py', 'compute_pctiles.py'):
                print(f"\n❌ Pipeline stopped at: {label}")
                sys.exit(1)
            # Data fetch failures are non-fatal — log and continue
            print(f"   ⚠️  Continuing despite failure…")

    print(f"\n{'='*60}")
    if failed_steps:
        print(f"⚠️  Daily update finished with {len(failed_steps)} failure(s):")
        for s in failed_steps:
            print(f"   - {s}")
    else:
        print(f"✅ Daily update complete — all steps passed")
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()