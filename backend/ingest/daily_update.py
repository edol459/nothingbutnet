"""
ydkball — Cloud Daily Update (Railway)
==========================================
python backend/ingest/daily_update.py

Runs only the steps that work from cloud IPs (stats.nba.com is blocked
on Railway — those steps run locally via daily_update_local.py instead).

Steps:
  1. fetch_players.py   — sync players table (CDN-friendly, works from cloud)
  2. fetch_darko.py     — DARKO DPM (darko.app)
  3. fetch_lebron.py    — LEBRON + O/D-LEBRON + WAR (fanspo.com)
  4. fetch_net_pts.py   — Net Points per 100 (ESPN via S3)

compute_pctiles runs at the end of daily_update_local.py after NBA stats
are refreshed, so percentiles always reflect the latest data.
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
    """Always returns Regular Season — impact metrics (DARKO, LEBRON, Net Pts)
    only publish regular season data, so we always write to those rows."""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur  = conn.cursor()
        cur.execute("""
            SELECT season FROM player_seasons
            WHERE season_type = 'Regular Season'
            ORDER BY season DESC LIMIT 1
        """)
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row:
            return row[0], 'Regular Season'
    except Exception as e:
        print(f"⚠️  Could not detect season from DB: {e}")
    return os.getenv('NBA_SEASON', '2025-26'), 'Regular Season'


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
    print(f"YDKBALL — Daily Update")
    print(f"Season: {season} | {season_type}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")

    base = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        'ingest'
    )

    season_args = ['--season', season, '--season-type', season_type]

    # WNBA season = calendar year of the second half of NBA season (e.g. "2025-26" → "2026")
    wnba_year = str(int(season.split('-')[0]) + 1) if '-' in season else season

    steps = [
        # ── WNBA team W-L (computed from games table — Railway-safe) ──
        (
            'fetch_wnba_team_seasons.py',
            'WNBA team season records',
            ['--seasons', wnba_year],
        ),
        # ── Players (CDN-friendly, works from cloud) ───────────
        (
            'fetch_players.py',
            'Players sync',
            ['--season', season],
        ),
        # ── External metrics (non-NBA endpoints) ──────────────
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
    ]

    failed_steps = []
    for script_name, label, args in steps:
        path = script_name if os.path.isabs(script_name) else os.path.join(base, script_name)
        if not os.path.exists(path):
            print(f"\n⚠️  Skipping '{label}' — {script_name} not found")
            continue
        if not run(path, label, args):
            failed_steps.append(label)
            # All failures are non-fatal — log and continue
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