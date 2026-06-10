"""
ydkball — Local Daily Update (Windows PC)
==========================================
python backend/ingest/daily_update_local.py

Runs all steps that require a residential IP (stats.nba.com blocks
Railway and other cloud datacenter IPs). Run this on your local
Windows machine via Task Scheduler — it writes directly to the
Railway Postgres DB using DATABASE_URL from your .env file.

Steps:
  1.  fetch_season.py          — re-fetch all season aggregate stats
  2.  fetch_new_pbp_stats.py   — incremental PBP (bad pass + lost ball TOV)
  3.  fetch_closest_defender.py — closest defender shot data
  4.  fetch_matchups.py        — opponent-adjusted matchup defensive metric
  5.  fetch_nba_stats.py       — gravity, shot quality, leverage
  6.  fetch_gamelogs.py        — per-game logs for Trends page
  7.  fetch_lineups.py         — 5-man lineup data for WoWY tool
  8.  compute_pctiles.py       — recompute percentiles for Builder

Season type is auto-detected from today's date (Playoffs from ~Apr 20–Jun,
Regular Season otherwise) — no manual change needed when playoffs start.

Scheduled via: run_daily_local.bat (Windows Task Scheduler)
"""

import os
import sys
import subprocess
from datetime import datetime, date
from dotenv import load_dotenv
import psycopg2

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')


def get_current_season_type() -> str:
    """Returns 'Playoffs' from ~Apr 20 through June, else 'Regular Season'.
    Mirrors the same logic used in server.py."""
    today = date.today()
    m, d  = today.month, today.day
    if (m == 4 and d >= 20) or m in (5, 6):
        return 'Playoffs'
    return 'Regular Season'


def get_current_season():
    """Season year from DB (always from Regular Season rows); type from today's date."""
    season_type = get_current_season_type()
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur  = conn.cursor()
        # Always anchor year to Regular Season rows — they're always present
        cur.execute("""
            SELECT season FROM player_seasons
            WHERE season_type = 'Regular Season'
            ORDER BY season DESC LIMIT 1
        """)
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row:
            return row[0], season_type
    except Exception as e:
        print(f"⚠️  Could not detect season from DB: {e}")
    return os.getenv('NBA_SEASON', '2025-26'), season_type


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
    print(f"YDKBALL — Local Daily Update")
    print(f"Season: {season} | {season_type}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")

    base         = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        'ingest'
    )
    base_backend = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    season_args = ['--season', season, '--season-type', season_type]

    steps = [
        # ── NBA API stats (require residential IP) ────────────
        (
            'fetch_season.py',
            'Season aggregate stats',
            season_args,
        ),
        (
            'fetch_new_pbp_stats.py',
            'Incremental PBP stats (bad pass + lost ball TOV)',
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
        (
            'fetch_gamelogs.py',
            'Per-game logs (Trends)',
            season_args,
        ),
        # ── Rosters (nba_api, requires residential IP) ────────
        (
            os.path.join(base_backend, 'fetch_roster.py'),
            'Roster data (WoWY)',
            ['--season', season],
        ),
        # ── WoWY lineups (pbpstats, leverage-filtered) ────────
        (
            'fetch_wowy_lineups.py',
            'WoWY lineups (leverage-filtered)',
            ['--season', season, '--recent-only'],
        ),
        # ── Compute (runs last, after all stats are fresh) ────
        (
            'compute_pctiles.py',
            'Percentiles (Builder)',
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
            # compute_pctiles is a hard dependency — stop if it fails
            if script_name == 'compute_pctiles.py':
                print(f"\n❌ Pipeline stopped at: {label}")
                sys.exit(1)
            print(f"   ⚠️  Continuing despite failure…")

    print(f"\n{'='*60}")
    if failed_steps:
        print(f"⚠️  Local update finished with {len(failed_steps)} failure(s):")
        for s in failed_steps:
            print(f"   - {s}")
    else:
        print(f"✅ Local update complete — all steps passed")
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
