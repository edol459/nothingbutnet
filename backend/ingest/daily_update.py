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
import pipeline_status
import season_util

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')


def get_current_season():
    """Always returns Regular Season — impact metrics (DARKO, LEBRON, Net Pts)
    only publish regular season data, so we always write to those rows."""
    return season_util.current_season(), 'Regular Season'


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

    # Resolved independently of the NBA season — deriving it as "NBA start year + 1"
    # coupled the leagues, so a stalled NBA season silently pinned the WNBA too.
    wnba_year = season_util.wnba_current_season()

    steps = [
        # ── Fill any games the live poller missed. MUST run before the team
        #    record steps below, which are computed straight from `games` — a
        #    missing game silently costs a team a W or an L in the standings
        #    for the rest of the season. ──────────────────────────────────────
        (
            'reconcile_games.py',
            'Reconcile games vs CDN schedules',
            ['--league', 'both'],
        ),
        # ── Forward schedule → scheduled_games. Independent of everything
        #    below (writes its own table, never `games`), but kept next to
        #    reconcile since both read the same CDN feeds. ──────────────────
        (
            'fetch_scheduled_games.py',
            'Scheduled games + tip times',
            ['--league', 'both'],
        ),
        # ── Team W-L records — both computed from games table, Railway-safe ──
        (
            'fetch_team_seasons.py',
            'NBA team season records',
            ['--seasons', season],
        ),
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

    run_id = pipeline_status.start_run(pipeline_status.CLOUD_DAILY)
    if run_id is None:
        # start_run swallows its own errors (best-effort), so a None here means
        # this run will NOT show up in pipeline_runs / the health report. Make
        # that loud — it's the difference between "cron down" and "cron ran but
        # didn't record" when debugging from the logs.
        print("⚠️  pipeline_status.start_run() returned None — this run will NOT "
              "be recorded. Check DATABASE_URL in THIS service and any "
              "'[pipeline_status]' errors above.", flush=True)
    else:
        print(f"📝 pipeline_status: recording run #{run_id} (cloud_daily)", flush=True)
    failed_steps = []
    step_results = []
    try:
        for script_name, label, args in steps:
            path = script_name if os.path.isabs(script_name) else os.path.join(base, script_name)
            if not os.path.exists(path):
                print(f"\n⚠️  Skipping '{label}' — {script_name} not found")
                step_results.append({"label": label, "ok": None, "skipped": True})
                continue
            ok = run(path, label, args)
            step_results.append({"label": label, "ok": ok, "skipped": False})
            if not ok:
                failed_steps.append(label)
                # All failures are non-fatal — log and continue
                print(f"   ⚠️  Continuing despite failure…")
        status = "success" if not failed_steps else "partial"
        pipeline_status.finish_run(run_id, status, step_results, failed_steps)
    except Exception as e:
        pipeline_status.finish_run(run_id, "failed", step_results, failed_steps, str(e))
        raise

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