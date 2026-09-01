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
  6b. fetch_wnba_player_stats.py — WNBA season averages (stats.wnba.com)
  7.  fetch_lineups.py         — 5-man lineup data for WoWY tool
  8.  compute_pctiles.py       — recompute percentiles for Builder

Season type is auto-detected from today's date (Playoffs from ~Apr 20–Jun,
Regular Season otherwise) — no manual change needed when playoffs start.

Scheduled via: run_daily_local.bat (Windows Task Scheduler)
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


def get_current_season_type() -> str:
    """Returns 'Playoffs' from ~Apr 20 through June, else 'Regular Season'."""
    return season_util.season_type_from_date()


def get_current_season():
    """Season year from the games table; type from today's date."""
    return season_util.current_season(), get_current_season_type()


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
    # Stats resolve from played games; membership resolves from the schedule.
    roster_season = season_util.roster_season()
    wnba_season  = season_util.wnba_current_season()

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
        # ── WNBA season averages ──────────────────────────────
        # The WNBA twin of fetch_season.py above, and it lives here for the
        # same reason: stats.wnba.com sits behind the same Akamai gate as
        # stats.nba.com, so it needs the residential IP. It was in NEITHER
        # pipeline until 2026-09-01 — wnba_player_seasons only ever moved when
        # someone ran the script by hand, so the whole 2026 season sat frozen
        # at the 6/27 backfill (Caitlin Clark showing 17 GP of the 36 she'd
        # played). The playoffs arm returns an empty set and no-ops until the
        # WNBA postseason tips in September.
        (
            'fetch_wnba_player_stats.py',
            'WNBA season averages',
            ['--season', wnba_season],
        ),
        (
            'fetch_wnba_player_stats.py',
            'WNBA season averages (playoffs)',
            ['--season', wnba_season, '--season-type', 'Playoffs'],
        ),
        # ── Rosters + team membership (nba_api, residential IP) ────
        # These take roster_season(), not season: the league publishes next
        # season's rosters months before tipoff, so pinning them to the stats
        # season refetches last season's roster all summer and every offseason
        # trade stays invisible until games are played.
        (
            os.path.join(base_backend, 'fetch_roster.py'),
            'Roster data (WoWY)',
            ['--season', roster_season],
        ),
        (
            'sync_player_teams.py',
            'Player team assignments (current_team)',
            [],
        ),
        # Gates itself: does nothing unless locked ballots exist for a season
        # whose winners aren't recorded yet, so it sleeps all year and wakes for
        # the fortnight around the announcements.
        (
            'grade_awards.py',
            'Award winners (grades ballots)',
            [],
        ),
        # ── WoWY lineups (pbpstats, leverage-filtered) ────────
        (
            'fetch_wowy_lineups.py',
            'WoWY lineups (leverage-filtered)',
            ['--season', season, '--recent-only'],
        ),
        # ── Team season stats (Base+Advanced) for team pages ──
        (
            'fetch_team_season_stats.py',
            'Team season stats (current, NBA+WNBA)',
            ['--current'],
        ),
        # ── Compute (runs last, after all stats are fresh) ────
        (
            'compute_pctiles.py',
            'Percentiles (Builder)',
            season_args,
        ),
    ]

    run_id = pipeline_status.start_run(pipeline_status.LOCAL_DAILY)
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
                # compute_pctiles is a hard dependency — stop if it fails
                if script_name == 'compute_pctiles.py':
                    print(f"\n❌ Pipeline stopped at: {label}")
                    pipeline_status.finish_run(run_id, "failed", step_results, failed_steps,
                                               f"hard stop at {label}")
                    sys.exit(1)
                print(f"   ⚠️  Continuing despite failure…")
        status = "success" if not failed_steps else "partial"
        pipeline_status.finish_run(run_id, status, step_results, failed_steps)
    except SystemExit:
        raise
    except Exception as e:
        pipeline_status.finish_run(run_id, "failed", step_results, failed_steps, str(e))
        raise

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
