"""
generate_daily.py — pre-generate Survival daily runs.
=====================================================

Generation is slow over a remote DB (many leaderboard round-trips), so it must NOT run
in the request path. A cron runs this once a day to populate `survival_daily`; the
`/api/survival/daily` endpoint then just reads the cached row.

    python backend/games/generate_daily.py                    # today (+ default look-ahead)
    python backend/games/generate_daily.py --days 3           # today + next 2 days
    python backend/games/generate_daily.py --date 2026-06-20
    python backend/games/generate_daily.py --force --fresh    # reset today & roll NEW questions
"""

import os
import sys
import argparse
import datetime

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ingest"))
import survival_api as sa  # noqa: E402
import pipeline_status      # noqa: E402  — from backend/ingest

load_dotenv()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None, help="generate one specific date (YYYY-MM-DD)")
    ap.add_argument("--days", type=int, default=1, help="generate today + this many days ahead")
    ap.add_argument("--force", action="store_true", help="regenerate even if already generated")
    ap.add_argument("--fresh", action="store_true", help="use a random seed so a forced regen yields NEW questions")
    args = ap.parse_args()

    conn = psycopg2.connect(os.getenv("DATABASE_URL"),
                            cursor_factory=psycopg2.extras.RealDictCursor)

    if args.date:
        dates = [args.date]
    else:
        try:
            from zoneinfo import ZoneInfo
        except ImportError:
            from backports.zoneinfo import ZoneInfo
        today = datetime.datetime.now(ZoneInfo("America/New_York")).date()   # daily = ET day
        dates = [(today + datetime.timedelta(days=d)).isoformat() for d in range(args.days)]

    run_id = pipeline_status.start_run(pipeline_status.PUZZLE_GEN)
    step_results = []
    failed = []
    try:
        for d in dates:
            if not args.force:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1 FROM survival_daily WHERE date = %s", (d,))
                    if cur.fetchone():
                        print(f"  {d}  already generated — skipping (use --force to regenerate)")
                        step_results.append({"label": d, "ok": None, "skipped": True})
                        continue
            t = datetime.datetime.now()
            try:
                run = sa.ensure_daily(conn, d, force=args.force, fresh=args.fresh)
                secs = (datetime.datetime.now() - t).total_seconds()
                verb = "regenerated" if args.force else "generated"
                print(f"  {d}  {verb} {len(run)} questions in {secs:.0f}s")
                step_results.append({"label": d, "ok": True, "skipped": False})
            except Exception as e:
                print(f"  {d}  FAILED: {e}")
                step_results.append({"label": d, "ok": False, "skipped": False})
                failed.append(d)
        status = "success" if not failed else "partial"
        pipeline_status.finish_run(run_id, status, step_results, failed)
    except Exception as e:
        pipeline_status.finish_run(run_id, "failed", step_results, failed, str(e))
        raise

    conn.close()


if __name__ == "__main__":
    main()
