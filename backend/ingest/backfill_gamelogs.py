"""
ydkball — Backfill ALL-TIME player game logs (1996-97 → current)
================================================================
python backend/ingest/backfill_gamelogs.py [--start 1996-97] [--types "Regular Season,Playoffs"]

Drives the existing per-season `fetch_gamelogs.py` over every season since 1996-97 so the
`player_gamelogs` table holds historical box scores (for the daily "guess the performance"
game). Each season is ONE bulk NBA-API call (~24k rows, ~6s); the whole run is ~10-15 min.

Robust: runs each (season, type) as an isolated subprocess so one season's failure (e.g. a
season with no Play-In) doesn't abort the rest. Idempotent — fetch_gamelogs upserts and skips
game_ids already in the DB, so it's safe to re-run / resume with --start.
"""

import os
import sys
import time
import argparse
import subprocess
from datetime import date

HERE = os.path.dirname(os.path.abspath(__file__))


def current_season() -> str:
    today = date.today()
    y, m = today.year, today.month
    return f"{y}-{str(y + 1)[2:]}" if m >= 10 else f"{y - 1}-{str(y)[2:]}"


def seasons_since(start: str) -> list[str]:
    out, y = [], int(start[:4])
    end = int(current_season()[:4])
    while y <= end:
        out.append(f"{y}-{str(y + 1)[2:]}")
        y += 1
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="1996-97", help="earliest season (data starts 1996-97)")
    ap.add_argument("--types", default="Regular Season,Playoffs",
                    help="comma-separated season types to pull")
    args = ap.parse_args()

    seasons = seasons_since(args.start)
    types = [t.strip() for t in args.types.split(",") if t.strip()]
    total = len(seasons) * len(types)
    print(f"Backfilling player_gamelogs: {len(seasons)} seasons × {len(types)} types = {total} calls")
    print(f"  {seasons[0]} → {seasons[-1]}  |  types: {types}\n")

    t0 = time.time()
    failures = []
    for i, season in enumerate(seasons):
        for stype in types:
            tag = f"[{season} · {stype}]"
            r = subprocess.run(
                [sys.executable, os.path.join(HERE, "fetch_gamelogs.py"),
                 "--season", season, "--season-type", stype],
                capture_output=True, text=True,
            )
            ok = r.returncode == 0
            # echo the child's last meaningful line
            last = next((ln for ln in reversed(r.stdout.splitlines()) if ln.strip()), "")
            print(f"  {tag:<28} {'✅' if ok else '❌'}  {last.strip()}")
            if not ok:
                failures.append(tag)
                if r.stderr.strip():
                    print(f"      ↳ {r.stderr.strip().splitlines()[-1]}")
            time.sleep(0.5)

    mins = (time.time() - t0) / 60
    print(f"\nDone in {mins:.1f} min. {total - len(failures)}/{total} ok"
          + (f"; failures: {failures}" if failures else ""))


if __name__ == "__main__":
    main()
