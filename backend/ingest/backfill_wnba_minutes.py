#!/usr/bin/env python3
"""Backfill wnba_player_game_stats.min from the WNBA CDN boxscores.

`wnba_player_game_stats` was created without a minutes column, so WNBA box lines have
always been pts/reb/ast only — visible anywhere the app shows a stat line next to a
player grade, where NBA rows carry MIN and WNBA rows silently do not.

The CDN boxscore has had real per-game minutes all along
(`statistics.minutes = "PT35M40.00S"`), so this is a backfill, not an estimate. Nothing
here is derived or interpolated: a game whose boxscore we cannot fetch is left alone.

    python backend/ingest/backfill_wnba_minutes.py --dry-run       # report only
    python backend/ingest/backfill_wnba_minutes.py --graded-first  # graded games first
    python backend/ingest/backfill_wnba_minutes.py --apply

Safe to re-run: it only fills rows where min IS NULL unless --refresh is passed, and it
never nulls a value it already set.
"""
from __future__ import annotations

import argparse
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
load_dotenv()

# Reuse the server's CDN client (curl_cffi impersonation) and minutes parser rather than
# re-implementing either — see docs/cdn-akamai-bot-manager.md for why the plain client
# does not work from a datacenter IP.
import server  # noqa: E402


def games_needing_minutes(cur, refresh: bool, graded_first: bool) -> list[str]:
    # `min` needs quoting: unquoted, the parser reads it as the aggregate function and
    # rejects the FILTER clause outright.
    having = 'COUNT(*) FILTER (WHERE w."min" IS NULL) > 0' if not refresh else "TRUE"
    cur.execute(f"""
        SELECT w.game_id,
               COUNT(*)                                   AS players,
               COUNT(*) FILTER (WHERE w."min" IS NULL)    AS missing,
               COALESCE(gr.n, 0)                          AS grades,
               g.game_date
        FROM wnba_player_game_stats w
        JOIN games g ON g.game_id = w.game_id
        LEFT JOIN (SELECT game_id, COUNT(*) n FROM performance_reviews GROUP BY game_id) gr
               ON gr.game_id = w.game_id
        GROUP BY w.game_id, gr.n, g.game_date
        HAVING {having}
        ORDER BY {"COALESCE(gr.n, 0) DESC, " if graded_first else ""} g.game_date DESC
    """)
    return [dict(r) for r in cur.fetchall()]


def minutes_for_game(game_id: str) -> dict[int, float]:
    """{player_id: minutes} from the CDN boxscore. Empty dict if unavailable."""
    url = f"https://cdn.wnba.com/static/json/liveData/boxscore/boxscore_{game_id}.json"
    try:
        resp = server._cdn_get(url, headers=server._WNBA_CDN_HEADERS, timeout=12)
        resp.raise_for_status()
        game = resp.json().get("game", {})
    except Exception as e:
        print(f"    fetch failed: {e}")
        return {}
    out: dict[int, float] = {}
    for side in ("awayTeam", "homeTeam"):
        for p in (game.get(side, {}) or {}).get("players", []) or []:
            pid = p.get("personId")
            if not pid:
                continue
            mins = server._cdn_minutes((p.get("statistics") or {}).get("minutes"))
            if mins is not None:
                out[int(pid)] = mins
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="write to the DB")
    ap.add_argument("--dry-run", action="store_true", help="report only (default)")
    ap.add_argument("--refresh", action="store_true", help="also overwrite rows that already have min")
    ap.add_argument("--graded-first", action="store_true", help="process games with player grades first")
    ap.add_argument("--limit", type=int, default=0, help="stop after N games")
    ap.add_argument("--sleep", type=float, default=0.4, help="seconds between CDN fetches")
    args = ap.parse_args()

    if not args.apply:
        print("DRY RUN — nothing will be written. Pass --apply to commit.\n")

    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    games = games_needing_minutes(cur, args.refresh, args.graded_first)
    if args.limit:
        games = games[: args.limit]
    total_missing = sum(g["missing"] for g in games)
    print(f"{len(games)} WNBA games need minutes ({total_missing} player rows)\n")

    filled = skipped = no_box = 0
    for i, g in enumerate(games, 1):
        gid = g["game_id"]
        tag = f"[{i}/{len(games)}] {gid} {g['game_date']}"
        if g["grades"]:
            tag += f"  ({g['grades']} grades)"
        mins = minutes_for_game(gid)
        if not mins:
            print(f"  {tag}  no boxscore — left untouched")
            no_box += 1
            continue
        n = 0
        for pid, m in mins.items():
            # Never clobber an existing value unless asked: the daily ingest is the
            # authority for current games, this script only fills gaps.
            cur.execute(
                'UPDATE wnba_player_game_stats SET "min" = %s '
                "WHERE game_id = %s AND player_id = %s" + ("" if args.refresh else ' AND "min" IS NULL'),
                (m, gid, pid),
            )
            n += cur.rowcount
        filled += n
        skipped += max(0, g["players"] - n) if not args.refresh else 0
        print(f"  {tag}  {n} rows")
        if args.apply:
            conn.commit()
        time.sleep(args.sleep)

    if not args.apply:
        conn.rollback()

    print(f"\n{'WROTE' if args.apply else 'WOULD WRITE'} {filled} rows across "
          f"{len(games) - no_box} games ({no_box} without a boxscore)")

    cur.execute("""
        SELECT COUNT(*) total, COUNT("min") with_min FROM wnba_player_game_stats
    """)
    r = cur.fetchone()
    print(f"coverage now: {r['with_min']}/{r['total']} rows have minutes")
    cur.close()
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
