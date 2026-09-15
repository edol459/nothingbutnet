#!/usr/bin/env python3
"""Backfill games.arena_name / arena_city / attendance / sellout from the CDN boxscore.

A game row could never say where it was played. The arena lived only in
`scheduled_games`, a rolling schedule feed that has no memory of prior seasons, so
"I was at this game" resolved a venue for almost nothing. The CDN boxscore has
carried it all along — `arena.arenaName`, `arena.arenaCity`, plus `attendance` and
`sellout` — for games going back seasons.

    python backend/ingest/backfill_game_venues.py --dry-run           # report only
    python backend/ingest/backfill_game_venues.py --attended-first --apply
    python backend/ingest/backfill_game_venues.py --apply --limit 200

Safe to re-run: it only touches rows where arena_name IS NULL unless --refresh is
passed, and a game whose boxscore cannot be fetched is left alone rather than
written as blank. Nothing here is derived — a venue we cannot read stays unknown.

League comes from `games.league`, NOT the game-id prefix: recent WNBA games carry
ESPN-style ids that the old '10' heuristic reads as NBA, which would send every one
of them to the wrong CDN host.
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

# Reuse the server's CDN client (curl_cffi Chrome impersonation). A plain client is
# refused from a datacenter IP — see docs/cdn-akamai-bot-manager.md.
import server  # noqa: E402


def games_needing_venue(cur, refresh: bool, attended_first: bool, limit: int, recent: int):
    where = "TRUE" if refresh else "g.arena_name IS NULL"
    # `recent` is what the daily pipeline passes: only chase games that just finished,
    # so a nightly run costs a handful of CDN calls rather than walking 21k rows.
    #
    # Attended games are exempt from that window ON PURPose. Somebody marking a 2024
    # game as attended today creates a venue gap in the past, which a date-bounded
    # query would never revisit — the ticket would print with no building on it.
    if recent:
        where += (f" AND (g.game_date >= CURRENT_DATE - {int(recent)}"
                  f"      OR COALESCE(a.n, 0) > 0)")
    cur.execute(f"""
        SELECT g.game_id, g.league, g.game_date,
               COALESCE(a.n, 0) AS attended_logs
          FROM games g
          LEFT JOIN (SELECT game_id, COUNT(*) n FROM game_reviews
                      WHERE attended GROUP BY game_id) a ON a.game_id = g.game_id
         WHERE {where}
         ORDER BY {"COALESCE(a.n, 0) DESC, " if attended_first else ""} g.game_date DESC
         {f"LIMIT {int(limit)}" if limit else ""}
    """)
    return [dict(r) for r in cur.fetchall()]


def venue_for_game(game_id: str, league: str) -> dict | None:
    """Venue + crowd from the CDN boxscore, or None if it cannot be read."""
    host = "cdn.wnba.com" if (league or "").lower() == "wnba" else "cdn.nba.com"
    headers = server._WNBA_CDN_HEADERS if (league or "").lower() == "wnba" else server._CDN_HEADERS
    url = f"https://{host}/static/json/liveData/boxscore/boxscore_{game_id}.json"
    try:
        resp = server._cdn_get(url, headers=headers, timeout=12)
        resp.raise_for_status()
        game = resp.json().get("game", {}) or {}
    except Exception as e:
        print(f"    fetch failed: {e}")
        return None

    arena = game.get("arena") or {}
    name  = (arena.get("arenaName") or "").strip() or None
    city  = (arena.get("arenaCity") or "").strip() or None
    if not name:
        return None                      # nothing worth writing

    # A missing attendance must stay NULL. Zero would assert an empty building.
    raw_att = game.get("attendance")
    try:
        attendance = int(raw_att) if raw_att not in (None, "", 0) else None
    except (TypeError, ValueError):
        attendance = None
    raw_sell = game.get("sellout")
    sellout = None
    if raw_sell not in (None, ""):
        sellout = str(raw_sell).strip() in ("1", "true", "True", "yes")

    return {"arena_name": name, "arena_city": city,
            "attendance": attendance, "sellout": sellout}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="write to the DB")
    ap.add_argument("--dry-run", action="store_true", help="report only (default)")
    ap.add_argument("--refresh", action="store_true", help="also overwrite rows that already have a venue")
    ap.add_argument("--attended-first", action="store_true",
                    help="process games somebody attended first — what a ticketbook renders")
    ap.add_argument("--limit", type=int, default=0, help="stop after N games")
    ap.add_argument("--recent", type=int, default=0,
                    help="only games finished in the last N days (plus any attended game "
                         "still missing a venue). What the daily pipeline passes.")
    ap.add_argument("--sleep", type=float, default=0.15, help="seconds between CDN calls")
    args = ap.parse_args()
    apply = args.apply and not args.dry_run

    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    todo = games_needing_venue(cur, args.refresh, args.attended_first, args.limit, args.recent)
    print(f"{len(todo)} game(s) need a venue"
          f"{f' (last {args.recent}d + any attended)' if args.recent else ''}"
          f"{' — attended games first' if args.attended_first else ''}")
    if not todo:
        print("nothing to do")
        cur.close(); conn.close()
        return 0
    if not apply:
        print("(dry run — nothing will be written; pass --apply to commit)\n")

    filled = skipped = 0
    for i, g in enumerate(todo, 1):
        tag = f"[{g['attended_logs']} attended]" if g["attended_logs"] else ""
        v = venue_for_game(g["game_id"], g["league"])
        if not v:
            skipped += 1
            print(f"  {i}/{len(todo)} {g['game_id']} {g['game_date']} — no venue in payload")
            time.sleep(args.sleep)
            continue
        crowd = f"{v['attendance']:,}" if v["attendance"] else "—"
        print(f"  {i}/{len(todo)} {g['game_id']} {g['game_date']} — "
              f"{v['arena_name']}, {v['arena_city']} · {crowd}"
              f"{' · SELLOUT' if v['sellout'] else ''} {tag}")
        if apply:
            cur.execute("""
                UPDATE games SET arena_name=%(arena_name)s, arena_city=%(arena_city)s,
                                 attendance=%(attendance)s, sellout=%(sellout)s
                 WHERE game_id=%(gid)s
            """, {**v, "gid": g["game_id"]})
            conn.commit()
        filled += 1
        time.sleep(args.sleep)

    print(f"\n{'wrote' if apply else 'would write'} {filled} · skipped {skipped}")
    cur.close(); conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
