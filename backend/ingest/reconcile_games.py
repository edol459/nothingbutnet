"""
ydkball — Games reconciliation
==============================
Fills gaps in the `games` table by diffing it against the league's static CDN
schedule.

Why this exists: `games` is otherwise written only by server.py's live scoreboard
poller, as games go Final. Any window the poller misses — a deploy, a restart, a
CDN blip — leaves a permanent hole. Nothing else ever backfills it, so the hole
silently skews team records and standings for the rest of the season. (Two WNBA
games from 2026-06-09 went missing this way and cost the Lynx a win in the
standings for two months.)

Only inserts games the schedule reports as Final and that we don't already have;
never updates or deletes an existing row. Safe to re-run.

Usage:
  python backend/ingest/reconcile_games.py                  # both leagues
  python backend/ingest/reconcile_games.py --league wnba
  python backend/ingest/reconcile_games.py --dry-run
"""

import os
import argparse
from datetime import datetime

import psycopg2
from dotenv import load_dotenv

# The NBA/WNBA CDNs sit behind Akamai Bot Manager, which fingerprints the TLS
# handshake and serves a challenge page to plain HTTP clients. curl_cffi mimics
# Chrome's fingerprint. Required for this to work from Railway — see
# docs/cdn-akamai-bot-manager.md.
from curl_cffi import requests as _cffi_requests

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

parser = argparse.ArgumentParser()
parser.add_argument("--league",  choices=["nba", "wnba", "both"], default="both")
parser.add_argument("--dry-run", action="store_true")
# Tolerate the shared daily-runner flags without erroring.
parser.add_argument("--season",      help=argparse.SUPPRESS)
parser.add_argument("--season-type", help=argparse.SUPPRESS)
args = parser.parse_args()

SCHEDULES = {
    "nba":  ("https://cdn.nba.com/static/json/staticData/scheduleLeagueV2_1.json",
             "https://www.nba.com"),
    "wnba": ("https://cdn.wnba.com/static/json/staticData/scheduleLeagueV2_1.json",
             "https://www.wnba.com"),
}

# WNBA CDN tricodes -> our canonical (games-table) abbreviations.
WNBA_ABBR = {"LVA": "LV", "LAS": "LA", "NYL": "NY", "GSV": "GS", "WAS": "WSH", "PDX": "POR"}

# game_id[2] -> season type, matching server._season_type_from_game_id.
SEASON_TYPE = {"1": "Pre Season", "2": "Regular Season", "4": "Playoffs", "5": "PlayIn"}

# Only backfill games that can affect a team's record. Preseason is deliberately
# excluded: it doesn't move the standings, and the WNBA slate includes
# exhibitions against national teams (NGR, JNT) whose tricodes would land in the
# games table as if they were league teams.
BACKFILL_TYPES = {"Regular Season", "Playoffs", "PlayIn"}


def fetch_schedule(league):
    url, origin = SCHEDULES[league]
    resp = _cffi_requests.get(
        url,
        headers={"Referer": f"{origin}/", "Origin": origin,
                 "Accept": "application/json",
                 "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"},
        timeout=30, impersonate="chrome",
    )
    resp.raise_for_status()
    return resp.json().get("leagueSchedule", {})


def collect_final_games(league, schedule):
    """Return [(game_id, season, season_type, game_date, home, away, hs, as_), ...]
    for every game the schedule reports as Final."""
    season_year = schedule.get("seasonYear", "")
    out = []
    for entry in schedule.get("gameDates", []):
        raw = entry.get("gameDate", "")          # "06/09/2026 00:00:00"
        try:
            game_date = datetime.strptime(raw, "%m/%d/%Y %H:%M:%S").date()
        except Exception:
            continue
        for g in entry.get("games", []):
            gid = str(g.get("gameId") or "")
            if len(gid) < 3:
                continue
            home, away = g.get("homeTeam", {}), g.get("awayTeam", {})
            hs = int(home.get("score") or 0)
            as_ = int(away.get("score") or 0)
            # gameStatus 3 == Final. Skip 0-0, which the feed also uses for
            # scheduled games and for games that were never played.
            if int(g.get("gameStatus") or 1) != 3 or (hs == 0 and as_ == 0):
                continue
            h_abbr = home.get("teamTricode", "")
            a_abbr = away.get("teamTricode", "")
            if league == "wnba":
                h_abbr = WNBA_ABBR.get(h_abbr, h_abbr)
                a_abbr = WNBA_ABBR.get(a_abbr, a_abbr)
            if not h_abbr or not a_abbr:
                continue
            season_type = SEASON_TYPE.get(gid[2], "Regular Season")
            if season_type not in BACKFILL_TYPES:
                continue
            out.append((gid, season_year, season_type,
                        game_date, h_abbr, a_abbr, hs, as_))
    return out


def reconcile(conn, league):
    try:
        schedule = fetch_schedule(league)
    except Exception as e:
        print(f"  ⚠️  {league.upper()}: schedule fetch failed — {e}")
        return 0

    games = collect_final_games(league, schedule)
    if not games:
        print(f"  {league.upper()}: schedule has no Final games yet — nothing to do")
        return 0

    cur = conn.cursor()
    cur.execute("SELECT game_id FROM games WHERE game_id = ANY(%s)", ([g[0] for g in games],))
    have = {r[0] for r in cur.fetchall()}
    missing = [g for g in games if g[0] not in have]

    print(f"  {league.upper()} {schedule.get('seasonYear','')}: "
          f"{len(games)} final on schedule, {len(missing)} missing locally")
    for g in missing:
        print(f"    + {g[0]}  {g[3]}  {g[5]} {g[7]} @ {g[4]} {g[6]}  ({g[2]})")

    if missing and not args.dry_run:
        cur.executemany("""
            INSERT INTO games (
                game_id, season, season_type, game_date,
                home_team_abbr, away_team_abbr,
                home_score, away_score, status, league
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'Final',%s)
            ON CONFLICT (game_id) DO NOTHING
        """, [(g[0], g[1], g[2], g[3], g[4], g[5], g[6], g[7], league) for g in missing])
        conn.commit()
    cur.close()
    return len(missing)


def main():
    leagues = ["nba", "wnba"] if args.league == "both" else [args.league]
    conn = psycopg2.connect(DATABASE_URL)
    total = 0
    print(f"{'(dry-run) ' if args.dry_run else ''}Reconciling games against CDN schedules")
    for lg in leagues:
        total += reconcile(conn, lg)
    conn.close()
    if total:
        print(f"\n✅ {'Would insert' if args.dry_run else 'Inserted'} {total} missing game(s). "
              f"Re-run the team-season steps to refresh records.")
    else:
        print("\n✅ No gaps — games table matches the schedules.")


if __name__ == "__main__":
    main()
