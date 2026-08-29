"""
ydkball — Scheduled games ingest
================================
Loads the forward-looking schedule for both leagues into `scheduled_games` from
the same static CDN feeds the site already reads at request time.

Why this exists: `games` only ever gains a row once a game is Final, so nothing
in Postgres knows what is *about* to be played. That blocks the watchlist (a row
needs a game to reference), calendar markers (a month of dots should be one
query, not a season-sized JSON parse per render), and tip-off notifications —
`games` has no time column at all. server.py holds the same feed in a
process-local dict, but that is per-worker, dropped on recycle, and unqueryable.

Upserts every game every run, so flexed tip times, postponements and status
changes self-heal, and playoff / play-in / NBA Cup games land as soon as the
league schedules them — they do not exist on the schedule published in autumn.

Deliberately writes nothing to `games`: that table is a results archive and
~12 of its ~30 read sites in server.py have no status filter, so unplayed rows
there would quietly skew feeds, browse and review counts.

Usage:
  python backend/ingest/fetch_scheduled_games.py              # both leagues
  python backend/ingest/fetch_scheduled_games.py --league nba
  python backend/ingest/fetch_scheduled_games.py --dry-run
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

# game_id[2] -> season type. The first five match the values already stored in
# `games`; "6" is the NBA Cup championship, which the league numbers separately
# because it does not count toward the standings. `games` has no precedent for
# it, but nothing reads this table yet, so the honest label is safe here.
SEASON_TYPE = {
    "1": "Pre Season", "2": "Regular Season", "3": "All Star",
    "4": "Playoffs",   "5": "PlayIn",         "6": "NBA Cup",
}

# CDN gameStatus -> our status text.
STATUS = {1: "Scheduled", 2: "Live", 3: "Final"}


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


def parse_tipoff(g):
    """Real tip-off instant, or None when the league hasn't set one.

    `gameTimeUTC` is a "1900-01-01T…" placeholder carrying only a time-of-day;
    `gameDateTimeUTC` is the actual instant. Prefer the latter and refuse
    anything still wearing the placeholder date.
    """
    raw = str(g.get("gameDateTimeUTC") or "")
    if not raw or raw.startswith("1900-"):
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def collect_games(league, schedule):
    """Return upsert tuples for every game on the league's published schedule."""
    season_year = schedule.get("seasonYear", "")
    out, skipped_tbd = [], 0

    for entry in schedule.get("gameDates", []):
        raw = entry.get("gameDate", "")          # "10/20/2026 00:00:00"
        try:
            game_date = datetime.strptime(raw, "%m/%d/%Y %H:%M:%S").date()
        except ValueError:
            continue

        for g in entry.get("games", []):
            gid = str(g.get("gameId") or "")
            if len(gid) < 3:
                continue

            # "If necessary" playoff games (a game 6 or 7 that may never be
            # played) sit on the schedule with a TBD status. Marking those on
            # someone's calendar would promise a game that often doesn't happen.
            if g.get("ifNecessary") and str(g.get("gameStatusText", "")).strip().upper() == "TBD":
                skipped_tbd += 1
                continue

            home, away = g.get("homeTeam", {}), g.get("awayTeam", {})
            h_abbr = home.get("teamTricode", "")
            a_abbr = away.get("teamTricode", "")
            if league == "wnba":
                h_abbr = WNBA_ABBR.get(h_abbr, h_abbr)
                a_abbr = WNBA_ABBR.get(a_abbr, a_abbr)
            # Bracket placeholders — NBA Cup knockouts and playoff rounds sit on
            # the schedule with empty tricodes until the field is decided. Skip
            # them rather than marking a calendar with "TBD @ TBD"; the daily
            # re-run picks each one up the moment the league fills it in.
            if not h_abbr or not a_abbr:
                continue

            out.append((
                gid, league, season_year,
                SEASON_TYPE.get(gid[2], "Regular Season"),
                game_date,
                parse_tipoff(g),
                h_abbr, a_abbr,
                STATUS.get(int(g.get("gameStatus") or 1), "Scheduled"),
                (g.get("gameStatusText") or "").strip(),
                g.get("arenaName") or None,
                g.get("arenaCity") or None,
                (g.get("gameLabel") or "").strip() or None,
                bool(g.get("isNeutral")),
                str(g.get("postponedStatus") or "N").upper() == "Y",
            ))

    return out, skipped_tbd


UPSERT = """
    INSERT INTO scheduled_games (
        game_id, league, season, season_type, game_date, game_time_utc,
        home_team_abbr, away_team_abbr, status, status_text,
        arena_name, arena_city, game_label, is_neutral, postponed, updated_at
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, NOW())
    ON CONFLICT (game_id) DO UPDATE SET
        league         = EXCLUDED.league,
        season         = EXCLUDED.season,
        season_type    = EXCLUDED.season_type,
        game_date      = EXCLUDED.game_date,
        game_time_utc  = EXCLUDED.game_time_utc,
        home_team_abbr = EXCLUDED.home_team_abbr,
        away_team_abbr = EXCLUDED.away_team_abbr,
        status         = EXCLUDED.status,
        status_text    = EXCLUDED.status_text,
        arena_name     = EXCLUDED.arena_name,
        arena_city     = EXCLUDED.arena_city,
        game_label     = EXCLUDED.game_label,
        is_neutral     = EXCLUDED.is_neutral,
        postponed      = EXCLUDED.postponed,
        updated_at     = NOW()
"""


def ingest(conn, league):
    try:
        schedule = fetch_schedule(league)
    except Exception as e:
        print(f"  ⚠️  {league.upper()}: schedule fetch failed — {e}")
        return 0

    games, skipped_tbd = collect_games(league, schedule)
    season_year = schedule.get("seasonYear", "")
    if not games:
        print(f"  {league.upper()}: schedule is empty — nothing to do")
        return 0

    dates = [g[4] for g in games]
    upcoming = sum(1 for g in games if g[8] == "Scheduled")
    no_time  = sum(1 for g in games if g[5] is None)
    print(f"  {league.upper()} {season_year}: {len(games)} games "
          f"({min(dates)} → {max(dates)}), {upcoming} not yet played"
          + (f", {skipped_tbd} if-necessary skipped" if skipped_tbd else "")
          + (f", {no_time} without a tip time" if no_time else ""))

    if args.dry_run:
        return len(games)

    cur = conn.cursor()
    cur.executemany(UPSERT, games)

    # A game pulled from the schedule (cancelled, or re-issued under a new id)
    # would otherwise linger forever and keep marking someone's calendar. Prune
    # within the seasons this feed actually covers, so other seasons are safe.
    cur.execute("""
        DELETE FROM scheduled_games
        WHERE league = %s AND season = %s AND NOT (game_id = ANY(%s))
    """, (league, season_year, [g[0] for g in games]))
    pruned = cur.rowcount
    conn.commit()
    cur.close()

    if pruned:
        print(f"    – pruned {pruned} game(s) no longer on the schedule")
    return len(games)


def main():
    leagues = ["nba", "wnba"] if args.league == "both" else [args.league]
    conn = psycopg2.connect(DATABASE_URL)
    total = 0
    print(f"{'(dry-run) ' if args.dry_run else ''}Loading scheduled games from CDN schedules")
    for lg in leagues:
        total += ingest(conn, lg)
    conn.close()
    verb = "Would upsert" if args.dry_run else "Upserted"
    print(f"\n✅ {verb} {total} scheduled game(s).")


if __name__ == "__main__":
    main()
