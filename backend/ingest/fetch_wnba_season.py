"""
Backfill WNBA games from ESPN for a given season year.

Usage:
    python backend/ingest/fetch_wnba_season.py          # current WNBA season
    python backend/ingest/fetch_wnba_season.py 2025     # specific year
    python backend/ingest/fetch_wnba_season.py 2024     # previous year

The WNBA regular season runs approximately May 16 – Oct 19.
Games are inserted into the shared `games` table with league='wnba'.
"""

import os, sys, time, requests
from datetime import date, timedelta

# Allow running from project root or backend/ingest/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from dotenv import load_dotenv
load_dotenv()

import psycopg2
import psycopg2.extras

DATABASE_URL = os.getenv("DATABASE_URL")

ESPN_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Accept":     "application/json",
}

# Approximate WNBA season windows (start_mmdd, end_mmdd) per year
WNBA_WINDOWS = {
    2024: (date(2024, 5, 14), date(2024, 10, 21)),
    2025: (date(2025, 5, 16), date(2025, 10, 19)),
    2026: (date(2026, 5, 15), date(2026, 10, 25)),
}


def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)


def espn_season_type(event: dict, comp: dict) -> str:
    """Map an ESPN scoreboard event to our season_type.

    ESPN's `season.type` is 1=preseason, 2=regular, 3=postseason. The All-Star
    game is season.type 2 (!) and is only distinguishable by the competition's
    type abbreviation, so check that first.
    """
    if (comp.get("type") or {}).get("abbreviation") == "ALLSTAR":
        return "All Star"
    return {1: "Pre Season", 2: "Regular Season", 3: "Playoffs"}.get(
        (event.get("season") or {}).get("type"), "Regular Season")


def fetch_day(date_str: str) -> list[dict]:
    """Fetch ESPN WNBA scoreboard for a YYYY-MM-DD string. Returns list of game dicts."""
    compact = date_str.replace("-", "")
    url = (f"https://site.api.espn.com/apis/site/v2/sports/basketball/wnba"
           f"/scoreboard?dates={compact}")
    try:
        resp = requests.get(url, headers=ESPN_HEADERS, timeout=12)
        resp.raise_for_status()
        games = []
        for event in resp.json().get("events", []):
            competitions = event.get("competitions", [])
            if not competitions:
                continue
            comp        = competitions[0]
            status_name = comp.get("status", {}).get("type", {}).get("name", "")
            if status_name != "STATUS_FINAL":
                continue  # only store completed games
            home_d, away_d = {}, {}
            for ct in comp.get("competitors", []):
                abbr  = ct.get("team", {}).get("abbreviation", "")
                score = int(ct.get("score", 0) or 0)
                if ct.get("homeAway") == "home":
                    home_d = {"abbr": abbr, "score": score}
                else:
                    away_d = {"abbr": abbr, "score": score}
            games.append({
                "game_id":     event.get("id", ""),
                "season_type": espn_season_type(event, comp),
                "game_date":   date_str,
                "home_abbr":  home_d.get("abbr", ""),
                "away_abbr":  away_d.get("abbr", ""),
                "home_score": home_d.get("score", 0),
                "away_score": away_d.get("score", 0),
            })
        return games
    except Exception as e:
        print(f"  [!] ESPN error for {date_str}: {e}")
        return []


def upsert_games(games: list[dict], season: str):
    if not games:
        return 0
    conn = get_conn()
    cur  = conn.cursor()
    count = 0
    for g in games:
        try:
            cur.execute("""
                INSERT INTO games (
                    game_id, season, season_type, game_date,
                    home_team_abbr, away_team_abbr,
                    home_score, away_score, status, league
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'Final', 'wnba')
                ON CONFLICT (game_id) DO UPDATE SET
                    home_score  = EXCLUDED.home_score,
                    away_score  = EXCLUDED.away_score,
                    season_type = EXCLUDED.season_type,
                    status      = 'Final',
                    updated_at  = NOW()
            """, (g["game_id"], season, g["season_type"], g["game_date"],
                  g["home_abbr"], g["away_abbr"],
                  g["home_score"], g["away_score"]))
            count += 1
        except Exception as e:
            print(f"  [!] DB error for {g['game_id']}: {e}")
    conn.commit()
    cur.close(); conn.close()
    return count


def main():
    year = int(sys.argv[1]) if len(sys.argv) > 1 else date.today().year
    if year < date.today().month < 5:
        year -= 1

    window = WNBA_WINDOWS.get(year)
    if not window:
        # Default: May 15 to Oct 20
        window = (date(year, 5, 15), date(year, 10, 20))

    start_d, end_d = window
    today = date.today()
    end_d = min(end_d, today)  # don't fetch future dates

    print(f"Fetching WNBA {year} season: {start_d} → {end_d}")
    total = 0
    d = start_d
    while d <= end_d:
        date_str = d.strftime("%Y-%m-%d")
        games = fetch_day(date_str)
        if games:
            n = upsert_games(games, str(year))
            total += n
            print(f"  {date_str}: {n} game(s)")
        d += timedelta(days=1)
        time.sleep(0.15)  # be polite to ESPN

    print(f"\nDone. {total} total games inserted/updated for WNBA {year}.")


if __name__ == "__main__":
    main()
