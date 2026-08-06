"""
ydkball — Game Fetcher
==============================
python backend/fetch_games.py

Fetches all completed 2025-26 NBA games from the NBA API and stores them
in the `games` table. Safe to re-run — uses INSERT ... ON CONFLICT DO UPDATE
so existing rows are refreshed, not duplicated.

Usage:
  python backend/fetch_games.py              # Regular Season
  python backend/fetch_games.py --playoffs   # Playoffs
  python backend/fetch_games.py --all        # Both
"""

import os, sys, time, argparse
from datetime import datetime, date
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("❌ DATABASE_URL not found."); sys.exit(1)

SEASON = None  # set from CLI args below

# NBA API season type strings → our labels
SEASON_TYPES = {
    "Regular Season": "Regular Season",
    "Playoffs":       "Playoffs",
}

# CDN headers (same as server.py)
CDN_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Referer":    "https://www.nba.com/",
    "Origin":     "https://www.nba.com",
    "Accept":     "application/json, text/plain, */*",
}


def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)


def normalize_season(s: str) -> str:
    """Convert '2020-2021' → '2020-21', leave '2020-21' unchanged."""
    if "-" in s:
        left, right = s.split("-", 1)
        if len(right) == 4:
            right = right[2:]
        return f"{left}-{right}"
    return s


def fetch_season_type(season_type_label: str, season_type_api: str):
    """Fetch all completed games for one season type."""
    import requests
    from nba_api.stats.endpoints import leaguegamefinder

    season_api = normalize_season(SEASON)
    print(f"\n📅 Fetching {season_api} {season_type_label}...")

    df = None
    for attempt in range(1, 4):
        try:
            if attempt > 1:
                wait = attempt * 10
                print(f"   ⏳ Retry {attempt}/3 in {wait}s...")
                time.sleep(wait)
            finder = leaguegamefinder.LeagueGameFinder(
                season_nullable=season_api,
                league_id_nullable="00",
                season_type_nullable=season_type_api,
                timeout=60,
            )
            time.sleep(1)
            df = finder.get_data_frames()[0]
            break
        except Exception as e:
            print(f"   ⚠️  Attempt {attempt} failed: {e}")
            if attempt == 3:
                print(f"   ❌ All retries exhausted.")
                return 0

    if df.empty:
        print("   ℹ️  No games found.")
        return 0

    # LeagueGameFinder returns one row per team per game — deduplicate to one per game
    # Keep only completed games (GAME_DATE is populated, WL is not null)
    df = df[df["WL"].notna()].copy()

    # Get unique game IDs
    game_ids = df["GAME_ID"].unique()
    print(f"   Found {len(game_ids)} completed games.")

    conn = get_conn()
    cur  = conn.cursor()
    upserted = 0

    for gid in game_ids:
        rows = df[df["GAME_ID"] == gid]
        if len(rows) < 2:
            continue  # need both team rows to know home/away

        # MATCHUP format: "BOS vs. LAL" (home team's row) or "BOS @ LAL" (away
        # team's row). Normally one row of each, but for a handful of games per
        # season the feed emits the "@" form on BOTH rows (e.g. 0022501229,
        # NYK @ ORL) — keying off the presence of a "vs." row dropped those
        # games silently. The "@" string itself already names away and home in
        # order, so resolve them from it and only fall back to the row form.
        away_abbr = home_abbr = None
        at_rows = rows[rows["MATCHUP"].str.contains("@", na=False)]
        if not at_rows.empty:
            parts = str(at_rows.iloc[0]["MATCHUP"]).split("@")
            if len(parts) == 2:
                away_abbr, home_abbr = parts[0].strip(), parts[1].strip()

        if not away_abbr or not home_abbr:
            print(f"   ⚠️  Game {gid}: cannot resolve home/away from MATCHUP "
                  f"{list(rows['MATCHUP'])} — skipping")
            continue

        home_rows = rows[rows["TEAM_ABBREVIATION"] == home_abbr]
        away_rows = rows[rows["TEAM_ABBREVIATION"] == away_abbr]
        if home_rows.empty or away_rows.empty:
            print(f"   ⚠️  Game {gid}: MATCHUP names {away_abbr}/{home_abbr} but "
                  f"rows are {list(rows['TEAM_ABBREVIATION'])} — skipping")
            continue

        home = home_rows.iloc[0]
        away = away_rows.iloc[0]

        # Parse date
        try:
            game_date = datetime.strptime(str(home["GAME_DATE"]), "%Y-%m-%d").date()
        except Exception:
            try:
                game_date = datetime.strptime(str(home["GAME_DATE"]), "%b %d, %Y").date()
            except Exception:
                continue

        # Skip future games
        if game_date > date.today():
            continue

        home_score = int(home["PTS"]) if home["PTS"] and str(home["PTS"]) != "nan" else None
        away_score = int(away["PTS"]) if away["PTS"] and str(away["PTS"]) != "nan" else None

        # If scores are missing (occasionally happens), try CDN boxscore
        if home_score is None or away_score is None:
            try:
                box_url  = f"https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{gid}.json"
                box_resp = requests.get(box_url, headers=CDN_HEADERS, timeout=8)
                if box_resp.status_code == 200:
                    box = box_resp.json().get("game", {})
                    home_score = int(box.get("homeTeam", {}).get("score", 0) or 0)
                    away_score = int(box.get("awayTeam", {}).get("score", 0) or 0)
            except Exception:
                pass

        try:
            cur.execute("""
                INSERT INTO games (
                    game_id, season, season_type, game_date,
                    home_team_abbr, away_team_abbr,
                    home_score, away_score, status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'Final')
                ON CONFLICT (game_id) DO UPDATE SET
                    home_score     = EXCLUDED.home_score,
                    away_score     = EXCLUDED.away_score,
                    status         = EXCLUDED.status,
                    updated_at     = NOW()
            """, (
                str(gid), season_api, season_type_label, game_date,
                str(home["TEAM_ABBREVIATION"]),
                str(away["TEAM_ABBREVIATION"]),
                home_score, away_score,
            ))
            upserted += 1
        except Exception as e:
            print(f"   ⚠️  Game {gid}: {e}")
            conn.rollback()
            continue

        conn.commit()

    cur.close()
    conn.close()
    print(f"   ✅ {upserted} games upserted.")
    return upserted


def verify():
    """Print a summary of what's in the games table."""
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("""
        SELECT season, season_type, COUNT(*) AS n,
               MIN(game_date) AS earliest, MAX(game_date) AS latest
        FROM games
        GROUP BY season, season_type
        ORDER BY season DESC, season_type
    """)
    rows = cur.fetchall()
    cur.close(); conn.close()

    if not rows:
        print("\n   (games table is empty)")
        return

    print("\n📊 Games table summary:")
    for r in rows:
        print(f"   {r['season']} {r['season_type']:<18} {r['n']:>4} games  "
              f"({r['earliest']} → {r['latest']})")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch NBA games into the games table.")
    parser.add_argument("--season",    default="2025-26", help="Season string, e.g. 2024-25")
    parser.add_argument("--playoffs",  action="store_true", help="Fetch Playoffs only")
    parser.add_argument("--all",       action="store_true", help="Fetch Regular Season + Playoffs")
    args = parser.parse_args()

    SEASON = args.season

    if args.all:
        fetch_season_type("Regular Season", "Regular Season")
        fetch_season_type("Playoffs",       "Playoffs")
    elif args.playoffs:
        fetch_season_type("Playoffs", "Playoffs")
    else:
        fetch_season_type("Regular Season", "Regular Season")

    verify()