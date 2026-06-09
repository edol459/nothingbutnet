"""
Build team_seasons table.

Wins/losses sources:
  - 2010-11 to present:   computed from our games table
  - 1996-97 to 2009-10:   NBA API LeagueStandingsV3

Usage:
  python backend/ingest/fetch_team_seasons.py
  python backend/ingest/fetch_team_seasons.py --dry-run
  python backend/ingest/fetch_team_seasons.py --seasons 2016-17 2017-18
"""

import os, sys, time, argparse
from dotenv import load_dotenv
import psycopg2, psycopg2.extras

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

parser = argparse.ArgumentParser()
parser.add_argument("--dry-run",  action="store_true")
parser.add_argument("--seasons",  nargs="*", help="Specific seasons e.g. 2016-17 2017-18")
parser.add_argument("--delay",    type=float, default=2.0)
args = parser.parse_args()

# Full name mapping for every abbr we track (current + historical)
TEAM_NAMES = {
    "ATL": "Atlanta Hawks",
    "BOS": "Boston Celtics",
    "BKN": "Brooklyn Nets",
    "NJN": "New Jersey Nets",
    "CHA": "Charlotte Hornets",
    "CHH": "Charlotte Hornets",       # original 1988-2002
    "CHI": "Chicago Bulls",
    "CLE": "Cleveland Cavaliers",
    "DAL": "Dallas Mavericks",
    "DEN": "Denver Nuggets",
    "DET": "Detroit Pistons",
    "GSW": "Golden State Warriors",
    "HOU": "Houston Rockets",
    "IND": "Indiana Pacers",
    "LAC": "Los Angeles Clippers",
    "LAL": "Los Angeles Lakers",
    "MEM": "Memphis Grizzlies",
    "VAN": "Vancouver Grizzlies",
    "MIA": "Miami Heat",
    "MIL": "Milwaukee Bucks",
    "MIN": "Minnesota Timberwolves",
    "NOP": "New Orleans Pelicans",
    "NOH": "New Orleans Hornets",
    "NOK": "New Orleans/Oklahoma City Hornets",
    "NYK": "New York Knicks",
    "OKC": "Oklahoma City Thunder",
    "SEA": "Seattle SuperSonics",
    "ORL": "Orlando Magic",
    "PHI": "Philadelphia 76ers",
    "PHX": "Phoenix Suns",
    "POR": "Portland Trail Blazers",
    "SAC": "Sacramento Kings",
    "SAS": "San Antonio Spurs",
    "TOR": "Toronto Raptors",
    "UTA": "Utah Jazz",
    "WAS": "Washington Wizards",
}

def season_list():
    """1996-97 through 2025-26."""
    seasons = []
    for y in range(1996, 2026):
        seasons.append(f"{y}-{str(y+1)[-2:]}")
    return seasons


def wins_losses_from_games(cur, season):
    """Compute W-L for each team in a season from our games table."""
    cur.execute("""
        SELECT home_team_abbr AS abbr,
               SUM(CASE WHEN home_score > away_score THEN 1 ELSE 0 END) AS wins,
               SUM(CASE WHEN home_score < away_score THEN 1 ELSE 0 END) AS losses
        FROM games
        WHERE season = %s AND season_type = 'Regular Season'
          AND home_score IS NOT NULL
        GROUP BY home_team_abbr
        UNION ALL
        SELECT away_team_abbr AS abbr,
               SUM(CASE WHEN away_score > home_score THEN 1 ELSE 0 END) AS wins,
               SUM(CASE WHEN away_score < home_score THEN 1 ELSE 0 END) AS losses
        FROM games
        WHERE season = %s AND season_type = 'Regular Season'
          AND away_score IS NOT NULL
        GROUP BY away_team_abbr
    """, (season, season))
    totals = {}
    for r in cur.fetchall():
        abbr = r["abbr"]
        if abbr not in totals:
            totals[abbr] = {"wins": 0, "losses": 0}
        totals[abbr]["wins"]   += int(r["wins"] or 0)
        totals[abbr]["losses"] += int(r["losses"] or 0)
    return totals  # {abbr: {wins, losses}}


def wins_losses_from_api(season, delay):
    """Fetch W-L from NBA API LeagueStandingsV3."""
    try:
        from nba_api.stats.endpoints import leaguestandingsv3
        time.sleep(delay)
        s = leaguestandingsv3.LeagueStandingsV3(
            season=season, season_type="Regular Season", timeout=30
        )
        df = s.get_data_frames()[0]
        result = {}
        for _, row in df.iterrows():
            abbr = row.get("TeamAbbreviation") or row.get("TeamSlug", "").upper()
            if not abbr:
                continue
            result[abbr] = {
                "wins":   int(row.get("WINS",   0) or 0),
                "losses": int(row.get("LOSSES", 0) or 0),
            }
        return result
    except Exception as e:
        print(f"  ⚠ API error for {season}: {e}")
        return {}


def teams_in_season(cur, season):
    """Return distinct team abbrs that appear in player_seasons for this season."""
    cur.execute("""
        SELECT DISTINCT team_abbr FROM player_seasons
        WHERE season = %s AND season_type = 'Regular Season' AND team_abbr IS NOT NULL
    """, (season,))
    return [r["team_abbr"] for r in cur.fetchall()]


def init_tables(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS team_seasons (
            id          SERIAL PRIMARY KEY,
            team_abbr   TEXT NOT NULL,
            team_name   TEXT NOT NULL,
            season      TEXT NOT NULL,
            wins        INTEGER,
            losses      INTEGER,
            UNIQUE (team_abbr, season)
        )
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_team_seasons_season   ON team_seasons(season)
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_team_seasons_abbr     ON team_seasons(team_abbr)
    """)
    conn.commit(); cur.close()
    print("✅ team_seasons table ready")


def upsert(conn, rows):
    cur = conn.cursor()
    for r in rows:
        cur.execute("""
            INSERT INTO team_seasons (team_abbr, team_name, season, wins, losses)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (team_abbr, season) DO UPDATE SET
              team_name = EXCLUDED.team_name,
              wins      = COALESCE(EXCLUDED.wins,   team_seasons.wins),
              losses    = COALESCE(EXCLUDED.losses, team_seasons.losses)
        """, (r["team_abbr"], r["team_name"], r["season"], r.get("wins"), r.get("losses")))
    conn.commit(); cur.close()


# First season we have games data for
GAMES_START_YEAR = 2010


def run():
    if not DATABASE_URL:
        print("❌ DATABASE_URL not set"); sys.exit(1)

    conn = psycopg2.connect(DATABASE_URL)
    conn.cursor_factory = psycopg2.extras.RealDictCursor

    if not args.dry_run:
        init_tables(conn)

    all_seasons = args.seasons if args.seasons else season_list()
    print(f"\n🏀 Processing {len(all_seasons)} seasons")

    cur = conn.cursor()

    for season in all_seasons:
        start_year = int(season[:4])
        print(f"\n{season}", end="  ", flush=True)

        team_abbrs = teams_in_season(cur, season)
        if not team_abbrs:
            print("no player data — skipping")
            continue

        # Get W-L
        if start_year >= GAMES_START_YEAR:
            wl = wins_losses_from_games(cur, season)
            source = "games table"
        else:
            wl = wins_losses_from_api(season, args.delay)
            source = "NBA API"

        rows = []
        for abbr in team_abbrs:
            name = TEAM_NAMES.get(abbr, abbr)
            rec = wl.get(abbr, {})
            rows.append({
                "team_abbr": abbr,
                "team_name": name,
                "season":    season,
                "wins":      rec.get("wins"),
                "losses":    rec.get("losses"),
            })

        if args.dry_run:
            for r in rows[:3]:
                print(f"  {r}")
            print(f"  ... {len(rows)} total")
        else:
            upsert(conn, rows)
            wl_count = sum(1 for r in rows if r.get("wins") is not None)
            print(f"{len(rows)} teams, {wl_count} with W-L  [{source}]")

    cur.close(); conn.close()
    print("\n✅ Done")


if __name__ == "__main__":
    run()
