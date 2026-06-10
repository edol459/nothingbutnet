"""
Build team_seasons rows for WNBA (league='wnba').

Fetches W-L records from the NBA API (WNBA league_id='10') for each season.

Usage:
  python backend/ingest/fetch_wnba_team_seasons.py
  python backend/ingest/fetch_wnba_team_seasons.py --dry-run
  python backend/ingest/fetch_wnba_team_seasons.py --seasons 2024 2025
"""

import os, sys, time, argparse, concurrent.futures
from dotenv import load_dotenv
import psycopg2, psycopg2.extras

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

parser = argparse.ArgumentParser()
parser.add_argument("--dry-run",  action="store_true")
parser.add_argument("--seasons",  nargs="*", help="Specific WNBA seasons e.g. 2024 2025")
parser.add_argument("--delay",    type=float, default=2.0)
args = parser.parse_args()

# The NBA API returns team nicknames as TeamAbbreviation for WNBA.
# Map those to the proper 2-3 letter abbreviations used everywhere else.
NICKNAME_TO_ABBR = {
    "ACES":         "LVA",
    "DREAM":        "ATL",
    "FEVER":        "IND",
    "FIRE":         "PDX",
    "LIBERTY":      "NYL",
    "LYNX":         "MIN",
    "MERCURY":      "PHX",
    "MYSTICS":      "WAS",
    "SKY":          "CHI",
    "SPARKS":       "LAS",
    "STARS":        "SAS",
    "STORM":        "SEA",
    "SUN":          "CON",
    "TEMPO":        "TOR",
    "VALKYRIES":    "GSV",
    "WINGS":        "DAL",
    # Historical defunct teams
    "COMETS":       "HOU",
    "MONARCHS":     "SAC",
    "MIRACLE":      "ORL",
    "ROCKERS":      "CLE",
    "SHOCK":        "DET",
    "SILVER-STARS": "SAS",
    "SOL":          "MIA",
    "STARZZ":       "UTA",
    "STING":        "CHA",
}

TEAM_NAMES = {
    "ATL": "Atlanta Dream",
    "CHI": "Chicago Sky",
    "CON": "Connecticut Sun",
    "DAL": "Dallas Wings",
    "GSV": "Golden State Valkyries",
    "IND": "Indiana Fever",
    "LVA": "Las Vegas Aces",
    "LAS": "Los Angeles Sparks",
    "MIN": "Minnesota Lynx",
    "NYL": "New York Liberty",
    "PHX": "Phoenix Mercury",
    "PDX": "Portland Fire",
    "SEA": "Seattle Storm",
    "SAS": "San Antonio Stars",
    "TOR": "Toronto Tempo",
    "WAS": "Washington Mystics",
    # Historical
    "CHA": "Charlotte Sting",
    "CLE": "Cleveland Rockers",
    "DET": "Detroit Shock",
    "HOU": "Houston Comets",
    "MIA": "Miami Sol",
    "ORL": "Orlando Miracle",
    "SAC": "Sacramento Monarchs",
    "UTA": "Utah Starzz",
}


def season_list():
    return [str(y) for y in range(1997, 2027)]


def wins_losses_from_games(cur, season):
    """Compute W-L for each WNBA team in a season from the games table.
    Works from Railway (no external API needed)."""
    cur.execute("""
        SELECT home_team_abbr AS abbr,
               SUM(CASE WHEN home_score > away_score THEN 1 ELSE 0 END) AS wins,
               SUM(CASE WHEN home_score < away_score THEN 1 ELSE 0 END) AS losses
        FROM games
        WHERE league = 'wnba' AND season = %s AND season_type = 'Regular Season'
          AND home_score IS NOT NULL
        GROUP BY home_team_abbr
        UNION ALL
        SELECT away_team_abbr AS abbr,
               SUM(CASE WHEN away_score > home_score THEN 1 ELSE 0 END) AS wins,
               SUM(CASE WHEN away_score < home_score THEN 1 ELSE 0 END) AS losses
        FROM games
        WHERE league = 'wnba' AND season = %s AND season_type = 'Regular Season'
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
    return totals


def wins_losses_from_api(season, delay):
    """Fetch WNBA W-L from NBA API with a hard thread timeout."""
    try:
        from nba_api.stats.endpoints import leaguestandingsv3
        time.sleep(delay)

        def _fetch():
            return leaguestandingsv3.LeagueStandingsV3(
                league_id="10",
                season=season,
                season_type="Regular Season",
                timeout=30,
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
            future = ex.submit(_fetch)
            s = future.result(timeout=45)

        df = s.get_data_frames()[0]
        result = {}
        for _, row in df.iterrows():
            raw = (row.get("TeamAbbreviation") or row.get("TeamSlug", "")).strip().upper()
            if not raw:
                continue
            abbr = NICKNAME_TO_ABBR.get(raw, raw)
            result[abbr] = {
                "wins":   int(row.get("WINS",   0) or 0),
                "losses": int(row.get("LOSSES", 0) or 0),
            }
        return result
    except concurrent.futures.TimeoutError:
        print(f"  ⚠ Timeout for {season} — skipping")
        return {}
    except Exception as e:
        print(f"  ⚠ API error for {season}: {e}")
        return {}


def get_conn():
    c = psycopg2.connect(DATABASE_URL)
    c.cursor_factory = psycopg2.extras.RealDictCursor
    return c


def ensure_league_column(conn):
    cur = conn.cursor()
    cur.execute("ALTER TABLE team_seasons ADD COLUMN IF NOT EXISTS league TEXT NOT NULL DEFAULT 'nba'")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_team_seasons_league ON team_seasons(league)")
    conn.commit(); cur.close()


def fix_nickname_abbrs(conn):
    """Rename any existing rows that used nickname abbreviations (ACES, DREAM, etc.)."""
    cur = conn.cursor()
    updated = 0
    for nickname, abbr in NICKNAME_TO_ABBR.items():
        name = TEAM_NAMES.get(abbr, abbr)
        # Update only if proper-abbr row doesn't already exist for that season
        cur.execute("""
            UPDATE team_seasons
            SET team_abbr = %s, team_name = %s
            WHERE league = 'wnba'
              AND team_abbr = %s
              AND NOT EXISTS (
                SELECT 1 FROM team_seasons t2
                WHERE t2.league = 'wnba' AND t2.team_abbr = %s AND t2.season = team_seasons.season
              )
        """, (abbr, name, nickname, abbr))
        updated += cur.rowcount
        # Delete any orphaned nickname rows that couldn't be renamed (duplicate season)
        cur.execute("DELETE FROM team_seasons WHERE league='wnba' AND team_abbr=%s", (nickname,))
    conn.commit(); cur.close()
    if updated:
        print(f"  ✅ Renamed {updated} nickname-based rows to proper abbreviations")


def upsert(conn, rows):
    cur = conn.cursor()
    for r in rows:
        cur.execute("""
            INSERT INTO team_seasons (team_abbr, team_name, season, wins, losses, league)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (team_abbr, season)
            DO UPDATE SET
              team_name = EXCLUDED.team_name,
              wins      = COALESCE(EXCLUDED.wins,   team_seasons.wins),
              losses    = COALESCE(EXCLUDED.losses, team_seasons.losses),
              league    = EXCLUDED.league
        """, (r["team_abbr"], r["team_name"], r["season"],
              r.get("wins"), r.get("losses"), "wnba"))
    conn.commit(); cur.close()


def run():
    if not DATABASE_URL:
        print("❌ DATABASE_URL not set"); sys.exit(1)

    conn = get_conn()
    ensure_league_column(conn)
    print("✅ league column ready")
    fix_nickname_abbrs(conn)
    conn.close()

    all_seasons = args.seasons if args.seasons else season_list()
    print(f"\n🏀 Processing {len(all_seasons)} WNBA seasons")

    for season in all_seasons:
        print(f"\n{season}", end="  ", flush=True)

        # Try games table first (works from Railway, no API rate limits)
        conn = get_conn()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        wl   = wins_losses_from_games(cur, season)
        cur.close(); conn.close()

        source = "games table"
        if not wl:
            # Fall back to NBA API for seasons without game data (pre-2024)
            wl     = wins_losses_from_api(season, args.delay)
            source = "nba api"

        if not wl:
            print("no data — skipping")
            continue

        rows = []
        for abbr, rec in wl.items():
            name = TEAM_NAMES.get(abbr, abbr)
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
            conn = get_conn()
            upsert(conn, rows)
            conn.close()
            print(f"{len(rows)} teams  [{season}]  ({source})")

    print("\n✅ Done")


if __name__ == "__main__":
    run()
