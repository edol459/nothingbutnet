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
    "TOR": "Toronto Tempo",
    "WAS": "Washington Mystics",
    # Historical
    "SAS": "San Antonio Stars",
    "TUL": "Tulsa Shock",
    "DET": "Detroit Shock",
    "HOU": "Houston Comets",
    "SAC": "Sacramento Monarchs",
    "CHA": "Charlotte Sting",
    "CLE": "Cleveland Rockers",
    "ORL": "Orlando Miracle",
    "UTA": "Utah Starzz",
    "MIA": "Miami Sol",
    "POR": "Portland Fire",
}

def season_list():
    """WNBA seasons from 1997 to current."""
    return [str(y) for y in range(1997, 2027)]


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
            abbr = row.get("TeamAbbreviation") or row.get("TeamSlug", "").upper()
            if not abbr:
                continue
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
    cur.execute("""
        ALTER TABLE team_seasons ADD COLUMN IF NOT EXISTS league TEXT NOT NULL DEFAULT 'nba'
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_team_seasons_league ON team_seasons(league)
    """)
    conn.commit(); cur.close()


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
    conn.close()
    print("✅ league column ready")

    all_seasons = args.seasons if args.seasons else season_list()
    print(f"\n🏀 Processing {len(all_seasons)} WNBA seasons")

    for season in all_seasons:
        print(f"\n{season}", end="  ", flush=True)

        wl = wins_losses_from_api(season, args.delay)

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
            print(f"{len(rows)} teams  [{season}]")

    print("\n✅ Done")


if __name__ == "__main__":
    run()
