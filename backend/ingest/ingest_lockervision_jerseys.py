"""
Ingest current NBA jersey images from the NBA LockerVision CDN.

URL pattern:
  https://appimages.nba.com/p/tr:n-slnfre/2025/uniform/{Team Full Name}/{ABBR}_{EDITION}.jpg

Editions:
  AE = Association Edition (white)
  IE = Icon Edition (primary color)
  SE = Statement Edition (alternate/dark)
  CE = City Edition (special/annual)

Outputs: rows in the jerseys table with source_slug='lockervision'.

Usage:
  python backend/ingest/ingest_lockervision_jerseys.py
  python backend/ingest/ingest_lockervision_jerseys.py --dry-run
  python backend/ingest/ingest_lockervision_jerseys.py --season 2024
"""

import os, sys, argparse
import urllib.request, urllib.parse, urllib.error
from dotenv import load_dotenv
import psycopg2

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

parser = argparse.ArgumentParser()
parser.add_argument("--dry-run", action="store_true", help="Print results, don't write to DB")
parser.add_argument("--season",  type=int, default=2025, help="NBA season start year (default: 2025 = 2025-26)")
args = parser.parse_args()

SEASON     = args.season
SEASON_STR = f"{SEASON}-{str(SEASON + 1)[-2:]}"  # "2025-26"
CDN_BASE   = f"https://appimages.nba.com/p/tr:n-slnfre/{SEASON}/uniform"
SOURCE     = "lockervision"

EDITION_NAMES = {
    "AE": "Association Edition",
    "IE": "Icon Edition",
    "SE": "Statement Edition",
    "CE": "City Edition",
}

TEAMS = [
    ("ATL", "Atlanta Hawks"),
    ("BOS", "Boston Celtics"),
    ("BKN", "Brooklyn Nets"),
    ("CHA", "Charlotte Hornets"),
    ("CHI", "Chicago Bulls"),
    ("CLE", "Cleveland Cavaliers"),
    ("DAL", "Dallas Mavericks"),
    ("DEN", "Denver Nuggets"),
    ("DET", "Detroit Pistons"),
    ("GSW", "Golden State Warriors"),
    ("HOU", "Houston Rockets"),
    ("IND", "Indiana Pacers"),
    ("LAC", "Los Angeles Clippers"),
    ("LAL", "Los Angeles Lakers"),
    ("MEM", "Memphis Grizzlies"),
    ("MIA", "Miami Heat"),
    ("MIL", "Milwaukee Bucks"),
    ("MIN", "Minnesota Timberwolves"),
    ("NOP", "New Orleans Pelicans"),
    ("NYK", "New York Knicks"),
    ("OKC", "Oklahoma City Thunder"),
    ("ORL", "Orlando Magic"),
    ("PHI", "Philadelphia 76ers"),
    ("PHX", "Phoenix Suns"),
    ("POR", "Portland Trail Blazers"),
    ("SAC", "Sacramento Kings"),
    ("SAS", "San Antonio Spurs"),
    ("TOR", "Toronto Raptors"),
    ("UTA", "Utah Jazz"),
    ("WAS", "Washington Wizards"),
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}


def check_url(url):
    req = urllib.request.Request(url, headers=HEADERS, method="HEAD")
    try:
        r = urllib.request.urlopen(req, timeout=10)
        return r.status == 200
    except Exception:
        return False


def build_jerseys():
    jerseys = []
    for abbr, team_name in TEAMS:
        enc = urllib.parse.quote(team_name)
        for edition_code, edition_name in EDITION_NAMES.items():
            image_hash = f"lv_{SEASON}_{abbr}_{edition_code}"
            image_url  = f"{CDN_BASE}/{enc}/{abbr}_{edition_code}.jpg"
            label      = f"{team_name} {SEASON_STR} {edition_name}"
            jerseys.append({
                "team_slug":  abbr.lower(),
                "team_name":  team_name,
                "team_abbr":  abbr,
                "year_range": SEASON_STR,
                "year_start": SEASON,
                "label":      label,
                "variant":    edition_name,
                "image_hash": image_hash,
                "image_url":  image_url,
                "source_slug": SOURCE,
            })
    return jerseys


def upsert_jerseys(conn, jerseys):
    cur = conn.cursor()
    count = 0
    for j in jerseys:
        cur.execute("""
            INSERT INTO jerseys (team_slug, team_name, year_range, year_start,
                                 label, variant, image_hash, image_url, source_slug)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (image_hash) DO UPDATE SET
              team_name  = EXCLUDED.team_name,
              year_range = EXCLUDED.year_range,
              year_start = EXCLUDED.year_start,
              label      = EXCLUDED.label,
              variant    = EXCLUDED.variant,
              image_url  = EXCLUDED.image_url
        """, (j["team_slug"], j["team_name"], j["year_range"], j["year_start"],
              j["label"], j["variant"], j["image_hash"], j["image_url"],
              j["source_slug"]))
        count += 1
    conn.commit()
    cur.close()
    return count


def run():
    jerseys = build_jerseys()
    print(f"Built {len(jerseys)} jersey entries for {SEASON_STR} season")

    if args.dry_run:
        for j in jerseys[:8]:
            print(f"  {j['image_hash']:30s}  {j['image_url']}")
        print("  ...")
        return

    if not DATABASE_URL:
        print("❌ DATABASE_URL not set"); sys.exit(1)

    conn = psycopg2.connect(DATABASE_URL)
    n = upsert_jerseys(conn, jerseys)
    conn.close()
    print(f"✅ {n} rows upserted into jerseys table (source=lockervision, season={SEASON_STR})")


if __name__ == "__main__":
    run()
