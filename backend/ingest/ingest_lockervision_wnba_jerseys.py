"""
Ingest WNBA jersey images from the NBA LockerVision CDN (lockervision.wnba.com).

URL pattern:
  https://appimages.nba.com/p/tr:n-wslnfre/WNBA/{YEAR}/uniform/{Team Full Name}/{ABBR}_{EDITION}.jpg

Editions:
  HE = Home Edition
  RE = Road Edition
  EE = Explorer Edition (city-style alternate)
  CO = Court Origins Edition (select teams only)

Usage:
  python backend/ingest/ingest_lockervision_wnba_jerseys.py
  python backend/ingest/ingest_lockervision_wnba_jerseys.py --dry-run
  python backend/ingest/ingest_lockervision_wnba_jerseys.py --season 2026
"""

import os, sys, argparse
import urllib.parse
from dotenv import load_dotenv
import psycopg2

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

parser = argparse.ArgumentParser()
parser.add_argument("--dry-run", action="store_true")
parser.add_argument("--season", type=int, default=2026, help="WNBA season year (default: 2026)")
args = parser.parse_args()

SEASON     = args.season
SEASON_STR = str(SEASON)
CDN_BASE   = f"https://appimages.nba.com/p/tr:n-wslnfre/WNBA/{SEASON}/uniform"
SOURCE     = "lockervision_wnba"

EDITION_NAMES = {
    "HE": "Home Edition",
    "RE": "Road Edition",
    "EE": "Explorer Edition",
    "CO": "Court Origins Edition",
}

# (abbr, full_name, editions)
TEAMS = [
    ("ATL", "Atlanta Dream",           ["HE", "RE", "EE"]),
    ("CHI", "Chicago Sky",             ["HE", "RE", "EE"]),
    ("CON", "Connecticut Sun",         ["HE", "RE", "EE"]),
    ("DAL", "Dallas Wings",            ["HE", "RE", "EE"]),
    ("GSV", "Golden State Valkyries",  ["HE", "RE", "EE"]),
    ("IND", "Indiana Fever",           ["HE", "RE", "EE"]),
    ("LVA", "Las Vegas Aces",          ["HE", "RE", "EE"]),
    ("LAS", "Los Angeles Sparks",      ["HE", "RE", "EE", "CO"]),
    ("MIN", "Minnesota Lynx",          ["HE", "RE", "EE"]),
    ("NYL", "New York Liberty",        ["HE", "RE", "EE", "CO"]),
    ("PHX", "Phoenix Mercury",         ["HE", "RE", "EE", "CO"]),
    ("PDX", "Portland Fire",           ["HE", "RE", "EE"]),
    ("SEA", "Seattle Storm",           ["HE", "RE", "EE"]),
    ("TOR", "Toronto Tempo",           ["HE", "RE", "EE"]),
    ("WAS", "Washington Mystics",      ["HE", "RE", "EE"]),
]


def build_jerseys():
    jerseys = []
    for abbr, team_name, editions in TEAMS:
        enc = urllib.parse.quote(team_name)
        for edition_code in editions:
            edition_name = EDITION_NAMES[edition_code]
            image_hash = f"lv_wnba_{SEASON}_{abbr}_{edition_code}"
            image_url  = f"{CDN_BASE}/{enc}/{abbr}_{edition_code}.jpg"
            label      = f"{team_name} {SEASON_STR} {edition_name}"
            jerseys.append({
                "team_slug":   abbr.lower(),
                "team_name":   team_name,
                "team_abbr":   abbr,
                "year_range":  SEASON_STR,
                "year_start":  SEASON,
                "label":       label,
                "variant":     edition_name,
                "image_hash":  image_hash,
                "image_url":   image_url,
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
    print(f"Built {len(jerseys)} WNBA jersey entries for {SEASON_STR} season")

    if args.dry_run:
        for j in jerseys[:8]:
            print(f"  {j['image_hash']:35s}  {j['image_url']}")
        print("  ...")
        return

    if not DATABASE_URL:
        print("❌ DATABASE_URL not set"); sys.exit(1)

    conn = psycopg2.connect(DATABASE_URL)
    n = upsert_jerseys(conn, jerseys)
    conn.close()
    print(f"✅ {n} rows upserted (source=lockervision_wnba, season={SEASON_STR})")


if __name__ == "__main__":
    run()
