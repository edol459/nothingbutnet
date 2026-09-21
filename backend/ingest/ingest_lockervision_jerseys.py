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
parser.add_argument("--no-verify", action="store_true",
                    help="skip the placeholder check (writes whatever the URL pattern predicts)")
# Pipeline contract. A season with no published art is the EXPECTED state for most of
# the year, not a failure, and re-probing 120 images daily once they exist is waste.
parser.add_argument("--skip-if-present", action="store_true",
                    help="exit 0 immediately if this season already has jerseys (no CDN calls)")
parser.add_argument("--ok-if-unpublished", action="store_true",
                    help="exit 0 rather than 1 when the art isn't published yet")
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


# ── Verification ──────────────────────────────────────────────────────────────
#
# The old check was `HEAD` + `status == 200`, and it was never called. Both halves
# were wrong:
#
# 1. LockerVision answers **200 for a season it has not published yet**, serving one
#    shared placeholder instead of a 404. Probed on 2026-09-21, every 2026-27 URL
#    returned the same 3,958-byte image — four teams, three editions, one sha256.
#    Trusting the status code would have written 120 rows of blank jerseys, and
#    /api/jerseys/seasons is driven by that table, so the app's picker would have
#    defaulted to a season of placeholders.
#
# 2. `urllib` gets fallback-served by the CDN's bot detection after a handful of
#    requests — the same Akamai behaviour documented in docs/cdn-akamai-bot-manager.md.
#    Mid-probe, even known-good 2025 URLs started returning the placeholder. So
#    verification has to go through the server's impersonating client or it will
#    cheerfully reject real art.
#
# The test that actually works: a real jersey image is unique to its team and
# edition, so any image whose hash appears more than once is a placeholder.
# Self-calibrating — it needs no hardcoded hash and keeps working if the CDN
# swaps the placeholder.

def fetch_hashes(jerseys, workers=6):
    """{image_hash: (sha256, bytes)} for every candidate. Failures are omitted."""
    import hashlib
    from concurrent.futures import ThreadPoolExecutor
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    import server   # curl_cffi Chrome impersonation

    def one(j):
        try:
            r = server._cdn_get(j["image_url"], timeout=20)
            if r.status_code != 200 or not r.content:
                return j["image_hash"], None
            return j["image_hash"], (hashlib.sha256(r.content).hexdigest(), len(r.content))
        except Exception:
            return j["image_hash"], None

    out = {}
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for key, val in ex.map(one, jerseys):
            if val:
                out[key] = val
    return out


def partition_real(jerseys, hashes):
    """(real, placeholder, unreachable) — split on hash uniqueness."""
    from collections import Counter
    counts = Counter(h for h, _ in hashes.values())
    real, placeholder, unreachable = [], [], []
    for j in jerseys:
        got = hashes.get(j["image_hash"])
        if not got:
            unreachable.append(j)
        elif counts[got[0]] > 1:
            placeholder.append(j)
        else:
            real.append(j)
    return real, placeholder, unreachable


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


def already_have():
    """Rows for this season+source already in the table."""
    if not DATABASE_URL:
        return 0
    conn = psycopg2.connect(DATABASE_URL); cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM jerseys WHERE year_range = %s AND source_slug = %s",
                (SEASON_STR, SOURCE))
    n = cur.fetchone()[0]
    cur.close(); conn.close()
    return n


def run():
    if args.skip_if_present:
        have = already_have()
        if have:
            print(f"{SEASON_STR}: already have {have} jersey(s) from {SOURCE} — nothing to do")
            return
    jerseys = build_jerseys()
    print(f"Built {len(jerseys)} candidate entries for {SEASON_STR}")

    if args.no_verify:
        print("⚠️  --no-verify: writing without checking the images exist")
        real = jerseys
    else:
        print(f"Verifying {len(jerseys)} images against the CDN…")
        hashes = fetch_hashes(jerseys)
        real, placeholder, unreachable = partition_real(jerseys, hashes)
        print(f"  real art:     {len(real)}")
        print(f"  placeholder:  {len(placeholder)}")
        print(f"  unreachable:  {len(unreachable)}")
        if not real:
            # Exit 1 so a pipeline or cron reports this as "nothing to do", not success.
            print(f"\n❌ {SEASON_STR} is not published yet — every image is the same "
                  f"placeholder. Nothing written. Re-run when the art lands.")
            sys.exit(0 if args.ok_if_unpublished else 1)
        if placeholder:
            ex = ", ".join(f"{j['team_abbr']}/{j['variant'][:3]}" for j in placeholder[:6])
            print(f"  skipping placeholders: {ex}{' …' if len(placeholder) > 6 else ''}")

    if args.dry_run:
        for j in real[:8]:
            print(f"  {j['image_hash']:30s}  {j['image_url']}")
        if len(real) > 8: print("  ...")
        print(f"(dry run — {len(real)} row(s) would be written)")
        return

    if not DATABASE_URL:
        print("❌ DATABASE_URL not set"); sys.exit(1)

    conn = psycopg2.connect(DATABASE_URL)
    n = upsert_jerseys(conn, real)
    conn.close()
    print(f"✅ {n} rows upserted into jerseys (source=lockervision, season={SEASON_STR})")


if __name__ == "__main__":
    run()
