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


# ── Verification ──────────────────────────────────────────────────────────────
#
# This script had no existence check at all: it built every predicted URL and wrote
# them straight in. LockerVision answers **200 with a shared placeholder** for a
# season it hasn't published, so an early run would fill the picker with blank
# jerseys — and /api/jerseys/seasons is driven by that table.
#
# A real jersey is unique to its team and edition, so any image whose hash repeats
# is a placeholder. Fetched through the server's impersonating client because the
# CDN fallback-serves plain clients after a few requests, which would make real art
# look fake (see docs/cdn-akamai-bot-manager.md).

def fetch_hashes(jerseys, workers=6):
    """{image_hash: (sha256, bytes)} for every candidate. Failures are omitted."""
    import hashlib
    from concurrent.futures import ThreadPoolExecutor
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    import server

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
    print(f"Built {len(jerseys)} WNBA candidate entries for {SEASON_STR}")

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
            print(f"\n❌ WNBA {SEASON_STR} is not published yet — every image is the same "
                  f"placeholder. Nothing written. Re-run when the art lands.")
            sys.exit(0 if args.ok_if_unpublished else 1)
        if placeholder:
            ex = ", ".join(f"{j['team_abbr']}/{j['variant'][:3]}" for j in placeholder[:6])
            print(f"  skipping placeholders: {ex}{' …' if len(placeholder) > 6 else ''}")

    if args.dry_run:
        for j in real[:8]:
            print(f"  {j['image_hash']:35s}  {j['image_url']}")
        if len(real) > 8: print("  ...")
        print(f"(dry run — {len(real)} row(s) would be written)")
        return

    if not DATABASE_URL:
        print("❌ DATABASE_URL not set"); sys.exit(1)

    conn = psycopg2.connect(DATABASE_URL)
    n = upsert_jerseys(conn, real)
    conn.close()
    print(f"✅ {n} rows upserted (source=lockervision_wnba, season={SEASON_STR})")


if __name__ == "__main__":
    run()
