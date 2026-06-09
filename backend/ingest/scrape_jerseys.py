"""
Scrape jersey images and metadata from bballjerseys.com.

Each team page is a Wix Pro Gallery. Images have alt text like:
  "Knicks 01_New York Knicks 1970-1971 Jersey"
  "Lakers 03_Los Angeles Lakers 1999-2004 Jersey Alternate"

Outputs: jerseys table in the DB + jerseys.json for inspection.

Usage:
  python backend/ingest/scrape_jerseys.py               # all NBA teams
  python backend/ingest/scrape_jerseys.py --teams knicks lakers bulls
  python backend/ingest/scrape_jerseys.py --include-aba  # also scrape ABA pages
"""

import os, sys, re, json, time, argparse
import urllib.request, urllib.error
from dotenv import load_dotenv
import psycopg2

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

parser = argparse.ArgumentParser()
parser.add_argument("--teams",       nargs="*", help="Specific team slugs to scrape")
parser.add_argument("--include-aba", action="store_true")
parser.add_argument("--dry-run",     action="store_true", help="Print results, don't write to DB")
parser.add_argument("--delay",       type=float, default=2.0)
args = parser.parse_args()

NBA_TEAMS = [
    "hawks", "celtics", "nets", "hornets", "bulls", "cavaliers",
    "mavericks", "nuggets", "pistons", "warriors", "rockets", "pacers",
    "clippers", "lakers", "grizzlies", "heat", "bucks", "timberwolves",
    "pelicans", "knicks", "thunder", "magic", "sixers", "suns",
    "blazers", "kings", "spurs", "raptors", "jazz", "wizards",
]

ABA_TEAMS = [
    "sonics",  # historical
    "aba-allstar", "aba-colonels", "aba-condors", "aba-floridians",
    "aba-sails", "aba-sounds", "aba-spirits", "aba-squires", "aba-stars",
    "defunct",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
}

def fetch_page(slug):
    url = f"https://www.bballjerseys.com/{slug}"
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        r = urllib.request.urlopen(req, timeout=20)
        return r.read().decode("utf-8", errors="ignore"), url
    except urllib.error.HTTPError as e:
        print(f"  ❌ HTTP {e.code} — {url}")
        return None, url
    except Exception as e:
        print(f"  ❌ {type(e).__name__}: {e}")
        return None, url

def extract_jerseys(html, slug):
    """Extract jersey data from a Wix Pro Gallery page.

    Two gallery formats exist on this site:
    - Standard: hash and alt text appear close together in HTML
    - Slug format (Celtics, Thunder): data-id contains jersey slug, images
      are in the same document order
    """
    jerseys = []
    seen_hashes = set()

    # ── Format 1: hash → alt text (most teams) ──────────────────
    pairs = re.findall(
        r'(31d308_[a-f0-9]+)~mv2\.png[^>]{0,800}?alt="([^"]{5,200})"',
        html
    )
    for h, alt in pairs:
        if h in seen_hashes:
            continue
        parsed = parse_alt(alt, slug)
        if parsed:
            parsed["image_hash"] = h
            parsed["image_url"] = f"https://static.wixstatic.com/media/{h}~mv2.png"
            parsed["source_slug"] = slug
            jerseys.append(parsed)
            seen_hashes.add(h)

    # ── Format 2: data-id slugs + in-order hashes (Celtics, Thunder) ──
    # Always compute format-2 results; use them if they beat format-1
    data_ids = re.findall(r'data-id="([a-z0-9\-]+_\d+)"', html)
    hashes_f2 = list(dict.fromkeys(re.findall(r'(31d308_[a-f0-9]+)~mv2\.png', html)))
    seen_ids = set()
    unique_ids = []
    for d in data_ids:
        if d not in seen_ids:
            seen_ids.add(d)
            unique_ids.append(d)

    jerseys_f2 = []
    seen_hashes_f2 = set()
    for data_id, h in zip(unique_ids, hashes_f2):
        if h in seen_hashes_f2:
            continue
        parsed = parse_slug(data_id, slug)
        if parsed:
            parsed["image_hash"] = h
            parsed["image_url"] = f"https://static.wixstatic.com/media/{h}~mv2.png"
            parsed["source_slug"] = slug
            jerseys_f2.append(parsed)
            seen_hashes_f2.add(h)

    if len(jerseys_f2) > len(jerseys):
        jerseys = jerseys_f2

    return jerseys

def _clean_label(s):
    """Strip noise: trailing .png, leading single-letter/code prefixes."""
    s = re.sub(r'\.png\s*$', '', s).strip()
    # Strip leading sort codes like "A2 ", "H ", "M ", "C ", "B1 ", "B2 "
    s = re.sub(r'^[A-Z]\d?\s+', '', s).strip()
    return s

def parse_alt(alt, slug):
    """
    Parse alt text like:
      "Knicks 01_New York Knicks 1970-1971 Jersey"
      "Lakers 03_Los Angeles Lakers 1999-2004 Jersey Alternate"
      "H San Diego Rockets 1967-1968 Jersey"
      "C Charlotte Hornets 1990-1997 .png"
    """
    # Strip the team code prefix (e.g. "Knicks 01_")
    cleaned = re.sub(r'^[A-Za-z\s\-]+\d+_', '', alt).strip()
    cleaned = _clean_label(cleaned)

    # Extract year range like 1970-1971 or 1995-B 1996-1997 → take last YYYY-YYYY
    year_match = re.search(r'(\d{4})-(\d{4})', cleaned)
    if not year_match:
        return None

    year_range = year_match.group(0)
    year_start = int(year_match.group(1))
    team_name  = _clean_label(cleaned[:year_match.start()].strip())
    rest       = cleaned[year_match.end():].strip()
    rest       = _clean_label(rest)

    # "Jersey" required — some alt texts are just player names
    if "Jersey" not in rest and "Jersey" not in cleaned and "jersey" not in alt.lower():
        return None

    variant_match = re.search(r'[Jj]ersey\s*(.*)', rest)
    variant = _clean_label(variant_match.group(1)) if variant_match else ""

    label = f"{team_name} {year_range}"
    if variant:
        label += f" {variant}"

    return {
        "team_name": team_name,
        "team_slug": slug,
        "year_range": year_range,
        "year_start": year_start,
        "label": label,
        "variant": variant or None,
    }

def parse_slug(data_id, slug):
    """
    Parse data-id slugs like:
      "boston-celtics-1946-1947-home-and-road-jersey_0"
      "oklahoma-city-thunder-2017-2019-association-jersey_3"
    """
    # Strip trailing _N index
    name_part = re.sub(r'_\d+$', '', data_id)
    # Extract year range
    year_match = re.search(r'(\d{4})-(\d{4})', name_part)
    if not year_match:
        return None
    year_range = year_match.group(0)
    year_start = int(year_match.group(1))
    # Team name: everything before the years, dashes → title case
    team_slug_part = name_part[:year_match.start()].strip('-')
    team_name = team_slug_part.replace('-', ' ').title()
    # Variant: everything after the years, before "jersey"
    after_years = name_part[year_match.end():].strip('-')
    variant = re.sub(r'-?jersey.*$', '', after_years).replace('-', ' ').strip().title()

    label = f"{team_name} {year_range}"
    if variant:
        label += f" {variant}"

    return {
        "team_name": team_name,
        "team_slug": slug,
        "year_range": year_range,
        "year_start": year_start,
        "label": label,
        "variant": variant or None,
    }

def init_db(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS jerseys (
            id          SERIAL PRIMARY KEY,
            team_slug   TEXT NOT NULL,
            team_name   TEXT NOT NULL,
            year_range  TEXT,
            year_start  INTEGER,
            label       TEXT NOT NULL,
            variant     TEXT,
            image_hash  TEXT UNIQUE NOT NULL,
            image_url   TEXT NOT NULL,
            source_slug TEXT,
            created_at  TIMESTAMP DEFAULT NOW()
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_jerseys_team_slug ON jerseys(team_slug)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_jerseys_year_start ON jerseys(year_start)")
    conn.commit()
    cur.close()
    print("✅ jerseys table ready")

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
    slugs = args.teams if args.teams else NBA_TEAMS
    if args.include_aba:
        slugs = slugs + ABA_TEAMS

    all_jerseys = []
    conn = None
    if not args.dry_run:
        if not DATABASE_URL:
            print("❌ DATABASE_URL not set"); sys.exit(1)
        conn = psycopg2.connect(DATABASE_URL)
        init_db(conn)

    print(f"\n🏀 Scraping {len(slugs)} pages from bballjerseys.com")
    for i, slug in enumerate(slugs, 1):
        print(f"\n[{i}/{len(slugs)}] /{slug}")
        html, url = fetch_page(slug)
        if not html:
            continue
        jerseys = extract_jerseys(html, slug)
        print(f"  Found {len(jerseys)} jerseys")
        for j in jerseys[:3]:
            print(f"    {j['label']}")
        if jerseys:
            all_jerseys.extend(jerseys)
            if not args.dry_run and conn:
                n = upsert_jerseys(conn, jerseys)
                print(f"  ✅ {n} rows upserted")
        if i < len(slugs):
            time.sleep(args.delay)

    if conn:
        conn.close()

    # Save JSON for inspection
    out_path = "backend/ingest/data/jerseys.json"
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(all_jerseys, f, indent=2)
    print(f"\n✅ Done. {len(all_jerseys)} total jerseys.")
    print(f"   JSON saved to {out_path}")

if __name__ == "__main__":
    run()
