"""
Report player_seasons coverage per season so you can spot which seasons the
historical ingest hasn't fully populated (and verify a backfill afterward).

For each (season, season_type) it prints:
  - rows:      total player_seasons rows
  - w/stats:   rows that actually have averages (gp > 0 AND pts IS NOT NULL)
  - empty:     placeholder rows (row exists but no stats)
A season with very few w/stats (⚠️) needs re-running:
  python backend/ingest/fetch_historical_seasons.py --start <s> --end <s>

Usage:
  python backend/ingest/check_season_coverage.py
  python backend/ingest/check_season_coverage.py --wnba
"""

import os, sys, argparse
from dotenv import load_dotenv
import psycopg2, psycopg2.extras

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("❌ DATABASE_URL not set"); sys.exit(1)

parser = argparse.ArgumentParser()
parser.add_argument("--wnba", action="store_true", help="Check wnba_player_seasons instead")
args = parser.parse_args()

TABLE = "wnba_player_seasons" if args.wnba else "player_seasons"
# A healthy regular season has ~450-550 players; playoffs ~200-250.
# Flag anything under this many populated rows as an incomplete batch.
THRESHOLD = 60

conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
cur.execute(f"""
    SELECT season, season_type,
           COUNT(*)                                                  AS rows,
           COUNT(*) FILTER (WHERE COALESCE(gp, 0) > 0
                              AND pts IS NOT NULL)                    AS with_stats
    FROM {TABLE}
    GROUP BY season, season_type
    ORDER BY season, season_type
""")
rows = cur.fetchall()
cur.close(); conn.close()

if not rows:
    print(f"(no rows in {TABLE})"); sys.exit(0)

print(f"\n{TABLE} coverage\n")
print(f"{'SEASON':10} {'TYPE':16} {'ROWS':>6} {'W/STATS':>8} {'EMPTY':>6}")
print("-" * 52)
gaps = []
for r in rows:
    total = r["rows"]; ws = r["with_stats"]; empty = total - ws
    flag = "  ⚠️" if ws < THRESHOLD else ""
    if flag:
        gaps.append(f"{r['season']} ({r['season_type']})")
    print(f"{r['season']:10} {r['season_type']:16} {total:6} {ws:8} {empty:6}{flag}")

print("-" * 52)
if gaps:
    print(f"\n⚠️  {len(gaps)} under-populated: " + ", ".join(gaps))
    print("   Re-run e.g.: python backend/ingest/fetch_historical_seasons.py "
          "--start 2001-02 --end 2002-03")
else:
    print("\n✅ Every season looks fully populated.")
