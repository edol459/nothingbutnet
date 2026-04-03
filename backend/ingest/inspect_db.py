"""
ydkball — DB & Ingest Inspector
======================================
python backend/ingest/inspect_db.py

Prints:
  1. All columns in player_seasons
  2. All scripts in backend/ingest/
  3. Cross-reference: which scripts daily_update.py calls vs what exists
"""

import os
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
cur  = conn.cursor()

# ── 1. player_seasons columns ─────────────────────────────────
print("\n" + "="*60)
print("PLAYER_SEASONS COLUMNS")
print("="*60)

cur.execute("""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = 'player_seasons'
    ORDER BY ordinal_position
""")
cols = cur.fetchall()
for c in cols:
    print(f"  {c['column_name']:<35} {c['data_type']}")

print(f"\n  Total: {len(cols)} columns")

# ── 2. Null coverage for key external metrics ─────────────────
print("\n" + "="*60)
print("METRIC COVERAGE (2025-26 Regular Season)")
print("="*60)

metric_cols = [
    "darko_dpm", "darko_odpm", "darko_ddpm", "darko_box",
    "lebron", "o_lebron", "d_lebron", "war",
    "net_pts100", "o_net_pts100", "d_net_pts100",
]

for col in metric_cols:
    try:
        cur.execute(f"""
            SELECT
                COUNT(*) FILTER (WHERE {col} IS NOT NULL) AS populated,
                COUNT(*) AS total
            FROM player_seasons
            WHERE season = '2025-26' AND season_type = 'Regular Season'
        """)
        r = cur.fetchone()
        pct = (r['populated'] / r['total'] * 100) if r['total'] else 0
        bar = '█' * int(pct / 5) + '░' * (20 - int(pct / 5))
        print(f"  {col:<20} {bar} {r['populated']:>3}/{r['total']} ({pct:.0f}%)")
    except Exception as e:
        print(f"  {col:<20} ❌ column missing — {e}")

# ── 3. players table columns ──────────────────────────────────
print("\n" + "="*60)
print("PLAYERS TABLE COLUMNS")
print("="*60)

cur.execute("""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = 'players'
    ORDER BY ordinal_position
""")
for c in cur.fetchall():
    print(f"  {c['column_name']:<35} {c['data_type']}")

# ── 4. All tables in DB ───────────────────────────────────────
print("\n" + "="*60)
print("ALL TABLES IN DATABASE")
print("="*60)

cur.execute("""
    SELECT table_name FROM information_schema.tables
    WHERE table_schema = 'public'
    ORDER BY table_name
""")
for r in cur.fetchall():
    print(f"  {r['table_name']}")

cur.close()
conn.close()

# ── 5. Ingest scripts on disk ─────────────────────────────────
print("\n" + "="*60)
print("INGEST SCRIPTS ON DISK")
print("="*60)

ingest_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)))
scripts = sorted(f for f in os.listdir(ingest_dir) if f.endswith('.py'))
for s in scripts:
    print(f"  {s}")

# ── 6. Cross-ref with daily_update.py ────────────────────────
print("\n" + "="*60)
print("DAILY_UPDATE.PY — SCRIPT STATUS")
print("="*60)

DAILY_UPDATE_SCRIPTS = [
    "fetch_season.py",
    "fetch_new_pbp_stats.py",
    "fetch_matchups.py",
    "fetch_nba_stats.py",
    "fetch_darko.py",
    "fetch_lebron.py",
    "fetch_net_pts.py",
    "compute_metrics.py",
]

for s in DAILY_UPDATE_SCRIPTS:
    path = os.path.join(ingest_dir, s)
    status = "✅ exists" if os.path.exists(path) else "❌ MISSING"
    print(f"  {s:<35} {status}")

print("\n" + "="*60)
print("OTHER .PY FILES (not in daily_update.py)")
print("="*60)

daily_set = set(DAILY_UPDATE_SCRIPTS + ["inspect_db.py"])
extras = [s for s in scripts if s not in daily_set]
for s in extras:
    print(f"  {s}")

print("\n✅ Inspection complete\n")