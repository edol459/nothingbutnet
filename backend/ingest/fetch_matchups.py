"""
fetch_matchups.py — Fetch opponent-adjusted defensive matchup data (Incremental)
=================================================================================
Computes matchup_def_fg_pct_adj per defender:
  For each defender × offensive player pairing:
    adj_delta = opp_season_fg_pct - matchup_fg_pct
  Weighted avg of adj_delta by possessions = matchup_def_fg_pct_adj
  Positive = held opponent below their season average (good defending)

Incremental behaviour:
  Skips the full fetch if player_matchups already has rows for this
  season/season_type that were updated today. Use --force to override.

Usage:
  python backend/ingest/fetch_matchups.py
  python backend/ingest/fetch_matchups.py --season 2025-26 --dry-run
  python backend/ingest/fetch_matchups.py --force
"""

import os, sys, time, argparse, math
from datetime import date
import pandas as pd
import psycopg2, psycopg2.extras
from dotenv import load_dotenv
load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')
DELAY        = 3.0

def try_import(name):
    try:
        import importlib
        mod = importlib.import_module("nba_api.stats.endpoints")
        return getattr(mod, name)
    except (ImportError, AttributeError):
        return None

LeagueSeasonMatchups = try_import("LeagueSeasonMatchups")

if not LeagueSeasonMatchups:
    print("ERROR: nba_api not installed. Run: pip install nba_api")
    sys.exit(1)

# ── Args ──────────────────────────────────────────────────────

parser = argparse.ArgumentParser()
parser.add_argument('--season',      default='2025-26')
parser.add_argument('--season-type', default='Regular Season')
parser.add_argument('--dry-run',     action='store_true')
parser.add_argument('--force',       action='store_true',
                    help='Re-fetch even if already updated today')
parser.add_argument('--min-poss',     type=int, default=50)
parser.add_argument('--min-opp-fga',  type=int, default=100)
parser.add_argument('--min-def-poss', type=int, default=500)
args = parser.parse_args()

SEASON       = args.season
SEASON_TYPE  = args.season_type
MIN_POSS     = args.min_poss
MIN_OPP_FGA  = args.min_opp_fga

print(f"Fetching matchup data — {SEASON} {SEASON_TYPE}")

# ── Incremental check ─────────────────────────────────────────
# Skip if we already ran today (matchup data is season-aggregate,
# re-fetching 142k rows daily is wasteful if nothing has changed).

if not args.dry_run and not args.force:
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur  = conn.cursor()
        cur.execute("""
            SELECT COUNT(*) FROM player_matchups
            WHERE season = %s AND season_type = %s
              AND updated_at::date = %s
        """, (SEASON, SEASON_TYPE, date.today()))
        count = cur.fetchone()[0]
        cur.close(); conn.close()
        if count > 0:
            print(f"  ✅ Already updated today ({count} pairings in DB) — skipping.")
            print(f"     Use --force to re-fetch anyway.")
            sys.exit(0)
    except Exception as e:
        print(f"  ⚠️  Could not check last update time: {e} — proceeding with fetch")

# ── Fetch helpers ─────────────────────────────────────────────

def fetch(label, fn, retries=3):
    print(f"  Fetching {label}...", end=" ", flush=True)
    for attempt in range(retries):
        try:
            time.sleep(DELAY * (attempt + 1))
            ep  = fn()
            dfs = ep.get_data_frames()
            if dfs and len(dfs[0]) > 0:
                print(f"✅ {len(dfs[0])} rows")
                return dfs[0]
            print("⚠️  empty")
            return pd.DataFrame()
        except Exception as e:
            if attempt < retries - 1:
                print(f"⚠️  retrying ({attempt+2}/{retries})...", end=" ", flush=True)
            else:
                print(f"❌ {e}")
    return pd.DataFrame()

def safe_float(val):
    if val is None: return None
    try:
        f = float(val)
        return None if math.isnan(f) else f
    except: return None

def safe_int(val):
    v = safe_float(val)
    if v is None: return None
    try: return int(v)
    except: return None

# ── 1. Fetch matchup data ─────────────────────────────────────

matchups = fetch("Matchup Defense (all pairings)",
    lambda: LeagueSeasonMatchups(
        season=SEASON,
        season_type_playoffs=SEASON_TYPE,
        per_mode_simple="Totals"
    ))

if matchups.empty:
    print("ERROR: No matchup data returned.")
    sys.exit(1)

print(f"  Columns: {list(matchups.columns)}")

# ── 2. Load season FG% for offensive players from DB ─────────
# Uses player_seasons instead of hitting the NBA API again —
# we already have fga and fg_pct there from fetch_season.py.

print(f"  Loading Season FG% from player_seasons...", end=" ", flush=True)
try:
    _conn = psycopg2.connect(DATABASE_URL)
    _cur  = _conn.cursor()
    _cur.execute("""
            SELECT player_id, fga, gp, fg_pct
            FROM player_seasons
            WHERE season = %s AND season_type = %s
            AND fga IS NOT NULL AND fg_pct IS NOT NULL AND gp IS NOT NULL
    """, (SEASON, SEASON_TYPE))
    _rows = _cur.fetchall()
    _cur.close(); _conn.close()
except Exception as e:
    print(f"ERROR: Could not load FG% from DB: {e}")
    sys.exit(1)

opp_fg_map = {}
for pid, fga, gp, fg_pct in _rows:
    # fga is per-game average — multiply by gp to get season total
    total_fga = (fga or 0) * (gp or 0)
    if pid and total_fga >= MIN_OPP_FGA and fg_pct is not None:
        opp_fg_map[int(pid)] = float(fg_pct)

# Build a name map from the matchup data itself for sanity-check output
name_map = {}

print(f"OK ({len(_rows)} players)")
print(f"  Built FG% map for {len(opp_fg_map)} offensive players (>={MIN_OPP_FGA} FGA)")

# ── 3. Find correct column names ──────────────────────────────

def find_col(df, candidates):
    for c in candidates:
        if c in df.columns: return c
    return None

def_id_col = find_col(matchups, ['DEF_PLAYER_ID', 'CLOSE_DEF_PERSON_ID', 'DEFENDER_PLAYER_ID'])
off_id_col = find_col(matchups, ['OFF_PLAYER_ID', 'PLAYER_ID', 'OFFENSIVE_PLAYER_ID'])
fga_col    = find_col(matchups, ['MATCHUP_FGA', 'FGA'])
fgm_col    = find_col(matchups, ['MATCHUP_FGM', 'FGM'])
fgpct_col  = find_col(matchups, ['MATCHUP_FG_PCT', 'FG_PCT'])
poss_col   = find_col(matchups, ['PARTIAL_POSS', 'PLAYER_POSS', 'POSS'])

print(f"\n  Column mapping:")
print(f"    defender_id:  {def_id_col}")
print(f"    off_player:   {off_id_col}")
print(f"    fga:          {fga_col}")
print(f"    fgm:          {fgm_col}")
print(f"    possessions:  {poss_col}")

if not all([def_id_col, off_id_col, fga_col, fgm_col]):
    print("ERROR: Could not identify required columns.")
    sys.exit(1)

# ── 4. Compute adj_delta per pairing ─────────────────────────

pairings = []
for _, row in matchups.iterrows():
    def_id = safe_int(row.get(def_id_col))
    off_id = safe_int(row.get(off_id_col))
    fga    = safe_float(row.get(fga_col)) or 0
    fgm    = safe_float(row.get(fgm_col)) or 0
    fg_pct = safe_float(row.get(fgpct_col))
    poss   = safe_float(row.get(poss_col)) if poss_col else fga

    if not def_id or not off_id: continue
    weight = poss or fga
    if weight < MIN_POSS: continue
    if fg_pct is None:
        fg_pct = fgm / fga if fga > 0 else None
    if fg_pct is None: continue

    opp_avg = opp_fg_map.get(off_id)
    if opp_avg is None: continue

    pairings.append({
        'defender_id':         def_id,
        'offensive_player_id': off_id,
        'fga':                 int(fga),
        'fgm':                 int(fgm),
        'fg_pct':              round(fg_pct, 4),
        'opp_season_fg_pct':   round(opp_avg, 4),
        'adj_delta':           round(opp_avg - fg_pct, 4),
        'possessions':         round(weight, 1),
    })

print(f"\n  {len(pairings)} qualifying pairings (≥{MIN_POSS} poss, opp ≥{MIN_OPP_FGA} FGA)")

# ── 5. Aggregate per defender ─────────────────────────────────

from collections import defaultdict
totals = defaultdict(lambda: {'wt_delta': 0.0, 'poss': 0.0, 'n': 0})
for p in pairings:
    d = totals[p['defender_id']]
    d['wt_delta'] += p['adj_delta'] * p['possessions']
    d['poss']     += p['possessions']
    d['n']        += 1

MIN_DEF_POSS = args.min_def_poss
defender_adj = {
    did: {
        'matchup_def_fg_pct_adj': round(t['wt_delta'] / t['poss'], 4),
        'matchup_poss':           round(t['poss'], 1),
        'matchup_n':              t['n'],
    }
    for did, t in totals.items()
    if t['poss'] >= MIN_DEF_POSS
}

print(f"  Aggregated to {len(defender_adj)} defenders")

# Sanity check — show top/bottom 15
sorted_adj = sorted(defender_adj.items(),
                    key=lambda x: x[1]['matchup_def_fg_pct_adj'], reverse=True)
# Build name_map from matchup data
name_map = {}
for _, row in matchups.iterrows():
    did = safe_int(row.get(def_id_col))
    if did and row.get('DEF_PLAYER_NAME'):
        name_map[did] = row['DEF_PLAYER_NAME']

print("\n  Top 15 (held opponents furthest below season avg):")
for did, d in sorted_adj[:15]:
    print(f"    {name_map.get(did, did):<26} adj={d['matchup_def_fg_pct_adj']:+.4f}  "
          f"poss={d['matchup_poss']:.0f}  n={d['matchup_n']}")
print("\n  Bottom 15:")
for did, d in sorted_adj[-15:]:
    print(f"    {name_map.get(did, did):<26} adj={d['matchup_def_fg_pct_adj']:+.4f}  "
          f"poss={d['matchup_poss']:.0f}  n={d['matchup_n']}")

players_of_interest = [
    'Kawhi Leonard', 'OG Anunoby', 'Bam Adebayo', 'Evan Mobley',
    'Jarrett Allen', 'Rudy Gobert', 'Victor Wembanyama',
    'Shai Gilgeous-Alexander', 'Jaylen Brown', 'Kevin Durant',
]
print("\n  Players of interest (all thresholds, raw data):")
raw_totals = defaultdict(lambda: {'wt_delta': 0.0, 'poss': 0.0, 'n': 0, 'name': ''})
for _, row in matchups.iterrows():
    did = safe_int(row.get(def_id_col))
    oid = safe_int(row.get(off_id_col))
    if not did or not oid: continue
    opp_avg = opp_fg_map.get(oid)
    if opp_avg is None: continue
    fg_pct = safe_float(row.get(fgpct_col))
    poss   = safe_float(row.get(poss_col)) or 0
    if poss < 5 or fg_pct is None: continue
    adj = opp_avg - fg_pct
    raw_totals[did]['wt_delta'] += adj * poss
    raw_totals[did]['poss']     += poss
    raw_totals[did]['n']        += 1
    raw_totals[did]['name']      = row.get('DEF_PLAYER_NAME', '')

for name in players_of_interest:
    match = [(did, t) for did, t in raw_totals.items()
             if t['name'] and name.lower() in t['name'].lower()]
    if match:
        did, t = match[0]
        adj = t['wt_delta'] / t['poss'] if t['poss'] > 0 else 0
        in_pool = did in defender_adj
        print(f"    {t['name']:<26} adj={adj:+.4f}  poss={t['poss']:.0f}  "
              f"n={t['n']}  {'✅ in pool' if in_pool else '❌ below threshold'}")
    else:
        print(f"    {name:<26} not found in matchup data")

if args.dry_run:
    print("\nDry run — not writing to DB.")
    sys.exit(0)

# ── 6. Write to DB ────────────────────────────────────────────

conn = psycopg2.connect(DATABASE_URL)
cur  = conn.cursor()

cur.execute("""
    CREATE TABLE IF NOT EXISTS player_matchups (
        id                   SERIAL PRIMARY KEY,
        defender_id          INTEGER NOT NULL,
        offensive_player_id  INTEGER NOT NULL,
        season               VARCHAR(10) NOT NULL,
        season_type          VARCHAR(30) NOT NULL,
        fga                  INTEGER,
        fgm                  INTEGER,
        fg_pct               REAL,
        opp_season_fg_pct    REAL,
        adj_delta            REAL,
        possessions          REAL,
        updated_at           TIMESTAMP DEFAULT NOW(),
        UNIQUE (defender_id, offensive_player_id, season, season_type)
    )
""")

print(f"\n  Writing {len(pairings)} pairings to player_matchups...")
for p in pairings:
    cur.execute("""
        INSERT INTO player_matchups
            (defender_id, offensive_player_id, season, season_type,
             fga, fgm, fg_pct, opp_season_fg_pct, adj_delta, possessions, updated_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        ON CONFLICT (defender_id, offensive_player_id, season, season_type)
        DO UPDATE SET
            fga=EXCLUDED.fga, fgm=EXCLUDED.fgm, fg_pct=EXCLUDED.fg_pct,
            opp_season_fg_pct=EXCLUDED.opp_season_fg_pct,
            adj_delta=EXCLUDED.adj_delta, possessions=EXCLUDED.possessions,
            updated_at=NOW()
    """, (p['defender_id'], p['offensive_player_id'], SEASON, SEASON_TYPE,
          p['fga'], p['fgm'], p['fg_pct'],
          p['opp_season_fg_pct'], p['adj_delta'], p['possessions']))

cur.execute("ALTER TABLE player_seasons ADD COLUMN IF NOT EXISTS matchup_def_fg_pct_adj REAL")
cur.execute("ALTER TABLE player_seasons ADD COLUMN IF NOT EXISTS matchup_poss REAL")

print(f"  Writing matchup_def_fg_pct_adj to {len(defender_adj)} player_seasons rows...")
updated = 0
for did, d in defender_adj.items():
    cur.execute("""
        UPDATE player_seasons
        SET matchup_def_fg_pct_adj = %s, matchup_poss = %s
        WHERE player_id = %s AND season = %s AND season_type = %s
    """, (d['matchup_def_fg_pct_adj'], d['matchup_poss'], did, SEASON, SEASON_TYPE))
    if cur.rowcount > 0: updated += 1

conn.commit()
cur.close()
conn.close()

print(f"\n  Done.")
print(f"    player_matchups rows: {len(pairings)}")
print(f"    player_seasons updated: {updated}")
print(f"\nNext steps:")
print(f"  1. Add matchup_def_fg_pct_adj to scoring_engine.py DEFENDER_EXTRAS_SCORE")
print(f"  2. Add to ALL_METRICS_POS in compute_metrics.py")
print(f"  3. Rerun compute_metrics.py")