"""
Show Curry's exact percentile rank for each shooting stat.
python backend/ingest/check_curry_pctiles.py
"""
import os
from dotenv import load_dotenv
load_dotenv()
import psycopg2, psycopg2.extras

conn = psycopg2.connect(os.getenv('DATABASE_URL'))
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

SEASON      = os.getenv('NBA_SEASON',      '2024-25')
SEASON_TYPE = os.getenv('NBA_SEASON_TYPE', 'Regular Season')

SHOOTING_STATS = [
    ('spotup_efg_pct',           'ps', 'cs_fga',       4.0),
    ('pull_up_efg_pct',          'ps', 'pull_up_fga',   1.5),
    ('all3_efg_vw',              'pm', None,            None),
    ('midrange_efg_vw',          'pm', None,            None),
    ('sq_fg_pct_above_expected', 'ps', None,            None),
]

print(f"\n── CURRY PERCENTILE IN EACH SHOOTING STAT ───────────────")
print(f"{'Stat':<28} {'CurryVal':>9} {'Pool':>5} {'CurryRank':>10} {'Pctile':>7}")
print("─" * 65)

for stat, tbl, gate_col, gate_min in SHOOTING_STATS:
    # Build pool query
    if gate_col and gate_min:
        gate_clause = f"AND ps.{gate_col}/NULLIF(ps.gp,0) >= {gate_min}"
    else:
        gate_clause = ""

    tbl_ref = "pm" if tbl == "pm" else "ps"

    # Get all values in pool (ordered)
    cur.execute(f"""
        SELECT p.player_name, {tbl_ref}.{stat} AS val, ps.gp,
               ps.cs_fga, ps.pull_up_fga
        FROM player_metrics pm
        JOIN player_seasons ps ON pm.player_id = ps.player_id
            AND pm.season = ps.season AND pm.season_type = ps.season_type
        JOIN players p ON pm.player_id = p.player_id
        WHERE pm.season = %s AND pm.season_type = %s
          AND {tbl_ref}.{stat} IS NOT NULL
          {gate_clause}
        ORDER BY {tbl_ref}.{stat} ASC
    """, (SEASON, SEASON_TYPE))

    rows = cur.fetchall()
    pool_size = len(rows)

    # Find Curry
    curry_val = None
    curry_rank = None
    for i, r in enumerate(rows):
        if 'Curry' in (r['player_name'] or '') and 'Stephen' in (r['player_name'] or ''):
            curry_val = float(r['val'])
            curry_rank = i + 1  # 1-indexed from bottom
            break

    if curry_val is not None:
        pctile = (curry_rank - 1) / max(pool_size - 1, 1) * 100
        print(f"  {stat:<26} {curry_val:>9.4f} {pool_size:>5} {curry_rank:>5}/{pool_size:<5} {pctile:>6.1f}p")
    else:
        print(f"  {stat:<26} {'NULL':>9} {pool_size:>5} {'N/A':>10} {'N/A':>7}")

# Also show top 5 in each stat so we know who's beating him
print(f"\n── TOP 10 IN EACH SHOOTING STAT ─────────────────────────")
for stat, tbl, gate_col, gate_min in SHOOTING_STATS:
    if gate_col and gate_min:
        gate_clause = f"AND ps.{gate_col}/NULLIF(ps.gp,0) >= {gate_min}"
    else:
        gate_clause = ""
    tbl_ref = "pm" if tbl == "pm" else "ps"

    cur.execute(f"""
        SELECT p.player_name, {tbl_ref}.{stat} AS val
        FROM player_metrics pm
        JOIN player_seasons ps ON pm.player_id = ps.player_id
            AND pm.season = ps.season AND pm.season_type = ps.season_type
        JOIN players p ON pm.player_id = p.player_id
        WHERE pm.season = %s AND pm.season_type = %s
          AND {tbl_ref}.{stat} IS NOT NULL
          {gate_clause}
        ORDER BY {tbl_ref}.{stat} DESC
        LIMIT 10
    """, (SEASON, SEASON_TYPE))
    rows = cur.fetchall()
    names = [f"{r['player_name']} ({float(r['val']):.3f})" for r in rows]
    print(f"\n  {stat}:")
    for i, n in enumerate(names, 1):
        marker = " ◄" if 'Curry' in n else ""
        print(f"    {i:>2}. {n}{marker}")

cur.close()
conn.close()