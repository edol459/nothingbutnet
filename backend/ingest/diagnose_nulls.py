"""
diagnose_nulls.py
=================
Connects to the DB and shows exactly which gate or stat is causing
passing_score, shooting_score, decision_making_score, and playmaker_score
to be null for a sample of players.

Run: python backend/ingest/diagnose_nulls.py
"""
import os, sys
from dotenv import load_dotenv
import psycopg2, psycopg2.extras

load_dotenv()
DATABASE_URL = os.getenv('DATABASE_URL')
SEASON       = os.getenv('NBA_SEASON', '2024-25')
SEASON_TYPE  = os.getenv('NBA_SEASON_TYPE', 'Regular Season')

def s(v, default=0):
    try: return float(v) if v is not None else default
    except: return default

conn = psycopg2.connect(DATABASE_URL)
cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

# ── 1. How many players have non-null vs null for each composite ──
print(f"\n{'='*65}")
print(f"NULL COUNTS — {SEASON} {SEASON_TYPE}")
print(f"{'='*65}")

cur.execute("""
    SELECT
        COUNT(*) FILTER (WHERE passing_score       IS NOT NULL) AS passing_ok,
        COUNT(*) FILTER (WHERE passing_score       IS NULL)     AS passing_null,
        COUNT(*) FILTER (WHERE shooting_score      IS NOT NULL) AS shooting_ok,
        COUNT(*) FILTER (WHERE shooting_score      IS NULL)     AS shooting_null,
        COUNT(*) FILTER (WHERE decision_making_score IS NOT NULL) AS dm_ok,
        COUNT(*) FILTER (WHERE decision_making_score IS NULL)   AS dm_null,
        COUNT(*) FILTER (WHERE playmaker_score     IS NOT NULL) AS playmaker_ok,
        COUNT(*) FILTER (WHERE playmaker_score     IS NULL)     AS playmaker_null,
        COUNT(*) FILTER (WHERE creator_score       IS NOT NULL) AS creator_ok,
        COUNT(*) FILTER (WHERE creator_score       IS NULL)     AS creator_null,
        COUNT(*) AS total
    FROM player_metrics pm
    WHERE pm.season = %s AND pm.season_type = %s
""", (SEASON, SEASON_TYPE))
r = cur.fetchone()
print(f"  Total players in player_metrics: {r['total']}")
print(f"  passing_score:         {r['passing_ok']} non-null  / {r['passing_null']} null")
print(f"  shooting_score:        {r['shooting_ok']} non-null  / {r['shooting_null']} null")
print(f"  decision_making_score: {r['dm_ok']} non-null  / {r['dm_null']} null")
print(f"  playmaker_score:       {r['playmaker_ok']} non-null  / {r['playmaker_null']} null")
print(f"  creator_score:         {r['creator_ok']} non-null  / {r['creator_null']} null")

# ── 2. Sample players with null passing_score — show gate inputs ──
print(f"\n{'='*65}")
print(f"SAMPLE — players with NULL passing_score (top 10 by AST)")
print(f"  Gate needs: ast/gp >= 2.0 AND potential_ast/gp >= 3.0")
print(f"  Stats need: pot_ast_per_tov, ast_pct, pass_quality_index (2 of 3)")
print(f"{'='*65}")

cur.execute("""
    SELECT p.player_name, p.position_group,
           ps.gp, ps.ast, ps.min,
           ps.potential_ast,
           pm.pot_ast_per_tov,
           ps.ast_pct,
           pm.pass_quality_index,
           pm.passing_score
    FROM player_metrics pm
    JOIN player_seasons ps ON pm.player_id = ps.player_id
        AND pm.season = ps.season AND pm.season_type = ps.season_type
    JOIN players p ON pm.player_id = p.player_id
    WHERE pm.season = %s AND pm.season_type = %s
      AND pm.passing_score IS NULL
    ORDER BY ps.ast DESC NULLS LAST
    LIMIT 10
""", (SEASON, SEASON_TYPE))

print(f"  {'Player':<22} {'Pos':<4} {'GP':>3} {'AST/g':>6} {'PotA/g':>7} {'Gate?':<8} "
      f"{'POT/TOV':>8} {'AST%':>6} {'PQ':>7} | why null")
print(f"  {'─'*100}")

for r in cur.fetchall():
    gp       = max(s(r['gp'],1), 1)
    ast_pg   = s(r['ast']) / gp
    pota_pg  = s(r['potential_ast']) / gp
    gate_ok  = ast_pg >= 2.0 and pota_pg >= 3.0
    gate_str = 'PASS' if gate_ok else 'FAIL'

    pot  = r['pot_ast_per_tov']
    apct = r['ast_pct']
    pq   = r['pass_quality_index']
    non_null = sum(1 for v in [pot, apct, pq] if v is not None)

    if not gate_ok:
        reason = f"gate fail: ast/g={ast_pg:.1f} potA/g={pota_pg:.1f}"
    elif non_null < 2:
        present = [n for n,v in [('pot_ast_per_tov',pot),('ast_pct',apct),('pass_quality_index',pq)] if v is not None]
        missing = [n for n,v in [('pot_ast_per_tov',pot),('ast_pct',apct),('pass_quality_index',pq)] if v is None]
        reason = f"only {non_null} stat(s) non-null: missing {', '.join(missing)}"
    else:
        reason = f"unknown ({non_null} stats present but score=null)"

    print(f"  {r['player_name']:<22} {r['position_group'] or '':<4} "
          f"{int(gp):>3} {ast_pg:>6.1f} {pota_pg:>7.1f} {gate_str:<8} "
          f"{pot or 0:>8.2f} {apct or 0:>6.3f} {pq or 0:>7.3f} | {reason}")

# ── 3. Sample players with null shooting_score ──
print(f"\n{'='*65}")
print(f"SAMPLE — players with NULL shooting_score (top 10 by FGA)")
print(f"  Gate needs: fga/gp >= 3.0")
print(f"  Stats need: spotup_efg_pct, all3_efg_vw, midrange_efg_vw, sq_fg_pct_above_expected (2 of 4)")
print(f"{'='*65}")

cur.execute("""
    SELECT p.player_name, p.position_group,
           ps.gp, ps.fga, ps.cs_efg_pct,
           ps.sq_fg_pct_above_expected,
           pm.all3_efg_vw, pm.midrange_efg_vw,
           pm.shooting_score
    FROM player_metrics pm
    JOIN player_seasons ps ON pm.player_id = ps.player_id
        AND pm.season = ps.season AND pm.season_type = ps.season_type
    JOIN players p ON pm.player_id = p.player_id
    WHERE pm.season = %s AND pm.season_type = %s
      AND pm.shooting_score IS NULL
    ORDER BY ps.fga DESC NULLS LAST
    LIMIT 10
""", (SEASON, SEASON_TYPE))

print(f"  {'Player':<22} {'Pos':<4} {'GP':>3} {'FGA/g':>6} {'Gate?':<6} "
      f"{'spotup':>8} {'all3_vw':>8} {'mid_vw':>8} {'sq_fg+':>8} {'non-null':>9} | why")
print(f"  {'─'*110}")

for r in cur.fetchall():
    gp      = max(s(r['gp'],1), 1)
    fga_pg  = s(r['fga']) / gp
    gate_ok = fga_pg >= 3.0
    gate_str = 'PASS' if gate_ok else 'FAIL'

    spotup = r['cs_efg_pct']       # note: api uses cs_efg_pct, db stores as spotup_efg_pct
    a3vw   = r['all3_efg_vw']
    midvw  = r['midrange_efg_vw']
    sqfg   = r['sq_fg_pct_above_expected']
    non_null = sum(1 for v in [spotup, a3vw, midvw, sqfg] if v is not None)

    if not gate_ok:
        reason = f"gate fail: fga/g={fga_pg:.1f}"
    elif non_null < 2:
        missing = [n for n,v in [('spotup',spotup),('all3_vw',a3vw),('mid_vw',midvw),('sq_fg',sqfg)] if v is None]
        reason = f"only {non_null} non-null: missing {', '.join(missing)}"
    else:
        reason = f"unknown ({non_null} stats present)"

    print(f"  {r['player_name']:<22} {r['position_group'] or '':<4} "
          f"{int(gp):>3} {fga_pg:>6.1f} {gate_str:<6} "
          f"{spotup or 0:>8.3f} {a3vw or 0:>8.3f} {midvw or 0:>8.3f} {sqfg or 0:>8.3f} "
          f"{non_null:>9} | {reason}")

# ── 3b. Verify per-game vs total conventions ──
print(f"\n{'='*65}")
print("CONVENTION CHECK — ast and fga should be per-game averages")
print("(Jokic should show ~9 AST, ~14 FGA)")
print(f"{'='*65}")
cur.execute(
    "SELECT p.player_name, ps.gp, ps.ast, ps.fga, ps.pts,"
    "       ps.potential_ast, ps.drives, ps.touches"
    " FROM player_seasons ps JOIN players p ON ps.player_id = p.player_id"
    " WHERE ps.season = %s AND ps.season_type = %s"
    " AND p.player_name ILIKE %s"
    " LIMIT 3",
    (SEASON, SEASON_TYPE, 'Nikola Jok%')
)
for r in cur.fetchall():
    gp = max(s(r['gp']), 1)
    print(f"  {r['player_name']}: gp={r['gp']} ast={r['ast']} fga={r['fga']} pts={r['pts']}")
    print(f"    potential_ast={r['potential_ast']} drives={r['drives']} touches={r['touches']}")
    print(f"    ast is per-game avg: {'YES' if s(r['ast']) > 1 else 'NO — looks like total or truncated'}")
    print(f"    fga is per-game avg: {'YES' if s(r['fga']) > 5 else 'NO — looks like total or very small'}")
    print(f"    potential_ast/gp = {s(r['potential_ast'])/gp:.1f}  (Jokic should be ~12-15)")

# Also spot-check Jalen Brunson and Ivica Zubac
print()
cur.execute(
    "SELECT p.player_name, ps.gp, ps.ast, ps.fga, ps.pts, ps.min,"
    "       ps.potential_ast, ps.drives, ps.touches"
    " FROM player_seasons ps JOIN players p ON ps.player_id = p.player_id"
    " WHERE ps.season = %s AND ps.season_type = %s"
    " AND p.player_name IN ('Jalen Brunson', 'Ivica Zubac', 'Luke Kennard', 'Peyton Watson')"
    " ORDER BY p.player_name",
    (SEASON, SEASON_TYPE)
)
for r in cur.fetchall():
    gp = max(s(r['gp']), 1)
    print(f"  {r['player_name']}: gp={r['gp']} min={r['min']} ast={r['ast']} fga={r['fga']}")
    print(f"    pot_ast={r['potential_ast']} pot_ast/gp={s(r['potential_ast'])/gp:.1f}"
          f"  drives={r['drives']} drives/gp={s(r['drives'])/gp:.1f}")

# ── 4. Check if key stats exist in player_seasons at all ──
print(f"\n{'='*65}")
print(f"STAT AVAILABILITY — how many players have each key stat non-null")
print(f"{'='*65}")

cur.execute("""
    SELECT
        COUNT(*) FILTER (WHERE ps.potential_ast IS NOT NULL)         AS has_potential_ast,
        COUNT(*) FILTER (WHERE ps.ast_pct       IS NOT NULL)         AS has_ast_pct,
        COUNT(*) FILTER (WHERE pm.pot_ast_per_tov IS NOT NULL)       AS has_pot_ast_per_tov,
        COUNT(*) FILTER (WHERE pm.pass_quality_index IS NOT NULL)    AS has_pass_quality_index,
        COUNT(*) FILTER (WHERE ps.cs_efg_pct    IS NOT NULL)         AS has_spotup,
        COUNT(*) FILTER (WHERE pm.all3_efg_vw   IS NOT NULL)         AS has_all3_vw,
        COUNT(*) FILTER (WHERE pm.midrange_efg_vw IS NOT NULL)       AS has_mid_vw,
        COUNT(*) FILTER (WHERE ps.sq_fg_pct_above_expected IS NOT NULL) AS has_sq_fg,
        COUNT(*) FILTER (WHERE ps.drives        IS NOT NULL)         AS has_drives,
        COUNT(*) FILTER (WHERE ps.touches       IS NOT NULL)         AS has_touches,
        COUNT(*) FILTER (WHERE ps.pnr_bh_fga    IS NOT NULL)         AS has_pnr_bh_fga,
        COUNT(*) FILTER (WHERE pm.lost_ball_tov_pg IS NOT NULL)      AS has_lost_ball,
        COUNT(*) FILTER (WHERE ps.pnr_bh_ppp    IS NOT NULL)         AS has_pnr_bh_ppp,
        COUNT(*) AS total
    FROM player_metrics pm
    JOIN player_seasons ps ON pm.player_id = ps.player_id
        AND pm.season = ps.season AND pm.season_type = ps.season_type
    WHERE pm.season = %s AND pm.season_type = %s
""", (SEASON, SEASON_TYPE))

r = cur.fetchone()
total = r['total']
stats = [
    ('potential_ast',       r['has_potential_ast']),
    ('ast_pct',             r['has_ast_pct']),
    ('pot_ast_per_tov',     r['has_pot_ast_per_tov']),
    ('pass_quality_index',  r['has_pass_quality_index']),
    ('spotup_efg_pct',      r['has_spotup']),
    ('all3_efg_vw',         r['has_all3_vw']),
    ('midrange_efg_vw',     r['has_mid_vw']),
    ('sq_fg_pct_above_exp', r['has_sq_fg']),
    ('drives',              r['has_drives']),
    ('touches',             r['has_touches']),
    ('pnr_bh_fga',          r['has_pnr_bh_fga']),
    ('lost_ball_tov_pg',    r['has_lost_ball']),
    ('pnr_bh_ppp (in pm)',  r['has_pnr_bh_ppp']),
]
print(f"  {'Stat':<28} {'non-null':>9} / {total}  {'pct':>6}")
print(f"  {'─'*55}")
for name, cnt in stats:
    pct = (cnt/total*100) if total else 0
    flag = ' ← PROBLEM' if pct < 50 else ''
    print(f"  {name:<28} {cnt:>9} / {total}  {pct:>5.1f}%{flag}")

cur.close()
conn.close()

# ── 5. Deep dive on Zubac shooting_score ──
print(f"\n{'='*65}")
print("DEEP DIVE — why does Ivica Zubac have null shooting_score?")
print(f"{'='*65}")
cur.execute("""
    SELECT p.player_name, ps.gp, ps.fga, ps.cs_efg_pct,
           ps.sq_fg_pct_above_expected,
           pm.all3_efg_vw, pm.midrange_efg_vw,
           pm.shooting_score
    FROM player_metrics pm
    JOIN player_seasons ps ON pm.player_id = ps.player_id
        AND pm.season = ps.season AND pm.season_type = ps.season_type
    JOIN players p ON pm.player_id = p.player_id
    WHERE pm.season = %s AND pm.season_type = %s
      AND p.player_name IN ('Ivica Zubac', 'Stephen Curry', 'Jalen Brunson')
""", (SEASON, SEASON_TYPE))
for r in cur.fetchall():
    gp = max(s(r['gp']), 1)
    print(f"  {r['player_name']}: fga={r['fga']:.2f} gate={'PASS' if s(r['fga'])>=3 else 'FAIL'}")
    print(f"    cs_efg_pct={r['cs_efg_pct']}  sq_fg_pct={r['sq_fg_pct_above_expected']}")
    print(f"    all3_efg_vw={r['all3_efg_vw']}  midrange_efg_vw={r['midrange_efg_vw']}")
    non_null = sum(1 for v in [r['cs_efg_pct'], r['sq_fg_pct_above_expected'],
                                r['all3_efg_vw'], r['midrange_efg_vw']] if v is not None)
    print(f"    non-null stats: {non_null}/4  shooting_score={r['shooting_score']}")
    print()

# ── 6. decision_making_score — why only 151? ──
print(f"\n{'='*65}")
print("DECISION_MAKING gate check — sample of null players with high drives/touches")
print(f"  Gate: drives/gp >= 4.0 AND touches/gp >= 40.0")
print(f"{'='*65}")
cur.execute("""
    SELECT p.player_name, ps.gp, ps.drives, ps.touches,
           pm.lost_ball_tov_pg, ps.pnr_bh_ppp,
           pm.decision_making_score
    FROM player_metrics pm
    JOIN player_seasons ps ON pm.player_id = ps.player_id
        AND pm.season = ps.season AND pm.season_type = ps.season_type
    JOIN players p ON pm.player_id = p.player_id
    WHERE pm.season = %s AND pm.season_type = %s
      AND pm.decision_making_score IS NULL
    ORDER BY (ps.drives::float / NULLIF(ps.gp,0)) DESC NULLS LAST
    LIMIT 10
""", (SEASON, SEASON_TYPE))
print(f"  {'Player':<22} {'GP':>3} {'drv/g':>6} {'tch/g':>6} {'gate':>6} {'lost_ball':>10} {'pnr_bh':>8} | reason")
print(f"  {'─'*85}")
for r in cur.fetchall():
    gp = max(s(r['gp']), 1)
    drv_pg = s(r['drives']) / gp
    tch_pg = s(r['touches']) / gp
    gate = drv_pg >= 4.0 and tch_pg >= 40.0
    lb = r['lost_ball_tov_pg']
    pnr = r['pnr_bh_ppp']
    non_null = sum(1 for v in [lb, pnr] if v is not None)
    if not gate:
        reason = f"gate fail: drv/g={drv_pg:.1f} tch/g={tch_pg:.1f}"
    else:
        reason = f"gate ok, {non_null} stats (lb={'Y' if lb else 'N'} pnr={'Y' if pnr else 'N'})"
    print(f"  {r['player_name']:<22} {int(gp):>3} {drv_pg:>6.1f} {tch_pg:>6.1f} "
          f"{'PASS' if gate else 'FAIL':>6} {lb or 0:>10.3f} {pnr or 0:>8.3f} | {reason}")