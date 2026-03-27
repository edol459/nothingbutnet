"""
check_asap_vs_darko.py
Compare ASAP vs DARKO DPM rankings to identify where they diverge.
Run from project root: python backend/ingest/check_asap_vs_darko.py
"""
import os, psycopg2, psycopg2.extras
from dotenv import load_dotenv
load_dotenv()

conn = psycopg2.connect(os.getenv('DATABASE_URL'))
cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

SEASON      = '2025-26'
SEASON_TYPE = 'Regular Season'
TOP_N       = 30

cur.execute("""
    SELECT
        p.player_name,
        p.position_group                        AS pos,
        ps.min,
        pm.asap_score,
        ps.darko_dpm,
        pm.creator_score,
        pm.playmaker_score,
        pm.defender_score,
        pm.intangibles_score
    FROM player_metrics pm
    JOIN players      p  ON pm.player_id  = p.player_id
    JOIN player_seasons ps ON ps.player_id = pm.player_id
                          AND ps.season     = pm.season
                          AND ps.season_type= pm.season_type
    WHERE pm.season      = %s
      AND pm.season_type = %s
      AND ps.min        >= 1000
      AND ps.league      = 'NBA'
    ORDER BY pm.asap_score DESC NULLS LAST
    LIMIT %s
""", (SEASON, SEASON_TYPE, TOP_N))
asap_rows = cur.fetchall()

cur.execute("""
    SELECT
        p.player_name,
        p.position_group                        AS pos,
        ps.min,
        pm.asap_score,
        ps.darko_dpm,
        pm.creator_score,
        pm.playmaker_score,
        pm.defender_score,
        pm.intangibles_score
    FROM player_metrics pm
    JOIN players      p  ON pm.player_id  = p.player_id
    JOIN player_seasons ps ON ps.player_id = pm.player_id
                          AND ps.season     = pm.season
                          AND ps.season_type= pm.season_type
    WHERE pm.season      = %s
      AND pm.season_type = %s
      AND ps.min        >= 1000
      AND ps.league      = 'NBA'
      AND ps.darko_dpm  IS NOT NULL
    ORDER BY ps.darko_dpm DESC NULLS LAST
    LIMIT %s
""", (SEASON, SEASON_TYPE, TOP_N))
darko_rows = cur.fetchall()

def fmt(v, decimals=1):
    if v is None: return '  —  '
    return f'{float(v):.{decimals}f}'

def print_table(rows, rank_col, title):
    print(f'\n{"═"*90}')
    print(f'  {title}')
    print(f'{"═"*90}')
    print(f'  {"#":<4} {"Player":<26} {"POS":<5} {"MIN":<6} {"ASAP":<7} {"DARKO":<7} {"Scoring":<8} {"Playmaking":<11} {"Defense":<8} {"Intangibles"}')
    print(f'  {"─"*85}')
    for i, r in enumerate(rows, 1):
        print(
            f'  {i:<4} {r["player_name"]:<26} {r["pos"] or "?":<5} '
            f'{fmt(r["min"],0):<6} '
            f'{fmt(r["asap_score"]):<7} '
            f'{fmt(r["darko_dpm"],2):<7} '
            f'{fmt(r["creator_score"]):<8} '
            f'{fmt(r["playmaker_score"]):<11} '
            f'{fmt(r["defender_score"]):<8} '
            f'{fmt(r["intangibles_score"])}'
        )

print_table(asap_rows,  'asap_score',  f'TOP {TOP_N} BY ASAP SCORE — {SEASON}')
print_table(darko_rows, 'darko_dpm',   f'TOP {TOP_N} BY DARKO DPM — {SEASON}')

# ── Players who appear in DARKO top-N but not ASAP top-N ────────────────────
asap_names  = {r['player_name'] for r in asap_rows}
darko_names = {r['player_name'] for r in darko_rows}

in_darko_not_asap = [r for r in darko_rows if r['player_name'] not in asap_names]
in_asap_not_darko = [r for r in asap_rows  if r['player_name'] not in darko_names]

if in_darko_not_asap:
    print(f'\n{"─"*90}')
    print(f'  In DARKO top-{TOP_N} but NOT in ASAP top-{TOP_N}:')
    for r in in_darko_not_asap:
        print(f'    {r["player_name"]:<26} ({r["pos"]})  ASAP={fmt(r["asap_score"])}  DARKO={fmt(r["darko_dpm"],2)}  '
              f'Scoring={fmt(r["creator_score"])}  Playmaking={fmt(r["playmaker_score"])}  '
              f'Defense={fmt(r["defender_score"])}  Intangibles={fmt(r["intangibles_score"])}')

if in_asap_not_darko:
    print(f'\n{"─"*90}')
    print(f'  In ASAP top-{TOP_N} but NOT in DARKO top-{TOP_N}:')
    for r in in_asap_not_darko:
        print(f'    {r["player_name"]:<26} ({r["pos"]})  ASAP={fmt(r["asap_score"])}  DARKO={fmt(r["darko_dpm"],2)}  '
              f'Scoring={fmt(r["creator_score"])}  Playmaking={fmt(r["playmaker_score"])}  '
              f'Defense={fmt(r["defender_score"])}  Intangibles={fmt(r["intangibles_score"])}')

# ── Big divergences: players in both lists but far apart in rank ─────────────
print(f'\n{"─"*90}')
print('  BIGGEST RANK DIVERGENCES (in both lists, ranked differently):')
asap_rank  = {r['player_name']: i+1 for i, r in enumerate(asap_rows)}
darko_rank = {r['player_name']: i+1 for i, r in enumerate(darko_rows)}
both = [(name, asap_rank[name], darko_rank[name]) for name in asap_names & darko_names]
both.sort(key=lambda x: abs(x[1]-x[2]), reverse=True)
for name, ar, dr in both[:10]:
    diff = ar - dr
    arrow = f'ASAP #{ar} → DARKO #{dr}  ({"ASAP ranks +" if diff<0 else "DARKO ranks +"}{abs(diff)} higher)'
    print(f'    {name:<26} {arrow}')

cur.close()
conn.close()