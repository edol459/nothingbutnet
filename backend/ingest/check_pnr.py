"""
Investigate: Scottie Barnes defense, Jalen Johnson playmaking, Chet playmaking
Run from project root.
"""
import os, json, math, psycopg2, psycopg2.extras
from dotenv import load_dotenv; load_dotenv()

with open('backend/ingest/data/player_percentiles_2025_26.json') as f:
    raw = json.load(f)['percentiles']
with open('backend/ingest/data/win_correlations_2025_26.json') as f:
    wdata = json.load(f)
sw = wdata['subcomp_weights']

conn = psycopg2.connect(os.getenv('DATABASE_URL'))
cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
cur.execute("""
    SELECT ps.player_id, p.player_name, p.position_group,
           ps.gp, ps.min,
           -- playmaking raw
           ps.ast, ps.potential_ast, ps.touches, ps.drives,
           ps.bad_pass_tov, ps.passes_made, ps.ast_pts_created,
           ps.ft_ast, ps.pnr_bh_poss,
           -- defense raw
           ps.stl, ps.blk, ps.deflections, ps.contested_shots,
           ps.def_ws, ps.leverage_defense, ps.matchup_def_fg_pct_adj,
           ps.def_rim_fga,
           -- scores
           pm.playmaker_score, pm.passing_score,
           pm.creation_score, pm.decision_making_score,
           pm.defender_score, pm.perimeter_def_score,
           pm.interior_def_score, pm.defender_extras_score,
           pm.creator_score, pm.intangibles_score, pm.asap_score,
           -- computed def stats
           pm.def_delta_3pt, pm.def_disruption_rate,
           pm.rim_protection_score, pm.def_delta_overall,
           pm.def_delta_2pt
    FROM player_seasons ps
    JOIN players p ON ps.player_id = p.player_id
    JOIN player_metrics pm ON pm.player_id = ps.player_id
                           AND pm.season = ps.season AND pm.season_type = ps.season_type
    WHERE ps.season = '2025-26' AND ps.season_type = 'Regular Season'
      AND p.player_name IN (
        'Scottie Barnes', 'Jalen Johnson', 'Chet Holmgren',
        'Kawhi Leonard', 'OG Anunoby', 'Rudy Gobert'
      ) AND ps.league = 'NBA'
    ORDER BY pm.asap_score DESC NULLS LAST
""")
rows = cur.fetchall()

def s(v):
    if v is None: return None
    try:
        f = float(v)
        return None if math.isnan(f) else f
    except: return None

def get_pctile(pid, stat, pool='lg'):
    pid_s = str(pid)
    suffix = '__pos' if pool == 'pos' else ''
    pmap = raw.get(stat+suffix) or raw.get(stat)
    if not pmap: return None
    return pmap.get(pid_s)

PM_STATS = [
    ('pot_ast_per_tov',    'pos', 'passing_score'),
    ('ast_pct',            'pos', 'passing_score'),
    ('pass_quality_index', 'pos', 'passing_score'),
    ('leverage_creation',  'lg',  'creation_score'),
    ('ast_pts_created_pg', 'lg',  'creation_score'),
    ('ft_ast_per75',       'lg',  'creation_score'),
    ('lost_ball_tov_pg',   'lg',  'decision_making_score'),
    ('pnr_bh_ppp',         'lg',  'decision_making_score'),
]

DEF_STATS = [
    ('def_delta_3pt',          'pos', 'perimeter_def_score'),
    ('def_disruption_rate',    'pos', 'perimeter_def_score'),
    ('contested_shots',        'pos', 'perimeter_def_score'),
    ('stl',                    'pos', 'perimeter_def_score'),
    ('def_spotup_ppp',         'pos', 'perimeter_def_score'),
    ('rim_protection_score',   'pos', 'interior_def_score'),
    ('dreb_pct',               'pos', 'interior_def_score'),
    ('blk',                    'pos', 'interior_def_score'),
    ('def_post_ppp',           'pos', 'interior_def_score'),
    ('def_pnr_roll_ppp',       'pos', 'interior_def_score'),
    ('leverage_defense',       'pos', 'defender_extras_score'),
    ('def_ws',                 'pos', 'defender_extras_score'),
    ('matchup_def_fg_pct_adj', 'pos', 'defender_extras_score'),
    # display only
    ('def_delta_overall',      'pos', '(display only)'),
    ('def_delta_2pt',          'pos', '(display only)'),
]

for r in rows:
    gp  = max(r['gp'] or 1, 1)
    pid = str(r['player_id'])

    print(f"\n{'='*75}")
    print(f"  {r['player_name']} ({r['position_group']}) — {r['min']:.0f} min")
    print(f"  ASAP={r['asap_score']}  scoring={r['creator_score']}  "
          f"playmaking={r['playmaker_score']}  defense={r['defender_score']}  "
          f"intangibles={r['intangibles_score']}")

    print(f"\n  RAW PLAYMAKING:")
    print(f"    ast/g={s(r['ast']) or 0:.1f}  pot_ast/g={s(r['potential_ast'])/gp if r['potential_ast'] else 0:.1f}  "
          f"touches/g={s(r['touches'])/gp if r['touches'] else 0:.1f}  "
          f"drives/g={s(r['drives'])/gp if r['drives'] else 0:.1f}  "
          f"passes/g={s(r['passes_made'])/gp if r['passes_made'] else 0:.1f}")
    print(f"    passing={r['passing_score']}  creation={r['creation_score']}  bh={r['decision_making_score']}")
    print()
    for stat, pool, subcomp in PM_STATS:
        v = get_pctile(pid, stat, pool)
        w = sw.get(subcomp, {}).get(stat)
        null_flag = ' ← NULL' if v is None else ''
        low_flag  = ' ← LOW'  if v is not None and v < 30 else ''
        high_flag = ' ← HIGH' if v is not None and v > 85 else ''
        print(f"    {stat:<28} {str(round(v,1)) if v is not None else 'NULL':>7}p  "
              f"w={f'{w:.4f}' if w else '  —  ':>8}  [{subcomp}]{null_flag}{low_flag}{high_flag}")

    print(f"\n  RAW DEFENSE:")
    print(f"    stl/g={s(r['stl']) or 0:.2f}  blk/g={s(r['blk']) or 0:.2f}  "
          f"defl/g={s(r['deflections'])/gp if r['deflections'] else 0:.2f}  "
          f"cont/g={s(r['contested_shots'])/gp if r['contested_shots'] else 0:.2f}  "
          f"rim_fga/g={s(r['def_rim_fga'])/gp if r['def_rim_fga'] else 0:.2f}")
    print(f"    def_ws={s(r['def_ws']) or 0:.3f}  lev_def={s(r['leverage_defense']) or 0:.3f}  "
          f"matchup_adj={s(r['matchup_def_fg_pct_adj']) or 0:.3f}")
    print(f"    perim={r['perimeter_def_score']}  interior={r['interior_def_score']}  extras={r['defender_extras_score']}")
    print()
    for stat, pool, subcomp in DEF_STATS:
        v = get_pctile(pid, stat, pool)
        w = sw.get(subcomp, {}).get(stat)
        null_flag = ' ← NULL' if v is None else ''
        low_flag  = ' ← LOW'  if v is not None and v < 30 else ''
        high_flag = ' ← HIGH' if v is not None and v > 85 else ''
        print(f"    {stat:<28} {str(round(v,1)) if v is not None else 'NULL':>7}p  "
              f"w={f'{w:.4f}' if w else '  —  ':>8}  [{subcomp}]{null_flag}{low_flag}{high_flag}")

cur.close(); conn.close()