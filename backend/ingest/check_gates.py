import os, psycopg2, psycopg2.extras
from dotenv import load_dotenv
load_dotenv()

conn = psycopg2.connect(os.getenv('DATABASE_URL'))
cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
cur.execute("""
    SELECT p.player_name, p.position_group,
           ps.gp, ps.min,
           ps.drives, ps.touches, ps.paint_touches,
           ps.ast, ps.potential_ast,
           ps.fga,
           pm.creator_score, pm.finishing_score,
           pm.shooting_score, pm.shot_creation_score,
           pm.playmaker_score, pm.passing_score,
           pm.creation_score, pm.decision_making_score
    FROM player_seasons ps
    JOIN players p ON ps.player_id = p.player_id
    JOIN player_metrics pm ON pm.player_id = ps.player_id
                           AND pm.season = ps.season
                           AND pm.season_type = ps.season_type
    WHERE ps.season = '2025-26' AND ps.season_type = 'Regular Season'
      AND ps.min >= 1000 AND ps.league = 'NBA'
      AND (pm.creator_score IS NULL OR pm.playmaker_score IS NULL)
    ORDER BY pm.asap_score DESC NULLS LAST
    LIMIT 25
""")
rows = cur.fetchall()

print(f"{'Player':<26} {'POS':<5} {'MIN':<6} {'drives/g':<10} {'touches/g':<11} "
      f"{'paint/g':<9} {'ast/g':<7} {'pot_ast/g':<11} {'fga/g':<7} "
      f"{'creator':<9} {'playmaker'}")
print('─' * 120)
for r in rows:
    gp = max(r['gp'] or 1, 1)
    drives_g    = (r['drives'] or 0) / gp
    touches_g   = (r['touches'] or 0) / gp
    paint_g     = (r['paint_touches'] or 0) / gp
    ast_g       = r['ast'] or 0
    pot_ast_g   = (r['potential_ast'] or 0) / gp
    fga_g       = r['fga'] or 0

    creator_null    = '—' if r['creator_score'] is None else f"{r['creator_score']:.1f}"
    playmaker_null  = '—' if r['playmaker_score'] is None else f"{r['playmaker_score']:.1f}"

    # Flag which gates they fail
    flags = []
    if r['shot_creation_score'] is None and drives_g < 2.0:
        flags.append(f'drives({drives_g:.1f}<2)')
    if r['finishing_score'] is None and paint_g < 3.0:
        flags.append(f'paint({paint_g:.1f}<3)')
    if r['shooting_score'] is None and fga_g < 3.0:
        flags.append(f'fga({fga_g:.1f}<3)')
    if r['passing_score'] is None:
        if ast_g < 1.5: flags.append(f'ast({ast_g:.1f}<1.5)')
        if pot_ast_g < 3.0: flags.append(f'pot_ast({pot_ast_g:.1f}<3)')

    print(f"  {r['player_name']:<24} {r['position_group']:<5} {r['min']:<6.0f} "
          f"{drives_g:<10.1f} {touches_g:<11.1f} {paint_g:<9.1f} "
          f"{ast_g:<7.1f} {pot_ast_g:<11.1f} {fga_g:<7.1f} "
          f"{creator_null:<9} {playmaker_null:<10}  {', '.join(flags)}")

cur.close()
conn.close()