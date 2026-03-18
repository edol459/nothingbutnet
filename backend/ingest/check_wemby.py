import os, psycopg2, psycopg2.extras
from dotenv import load_dotenv
load_dotenv()
conn = psycopg2.connect(os.getenv('DATABASE_URL'))
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
cur.execute("""
    SELECT p.player_name, p.position_group,
           ps.gp, ps.drives, ps.fga,
           pm.creation_score, pm.shooting_score, pm.creator_score
    FROM player_seasons ps
    JOIN players p ON ps.player_id = p.player_id
    JOIN player_metrics pm ON ps.player_id = pm.player_id
        AND ps.season = pm.season AND ps.season_type = pm.season_type
    WHERE ps.season = '2025-26' AND ps.season_type = 'Regular Season'
    AND p.player_name IN (
        'Shai Gilgeous-Alexander', 'Luka Doncic', 'Stephen Curry',
        'Anthony Edwards', 'Donovan Mitchell', 'AJ Green',
        'Rui Hachimura', 'Luke Kennard', 'Jamal Murray'
    )
    ORDER BY pm.creator_score DESC NULLS LAST
""")
print(f"{'Player':<25} {'GP':>4} {'DRV':>6} {'DRV/G':>6} {'GATE':>5} {'CREAT':>7} {'SHOOT':>7} {'OVRL':>7}")
print("-" * 75)
for r in cur.fetchall():
    gp = r['gp'] or 1
    drv = r['drives'] or 0
    drv_pg = drv / gp
    gate = drv_pg >= 2.0
    print(f"{r['player_name']:<25} {gp:>4} {drv:>6.0f} {drv_pg:>6.2f} "
          f"{'PASS' if gate else 'FAIL':>5} "
          f"{r['creation_score'] or 0:>7.1f} "
          f"{r['shooting_score'] or 0:>7.1f} "
          f"{r['creator_score'] or 0:>7.1f}")
cur.close()
conn.close()