import os, psycopg2, psycopg2.extras
from dotenv import load_dotenv
load_dotenv()
conn = psycopg2.connect(os.getenv('DATABASE_URL'))
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
cur.execute("""
    SELECT p.player_name, ps.gp, ps.min, ps.min_per_game,
           pm.paint_fga_pg, pm.midrange_fga_pg, pm.all3_fga_pg
    FROM player_seasons ps
    JOIN players p ON ps.player_id = p.player_id
    JOIN player_metrics pm ON ps.player_id = pm.player_id
        AND ps.season = pm.season AND ps.season_type = pm.season_type
    WHERE ps.season = '2025-26' AND ps.season_type = 'Regular Season'
    AND p.player_name IN ('Ryan Kalkbrenner', 'Rudy Gobert', 'Victor Wembanyama', 'Luke Kennard')
""")
for r in cur.fetchall():
    print(f"{r['player_name']:<25} gp={r['gp']} min={r['min']} min_pg={r['min_per_game']} "
          f"paint_fga_pg={r['paint_fga_pg']} mid_fga_pg={r['midrange_fga_pg']} 3pt_fga_pg={r['all3_fga_pg']}")
conn.close()