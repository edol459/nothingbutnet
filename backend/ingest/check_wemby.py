import os, psycopg2, psycopg2.extras
from dotenv import load_dotenv
load_dotenv()
conn = psycopg2.connect(os.getenv('DATABASE_URL'))
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
cur.execute("""
    SELECT pm.finishing_score, pm.paint_fga_pg, pm.paint_efg_vw,
           pm.paint_scoring_rate, pm.drive_foul_rate, pm.drive_pts_per_drive
    FROM player_metrics pm
    JOIN players p ON pm.player_id = p.player_id
    WHERE pm.season = '2025-26' AND pm.season_type = 'Regular Season'
    AND p.player_name ILIKE '%hardaway%'
""")
print(cur.fetchone())
conn.close()