import os
from dotenv import load_dotenv
load_dotenv()
import psycopg2
conn = psycopg2.connect(os.getenv('DATABASE_URL'))
cur = conn.cursor()
cur.execute("""
    SELECT player_id, fga, fg_pct 
    FROM player_seasons 
    WHERE season = '2025-26' AND season_type = 'Regular Season'
    AND fga IS NOT NULL
    LIMIT 5
""")
for row in cur.fetchall():
    print(row)
cur.close()
conn.close()