import os, math, psycopg2, psycopg2.extras
from dotenv import load_dotenv
load_dotenv()

conn = psycopg2.connect(os.getenv('DATABASE_URL'))
cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

cur.execute("""
    SELECT ps.player_id, p.player_name, p.position_group,
           ps.net_pts100,
           ps.gravity_onball_perimeter, ps.gravity_offball_perimeter,
           ps.gravity_onball_interior, ps.gravity_offball_interior,
           pm.gravity_perimeter_score, pm.gravity_interior_score,
           pm.intangibles_score
    FROM player_seasons ps
    JOIN players p ON ps.player_id = p.player_id
    JOIN player_metrics pm ON pm.player_id = ps.player_id
                           AND pm.season = ps.season AND pm.season_type = ps.season_type
    WHERE ps.season = '2025-26' AND ps.season_type = 'Regular Season'
      AND ps.min >= 500 AND ps.league = 'NBA'
      AND ps.net_pts100 IS NOT NULL
""")
rows = cur.fetchall()
print(f"Pool: {len(rows)} players")

def s(v):
    if v is None: return None
    try:
        f = float(v)
        return None if math.isnan(f) else f
    except: return None

def pearson(pairs):
    n = len(pairs)
    if n < 10: return None
    xs, ys = zip(*pairs)
    mx, my = sum(xs)/n, sum(ys)/n
    num = sum((x-mx)*(y-my) for x,y in zip(xs,ys))
    dx = sum((x-mx)**2 for x in xs)**0.5
    dy = sum((y-my)**2 for y in ys)**0.5
    if dx==0 or dy==0: return None
    return round(num/(dx*dy), 4)

net = [s(r['net_pts100']) for r in rows]

stats = [
    'gravity_onball_perimeter', 'gravity_offball_perimeter',
    'gravity_onball_interior',  'gravity_offball_interior',
]

print(f"\n{'Stat':<30} {'r':>7}  {'n':>5}  {'non-null'}")
print('─'*55)
for col in stats:
    vals = [s(r[col]) for r in rows]
    pairs = [(v, n) for v, n in zip(vals, net) if v is not None and n is not None]
    r = pearson(pairs)
    print(f"  {col:<28} {f'{r:+.4f}' if r else '  N/A':>7}  {len(pairs):>5}")

# Check gravity score distribution
print("\n  gravity_perimeter_score sample:")
g_perim = [(r['player_name'], r['position_group'], r['gravity_perimeter_score'])
           for r in rows if r['gravity_perimeter_score'] is not None]
print(f"    {len(g_perim)} players have perimeter gravity score")
print(f"    Top 5: {sorted(g_perim, key=lambda x: x[2], reverse=True)[:5]}")

print("\n  gravity_interior_score sample:")
g_int = [(r['player_name'], r['position_group'], r['gravity_interior_score'])
         for r in rows if r['gravity_interior_score'] is not None]
print(f"    {len(g_int)} players have interior gravity score")
print(f"    Top 5: {sorted(g_int, key=lambda x: x[2], reverse=True)[:5]}")

# Check how many have intangibles_score
has_int = sum(1 for r in rows if r['intangibles_score'] is not None)
print(f"\n  {has_int} players have intangibles_score")

cur.close()
conn.close()