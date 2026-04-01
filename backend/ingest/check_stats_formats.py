"""
Check stat formats — per game vs totals
python backend/ingest/check_stat_formats.py
"""
import os, psycopg2, psycopg2.extras
from dotenv import load_dotenv
load_dotenv()

conn = psycopg2.connect(os.getenv('DATABASE_URL'), cursor_factory=psycopg2.extras.RealDictCursor)
cur = conn.cursor()
cur.execute("""
    SELECT p.player_name, ps.gp, ps.min,
        ps.pts, ps.ast, ps.reb, ps.stl, ps.blk, ps.tov,
        ps.fga, ps.fgm, ps.fg3a, ps.fg3m, ps.fta, ps.ftm,
        ps.drives, ps.drive_fga, ps.drive_pts, ps.drive_ast,
        ps.passes_made, ps.ast_pts_created, ps.potential_ast, ps.secondary_ast, ps.ft_ast,
        ps.touches, ps.time_of_poss, ps.elbow_touches, ps.post_touches, ps.paint_touches,
        ps.pull_up_fga, ps.cs_fga,
        ps.contested_shots, ps.contested_2pt, ps.contested_3pt,
        ps.deflections, ps.charges_drawn, ps.screen_assists, ps.loose_balls,
        ps.box_outs, ps.off_box_outs, ps.def_box_outs,
        ps.dist_miles, ps.bad_pass_tov, ps.lost_ball_tov,
        ps.def_rim_fga, ps.pts_paint, ps.plus_minus
    FROM player_seasons ps
    JOIN players p ON ps.player_id = p.player_id
    WHERE ps.season = '2025-26' AND ps.season_type = 'Regular Season'
      AND ps.gp >= 40 AND ps.min >= 1000
    ORDER BY ps.pts DESC
    LIMIT 5
""")
rows = cur.fetchall()
cur.close()
conn.close()

# Print each player's stats in a readable format
scalable = [
    'pts','ast','reb','stl','blk','tov','fga','fgm','fg3a','fg3m','fta','ftm',
    'drives','drive_fga','drive_pts','drive_ast',
    'passes_made','ast_pts_created','potential_ast','secondary_ast','ft_ast',
    'touches','time_of_poss','elbow_touches','post_touches','paint_touches',
    'pull_up_fga','cs_fga',
    'contested_shots','contested_2pt','contested_3pt',
    'deflections','charges_drawn','screen_assists','loose_balls',
    'box_outs','off_box_outs','def_box_outs',
    'dist_miles','bad_pass_tov','lost_ball_tov','def_rim_fga','pts_paint','plus_minus',
]

print(f"\n{'='*80}")
print(f"{'Stat':<22} {'Jokic':>10} {'SGA':>10} {'Wemby':>10}  looks like...")
print(f"{'='*80}")

for stat in scalable:
    vals = [row[stat] for row in rows[:3]]
    gps  = [row['gp'] for row in rows[:3]]
    # Show raw value and value/gp
    raw  = [f"{v:.1f}" if v is not None else "—" for v in vals]
    pergame = [f"{v/g:.1f}" if v is not None else "—" for v, g in zip(vals, gps)]
    # Guess format: if raw value is plausibly per-game (pts ~25-35, ast ~5-10) label it
    first = vals[0]
    gp0   = gps[0]
    if first is None:
        guess = "NULL"
    elif first > gp0 * 3:
        guess = "← TOTAL (divide by GP)"
    else:
        guess = "← per game ✓"
    print(f"  {stat:<20} {raw[0]:>10}  /GP={pergame[0]:>6}   {guess}")

player_info = [r['player_name'].split()[1] + f" GP={r['gp']}" for r in rows[:3]]
print(f"\nPlayer GPs: {player_info}")
print()