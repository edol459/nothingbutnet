"""
The Impact Board — Migration
=============================
Adds closest-defender shooting columns to player_seasons,
and new computed metric columns to player_metrics.

python backend/migrate.py
"""

import os
import sys
from dotenv import load_dotenv
import psycopg2

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')
if not DATABASE_URL:
    print("❌ DATABASE_URL not set.")
    sys.exit(1)

conn = psycopg2.connect(DATABASE_URL)
cur  = conn.cursor()

migrations = [
    # ── player_seasons: closest defender shooting raw data ──────────────────
    # VT = Very Tight (0-2ft), TG = Tight (2-4ft), OP = Open (4-6ft), WO = Wide Open (6ft+)
    ("player_seasons", "cd_fga_vt",  "REAL"),
    ("player_seasons", "cd_fgm_vt",  "REAL"),
    ("player_seasons", "cd_fg3a_vt", "REAL"),
    ("player_seasons", "cd_fg3m_vt", "REAL"),
    ("player_seasons", "cd_fga_tg",  "REAL"),
    ("player_seasons", "cd_fgm_tg",  "REAL"),
    ("player_seasons", "cd_fg3a_tg", "REAL"),
    ("player_seasons", "cd_fg3m_tg", "REAL"),
    ("player_seasons", "cd_fga_op",  "REAL"),
    ("player_seasons", "cd_fgm_op",  "REAL"),
    ("player_seasons", "cd_fg3a_op", "REAL"),
    ("player_seasons", "cd_fg3m_op", "REAL"),
    ("player_seasons", "cd_fga_wo",  "REAL"),
    ("player_seasons", "cd_fgm_wo",  "REAL"),
    ("player_seasons", "cd_fg3a_wo", "REAL"),
    ("player_seasons", "cd_fg3m_wo", "REAL"),

    # ── player_metrics: new computed metrics ────────────────────────────────
    ("player_metrics", "contested_fg_making", "REAL"),  # player EFG% on 0-4ft contested - league avg
    ("player_metrics", "open_fg_making",      "REAL"),  # player EFG% on 4ft+ open - league avg
    ("player_metrics", "drive_foul_rate",     "REAL"),  # drive_pf / drives
    ("player_metrics", "tov_pct",             "REAL"),  # tov / (fga + 0.44*fta + tov)
    ("player_metrics", "ast_pts_created_pg",  "REAL"),  # ast_pts_created / gp
    ("player_metrics", "drive_pts_per_drive", "REAL"),  # drive_pts / drives
    # ── playmaking refactor additions ───────────────────────────────────────
    ("player_metrics", "ft_ast_per75",        "REAL"),  # ft assists per 75 poss
    ("player_metrics", "drive_ast_per75",     "REAL"),  # drive assists per 75 poss
    ("player_metrics", "drive_passes_per75",  "REAL"),  # drive passes per 75 poss (proxy for pot ast from drives)
    ("player_metrics", "lost_ball_tov_pg",    "REAL"),  # live-ball turnovers per game (inverted in BH composite)
    # ── turnover type columns (player_seasons) ───────────────────────────────
    ("player_seasons", "bad_pass_tov",        "REAL"),  # bad-pass turnovers (season total) — fixes pot_ast_per_tov
    ("player_seasons", "lost_ball_tov",       "REAL"),  # lost-ball turnovers (season total) — dribbling/handling TOVs
]

print(f"\nRunning migrations...")
print(f"{'─'*50}")

for table, col, dtype in migrations:
    sql = f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col} {dtype}"
    try:
        cur.execute(sql)
        print(f"  ✅  {table}.{col} ({dtype})")
    except Exception as e:
        print(f"  ❌  {table}.{col} — {e}")

conn.commit()
cur.close()
conn.close()

print(f"{'─'*50}")
print(f"\n✅ Migration complete.")
print(f"\nNext step: python backend/ingest/compute_metrics.py")