"""
NothingButNet — Schema
========================
python backend/schema.py

Drops all tables and recreates cleanly.
"""
import os, sys
from dotenv import load_dotenv
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("❌ DATABASE_URL not found."); sys.exit(1)

DROP = """
DROP TABLE IF EXISTS player_shot_zones CASCADE;
DROP TABLE IF EXISTS player_metrics    CASCADE;
DROP TABLE IF EXISTS player_pctiles    CASCADE;
DROP TABLE IF EXISTS player_seasons    CASCADE;
DROP TABLE IF EXISTS players           CASCADE;
"""

SCHEMA = """
CREATE TABLE IF NOT EXISTS players (
    player_id       INTEGER PRIMARY KEY,
    player_name     TEXT    NOT NULL,
    position        TEXT,
    position_group  TEXT,
    height_inches   REAL,
    weight          INTEGER,
    draft_year      INTEGER,
    draft_round     INTEGER,
    draft_number    INTEGER,
    college         TEXT,
    country         TEXT,
    is_active       BOOLEAN DEFAULT TRUE,
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS player_seasons (
    id              SERIAL PRIMARY KEY,
    player_id       INTEGER REFERENCES players(player_id) ON DELETE CASCADE,
    season          TEXT NOT NULL,
    season_type     TEXT NOT NULL,
    team_id         INTEGER,
    team_abbr       TEXT,

    -- Playing time
    gp              INTEGER,
    min             REAL,
    min_per_game    REAL,

    -- Box score (per game)
    pts             REAL, ast          REAL, reb          REAL,
    oreb            REAL, dreb         REAL, stl          REAL,
    blk             REAL, tov          REAL, pf           REAL,
    pfd             REAL, fgm          REAL, fga          REAL,
    fg_pct          REAL, fg3m         REAL, fg3a         REAL,
    fg3_pct         REAL, ftm          REAL, fta          REAL,
    ft_pct          REAL, plus_minus   REAL,

    -- Advanced (per game)
    off_rating      REAL, def_rating   REAL, net_rating   REAL,
    ast_pct         REAL, ast_to       REAL,
    oreb_pct        REAL, dreb_pct     REAL, reb_pct      REAL,
    efg_pct         REAL, ts_pct       REAL, usg_pct      REAL, pie REAL,

    -- Scoring breakdown (per game)
    pct_uast_fgm    REAL, pct_pts_paint REAL, pct_pts_3pt REAL,
    pct_pts_ft      REAL, pts_paint     REAL,

    -- Misc / turnover types (season totals)
    bad_pass_tov    REAL, lost_ball_tov REAL,

    -- Defense dash
    def_ws          REAL,

    -- Tracking: Drives (per game)
    drives          REAL, drive_fga    REAL, drive_fgm    REAL,
    drive_fg_pct    REAL, drive_pts    REAL, drive_ast    REAL,
    drive_tov       REAL, drive_passes REAL, drive_pf     REAL,

    -- Tracking: Passing (per game)
    passes_made     REAL, passes_received REAL, ast_pts_created REAL,
    potential_ast   REAL, secondary_ast   REAL, ft_ast          REAL,

    -- Tracking: Touches (per game)
    touches            REAL, time_of_poss       REAL,
    avg_sec_per_touch  REAL, avg_drib_per_touch  REAL,
    elbow_touches      REAL, post_touches        REAL, paint_touches REAL,

    -- Tracking: Pull-Up (per game)
    pull_up_fga     REAL, pull_up_fgm     REAL, pull_up_fg_pct  REAL,
    pull_up_fg3a    REAL, pull_up_fg3_pct REAL, pull_up_efg_pct REAL,

    -- Tracking: Catch & Shoot (per game)
    cs_fga          REAL, cs_fgm     REAL, cs_fg_pct  REAL,
    cs_fg3a         REAL, cs_fg3_pct REAL, cs_efg_pct REAL,

    -- Tracking: Post-Up (per game)
    post_touch_fga    REAL, post_touch_fg_pct REAL, post_touch_pts REAL,
    post_touch_ast    REAL, post_touch_tov    REAL,

    -- Tracking: Speed / Distance (per game)
    dist_miles      REAL, dist_miles_off REAL, dist_miles_def REAL,
    avg_speed       REAL, avg_speed_off  REAL, avg_speed_def  REAL,

    -- Tracking: Defense / Rim protection (per game)
    def_rim_fga     REAL, def_rim_fgm REAL, def_rim_fg_pct REAL,

    -- Hustle (season totals)
    contested_shots REAL, contested_2pt REAL, contested_3pt REAL,
    deflections     REAL, charges_drawn REAL, screen_assists REAL,
    screen_ast_pts  REAL, loose_balls   REAL, box_outs       REAL,
    off_box_outs    REAL, def_box_outs  REAL,

    -- Closest defender shooting (season totals, 4 distance buckets)
    cd_fga_vt REAL, cd_fgm_vt REAL, cd_fg3a_vt REAL, cd_fg3m_vt REAL,
    cd_fga_tg REAL, cd_fgm_tg REAL, cd_fg3a_tg REAL, cd_fg3m_tg REAL,
    cd_fga_op REAL, cd_fgm_op REAL, cd_fg3a_op REAL, cd_fg3m_op REAL,
    cd_fga_wo REAL, cd_fgm_wo REAL, cd_fg3a_wo REAL, cd_fg3m_wo REAL,

    -- Synergy offensive (per game)
    iso_ppp         REAL, iso_fga      REAL, iso_efg_pct  REAL, iso_tov_pct   REAL,
    pnr_bh_ppp      REAL, pnr_bh_fga   REAL,
    pnr_roll_ppp    REAL, pnr_roll_poss REAL,
    post_ppp        REAL, post_poss     REAL,
    spotup_ppp      REAL, spotup_efg_pct REAL,
    transition_ppp  REAL, transition_fga REAL,

    -- Synergy defensive (per game)
    def_iso_ppp      REAL, def_pnr_bh_ppp  REAL, def_post_ppp     REAL,
    def_spotup_ppp   REAL, def_pnr_roll_ppp REAL,

    -- Clutch (last 5 min ±5)
    clutch_net_rating REAL, clutch_ts_pct REAL,
    clutch_usg_pct    REAL, clutch_min    REAL,

    -- On/Off (filled by fetch_external.py)
    on_net_rating   REAL, off_net_rating REAL, on_off_diff REAL,

    -- External metrics (filled by fetch_external.py)
    darko           REAL,
    lebron          REAL,
    net_pts100      REAL,
    o_net_pts100    REAL,
    d_net_pts100    REAL,

    updated_at      TIMESTAMP DEFAULT NOW(),
    UNIQUE(player_id, season, season_type)
);

CREATE TABLE IF NOT EXISTS player_pctiles (
    id          SERIAL PRIMARY KEY,
    season      TEXT NOT NULL,
    season_type TEXT NOT NULL,
    stat_key    TEXT NOT NULL,
    pctile_map  JSONB NOT NULL,
    updated_at  TIMESTAMP DEFAULT NOW(),
    UNIQUE(season, season_type, stat_key)
);

CREATE INDEX IF NOT EXISTS idx_ps_player_season ON player_seasons(player_id, season, season_type);
CREATE INDEX IF NOT EXISTS idx_ps_season        ON player_seasons(season, season_type);
CREATE INDEX IF NOT EXISTS idx_ps_pts           ON player_seasons(season, pts DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_pctiles_season   ON player_pctiles(season, season_type);
CREATE INDEX IF NOT EXISTS idx_players_active   ON players(is_active);
"""

def reset_schema():
    print("⚠️  Connecting to database...")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur  = conn.cursor()
        print("🗑️  Dropping all tables...")
        cur.execute(DROP)
        print("🏗️  Creating schema...")
        cur.execute(SCHEMA)
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name")
        tables = [r[0] for r in cur.fetchall()]
        print(f"\n✅ Tables created:")
        for t in tables:
            cur.execute("SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='public' AND table_name=%s", (t,))
            print(f"   {t:<25} {cur.fetchone()[0]} columns")
        cur.close(); conn.close()
        print("\n✅ Done.")
    except Exception as e:
        print(f"❌ Error: {e}"); sys.exit(1)

if __name__ == "__main__":
    confirm = input("\n⚠️  DROP all tables and rebuild? Type 'yes': ")
    if confirm.strip().lower() != "yes":
        print("Aborted."); sys.exit(0)
    reset_schema()