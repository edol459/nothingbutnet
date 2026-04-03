"""
ydkball — NBA Stats Fetcher
===================================
python backend/ingest/fetch_stats.py

Fetches every NBA API endpoint. Matches theimpactboard coverage.

Endpoints:
  1.  Bio + Base (PerGame + Totals)
  2.  Advanced
  3.  Scoring breakdown
  4.  Misc (bad pass / lost ball TOV)
  5.  Defense dash (DEF_WS)
  6-13. Tracking: Drives, Passing, Touches, Pull-Up, Catch&Shoot, Post, Speed, Defense
  14. Hustle (LeagueHustleStatsPlayer)
  15. Closest defender shooting (VT/TG/OP/WO)
  16. Synergy offensive (ISO, PnR BH, PnR Roll, Post, SpotUp, Transition)
  17. Synergy defensive (ISO, PnR BH, Post, SpotUp, PnR Roll)
  18. Clutch (Advanced, last 5 min ±5)

External metrics (DARKO, LEBRON, net_pts100) → fetch_external.py
"""

import os, sys, time, argparse
from dotenv import load_dotenv
import psycopg2

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("❌ DATABASE_URL not set."); sys.exit(1)

parser = argparse.ArgumentParser()
parser.add_argument("--season",      default=os.getenv("NBA_SEASON", "2025-26"))
parser.add_argument("--season-type", default=os.getenv("NBA_SEASON_TYPE", "Regular Season"))
args = parser.parse_args()
SEASON, SEASON_TYPE = args.season, args.season_type
DELAY = 1.5

def sleep(extra=0): time.sleep(DELAY + extra)
def get_conn(): return psycopg2.connect(DATABASE_URL)

def _safe_num(val):
    if val is None: return None
    try:
        import math; f = float(val)
        return None if math.isnan(f) else f
    except: return None

def _safe_int(val):
    f = _safe_num(val)
    return None if f is None else int(f)

def _get_df(ep, index=0):
    try:
        frames = ep.get_data_frames()
        if frames and len(frames) > index: return frames[index]
        return frames[0] if frames else None
    except (KeyError, IndexError):
        nd = ep.get_normalized_dict()
        for v in nd.values():
            if isinstance(v, list) and v:
                import pandas as pd; return pd.DataFrame(v)
        return None

def _fetch(label, fn, retries=3):
    print(f"  → {label}...", end=" ", flush=True)
    for attempt in range(retries):
        try:
            sleep(attempt * 1.5)
            r = fn()
            if r is not None and len(r) > 0:
                print(f"✅ {len(r)} rows"); return r
            print("⚠️  empty"); return None
        except Exception as e:
            if attempt < retries-1: print(f"⚠️  retry {attempt+2}/{retries}...", end=" ", flush=True)
            else: print(f"❌ {e}")
    return None

def _idx(df):
    if df is None or len(df) == 0: return {}
    id_col = None
    if "PLAYER_ID" in df.columns: id_col = "PLAYER_ID"
    elif "CLOSE_DEF_PERSON_ID" in df.columns: id_col = "CLOSE_DEF_PERSON_ID"
    if not id_col: return {}
    # Synergy and some other endpoints return multiple rows per player
    # (e.g. player on multiple teams). Keep the row with the most possessions/FGA.
    if df[id_col].duplicated().any():
        poss_col = next((c for c in ["POSS","FGA","FGA_FREQUENCY","GP"] if c in df.columns), None)
        if poss_col:
            df = df.sort_values(poss_col, ascending=False).drop_duplicates(subset=[id_col])
        else:
            df = df.drop_duplicates(subset=[id_col])
    return df.set_index(id_col).to_dict(orient="index")

def _g(row, *cols):
    if row is None: return None
    for c in cols:
        if c in row: return _safe_num(row[c])
    return None

# ── Fetchers ──────────────────────────────────────────────────

def fetch_bio_and_base(s, st):
    from nba_api.stats.endpoints import LeagueDashPlayerBioStats, LeagueDashPlayerStats
    bio    = _fetch("Bio",            lambda: _get_df(LeagueDashPlayerBioStats(season=s, season_type_all_star=st, per_mode_simple="PerGame")))
    base   = _fetch("Base PerGame",   lambda: _get_df(LeagueDashPlayerStats(season=s, season_type_all_star=st, per_mode_detailed="PerGame")))
    totals = _fetch("Base Totals",    lambda: _get_df(LeagueDashPlayerStats(season=s, season_type_all_star=st, per_mode_detailed="Totals")))
    return bio, base, totals

def fetch_advanced(s, st):
    from nba_api.stats.endpoints import LeagueDashPlayerStats
    return _fetch("Advanced",         lambda: _get_df(LeagueDashPlayerStats(season=s, season_type_all_star=st, per_mode_detailed="PerGame", measure_type_detailed_defense="Advanced")))

def fetch_scoring(s, st):
    from nba_api.stats.endpoints import LeagueDashPlayerStats
    return _fetch("Scoring",          lambda: _get_df(LeagueDashPlayerStats(season=s, season_type_all_star=st, per_mode_detailed="PerGame", measure_type_detailed_defense="Scoring")))

def fetch_misc(s, st):
    from nba_api.stats.endpoints import LeagueDashPlayerStats
    return _fetch("Misc (TOV types)", lambda: _get_df(LeagueDashPlayerStats(season=s, season_type_all_star=st, per_mode_detailed="Totals", measure_type_detailed_defense="Misc")))

def fetch_defense_dash(s, st):
    from nba_api.stats.endpoints import LeagueDashPlayerStats
    return _fetch("Defense dash",     lambda: _get_df(LeagueDashPlayerStats(season=s, season_type_all_star=st, per_mode_detailed="PerGame", measure_type_detailed_defense="Defense")))

def fetch_tracking(s, st):
    from nba_api.stats.endpoints import LeagueDashPtStats
    results = {}
    for pt_type, key in [("Drives","drives"),("Passing","passing"),("Touches","touches"),
                          ("PullUpShot","pullup"),("CatchShoot","catchshoot"),("PostTouch","post"),
                          ("SpeedDistance","speed"),("Defense","def_track")]:
        results[key] = _fetch(f"Tracking {pt_type}", lambda t=pt_type: _get_df(LeagueDashPtStats(season=s, season_type_all_star=st, per_mode_simple="PerGame", pt_measure_type=t, player_or_team="Player")))
    return results

def fetch_hustle(s, st):
    from nba_api.stats.endpoints import LeagueHustleStatsPlayer
    return _fetch("Hustle",           lambda: _get_df(LeagueHustleStatsPlayer(season=s, season_type_all_star=st, per_mode_time="Totals")))

def fetch_closest_defender(s, st):
    from nba_api.stats.endpoints import LeagueDashPlayerPtShot
    results = {}
    for dist_range, key in [("0-2 Feet - Very Tight","vt"),("2-4 Feet - Tight","tg"),
                              ("4-6 Feet - Open","op"),("6+ Feet - Wide Open","wo")]:
        results[key] = _fetch(f"ClosestDef {key.upper()}", lambda r=dist_range: _get_df(LeagueDashPlayerPtShot(season=s, season_type_all_star=st, per_mode_simple="Totals", close_def_dist_range_nullable=r)))
    return results

def fetch_synergy(s, st):
    from nba_api.stats.endpoints import SynergyPlayTypes
    results = {}
    for play_type, grouping, key in [
        ("Isolation","offensive","iso_off"),("PRBallHandler","offensive","pnr_bh_off"),
        ("PRRollman","offensive","pnr_roll_off"),("Postup","offensive","post_off"),
        ("Spotup","offensive","spotup_off"),("Transition","offensive","transition_off"),
        ("Isolation","defensive","iso_def"),("PRBallHandler","defensive","pnr_bh_def"),
        ("Postup","defensive","post_def"),("Spotup","defensive","spotup_def"),
        ("PRRollman","defensive","pnr_roll_def"),
    ]:
        results[key] = _fetch(f"Synergy {key}", lambda pt=play_type, g=grouping: _get_df(SynergyPlayTypes(season=s, season_type_all_star=st, per_mode_simple="PerGame", play_type_nullable=pt, type_grouping_nullable=g, player_or_team_abbreviation="P")))
    return results

def fetch_clutch(s, st):
    from nba_api.stats.endpoints import LeagueDashPlayerClutch
    return _fetch("Clutch Advanced",  lambda: _get_df(LeagueDashPlayerClutch(season=s, season_type_all_star=st, measure_type_detailed_defense="Advanced", clutch_time="Last 5 Minutes", ahead_behind="Ahead or Behind", point_diff=5)))

# ── Upserts ───────────────────────────────────────────────────

def upsert_players(conn, bio_df):
    cur = conn.cursor()
    def pg(p):
        if not p: return None
        m = {"PG":"G","SG":"G","G":"G","G-F":"GF","F-G":"GF","GUARD-FORWARD":"GF",
             "FORWARD-GUARD":"GF","SF":"F","PF":"F","F":"F","FORWARD":"F",
             "F-C":"FC","C-F":"FC","FORWARD-CENTER":"FC","CENTER-FORWARD":"FC","C":"C","CENTER":"C"}
        return m.get(str(p).upper())
    for _, row_series in bio_df.iterrows():
        row  = row_series.to_dict()
        pid  = int(row["PLAYER_ID"]); pos = row.get("POSITION")
        h_str = row.get("PLAYER_HEIGHT_FEET"); height = None
        if h_str and str(h_str) not in ("None","nan",""):
            try: parts=str(h_str).split("-"); height=int(parts[0])*12+int(parts[1])
            except: pass
        cur.execute("""INSERT INTO players (player_id,player_name,position,position_group,height_inches,weight,is_active,updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,TRUE,NOW()) ON CONFLICT (player_id) DO UPDATE SET
            player_name=EXCLUDED.player_name,position=EXCLUDED.position,position_group=EXCLUDED.position_group,
            height_inches=COALESCE(EXCLUDED.height_inches,players.height_inches),
            weight=COALESCE(EXCLUDED.weight,players.weight),is_active=TRUE,updated_at=NOW()""",
            (pid,row["PLAYER_NAME"],pos,pg(pos),height,_safe_int(row.get("PLAYER_WEIGHT"))))
    conn.commit(); cur.close()
    print(f"  ✅ {len(bio_df)} players upserted")

def build_row(pid, season, season_type, base, adv, scoring, misc, def_dash, tr, hustle, cd, sy, clutch, totals):
    g = _g
    drv=tr.get("drives"); pas=tr.get("passing"); tch=tr.get("touches")
    pul=tr.get("pullup"); cs=tr.get("catchshoot"); post=tr.get("post")
    spd=tr.get("speed"); dtr=tr.get("def_track")
    return dict(
        player_id=pid, season=season, season_type=season_type,
        team_id=g(base,"TEAM_ID"), team_abbr=base.get("TEAM_ABBREVIATION") if base else None,
        gp=g(totals,"GP","G"), min=g(totals,"MIN"), min_per_game=g(base,"MIN"),
        pts=g(base,"PTS"), ast=g(base,"AST"), reb=g(base,"REB"),
        oreb=g(base,"OREB"), dreb=g(base,"DREB"), stl=g(base,"STL"), blk=g(base,"BLK"),
        tov=g(base,"TOV"), pf=g(base,"PF"), pfd=g(base,"PFD"),
        fgm=g(base,"FGM"), fga=g(base,"FGA"), fg_pct=g(base,"FG_PCT"),
        fg3m=g(base,"FG3M"), fg3a=g(base,"FG3A"), fg3_pct=g(base,"FG3_PCT"),
        ftm=g(base,"FTM"), fta=g(base,"FTA"), ft_pct=g(base,"FT_PCT"),
        plus_minus=g(base,"PLUS_MINUS"),
        off_rating=g(adv,"OFF_RATING"), def_rating=g(adv,"DEF_RATING"), net_rating=g(adv,"NET_RATING"),
        ast_pct=g(adv,"AST_PCT"), ast_to=g(adv,"AST_TO"),
        oreb_pct=g(adv,"OREB_PCT"), dreb_pct=g(adv,"DREB_PCT"), reb_pct=g(adv,"REB_PCT"),
        efg_pct=g(adv,"EFG_PCT"), ts_pct=g(adv,"TS_PCT"), usg_pct=g(adv,"USG_PCT"), pie=g(adv,"PIE"),
        pct_uast_fgm=g(scoring,"PCT_UAST_FGM"), pct_pts_paint=g(scoring,"PCT_PTS_PAINT"),
        pct_pts_3pt=g(scoring,"PCT_PTS_3PT","PCT_PTS_FG3"), pct_pts_ft=g(scoring,"PCT_PTS_FT"),
        pts_paint=g(scoring,"PTS_PAINT"),
        bad_pass_tov=g(misc,"BAD_PASS"), lost_ball_tov=g(misc,"LOST_BALL"),
        def_ws=g(def_dash,"DEF_WS"),
        drives=g(drv,"DRIVES"), drive_fga=g(drv,"DRIVE_FGA"), drive_fgm=g(drv,"DRIVE_FGM"),
        drive_fg_pct=g(drv,"DRIVE_FG_PCT"), drive_pts=g(drv,"DRIVE_PTS"),
        drive_ast=g(drv,"DRIVE_AST"), drive_tov=g(drv,"DRIVE_TOV"),
        drive_passes=g(drv,"DRIVE_PASSES"), drive_pf=g(drv,"DRIVE_PF"),
        passes_made=g(pas,"PASSES_MADE"), passes_received=g(pas,"PASSES_RECEIVED"),
        ast_pts_created=g(pas,"AST_POINTS_CREATED"), potential_ast=g(pas,"POTENTIAL_AST"),
        secondary_ast=g(pas,"SECONDARY_AST"), ft_ast=g(pas,"FT_AST"),
        touches=g(tch,"TOUCHES"), time_of_poss=g(tch,"TIME_OF_POSS"),
        avg_sec_per_touch=g(tch,"AVG_SEC_PER_TOUCH"), avg_drib_per_touch=g(tch,"AVG_DRIB_PER_TOUCH"),
        elbow_touches=g(tch,"ELBOW_TOUCHES"), post_touches=g(tch,"POST_TOUCHES"), paint_touches=g(tch,"PAINT_TOUCHES"),
        pull_up_fga=g(pul,"PULL_UP_FGA"), pull_up_fgm=g(pul,"PULL_UP_FGM"),
        pull_up_fg_pct=g(pul,"PULL_UP_FG_PCT"), pull_up_fg3a=g(pul,"PULL_UP_FG3A"),
        pull_up_fg3_pct=g(pul,"PULL_UP_FG3_PCT"), pull_up_efg_pct=g(pul,"PULL_UP_EFG_PCT"),
        cs_fga=g(cs,"CATCH_SHOOT_FGA"), cs_fgm=g(cs,"CATCH_SHOOT_FGM"),
        cs_fg_pct=g(cs,"CATCH_SHOOT_FG_PCT"), cs_fg3a=g(cs,"CATCH_SHOOT_FG3A"),
        cs_fg3_pct=g(cs,"CATCH_SHOOT_FG3_PCT"), cs_efg_pct=g(cs,"CATCH_SHOOT_EFG_PCT"),
        post_touch_fga=g(post,"POST_TOUCH_FGA"), post_touch_fg_pct=g(post,"POST_TOUCH_FG_PCT"),
        post_touch_pts=g(post,"POST_TOUCH_PTS"), post_touch_ast=g(post,"POST_TOUCH_AST"),
        post_touch_tov=g(post,"POST_TOUCH_TOV"),
        dist_miles=g(spd,"DIST_MILES"), dist_miles_off=g(spd,"DIST_MILES_OFF"),
        dist_miles_def=g(spd,"DIST_MILES_DEF"), avg_speed=g(spd,"AVG_SPEED"),
        avg_speed_off=g(spd,"AVG_SPEED_OFF"), avg_speed_def=g(spd,"AVG_SPEED_DEF"),
        def_rim_fga=g(dtr,"DEF_RIM_FGA"), def_rim_fgm=g(dtr,"DEF_RIM_FGM"), def_rim_fg_pct=g(dtr,"DEF_RIM_FG_PCT"),
        contested_shots=g(hustle,"CONTESTED_SHOTS"), contested_2pt=g(hustle,"CONTESTED_2PT_SHOTS"),
        contested_3pt=g(hustle,"CONTESTED_3PT_SHOTS"), deflections=g(hustle,"DEFLECTIONS"),
        charges_drawn=g(hustle,"CHARGES_DRAWN"), screen_assists=g(hustle,"SCREEN_ASSISTS"),
        screen_ast_pts=g(hustle,"SCREEN_AST_PTS"), loose_balls=g(hustle,"LOOSE_BALLS_RECOVERED"),
        box_outs=g(hustle,"BOX_OUTS"), off_box_outs=g(hustle,"OFF_BOXOUTS","OFFENSIVE_BOX_OUTS"),
        def_box_outs=g(hustle,"DEF_BOXOUTS","DEFENSIVE_BOX_OUTS"),
        cd_fga_vt=g(cd.get("vt"),"FGA"), cd_fgm_vt=g(cd.get("vt"),"FGM"),
        cd_fg3a_vt=g(cd.get("vt"),"FG3A"), cd_fg3m_vt=g(cd.get("vt"),"FG3M"),
        cd_fga_tg=g(cd.get("tg"),"FGA"), cd_fgm_tg=g(cd.get("tg"),"FGM"),
        cd_fg3a_tg=g(cd.get("tg"),"FG3A"), cd_fg3m_tg=g(cd.get("tg"),"FG3M"),
        cd_fga_op=g(cd.get("op"),"FGA"), cd_fgm_op=g(cd.get("op"),"FGM"),
        cd_fg3a_op=g(cd.get("op"),"FG3A"), cd_fg3m_op=g(cd.get("op"),"FG3M"),
        cd_fga_wo=g(cd.get("wo"),"FGA"), cd_fgm_wo=g(cd.get("wo"),"FGM"),
        cd_fg3a_wo=g(cd.get("wo"),"FG3A"), cd_fg3m_wo=g(cd.get("wo"),"FG3M"),
        iso_ppp=g(sy.get("iso_off"),"PPP"), iso_fga=g(sy.get("iso_off"),"FGA"),
        iso_efg_pct=g(sy.get("iso_off"),"EFG_PCT"), iso_tov_pct=g(sy.get("iso_off"),"TOV_PCT"),
        pnr_bh_ppp=g(sy.get("pnr_bh_off"),"PPP"), pnr_bh_fga=g(sy.get("pnr_bh_off"),"FGA"),
        pnr_roll_ppp=g(sy.get("pnr_roll_off"),"PPP"), pnr_roll_poss=g(sy.get("pnr_roll_off"),"POSS"),
        post_ppp=g(sy.get("post_off"),"PPP"), post_poss=g(sy.get("post_off"),"POSS"),
        spotup_ppp=g(sy.get("spotup_off"),"PPP"), spotup_efg_pct=g(sy.get("spotup_off"),"EFG_PCT"),
        transition_ppp=g(sy.get("transition_off"),"PPP"), transition_fga=g(sy.get("transition_off"),"FGA"),
        def_iso_ppp=g(sy.get("iso_def"),"PPP"), def_pnr_bh_ppp=g(sy.get("pnr_bh_def"),"PPP"),
        def_post_ppp=g(sy.get("post_def"),"PPP"), def_spotup_ppp=g(sy.get("spotup_def"),"PPP"),
        def_pnr_roll_ppp=g(sy.get("pnr_roll_def"),"PPP"),
        clutch_net_rating=g(clutch,"NET_RATING"), clutch_ts_pct=g(clutch,"TS_PCT"),
        clutch_usg_pct=g(clutch,"USG_PCT"), clutch_min=g(clutch,"MIN"),
        on_net_rating=None, off_net_rating=None, on_off_diff=None,
        darko=None, lebron=None, net_pts100=None, o_net_pts100=None, d_net_pts100=None,
    )

def upsert_seasons(conn, rows):
    if not rows: return
    cur = conn.cursor()
    cols = [k for k in rows[0] if k not in ("player_id","season","season_type")]
    set_cl = ", ".join(f"{c}=EXCLUDED.{c}" for c in cols)
    col_l  = ", ".join(["player_id","season","season_type"]+cols)
    val_l  = ", ".join(["%s"]*(3+len(cols)))
    for row in rows:
        vals = [row["player_id"],row["season"],row["season_type"]]+[row[c] for c in cols]
        cur.execute(f"INSERT INTO player_seasons ({col_l}) VALUES ({val_l}) ON CONFLICT (player_id,season,season_type) DO UPDATE SET {set_cl}, updated_at=NOW()", vals)
    conn.commit(); cur.close()
    print(f"  ✅ {len(rows)} season rows upserted")

# ── Run ───────────────────────────────────────────────────────

def run():
    print(f"\n🏀 ydkball — Fetching {SEASON} {SEASON_TYPE}")
    print("=" * 60)
    print("\n[1/9] Bio + Base + Totals")
    bio_df, base_df, totals_df = fetch_bio_and_base(SEASON, SEASON_TYPE)
    print("\n[2/9] Advanced")
    adv_df = fetch_advanced(SEASON, SEASON_TYPE)
    print("\n[3/9] Scoring / Misc / Defense dash")
    scoring_df  = fetch_scoring(SEASON, SEASON_TYPE)
    misc_df     = fetch_misc(SEASON, SEASON_TYPE)
    def_dash_df = fetch_defense_dash(SEASON, SEASON_TYPE)
    print("\n[4/9] Tracking (8 types)")
    tracking = fetch_tracking(SEASON, SEASON_TYPE)
    print("\n[5/9] Hustle")
    hustle_df = fetch_hustle(SEASON, SEASON_TYPE)
    print("\n[6/9] Closest defender shooting")
    closest_def = fetch_closest_defender(SEASON, SEASON_TYPE)
    print("\n[7/9] Synergy (11 play types)")
    synergy = fetch_synergy(SEASON, SEASON_TYPE)
    print("\n[8/9] Clutch")
    clutch_df = fetch_clutch(SEASON, SEASON_TYPE)

    adv_map      = _idx(adv_df)
    scoring_map  = _idx(scoring_df)
    misc_map     = _idx(misc_df)
    def_dash_map = _idx(def_dash_df)
    hustle_map   = _idx(hustle_df)
    totals_map   = _idx(totals_df)
    clutch_map   = _idx(clutch_df)
    tracking_maps = {k: _idx(df) for k,df in tracking.items()}
    cd_maps       = {k: _idx(df) for k,df in closest_def.items()}
    syn_maps      = {k: _idx(df) for k,df in synergy.items() if df is not None}

    print("\n[9/9] Upserting to database...")
    conn = get_conn()
    upsert_players(conn, bio_df)
    rows = []
    for _, base_row in base_df.iterrows():
        pid = int(base_row["PLAYER_ID"])
        base_dict = base_row.to_dict()  # convert Series → dict once
        rows.append(build_row(
            pid=pid, season=SEASON, season_type=SEASON_TYPE,
            base=base_dict, adv=adv_map.get(pid), scoring=scoring_map.get(pid),
            misc=misc_map.get(pid), def_dash=def_dash_map.get(pid),
            tr={k: v.get(pid) for k,v in tracking_maps.items()},
            hustle=hustle_map.get(pid),
            cd={k: v.get(pid) for k,v in cd_maps.items()},
            sy={k: v.get(pid) for k,v in syn_maps.items()},
            clutch=clutch_map.get(pid), totals=totals_map.get(pid),
        ))
    upsert_seasons(conn, rows)
    conn.close()
    print(f"\n✅ Done. {len(rows)} players for {SEASON} {SEASON_TYPE}.")
    print("Next: python backend/ingest/compute_pctiles.py")

if __name__ == "__main__":
    run()