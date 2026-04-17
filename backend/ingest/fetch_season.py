"""
The Impact Board — Season Data Ingestion
==========================================
python backend/ingest/fetch_season.py

Fetches all NBA API endpoints for a given season and upserts
into player_seasons and players tables.

Run once for full season load. Safe to re-run — uses upsert.

Usage:
    python backend/ingest/fetch_season.py
    python backend/ingest/fetch_season.py --season 2023-24
"""

import os
import sys
import time
import argparse
from datetime import datetime
from dotenv import load_dotenv
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')
SEASON      = os.getenv('NBA_SEASON', '2025-26')
SEASON_TYPE = os.getenv('NBA_SEASON_TYPE', 'Regular Season')
DELAY       = 3.0

if not DATABASE_URL:
    print("❌ DATABASE_URL not set.")
    sys.exit(1)

def try_import(name):
    try:
        import importlib
        mod = importlib.import_module("nba_api.stats.endpoints")
        return getattr(mod, name)
    except (ImportError, AttributeError):
        return None

PlayerGameLogs                = try_import("PlayerGameLogs")
LeagueDashPlayerBioStats      = try_import("LeagueDashPlayerBioStats")
LeagueDashPtStats             = try_import("LeagueDashPtStats")
LeagueDashPtDefend            = try_import("LeagueDashPtDefend")
LeagueSeasonMatchups          = try_import("LeagueSeasonMatchups")
LeagueHustleStatsPlayer       = try_import("LeagueHustleStatsPlayer")
LeagueDashLineups             = try_import("LeagueDashLineups")
LeagueDashPlayerClutch        = try_import("LeagueDashPlayerClutch")
LeagueDashPlayerBioStats      = try_import("LeagueDashPlayerBioStats")
SynergyPlayTypes              = try_import("SynergyPlayTypes")
TeamPlayerOnOffDetails        = try_import("TeamPlayerOnOffDetails")
LeagueDashPlayerShotLocations = try_import("LeagueDashPlayerShotLocations")
LeagueDashPlayerStats         = try_import("LeagueDashPlayerStats")
PlayerIndex                   = try_import("PlayerIndex")


# ── Fetch helpers ─────────────────────────────────────────────
def fetch(label, fn, retries=3):
    print(f"  Fetching {label}...", end=" ", flush=True)
    for attempt in range(retries):
        try:
            time.sleep(DELAY * (attempt + 1))
            ep  = fn()
            dfs = ep.get_data_frames()
            if dfs and len(dfs[0]) > 0:
                print(f"✅ {len(dfs[0])} rows")
                return dfs[0]
            print("⚠️  empty")
            return pd.DataFrame()
        except Exception as e:
            if attempt < retries - 1:
                print(f"⚠️  timeout, retrying ({attempt+2}/{retries})...", end=" ", flush=True)
            else:
                print(f"❌ {e}")
    return pd.DataFrame()


def fetch_tracking_endpoint(label, fn, retries=3):
    """Like fetch() but falls back to get_normalized_dict on resultSet KeyErrors."""
    print(f"  Fetching {label}...", end=" ", flush=True)
    for attempt in range(retries):
        try:
            time.sleep(DELAY * (attempt + 1))
            ep = fn()
            try:
                dfs = ep.get_data_frames()
                if dfs and len(dfs[0]) > 0:
                    print(f"✅ {len(dfs[0])} rows")
                    return dfs[0]
            except KeyError:
                nd = ep.get_normalized_dict()
                for v in nd.values():
                    if isinstance(v, list) and v:
                        df = pd.DataFrame(v)
                        if len(df) > 0:
                            print(f"✅ {len(df)} rows (via normalized dict)")
                            return df
            print("⚠️  empty")
            return pd.DataFrame()
        except Exception as e:
            if attempt < retries - 1:
                print(f"⚠️  timeout, retrying ({attempt+2}/{retries})...", end=" ", flush=True)
            else:
                print(f"❌ {e}")
    return pd.DataFrame()


def safe(val, default=None):
    """Convert pandas value to Python native, handling NaN."""
    if val is None:
        return default
    try:
        import math
        if isinstance(val, float) and math.isnan(val):
            return default
        if hasattr(val, 'item'):
            return val.item()
        return val
    except:
        return default

def safe_float(val):
    return safe(val, None)

def safe_int(val):
    v = safe(val, None)
    if v is None:
        return None
    try:
        return int(float(v))
    except:
        return None

def parse_minutes(val):
    """Convert '36:24' or 36.4 to float minutes."""
    try:
        s = str(val).strip()
        if ':' in s:
            parts = s.split(':')
            return int(parts[0]) + int(parts[1]) / 60.0
        return float(s)
    except:
        return None

def normalize_position(pos):
    """Map raw NBA position string to position group."""
    pos = str(pos).strip().upper() if pos else ''
    mapping = {
        # Abbreviations from some endpoints
        'PG': 'G', 'SG': 'G', 'G': 'G',
        'G-F': 'GF', 'F-G': 'GF',
        'SF': 'F', 'PF': 'F', 'F': 'F',
        'F-C': 'FC', 'C-F': 'FC',
        'C': 'C',
        # Full words from LeagueDashPlayerBioStats PLAYER_POSITION
        'GUARD': 'G',
        'GUARD-FORWARD': 'GF', 'FORWARD-GUARD': 'GF',
        'FORWARD': 'F',
        'FORWARD-CENTER': 'FC', 'CENTER-FORWARD': 'FC',
        'CENTER': 'C',
    }
    return mapping.get(pos, 'F')


# ── Main fetch ────────────────────────────────────────────────
def fetch_all(season, season_type):
    print(f"\n{'='*60}")
    print(f"Fetching season data: {season} {season_type}")
    print(f"{'='*60}\n")

    data = {}

    # Game logs
    data['base'] = fetch("Game Logs — Base",
        lambda: PlayerGameLogs(season_nullable=season,
            season_type_nullable=season_type, league_id_nullable="00",
            measure_type_player_game_logs_nullable="Base"))

    data['adv'] = fetch("Game Logs — Advanced",
        lambda: PlayerGameLogs(season_nullable=season,
            season_type_nullable=season_type, league_id_nullable="00",
            measure_type_player_game_logs_nullable="Advanced"))

    data['misc'] = fetch("Game Logs — Misc",
        lambda: PlayerGameLogs(season_nullable=season,
            season_type_nullable=season_type, league_id_nullable="00",
            measure_type_player_game_logs_nullable="Misc"))

    data['scoring'] = fetch("Game Logs — Scoring",
        lambda: PlayerGameLogs(season_nullable=season,
            season_type_nullable=season_type, league_id_nullable="00",
            measure_type_player_game_logs_nullable="Scoring"))

    data['usage'] = fetch("Game Logs — Usage",
        lambda: PlayerGameLogs(season_nullable=season,
            season_type_nullable=season_type, league_id_nullable="00",
            measure_type_player_game_logs_nullable="Usage"))

    data['bio'] = fetch("Player Bio Stats",
        lambda: LeagueDashPlayerBioStats(season=season,
            season_type_all_star=season_type, per_mode_simple="PerGame"))

    data['player_index'] = fetch("Player Index (positions)",
        lambda: PlayerIndex(league_id="00", season=season))

    # Tracking
    for key, measure in [
        ('drives',    'Drives'),
        ('passing',   'Passing'),
        ('pullup',    'PullUpShot'),
        ('catchshoot','CatchShoot'),
        ('post',      'PostTouch'),
        ('speed',     'SpeedDistance'),
        ('def_track', 'Defense'),
    ]:
        data[key] = fetch(f"Tracking — {measure}",
            lambda m=measure: LeagueDashPtStats(season=season,
                season_type_all_star=season_type,
                per_mode_simple="Totals",
                pt_measure_type=m, player_or_team="Player"))

    # Touches uses a different response structure on some API versions
    data['touches'] = fetch_tracking_endpoint("Tracking — Touches",
        lambda: LeagueDashPtStats(season=season,
            season_type_all_star=season_type,
            per_mode_simple="Totals",
            pt_measure_type="Touches", player_or_team="Player"))

    # Defense
    data['def_overall'] = fetch("Defender Shooting — Overall",
        lambda: LeagueDashPtDefend(season=season,
            season_type_all_star=season_type,
            per_mode_simple="Totals", defense_category="Overall"))

    data['def_2pt'] = fetch("Defender Shooting — 2PT",
        lambda: LeagueDashPtDefend(season=season,
            season_type_all_star=season_type,
            per_mode_simple="Totals", defense_category="2 Pointers"))

    data['def_3pt'] = fetch("Defender Shooting — 3PT",
        lambda: LeagueDashPtDefend(season=season,
            season_type_all_star=season_type,
            per_mode_simple="Totals", defense_category="3 Pointers"))

    # Hustle
    data['hustle'] = fetch("Hustle Stats",
        lambda: LeagueHustleStatsPlayer(season=season,
            season_type_all_star=season_type, per_mode_time="Totals"))

    # Source: LeagueDashPlayerStats Defense — exposes DEF_WS
    data['adv_dash'] = fetch("Defense Dash Stats",
        lambda: LeagueDashPlayerStats(season=season,
            season_type_all_star=season_type,
            per_mode_detailed="PerGame",
            measure_type_detailed_defense="Defense"))

    # Matchup data — DEF_FG_PCT (opponent FG% when guarded by this defender)
    data['matchups'] = fetch("Matchup Defense",
        lambda: LeagueSeasonMatchups(season=season,
            season_type_playoffs=season_type,
            per_mode_simple="Totals"))

    # Clutch
    data['clutch'] = fetch("Clutch Stats",
        lambda: LeagueDashPlayerClutch(season=season,
            season_type_all_star=season_type,
            measure_type_detailed_defense="Advanced",
            clutch_time="Last 5 Minutes",
            ahead_behind="Ahead or Behind", point_diff=5))
    data['clutch_base'] = fetch("Clutch Base Stats",
        lambda: LeagueDashPlayerClutch(season=season,
            season_type_all_star=season_type,
            measure_type_detailed_defense="Base",
            clutch_time="Last 5 Minutes",
            ahead_behind="Ahead or Behind", point_diff=5))

    # Synergy
    for key, play_type, grouping in [
        ('syn_iso_off',    'Isolation',    'offensive'),
        ('syn_pnr_off',    'PRBallHandler','offensive'),
        ('syn_pnr_roll',   'PRRollman',    'offensive'),
        ('syn_post_off',   'Postup',       'offensive'),
        ('syn_spotup',     'Spotup',       'offensive'),
        ('syn_transition', 'Transition',   'offensive'),
        ('syn_iso_def',    'Isolation',    'defensive'),
        ('syn_pnr_def',    'PRBallHandler','defensive'),
        ('syn_post_def',   'Postup',       'defensive'),
        ('syn_spotup_def', 'Spotup',       'defensive'),
        ('syn_roll_def',   'PRRollman',    'defensive'),
    ]:
        data[key] = fetch(f"Synergy — {play_type} ({grouping})",
            lambda pt=play_type, g=grouping: SynergyPlayTypes(
                season=season, season_type_all_star=season_type,
                per_mode_simple="PerGame", play_type_nullable=pt,
                type_grouping_nullable=g, player_or_team_abbreviation="P"))

    # Shot locations
    data['shot_zones'] = fetch("Player Shot Locations",
        lambda: LeagueDashPlayerShotLocations(season=season,
            season_type_all_star=season_type, distance_range="By Zone"))

    # Closest defender shooting
    # VT = Very Tight 0-2ft, TG = Tight 2-4ft, OP = Open 4-6ft, WO = Wide Open 6ft+
    LeagueDashPlayerPtShot = try_import("LeagueDashPlayerPtShot")
    for key, close_def_range in [
        ('shot_vt', '0-2 Feet - Very Tight'),
        ('shot_tg', '2-4 Feet - Tight'),
        ('shot_op', '4-6 Feet - Open'),
        ('shot_wo', '6+ Feet - Wide Open'),
    ]:
        data[key] = fetch("Closest Defender - " + close_def_range,
            lambda r=close_def_range: LeagueDashPlayerPtShot(
                season=season,
                season_type_all_star=season_type,
                per_mode_simple="Totals",
                close_def_dist_range_nullable=r))

    return data


# ── Aggregate game logs to season totals ──────────────────────
def aggregate_game_logs(df, value_cols, id_cols=None):
    """
    Aggregate per-game logs to season totals/averages.
    Returns one row per player.
    """
    if df.empty:
        return pd.DataFrame()

    if id_cols is None:
        id_cols = ['PLAYER_ID', 'PLAYER_NAME', 'TEAM_ID', 'TEAM_ABBREVIATION']
    id_cols = [c for c in id_cols if c in df.columns]

    # For per-game stats, average across games
    # For counts (GP), sum
    agg = {}
    for col in value_cols:
        if col not in df.columns:
            continue
        if col == 'GP':
            agg[col] = 'sum'
        else:
            agg[col] = 'mean'

    if not agg:
        return pd.DataFrame()

    grouped = df.groupby(id_cols)[list(agg.keys())].agg(agg).reset_index()
    return grouped


# ── Build player rows ─────────────────────────────────────────
def build_player_rows(data, season, season_type):

    base   = data.get('base',   pd.DataFrame())
    adv    = data.get('adv',    pd.DataFrame())
    misc   = data.get('misc',   pd.DataFrame())
    scoring = data.get('scoring', pd.DataFrame())
    usage  = data.get('usage',  pd.DataFrame())

    if base.empty:
        print("  ❌ No base game log data — aborting")
        return [], []

    # Build player list from base logs
    id_cols     = ['PLAYER_ID', 'PLAYER_NAME', 'TEAM_ID', 'TEAM_ABBREVIATION']
    id_cols     = [c for c in id_cols if c in base.columns]
    players_df  = base[id_cols].drop_duplicates('PLAYER_ID')

    # Aggregate game logs
    # GP = count of rows (one row per game), all stats averaged per game
    base_stat_cols = ['MIN', 'PTS', 'AST', 'REB', 'OREB', 'DREB',
                      'STL', 'BLK', 'TOV', 'PF', 'PFD',
                      'FGM', 'FGA', 'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT',
                      'FTM', 'FTA', 'FT_PCT', 'PLUS_MINUS', 'EFG_PCT']
    base_stat_cols = [c for c in base_stat_cols if c in base.columns]
    base_agg = base.groupby('PLAYER_ID')[base_stat_cols].mean().reset_index()
    # Add GP as row count per player
    gp_counts = base.groupby('PLAYER_ID').size().reset_index(name='GP')
    base_agg  = base_agg.merge(gp_counts, on='PLAYER_ID', how='left')

    # Total minutes (sum, not average)
    if 'MIN' in base.columns:
        # MIN may be formatted as "36:24" strings — parse to float first
        def parse_min(v):
            try:
                v = str(v).strip()
                if ':' in v:
                    p = v.split(':')
                    return int(p[0]) + int(p[1]) / 60.0
                return float(v)
            except:
                return 0.0
        min_series = base['MIN'].apply(parse_min)
        min_total  = min_series.groupby(base['PLAYER_ID']).sum().reset_index()
        min_total.columns = ['PLAYER_ID', 'MIN_TOTAL']
    else:
        min_total = pd.DataFrame(columns=['PLAYER_ID', 'MIN_TOTAL'])

    # Advanced logs
    adv_cols = ['OFF_RATING', 'DEF_RATING', 'NET_RATING', 'AST_PCT', 'AST_TO',
                'AST_RATIO', 'OREB_PCT', 'DREB_PCT', 'REB_PCT', 'TM_TOV_PCT',
                'EFG_PCT', 'TS_PCT', 'USG_PCT', 'PACE', 'PIE', 'POSS']
    adv_agg  = pd.DataFrame()
    if not adv.empty:
        adv_cols = [c for c in adv_cols if c in adv.columns]
        adv_agg  = adv.groupby('PLAYER_ID')[adv_cols].mean().reset_index()

    # Misc logs
    misc_cols = ['PTS_OFF_TOV', 'PTS_2ND_CHANCE', 'PTS_FB',
                 'OPP_PTS_OFF_TOV', 'OPP_PTS_PAINT']
    misc_agg  = pd.DataFrame()
    if not misc.empty:
        misc_cols = [c for c in misc_cols if c in misc.columns]
        misc_agg  = misc.groupby('PLAYER_ID')[misc_cols].mean().reset_index()

    # Scoring logs
    scoring_cols = ['PCT_UAST_2PM', 'PCT_UAST_3PM', 'PCT_UAST_FGM', 'PCT_AST_FGM',
                    'PCT_PTS_PAINT', 'PCT_PTS_3PT', 'PCT_PTS_FT', 'PCT_PTS_2PT_MR',
                    'PTS_PAINT']
    scoring_agg  = pd.DataFrame()
    if not scoring.empty:
        scoring_cols = [c for c in scoring_cols if c in scoring.columns]
        scoring_agg  = scoring.groupby('PLAYER_ID')[scoring_cols].mean().reset_index()

    # Usage logs
    usage_agg = pd.DataFrame()
    if not usage.empty:
        us_cols = ['PCT_FGA', 'PCT_FTA', 'PCT_AST', 'PCT_TOV']
        us_cols = [c for c in us_cols if c in usage.columns]
        usage_agg = usage.groupby('PLAYER_ID')[us_cols].mean().reset_index()

    # Merge everything into players_df
    merged = players_df.merge(base_agg, on='PLAYER_ID', how='left')
    merged = merged.merge(min_total, on='PLAYER_ID', how='left')
    if not adv_agg.empty:
        merged = merged.merge(adv_agg, on='PLAYER_ID', how='left')
    if not misc_agg.empty:
        merged = merged.merge(misc_agg, on='PLAYER_ID', how='left')
    if not scoring_agg.empty:
        merged = merged.merge(scoring_agg, on='PLAYER_ID', how='left')
    if not usage_agg.empty:
        merged = merged.merge(usage_agg, on='PLAYER_ID', how='left')

    # Tracking datasets (already season totals, one row per player)
    tracking_merges = [
        ('drives',     'PLAYER_ID', ['DRIVES', 'DRIVE_FGA', 'DRIVE_FGM', 'DRIVE_FG_PCT',
                                      'DRIVE_PTS', 'DRIVE_AST', 'DRIVE_TOV', 'DRIVE_PF',
                                      'DRIVE_PASSES', 'DRIVE_FT_PCT']),
        ('passing',    'PLAYER_ID', ['PASSES_MADE', 'PASSES_RECEIVED', 'AST_PTS_CREATED',
                                      'SECONDARY_AST', 'POTENTIAL_AST', 'FT_AST',
                                      'AST_TO_PASS_PCT']),
        ('touches',    'PLAYER_ID', ['TOUCHES', 'FRONT_CT_TOUCHES', 'TIME_OF_POSS',
                                      'AVG_SEC_PER_TOUCH', 'AVG_DRIB_PER_TOUCH',
                                      'ELBOW_TOUCHES', 'POST_TOUCHES', 'PAINT_TOUCHES']),
        ('pullup',     'PLAYER_ID', ['PULL_UP_FGA', 'PULL_UP_FGM', 'PULL_UP_FG_PCT',
                                      'PULL_UP_FG3A', 'PULL_UP_FG3_PCT', 'PULL_UP_EFG_PCT']),
        ('catchshoot', 'PLAYER_ID', ['CATCH_SHOOT_FGA', 'CATCH_SHOOT_FGM', 'CATCH_SHOOT_FG_PCT',
                                      'CATCH_SHOOT_FG3A', 'CATCH_SHOOT_FG3_PCT',
                                      'CATCH_SHOOT_EFG_PCT']),
        ('post',       'PLAYER_ID', ['POST_TOUCH_FGA', 'POST_TOUCH_FG_PCT', 'POST_TOUCH_PTS',
                                      'POST_TOUCH_AST', 'POST_TOUCH_TOV']),
        ('speed',      'PLAYER_ID', ['DIST_MILES', 'DIST_MILES_OFF', 'DIST_MILES_DEF',
                                      'AVG_SPEED', 'AVG_SPEED_OFF', 'AVG_SPEED_DEF']),
        ('def_track',  'PLAYER_ID', ['DEF_RIM_FGA', 'DEF_RIM_FGM', 'DEF_RIM_FG_PCT']),
    ]

    for key, join_col, cols in tracking_merges:
        df = data.get(key, pd.DataFrame())
        if not df.empty:
            cols_present = [c for c in cols if c in df.columns]
            if cols_present:
                sub = df[['PLAYER_ID'] + cols_present].copy()
                merged = merged.merge(sub, on='PLAYER_ID', how='left')

    # Defender shooting
    def_overall = data.get('def_overall', pd.DataFrame())
    if not def_overall.empty:
        def_o = def_overall[['CLOSE_DEF_PERSON_ID', 'D_FGA', 'D_FG_PCT',
                               'NORMAL_FG_PCT']].copy()
        def_o.columns = ['PLAYER_ID', 'D_FGA_OVERALL', 'D_FG_PCT_OVERALL', 'NORMAL_FG_PCT']
        merged = merged.merge(def_o, on='PLAYER_ID', how='left')

    def_2pt = data.get('def_2pt', pd.DataFrame())
    if not def_2pt.empty:
        def_2 = def_2pt[['CLOSE_DEF_PERSON_ID', 'FG2A', 'FG2_PCT', 'NS_FG2_PCT']].copy()
        def_2.columns = ['PLAYER_ID', 'D_FGA_2PT', 'D_FG_PCT_2PT', 'NS_FG2_PCT']
        merged = merged.merge(def_2, on='PLAYER_ID', how='left')

    def_3pt = data.get('def_3pt', pd.DataFrame())
    if not def_3pt.empty:
        def_3 = def_3pt[['CLOSE_DEF_PERSON_ID', 'FG3A', 'FG3_PCT', 'NS_FG3_PCT']].copy()
        def_3.columns = ['PLAYER_ID', 'D_FGA_3PT', 'D_FG_PCT_3PT', 'NS_FG3_PCT']
        merged = merged.merge(def_3, on='PLAYER_ID', how='left')

    # Hustle
    hustle = data.get('hustle', pd.DataFrame())
    if not hustle.empty:
        hust_cols = ['PLAYER_ID', 'CONTESTED_SHOTS', 'CONTESTED_2PT_SHOTS',
                     'CONTESTED_3PT_SHOTS', 'DEFLECTIONS', 'CHARGES_DRAWN',
                     'SCREEN_ASSISTS', 'SCREEN_AST_PTS', 'LOOSE_BALLS_RECOVERED',
                     'BOX_OUTS', 'OFF_BOXOUTS', 'DEF_BOXOUTS']
        hust_cols = [c for c in hust_cols if c in hustle.columns]
        merged = merged.merge(hustle[hust_cols], on='PLAYER_ID', how='left')

    # Matchup defense — aggregate DEF_FG_PCT weighted by partial possessions per defender
    matchups = data.get('matchups', pd.DataFrame())
    if not matchups.empty:
        if 'DEF_PLAYER_ID' in matchups.columns and 'MATCHUP_FGA' in matchups.columns and 'MATCHUP_FGM' in matchups.columns:
            mu = matchups[['DEF_PLAYER_ID', 'MATCHUP_FGA', 'MATCHUP_FGM']].copy()
            mu['DEF_PLAYER_ID'] = mu['DEF_PLAYER_ID'].astype(int)
            agg = mu.groupby('DEF_PLAYER_ID').agg(
                MATCHUP_FGA=('MATCHUP_FGA', 'sum'),
                MATCHUP_FGM=('MATCHUP_FGM', 'sum'),
            ).reset_index()
            agg['MATCHUP_DEF_FG_PCT'] = agg.apply(
                lambda r: r['MATCHUP_FGM'] / r['MATCHUP_FGA'] if r['MATCHUP_FGA'] >= 50 else None, axis=1)
            agg = agg[['DEF_PLAYER_ID', 'MATCHUP_DEF_FG_PCT']].rename(columns={'DEF_PLAYER_ID': 'PLAYER_ID'})
            merged = merged.merge(agg, on='PLAYER_ID', how='left')
        else:
            print(f"  matchup: missing expected columns, found: {list(matchups.columns)}")

    # Advanced dash (DEF_WS, OFF_WS, WIN_SHARES)
    adv_dash = data.get('adv_dash', pd.DataFrame())
    if not adv_dash.empty:
        # Try all possible win shares column names
        ws_candidates = ['DEF_WS', 'OFF_WS', 'WS', 'WS_48', 'WIN_SHARES', 'DEF_WIN_SHARES']
        found = [c for c in ws_candidates if c in adv_dash.columns]
        print(f"  Win shares columns found: {found}")
        adv_dash_cols = ['PLAYER_ID'] + found
        if len(found) > 0:
            merged = merged.merge(adv_dash[adv_dash_cols], on='PLAYER_ID', how='left')
    else:
        print("  adv_dash is empty")

    # Clutch
    clutch = data.get('clutch', pd.DataFrame())
    if not clutch.empty:
        cl_cols = ['PLAYER_ID', 'NET_RATING', 'TS_PCT', 'USG_PCT', 'MIN']
        cl_cols = [c for c in cl_cols if c in clutch.columns]
        cl_sub = clutch[cl_cols].copy()
        cl_sub.columns = ['PLAYER_ID'] + [f'CLUTCH_{c}' for c in cl_sub.columns[1:]]
        merged = merged.merge(cl_sub, on='PLAYER_ID', how='left')

    clutch_base = data.get('clutch_base', pd.DataFrame())
    if not clutch_base.empty and 'FGM' in clutch_base.columns:
        cb_sub = clutch_base[['PLAYER_ID', 'FGM']].copy()
        cb_sub.columns = ['PLAYER_ID', 'CLUTCH_FGM']
        merged = merged.merge(cb_sub, on='PLAYER_ID', how='left')

    # Synergy — pick PPP per play type
    synergy_map = [
        ('syn_iso_off',    'ISO_PPP',        'ISO_EFG_PCT',    'ISO_FGA',     'ISO_TOV_PCT'),
        ('syn_pnr_off',    'PNR_BH_PPP',     None,             'PNR_BH_FGA',  None),
        ('syn_pnr_roll',   'PNR_ROLL_PPP',   None,             'PNR_ROLL_POSS', None),
        ('syn_post_off',   'POST_PPP',        None,             'POST_POSS',     None),
        ('syn_spotup',     'SPOTUP_PPP',      'SPOTUP_EFG_PCT', None,          None),
        ('syn_transition', 'TRANSITION_PPP',  None,             'TRANSITION_FGA', None),
        ('syn_iso_def',    'DEF_ISO_PPP',        None, None,               None),
        ('syn_pnr_def',    'DEF_PNR_BH_PPP',     None, None,               None),
        ('syn_post_def',   'DEF_POST_PPP',        None, 'DEF_POST_POSS',    None),
        ('syn_spotup_def', 'DEF_SPOTUP_PPP',      None, 'DEF_SPOTUP_POSS',  None),
        ('syn_roll_def',   'DEF_PNR_ROLL_PPP',    None, 'DEF_PNR_ROLL_POSS',None),
    ]

    for key, ppp_col, efg_col, fga_col, tov_col in synergy_map:
        df = data.get(key, pd.DataFrame())
        if not df.empty and 'PPP' in df.columns:
            syn_cols = ['PLAYER_ID']
            rename = {'PLAYER_ID': 'PLAYER_ID', 'PPP': ppp_col}
            if efg_col and 'EFG_PCT' in df.columns:
                syn_cols.append('EFG_PCT')
                rename['EFG_PCT'] = efg_col
            if fga_col and 'FGA' in df.columns:
                syn_cols.append('FGA')
                rename['FGA'] = fga_col
                if key == 'syn_pnr_off' and 'POSS' in df.columns:
                    syn_cols.append('POSS')
                    rename['POSS'] = 'PNR_BH_POSS'
            elif fga_col and 'POSS' in df.columns:
                # Roll man and post use POSS not FGA
                syn_cols.append('POSS')
                rename['POSS'] = fga_col
            if tov_col and 'TOV_POSS_PCT' in df.columns:
                syn_cols.append('TOV_POSS_PCT')
                rename['TOV_POSS_PCT'] = tov_col

            syn_cols.append('PPP')
            syn_sub = df[[c for c in syn_cols if c in df.columns]].rename(columns=rename)
            merged = merged.merge(syn_sub, on='PLAYER_ID', how='left')

    # Closest defender shooting
    # VT = Very Tight 0-2ft, TG = Tight 2-4ft, OP = Open 4-6ft, WO = Wide Open 6ft+
    for key, suffix in [
        ('shot_vt', '_VT'),
        ('shot_tg', '_TG'),
        ('shot_op', '_OP'),
        ('shot_wo', '_WO'),
    ]:
        df = data.get(key, pd.DataFrame())
        if not df.empty:
            cols_to_keep = ['PLAYER_ID']
            rename_map = {}
            for col in ['FGA', 'FGM', 'FG3A', 'FG3M']:
                if col in df.columns:
                    cols_to_keep.append(col)
                    rename_map[col] = f'{col}{suffix}'
            if len(cols_to_keep) > 1:
                sub = df[cols_to_keep].rename(columns=rename_map)
                merged = merged.merge(sub, on='PLAYER_ID', how='left')

    print(f"\n  Merged dataset: {len(merged)} players, {len(merged.columns)} columns")

    # ── Build player_seasons rows ─────────────────────────────
    season_rows = []
    player_rows = []

    bio = data.get('bio', pd.DataFrame())
    bio_dict = {}
    if not bio.empty:
        for _, row in bio.iterrows():
            pid = safe_int(row.get('PLAYER_ID'))
            if pid:
                bio_dict[pid] = row.to_dict()

    # Build position lookup from PlayerIndex — more reliable than bio stats
    pos_index = {}
    pi = data.get('player_index', pd.DataFrame())
    if not pi.empty:
        for _, row in pi.iterrows():
            pid = safe_int(row.get('PERSON_ID'))
            pos = str(row.get('POSITION', '')).strip()
            if pid and pos and pos != 'nan':
                pos_index[pid] = pos

    def g(row, col, default=None):
        """Safe get from merged row."""
        val = row.get(col)
        if val is None:
            return default
        try:
            import math
            if isinstance(val, float) and math.isnan(val):
                return default
            if hasattr(val, 'item'):
                return val.item()
            return val
        except:
            return default

    for _, row in merged.iterrows():
        pid = safe_int(row.get('PLAYER_ID'))
        if not pid:
            continue

        # Minutes per game
        min_total  = g(row, 'MIN_TOTAL') or 0
        gp         = safe_int(row.get('GP')) or 1
        min_pg     = min_total / gp if gp > 0 else 0
        poss       = g(row, 'POSS')

        # Tracking/hustle stats come from LeagueDashPtStats as season totals.
        # Divide by GP here so the DB stores per-game averages, consistent
        # with base stats (pts, ast, reb, etc.) which are already averaged.
        def pg(key):
            v = safe_float(row.get(key))
            return round(v / gp, 3) if v is not None and gp > 0 else None

        # Bio data
        b = bio_dict.get(pid, {})
        pos_raw    = safe(b.get('PLAYER_HEIGHT_INCHES')) or None
        # Prefer PlayerIndex position (reliable) over bio stats (often empty)
        position   = pos_index.get(pid) or safe(b.get('PLAYER_POSITION', b.get('POSITION', ''))) or ''
        pos_group  = normalize_position(position)

        # Player row
        player_rows.append({
            'player_id':      pid,
            'player_name':    str(row.get('PLAYER_NAME', '')).strip(),
            'position':       position,
            'position_group': pos_group,
            'height_inches':  safe_float(b.get('PLAYER_HEIGHT_INCHES')),
            'weight':         safe_int(b.get('PLAYER_WEIGHT')),
            'draft_year':     safe_int(b.get('DRAFT_YEAR')),
            'draft_round':    safe_int(b.get('DRAFT_ROUND')),
            'draft_number':   safe_int(b.get('DRAFT_NUMBER')),
            'college':        safe(b.get('COLLEGE')),
            'country':        safe(b.get('COUNTRY')),
            'is_active':      True,
        })

        # Season row — keys must exactly match player_seasons columns
        season_rows.append({
            'player_id':   pid,
            'season':      season,
            'season_type': season_type,
            'team_id':     safe_int(row.get('TEAM_ID')),
            'team_abbr':   safe(row.get('TEAM_ABBREVIATION')),

            'gp':          gp,
            'min':         round(min_total, 1) if min_total else None,
            'min_per_game': round(min_pg, 2) if min_pg else None,

            # Base
            'pts':         safe_float(row.get('PTS')),
            'ast':         safe_float(row.get('AST')),
            'reb':         safe_float(row.get('REB')),
            'oreb':        safe_float(row.get('OREB')),
            'dreb':        safe_float(row.get('DREB')),
            'stl':         safe_float(row.get('STL')),
            'blk':         safe_float(row.get('BLK')),
            'tov':         safe_float(row.get('TOV')),
            'pf':          safe_float(row.get('PF')),
            'pfd':         safe_float(row.get('PFD')),
            'fgm':         safe_float(row.get('FGM')),
            'fga':         safe_float(row.get('FGA')),
            # Compute pct from averaged makes/attempts — avoids the bug where
            # games with 0 attempts return FG_PCT=0.0, dragging the mean down.
            'fg_pct':      (safe_float(row.get('FGM')) / safe_float(row.get('FGA')))
                           if safe_float(row.get('FGA')) else None,
            'fg3m':        safe_float(row.get('FG3M')),
            'fg3a':        safe_float(row.get('FG3A')),
            'fg3_pct':     (safe_float(row.get('FG3M')) / safe_float(row.get('FG3A')))
                           if safe_float(row.get('FG3A')) else None,
            'ftm':         safe_float(row.get('FTM')),
            'fta':         safe_float(row.get('FTA')),
            'ft_pct':      (safe_float(row.get('FTM')) / safe_float(row.get('FTA')))
                           if safe_float(row.get('FTA')) else None,
            'plus_minus':  safe_float(row.get('PLUS_MINUS')),

            # Advanced
            'off_rating':  safe_float(row.get('OFF_RATING')),
            'def_rating':  safe_float(row.get('DEF_RATING')),
            'net_rating':  safe_float(row.get('NET_RATING')),
            'ast_pct':     safe_float(row.get('AST_PCT')),
            'ast_to':      safe_float(row.get('AST_TO')),
            'oreb_pct':    safe_float(row.get('OREB_PCT')),
            'dreb_pct':    safe_float(row.get('DREB_PCT')),
            'reb_pct':     safe_float(row.get('REB_PCT')),
            'efg_pct':     safe_float(row.get('EFG_PCT')),
            'ts_pct':      safe_float(row.get('TS_PCT')),
            'usg_pct':     safe_float(row.get('USG_PCT')),
            'pie':         safe_float(row.get('PIE')),

            # Win shares (only def_ws exists in DB)
            'def_ws':      safe_float(row.get('DEF_WS')),

            # Scoring breakdown
            'pts_paint':       safe_float(row.get('PTS_PAINT')),
            'pct_uast_fgm':    safe_float(row.get('PCT_UAST_FGM')),
            'pct_pts_paint':   safe_float(row.get('PCT_PTS_PAINT')),
            'pct_pts_3pt':     safe_float(row.get('PCT_PTS_3PT')),
            'pct_pts_ft':      safe_float(row.get('PCT_PTS_FT')),

            # Tracking: drives (season totals ÷ GP → per-game)
            'drives':        pg('DRIVES'),
            'drive_fga':     pg('DRIVE_FGA'),
            'drive_fgm':     pg('DRIVE_FGM'),
            'drive_fg_pct':  safe_float(row.get('DRIVE_FG_PCT')),   # rate
            'drive_pts':     pg('DRIVE_PTS'),
            'drive_ast':     pg('DRIVE_AST'),
            'drive_tov':     pg('DRIVE_TOV'),
            'drive_pf':      pg('DRIVE_PF'),
            'drive_passes':  pg('DRIVE_PASSES'),

            # Tracking: passing
            'passes_made':     pg('PASSES_MADE'),
            'passes_received': pg('PASSES_RECEIVED'),
            'ast_pts_created': pg('AST_PTS_CREATED'),
            'secondary_ast':   pg('SECONDARY_AST'),
            'potential_ast':   pg('POTENTIAL_AST'),
            'ft_ast':          pg('FT_AST'),

            # Tracking: touches
            'touches':            pg('TOUCHES'),
            'time_of_poss':       pg('TIME_OF_POSS'),
            'avg_sec_per_touch':  safe_float(row.get('AVG_SEC_PER_TOUCH')),  # rate
            'avg_drib_per_touch': safe_float(row.get('AVG_DRIB_PER_TOUCH')), # rate
            'elbow_touches':      pg('ELBOW_TOUCHES'),
            'post_touches':       pg('POST_TOUCHES'),
            'paint_touches':      pg('PAINT_TOUCHES'),

            # Tracking: pull-up
            'pull_up_fga':     pg('PULL_UP_FGA'),
            'pull_up_fgm':     pg('PULL_UP_FGM'),
            'pull_up_fg_pct':  safe_float(row.get('PULL_UP_FG_PCT')),   # rate
            'pull_up_fg3a':    pg('PULL_UP_FG3A'),
            'pull_up_fg3_pct': safe_float(row.get('PULL_UP_FG3_PCT')),  # rate
            'pull_up_efg_pct': safe_float(row.get('PULL_UP_EFG_PCT')),  # rate

            # Tracking: catch & shoot
            'cs_fga':     pg('CATCH_SHOOT_FGA'),
            'cs_fgm':     pg('CATCH_SHOOT_FGM'),
            'cs_fg_pct':  safe_float(row.get('CATCH_SHOOT_FG_PCT')),   # rate
            'cs_fg3a':    pg('CATCH_SHOOT_FG3A'),
            'cs_fg3_pct': safe_float(row.get('CATCH_SHOOT_FG3_PCT')),  # rate
            'cs_efg_pct': safe_float(row.get('CATCH_SHOOT_EFG_PCT')),  # rate

            # Tracking: post-up
            'post_touch_fga':    pg('POST_TOUCH_FGA'),
            'post_touch_fg_pct': safe_float(row.get('POST_TOUCH_FG_PCT')),  # rate
            'post_touch_pts':    pg('POST_TOUCH_PTS'),
            'post_touch_ast':    pg('POST_TOUCH_AST'),
            'post_touch_tov':    pg('POST_TOUCH_TOV'),

            # Tracking: speed (dist is per-game; avg_speed is a rate)
            'dist_miles':     pg('DIST_MILES'),
            'dist_miles_off': pg('DIST_MILES_OFF'),
            'dist_miles_def': pg('DIST_MILES_DEF'),
            'avg_speed':      safe_float(row.get('AVG_SPEED')),      # rate
            'avg_speed_off':  safe_float(row.get('AVG_SPEED_OFF')),  # rate
            'avg_speed_def':  safe_float(row.get('AVG_SPEED_DEF')),  # rate

            # Tracking: rim defense
            'def_rim_fga':    pg('DEF_RIM_FGA'),
            'def_rim_fgm':    pg('DEF_RIM_FGM'),
            'def_rim_fg_pct': safe_float(row.get('DEF_RIM_FG_PCT')),  # rate

            # Hustle
            'contested_shots': pg('CONTESTED_SHOTS'),
            'contested_2pt':   pg('CONTESTED_2PT_SHOTS'),
            'contested_3pt':   pg('CONTESTED_3PT_SHOTS'),
            'deflections':     pg('DEFLECTIONS'),
            'charges_drawn':   pg('CHARGES_DRAWN'),
            'screen_assists':  pg('SCREEN_ASSISTS'),
            'screen_ast_pts':  pg('SCREEN_AST_PTS'),
            'loose_balls':     pg('LOOSE_BALLS_RECOVERED'),
            'box_outs':        pg('BOX_OUTS'),
            'off_box_outs':    pg('OFF_BOXOUTS'),
            'def_box_outs':    pg('DEF_BOXOUTS'),

            # Closest defender shooting (counts ÷ GP; percentages are rates)
            'cd_fga_vt':  pg('FGA_VT'),
            'cd_fgm_vt':  pg('FGM_VT'),
            'cd_fg3a_vt': pg('FG3A_VT'),
            'cd_fg3m_vt': pg('FG3M_VT'),
            'cd_fga_tg':  pg('FGA_TG'),
            'cd_fgm_tg':  pg('FGM_TG'),
            'cd_fg3a_tg': pg('FG3A_TG'),
            'cd_fg3m_tg': pg('FG3M_TG'),
            'cd_fga_op':  pg('FGA_OP'),
            'cd_fgm_op':  pg('FGM_OP'),
            'cd_fg3a_op': pg('FG3A_OP'),
            'cd_fg3m_op': pg('FG3M_OP'),
            'cd_fga_wo':  pg('FGA_WO'),
            'cd_fgm_wo':  pg('FGM_WO'),
            'cd_fg3a_wo': pg('FG3A_WO'),
            'cd_fg3m_wo': pg('FG3M_WO'),

            # Synergy
            'iso_ppp':          safe_float(row.get('ISO_PPP')),
            'iso_fga':          safe_float(row.get('ISO_FGA')),
            'iso_efg_pct':      safe_float(row.get('ISO_EFG_PCT')),
            'iso_tov_pct':      safe_float(row.get('ISO_TOV_PCT')),
            'pnr_bh_ppp':       safe_float(row.get('PNR_BH_PPP')),
            'pnr_bh_fga':       safe_float(row.get('PNR_BH_FGA')),
            'pnr_roll_ppp':     safe_float(row.get('PNR_ROLL_PPP')),
            'pnr_roll_poss':    safe_float(row.get('PNR_ROLL_POSS')),
            'post_ppp':         safe_float(row.get('POST_PPP')),
            'post_poss':        safe_float(row.get('POST_POSS')),
            'spotup_ppp':       safe_float(row.get('SPOTUP_PPP')),
            'spotup_efg_pct':   safe_float(row.get('SPOTUP_EFG_PCT')),
            'transition_ppp':   safe_float(row.get('TRANSITION_PPP')),
            'transition_fga':   safe_float(row.get('TRANSITION_FGA')),
            'def_iso_ppp':      safe_float(row.get('DEF_ISO_PPP')),
            'def_pnr_bh_ppp':   safe_float(row.get('DEF_PNR_BH_PPP')),
            'def_post_ppp':     safe_float(row.get('DEF_POST_PPP')),
            'def_spotup_ppp':   safe_float(row.get('DEF_SPOTUP_PPP')),
            'def_pnr_roll_ppp': safe_float(row.get('DEF_PNR_ROLL_PPP')),

            # Clutch
            'clutch_net_rating': safe_float(row.get('CLUTCH_NET_RATING')),
            'clutch_ts_pct':     safe_float(row.get('CLUTCH_TS_PCT')),
            'clutch_usg_pct':    safe_float(row.get('CLUTCH_USG_PCT')),
            'clutch_min':        safe_float(row.get('CLUTCH_MIN')),
            'clutch_fgm':        round(safe_float(row.get('CLUTCH_FGM')) / gp, 2) if safe_float(row.get('CLUTCH_FGM')) is not None and gp > 0 else None,

            # Closest defender shooting
            # VT = Very Tight 0-2ft, TG = Tight 2-4ft, OP = Open 4-6ft, WO = Wide Open 6ft+
            'cd_fga_vt':  safe_float(row.get('FGA_VT')),
            'cd_fgm_vt':  safe_float(row.get('FGM_VT')),
            'cd_fg3a_vt': safe_float(row.get('FG3A_VT')),
            'cd_fg3m_vt': safe_float(row.get('FG3M_VT')),
            'cd_fga_tg':  safe_float(row.get('FGA_TG')),
            'cd_fgm_tg':  safe_float(row.get('FGM_TG')),
            'cd_fg3a_tg': safe_float(row.get('FG3A_TG')),
            'cd_fg3m_tg': safe_float(row.get('FG3M_TG')),
            'cd_fga_op':  safe_float(row.get('FGA_OP')),
            'cd_fgm_op':  safe_float(row.get('FGM_OP')),
            'cd_fg3a_op': safe_float(row.get('FG3A_OP')),
            'cd_fg3m_op': safe_float(row.get('FG3M_OP')),
            'cd_fga_wo':  safe_float(row.get('FGA_WO')),
            'cd_fgm_wo':  safe_float(row.get('FGM_WO')),
            'cd_fg3a_wo': safe_float(row.get('FG3A_WO')),
            'cd_fg3m_wo': safe_float(row.get('FG3M_WO')),
        })

    return player_rows, season_rows


# ── Upsert to database ────────────────────────────────────────
def upsert_players(conn, rows):
    if not rows:
        return
    cur = conn.cursor()
    sql = """
        INSERT INTO players (
            player_id, player_name, position, position_group,
            height_inches, weight, draft_year, draft_round, draft_number,
            college, country, is_active, updated_at
        ) VALUES %s
        ON CONFLICT (player_id) DO UPDATE SET
            player_name    = EXCLUDED.player_name,
            -- Only overwrite position if the new value is non-empty
            -- Prevents fetch_season from clobbering positions set by fix_positions.py
            position       = CASE WHEN EXCLUDED.position IS NOT NULL AND EXCLUDED.position != ''
                             THEN EXCLUDED.position ELSE players.position END,
            position_group = CASE WHEN EXCLUDED.position IS NOT NULL AND EXCLUDED.position != ''
                             THEN EXCLUDED.position_group ELSE players.position_group END,
            height_inches  = EXCLUDED.height_inches,
            weight         = EXCLUDED.weight,
            draft_year     = EXCLUDED.draft_year,
            draft_round    = EXCLUDED.draft_round,
            draft_number   = EXCLUDED.draft_number,
            college        = EXCLUDED.college,
            country        = EXCLUDED.country,
            is_active      = EXCLUDED.is_active,
            updated_at     = NOW()
    """
    values = [(
        r['player_id'], r['player_name'], r['position'], r['position_group'],
        r['height_inches'], r['weight'], r['draft_year'], r['draft_round'],
        r['draft_number'], r['college'], r['country'], r['is_active'], datetime.now()
    ) for r in rows]
    execute_values(cur, sql, values)
    conn.commit()
    cur.close()
    print(f"  ✅ Upserted {len(rows)} players")


def upsert_seasons(conn, rows):
    if not rows:
        return

    cols = [k for k in rows[0].keys()]

    col_str     = ', '.join(cols)
    placeholder = ', '.join(['%s'] * len(cols))
    update_str  = ', '.join([f"{c} = EXCLUDED.{c}" for c in cols
                              if c not in ('player_id', 'season', 'season_type')])
    sql = f"""
        INSERT INTO player_seasons ({col_str}, updated_at)
        VALUES ({placeholder}, NOW())
        ON CONFLICT (player_id, season, season_type) DO UPDATE SET
            {update_str},
            updated_at = NOW()
    """

    CHUNK = 25
    total = 0
    for i in range(0, len(rows), CHUNK):
        chunk = rows[i:i+CHUNK]
        values = [tuple(r[c] for c in cols) for r in chunk]
        cur = conn.cursor()
        cur.executemany(sql, values)
        conn.commit()
        cur.close()
        total += len(chunk)
        print(f"  ... {total}/{len(rows)} season rows written", end='\r')
    print(f"  ✅ Upserted {len(rows)} season rows          ")


# ── Shot zones ────────────────────────────────────────────────
def upsert_shot_zones(conn, df, season):
    if df.empty:
        return

    zone_cols = {
        'Restricted Area':       ('Restricted Area_FGM', 'Restricted Area_FGA', 'Restricted Area_FG_PCT'),
        'In The Paint (Non-RA)': ('In The Paint (Non-RA)_FGM', 'In The Paint (Non-RA)_FGA', 'In The Paint (Non-RA)_FG_PCT'),
        'Mid-Range':             ('Mid-Range_FGM', 'Mid-Range_FGA', 'Mid-Range_FG_PCT'),
        'Left Corner 3':         ('Left Corner 3_FGM', 'Left Corner 3_FGA', 'Left Corner 3_FG_PCT'),
        'Right Corner 3':        ('Right Corner 3_FGM', 'Right Corner 3_FGA', 'Right Corner 3_FG_PCT'),
        'Above the Break 3':     ('Above the Break 3_FGM', 'Above the Break 3_FGA', 'Above the Break 3_FG_PCT'),
    }

    league_avg = {}
    for zone, (fgm_col, fga_col, pct_col) in zone_cols.items():
        if fga_col in df.columns and fgm_col in df.columns:
            total_fga = df[fga_col].sum()
            total_fgm = df[fgm_col].sum()
            league_avg[zone] = total_fgm / total_fga if total_fga > 0 else 0.44

    cur  = conn.cursor()
    rows = []
    for _, row in df.iterrows():
        pid = safe_int(row.get('PLAYER_ID'))
        if not pid:
            continue
        for zone, (fgm_col, fga_col, pct_col) in zone_cols.items():
            fga = safe_int(row.get(fga_col))
            fgm = safe_int(row.get(fgm_col))
            pct = safe_float(row.get(pct_col))
            if fga is not None:
                rows.append((pid, season, zone, fga, fgm, pct,
                             league_avg.get(zone), datetime.now()))

    sql = """
        INSERT INTO player_shot_zones
            (player_id, season, zone, fga, fgm, fg_pct, league_fg_pct, updated_at)
        VALUES %s
        ON CONFLICT (player_id, season, zone) DO UPDATE SET
            fga           = EXCLUDED.fga,
            fgm           = EXCLUDED.fgm,
            fg_pct        = EXCLUDED.fg_pct,
            league_fg_pct = EXCLUDED.league_fg_pct,
            updated_at    = NOW()
    """
    execute_values(cur, sql, rows)
    conn.commit()
    cur.close()
    print(f"  ✅ Upserted {len(rows)} shot zone rows")


# ── Targeted group updaters ────────────────────────────────────
# Each function fetches a specific group of endpoints and does a
# direct UPDATE on only those columns — safe to run without a full ingest.

def _targeted_update(conn, updates, sql):
    """Execute a list of update tuples against a parameterised SQL statement."""
    if not updates:
        print("  ⚠️  No rows to update")
        return
    cur = conn.cursor()
    cur.executemany(sql, updates)
    conn.commit()
    cur.close()
    print(f"  ✅ Updated {len(updates)} rows")


def update_group_clutch(season, season_type):
    print("\nUpdating clutch stats...")
    clutch_adv  = fetch("Clutch Advanced Stats",
        lambda: LeagueDashPlayerClutch(season=season,
            season_type_all_star=season_type,
            measure_type_detailed_defense="Advanced",
            clutch_time="Last 5 Minutes",
            ahead_behind="Ahead or Behind", point_diff=5))
    clutch_base = fetch("Clutch Base Stats",
        lambda: LeagueDashPlayerClutch(season=season,
            season_type_all_star=season_type,
            measure_type_detailed_defense="Base",
            clutch_time="Last 5 Minutes",
            ahead_behind="Ahead or Behind", point_diff=5))

    if clutch_adv.empty:
        print("  ❌ No clutch data returned")
        return

    # Build adv lookup by player_id
    adv_map = {safe_int(r['PLAYER_ID']): r for _, r in clutch_adv.iterrows() if safe_int(r.get('PLAYER_ID'))}
    # Build base lookup by player_id — FGM is per-game (endpoint default)
    base_map = {}
    if not clutch_base.empty and 'FGM' in clutch_base.columns:
        base_map = {safe_int(r['PLAYER_ID']): r for _, r in clutch_base.iterrows() if safe_int(r.get('PLAYER_ID'))}

    updates = []
    for pid, adv in adv_map.items():
        base = base_map.get(pid, {})
        gp   = safe_int(adv.get('GP'))
        fgm  = safe_float(base.get('FGM'))
        fgm_pg = round(fgm / gp, 2) if (fgm is not None and gp and gp > 0) else None
        updates.append((
            safe_float(adv.get('NET_RATING')),
            safe_float(adv.get('TS_PCT')),
            safe_float(adv.get('USG_PCT')),
            safe_float(adv.get('MIN')),
            fgm_pg,
            pid, season, season_type,
        ))

    conn = psycopg2.connect(DATABASE_URL)
    _targeted_update(conn, updates, """
        UPDATE player_seasons SET
            clutch_net_rating = %s,
            clutch_ts_pct     = %s,
            clutch_usg_pct    = %s,
            clutch_min        = %s,
            clutch_fgm        = %s
        WHERE player_id = %s AND season = %s AND season_type = %s
    """)
    conn.close()


def update_group_tracking(season, season_type):
    print("\nUpdating tracking stats...")
    data = {}
    for key, measure in [
        ('drives',     'Drives'),
        ('passing',    'Passing'),
        ('pullup',     'PullUpShot'),
        ('catchshoot', 'CatchShoot'),
        ('post',       'PostTouch'),
        ('speed',      'SpeedDistance'),
        ('def_track',  'Defense'),
    ]:
        data[key] = fetch(f"Tracking — {measure}",
            lambda m=measure: LeagueDashPtStats(season=season,
                season_type_all_star=season_type,
                per_mode_simple="Totals",
                pt_measure_type=m, player_or_team="Player"))
    data['touches'] = fetch_tracking_endpoint("Tracking — Touches",
        lambda: LeagueDashPtStats(season=season,
            season_type_all_star=season_type,
            per_mode_simple="Totals",
            pt_measure_type="Touches", player_or_team="Player"))

    # Re-use build_player_rows with only tracking data + a minimal base stub
    # by injecting the tracking frames into a partial data dict then doing a
    # targeted UPDATE via the existing merge logic in build_player_rows.
    # Simpler: run through build_player_rows requires base game logs for player IDs.
    # Instead fetch a lightweight base to get player list.
    data['base'] = fetch("Game Logs — Base (player list)",
        lambda: PlayerGameLogs(season_nullable=season,
            season_type_nullable=season_type, league_id_nullable="00",
            measure_type_player_game_logs_nullable="Base"))

    print("  Building rows...")
    _, season_rows = build_player_rows(data, season, season_type)
    if not season_rows:
        print("  ❌ No rows built")
        return

    conn = psycopg2.connect(DATABASE_URL)
    tracking_cols = [
        'drives','drive_fga','drive_fgm','drive_fg_pct','drive_pts',
        'drive_ast','drive_tov','drive_passes','drive_pf',
        'passes_made','passes_received','ast_pts_created','potential_ast',
        'secondary_ast','ft_ast','touches','time_of_poss',
        'avg_sec_per_touch','avg_drib_per_touch',
        'elbow_touches','post_touches','paint_touches',
        'pull_up_fga','pull_up_fgm','pull_up_fg_pct',
        'pull_up_fg3a','pull_up_fg3_pct','pull_up_efg_pct',
        'cs_fga','cs_fgm','cs_fg_pct','cs_fg3a','cs_fg3_pct','cs_efg_pct',
        'post_touch_fga','post_touch_fg_pct','post_touch_pts',
        'post_touch_ast','post_touch_tov',
        'dist_miles','dist_miles_off','dist_miles_def',
        'avg_speed','avg_speed_off','avg_speed_def',
    ]
    set_clause = ', '.join(f"{c} = %s" for c in tracking_cols)
    sql = f"UPDATE player_seasons SET {set_clause} WHERE player_id = %s AND season = %s AND season_type = %s"
    updates = [
        tuple(r.get(c) for c in tracking_cols) + (r['player_id'], r['season'], r['season_type'])
        for r in season_rows
    ]
    _targeted_update(conn, updates, sql)
    conn.close()


def update_group_synergy(season, season_type):
    print("\nUpdating synergy stats...")
    data = {'base': fetch("Game Logs — Base (player list)",
        lambda: PlayerGameLogs(season_nullable=season,
            season_type_nullable=season_type, league_id_nullable="00",
            measure_type_player_game_logs_nullable="Base"))}
    for key, play_type, grouping in [
        ('syn_iso_off',    'Isolation',    'offensive'),
        ('syn_pnr_off',    'PRBallHandler','offensive'),
        ('syn_pnr_roll',   'PRRollman',    'offensive'),
        ('syn_post_off',   'Postup',       'offensive'),
        ('syn_spotup',     'Spotup',       'offensive'),
        ('syn_transition', 'Transition',   'offensive'),
        ('syn_iso_def',    'Isolation',    'defensive'),
        ('syn_pnr_def',    'PRBallHandler','defensive'),
        ('syn_post_def',   'Postup',       'defensive'),
        ('syn_spotup_def', 'Spotup',       'defensive'),
        ('syn_roll_def',   'PRRollman',    'defensive'),
    ]:
        data[key] = fetch(f"Synergy — {play_type} ({grouping})",
            lambda pt=play_type, g=grouping: SynergyPlayTypes(
                season=season, season_type_all_star=season_type,
                per_mode_simple="PerGame", play_type_nullable=pt,
                type_grouping_nullable=g, player_or_team_abbreviation="P"))

    _, season_rows = build_player_rows(data, season, season_type)
    if not season_rows:
        print("  ❌ No rows built")
        return

    conn = psycopg2.connect(DATABASE_URL)
    syn_cols = [
        'iso_ppp','iso_fga','iso_efg_pct','iso_tov_pct',
        'pnr_bh_ppp','pnr_bh_fga','pnr_bh_poss',
        'pnr_roll_ppp','pnr_roll_poss',
        'post_ppp','post_poss','spotup_ppp','spotup_efg_pct',
        'transition_ppp','transition_fga',
        'def_iso_ppp','def_pnr_bh_ppp','def_post_ppp',
        'def_spotup_ppp','def_pnr_roll_ppp',
    ]
    set_clause = ', '.join(f"{c} = %s" for c in syn_cols)
    sql = f"UPDATE player_seasons SET {set_clause} WHERE player_id = %s AND season = %s AND season_type = %s"
    updates = [
        tuple(r.get(c) for c in syn_cols) + (r['player_id'], r['season'], r['season_type'])
        for r in season_rows
    ]
    _targeted_update(conn, updates, sql)
    conn.close()


GROUP_UPDATERS = {
    'clutch':   update_group_clutch,
    'tracking': update_group_tracking,
    'synergy':  update_group_synergy,
}


# ── Main ──────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--season',      default=SEASON)
    parser.add_argument('--season-type', default=SEASON_TYPE)
    parser.add_argument('--only', choices=list(GROUP_UPDATERS.keys()),
                        help='Update only a specific group of stats')
    args = parser.parse_args()

    season      = args.season
    season_type = args.season_type

    print(f"\nThe Impact Board — Season Ingestion")
    print(f"Season: {season} | Type: {season_type}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")

    if args.only:
        GROUP_UPDATERS[args.only](season, season_type)
        print(f"\n✅ Done: {args.only} | {season} {season_type}")
        return

    data = fetch_all(season, season_type)

    print(f"\nBuilding rows...")
    player_rows, season_rows = build_player_rows(data, season, season_type)
    print(f"  {len(player_rows)} players")
    print(f"  {len(season_rows)} season rows")

    # Deduplicate — players who changed teams appear multiple times
    seen_players = {}
    for r in player_rows:
        seen_players[r['player_id']] = r
    player_rows = list(seen_players.values())

    seen_seasons = {}
    for r in season_rows:
        seen_seasons[r['player_id']] = r
    season_rows = list(seen_seasons.values())

    print(f"  {len(player_rows)} players (after dedup)")
    print(f"  {len(season_rows)} season rows (after dedup)")

    print(f"\nWriting to database...")
    conn = psycopg2.connect(DATABASE_URL)

    upsert_players(conn, player_rows)
    upsert_seasons(conn, season_rows)

    if not data.get('shot_zones', pd.DataFrame()).empty:
        upsert_shot_zones(conn, data['shot_zones'], season)

    conn.close()

    print(f"\n{'='*60}")
    print(f"✅ Ingestion complete: {season} {season_type}")
    print(f"   {len(player_rows)} players")
    print(f"   {len(season_rows)} season rows")
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*60}\n")
    print(f"Next step: python backend/ingest/compute_metrics.py --season {season}")


if __name__ == "__main__":
    main()