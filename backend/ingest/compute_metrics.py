"""
The Impact Board — Metrics Computation
========================================
python backend/ingest/compute_metrics.py

Computes derived metrics and percentile-based composites
for all players in a given season.

Composite scores use win-correlation weights:
  - Pearson r(stat, net_pts100) computed across 500+ min players
  - Weights normalized min-max within each sub-composite (best predictor = 1.0, worst = 0.1)
  - Same weights reused by the Builder's Impact Mode

Usage:
    python backend/ingest/compute_metrics.py
    python backend/ingest/compute_metrics.py --season 2023-24
"""

import os
import sys
import math
import json
import argparse
from datetime import datetime
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras
import numpy as np

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')
SEASON       = os.getenv('NBA_SEASON',      '2024-25')
SEASON_TYPE  = os.getenv('NBA_SEASON_TYPE', 'Regular Season')

if not DATABASE_URL:
    print("❌ DATABASE_URL not set.")
    sys.exit(1)

# ── Constants ─────────────────────────────────────────────────
MIN_MINUTES_TOTAL  = 1000
MIN_MINUTES_CORR   = 500   # lower threshold for correlation pool only
MIN_FGA_PER_GAME   = 2.0
MIN_PULL_UP_FGA    = 50
MIN_CS_FGA         = 50
MIN_DEF_FGA        = 150
MIN_RIM_FGA        = 20
RIM_XFG_BASELINE   = 0.650

LG_CONTESTED_EFG   = 0.398
LG_OPEN_EFG        = 0.548

ZONE_MIN_FGA_PG = {
    'paint':        2.0,
    'midrange':     1.0,
    'corner3':      0.5,
    'above_break3': 1.5,
}

# ── Sub-composite stat definitions ────────────────────────────
# Maps sub-composite name → raw stat keys that feed it.
# Keys must match what compute_player_metrics() puts in the metrics dict
# OR what seasons_map contains, and what the cols_srcs lists use.
# For inverted stats (e.g. tov_pct used as tov_pct_inv), use the BASE key.
SUBCOMP_STATS = {
    'finishing_score': [
        'paint_efg_vw', 'paint_scoring_rate', 'drive_pts_per_drive',
        'drive_foul_rate', 'pnr_roll_ppp', 'post_ppp', 'transition_ppp',
    ],
    'shooting_score': [
        'all3_efg_vw', 'midrange_efg_vw', 'sq_fg_pct_above_expected',
    ],
    'shot_creation_score': [
        'pct_uast_fgm', 'iso_ppp', 'pull_up_efg_pct', 'drive_fg_pct',
        'usg_pct', 'tov_pct', 'leverage_shooting',
    ],
    'passing_score': [
        'pot_ast_per_tov', 'ast_pct', 'pass_quality_index',
    ],
    'creation_score': [
        'leverage_creation', 'ast_pts_created_pg', 'ft_ast_per75',
    ],
    'decision_making_score': [
        'lost_ball_tov_pg', 'pnr_bh_ppp',
    ],
    'perimeter_def_score': [
        'def_delta_3pt', 'def_delta_overall', 'def_disruption_rate',
        'contested_shots', 'stl', 'def_spotup_ppp',
    ],
    'interior_def_score': [
        'rim_protection_score', 'def_delta_2pt', 'dreb_pct', 'blk',
        'def_post_ppp', 'def_pnr_roll_ppp',
    ],
    'activity_score': [
        'motor_score', 'hustle_composite', 'screen_assist_rate',
    ],
    'rebounding_score': [
        'dreb_pct', 'oreb_pct', 'box_out_rate',
    ],
}

# Category composites: maps category → sub-composite keys
CATCOMP_STATS = {
    'creator_score':   ['finishing_score', 'shooting_score', 'shot_creation_score'],
    'playmaker_score': ['passing_score', 'creation_score', 'decision_making_score'],
    'defender_score':  ['perimeter_def_score', 'interior_def_score'],
    'hustle_score':    ['activity_score', 'rebounding_score'],
}

# Stats where lower raw value = better; flip sign before weighting
LOWER_BETTER = {
    'tov_pct', 'lost_ball_tov_pg', 'bad_pass_tov_pg',
    'def_iso_ppp', 'def_pnr_bh_ppp', 'def_post_ppp',
    'def_spotup_ppp', 'def_pnr_roll_ppp', 'matchup_def_fg_pct',
}

# Inverted col name → base stat key (for weight lookup)
INVERT_MAP = {
    'tov_pct_inv':          'tov_pct',
    'lost_ball_tov_pg_inv': 'lost_ball_tov_pg',
    'def_spotup_ppp_inv':   'def_spotup_ppp',
    'def_post_ppp_inv':     'def_post_ppp',
    'def_pnr_roll_ppp_inv': 'def_pnr_roll_ppp',
}


# ── Utility ───────────────────────────────────────────────────
def div(a, b, default=None):
    if a is None or b is None: return default
    if b == 0: return default
    return a / b

def safe(val, default=None):
    if val is None: return default
    try:
        if isinstance(val, float) and math.isnan(val): return default
        return float(val)
    except: return default

def s(val, default=0.0):
    return safe(val, default)


# ── Pearson correlation ───────────────────────────────────────
def pearson(xs, ys):
    n = len(xs)
    if n < 10:
        return None
    mx = sum(xs) / n
    my = sum(ys) / n
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    dx  = sum((x - mx) ** 2 for x in xs) ** 0.5
    dy  = sum((y - my) ** 2 for y in ys) ** 0.5
    if dx == 0 or dy == 0:
        return None
    return round(num / (dx * dy), 4)


# ── Win correlation computation ───────────────────────────────
def compute_win_correlations(metrics_list, seasons_map, season, season_type):
    """
    Compute Pearson r(stat, net_pts100) for every stat that feeds a composite.
    Uses players with 500+ min AND net_pts100 data.

    Returns:
        raw_correlations: {stat_key: r}  (signed, lower-better flipped positive)
        subcomp_weights:  {comp_name: {stat_key: weight 0.1-1.0}}
        catcomp_weights:  {cat_name:  {sub_key:  weight 0.1-1.0}}
        flat_weights:     {stat_key:  weight}  (global, for Builder compat)
    """
    pid_to_metrics = {m['player_id']: m for m in metrics_list}

    # Pool: 500+ min AND has net_pts100
    qualifying_pids = [
        pid for pid, ps in seasons_map.items()
        if float(ps.get('min') or 0) >= MIN_MINUTES_CORR
        and ps.get('net_pts100') is not None
    ]
    print(f"  Correlation pool: {len(qualifying_pids)} players "
          f"(≥{MIN_MINUTES_CORR} min, net_pts100 available)")

    net_map = {pid: float(seasons_map[pid]['net_pts100']) for pid in qualifying_pids}

    def get_val(pid, key):
        m  = pid_to_metrics.get(pid, {})
        ps = seasons_map.get(pid, {})
        v  = m.get(key) if key in m else ps.get(key)
        if v is None:
            return None
        try:
            f = float(v)
            return None if math.isnan(f) else f
        except (TypeError, ValueError):
            return None

    # All unique stat keys across sub-composites + category composites
    all_stat_keys = set()
    for keys in SUBCOMP_STATS.values():
        all_stat_keys.update(keys)
    for keys in CATCOMP_STATS.values():
        all_stat_keys.update(keys)   # sub-composite score keys

    # Compute raw Pearson correlations
    raw_correlations = {}
    for key in all_stat_keys:
        pairs = [
            (get_val(pid, key), net_map[pid])
            for pid in qualifying_pids
            if get_val(pid, key) is not None
        ]
        if len(pairs) < 10:
            continue
        xs, ys = zip(*pairs)
        r = pearson(list(xs), list(ys))
        if r is not None:
            # Flip sign for lower-is-better stats so |r| always means "predicts winning"
            if key in LOWER_BETTER:
                r = -r
            raw_correlations[key] = r

    # ── Sub-composite weights = raw |r| ──────────────────────
    # Weight IS the absolute Pearson r — no normalization.
    # Stats with no correlation data fall back to weight=1.0.
    subcomp_weights = {}
    for comp_name, stat_keys in SUBCOMP_STATS.items():
        weights = {}
        for k in stat_keys:
            if k in raw_correlations:
                weights[k] = round(abs(raw_correlations[k]), 4)
            else:
                weights[k] = None
        subcomp_weights[comp_name] = weights

    # catcomp_weights kept for API compat but unused in scoring
    catcomp_weights = {}

    # ── Global normalized weights (Builder UI only) ────────────
    all_abs = [abs(v) for v in raw_correlations.values()]
    if all_abs:
        g_min = min(all_abs)
        g_max = max(all_abs)
        g_rng = g_max - g_min if g_max != g_min else 1.0
        flat_weights = {
            k: round(0.1 + 0.9 * (abs(v) - g_min) / g_rng, 4)
            for k, v in raw_correlations.items()
        }
    else:
        flat_weights = {}

    # ── Save JSON ─────────────────────────────────────────────
    out_dir  = os.path.join(os.path.dirname(__file__), 'data')
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(
        out_dir, f'win_correlations_{season.replace("-","_")}.json'
    )
    output = {
        'season':           season,
        'season_type':      season_type,
        'n_players':        len(qualifying_pids),
        'min_minutes_corr': MIN_MINUTES_CORR,
        'correlations':     raw_correlations,
        'weights':          flat_weights,         # Builder Impact Mode
        'subcomp_weights':  subcomp_weights,      # composite scoring
        'catcomp_weights':  catcomp_weights,      # category scoring
    }
    with open(out_path, 'w') as f:
        json.dump(output, f, indent=2)

    print(f"  Correlations computed for {len(raw_correlations)} stats")
    print(f"  Saved → {out_path}")

    # ── Sanity-check printout ─────────────────────────────────
    print(f"\n{'='*70}")
    print(f"  Sub-composite weights (|r| with net_pts100, normalized within group)")
    print(f"  1.0 = strongest win predictor in that group | 0.1 = weakest")
    print(f"{'='*70}")
    for comp_name, weights in subcomp_weights.items():
        print(f"\n  {comp_name}:")
        print(f"  {'Stat':<32} {'raw r':>7}  {'weight':>7}  {'n pairs':>8}")
        print(f"  {'─'*55}")
        for k, w in sorted(weights.items(), key=lambda x: -x[1]):
            r = raw_correlations.get(k)
            pairs_count = sum(
                1 for pid in qualifying_pids
                if (pid_to_metrics.get(pid, {}).get(k) is not None
                    or seasons_map.get(pid, {}).get(k) is not None)
            )
            r_str = f"{r:+.3f}" if r is not None else "   n/a"
            print(f"  {k:<32} {r_str:>7}  {w:>7.4f}  {pairs_count:>8}")

    print(f"\n  Category composite weights (sub-score → category):")
    for cat_name, weights in catcomp_weights.items():
        print(f"  {cat_name}:")
        for k, w in sorted(weights.items(), key=lambda x: -x[1]):
            r = raw_correlations.get(k)
            r_str = f"{r:+.3f}" if r is not None else "   n/a"
            print(f"    {k:<38} r={r_str}  w={w:.4f}")

    return raw_correlations, subcomp_weights, catcomp_weights, flat_weights


# ── Load season data ──────────────────────────────────────────
def load_seasons(conn, season, season_type):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT ps.*, p.position_group, p.position
        FROM player_seasons ps
        JOIN players p ON ps.player_id = p.player_id
        WHERE ps.season = %s AND ps.season_type = %s AND ps.league = 'NBA'
    """, (season, season_type))
    rows = cur.fetchall()
    cur.close()
    print(f"  Loaded {len(rows)} player season rows")
    return [dict(r) for r in rows]


# ── Per-player metric computation ─────────────────────────────
def compute_player_metrics(p):
    pid       = p['player_id']
    min_total = s(p.get('min'))
    min_pg    = s(p.get('min_per_game'))
    gp        = s(p.get('gp'), 1)
    poss      = s(p.get('poss'))
    pos_group = p.get('position_group', 'F')

    poss_pg = poss if poss and poss > 0 else 75.0
    per75   = 75.0 / poss_pg if poss_pg > 0 else 1.0

    pts    = s(p.get('pts'))
    fga    = s(p.get('fga'))
    fta    = s(p.get('fta'))
    fgm    = s(p.get('fgm'))
    fg3m   = s(p.get('fg3m'))
    fg3a   = s(p.get('fg3a'))
    tov_pg = s(p.get('tov'))

    # ── Shooting ──────────────────────────────────────────────
    ts_denom        = 2 * (fga + 0.44 * fta)
    ts_pct_computed = div(pts, ts_denom)
    ft_rate         = div(fta, fga)
    efg_pct         = s(p.get('efg_pct'))
    shot_quality_delta = None  # legacy placeholder

    pu_efg     = s(p.get('pull_up_efg_pct'))
    cs_efg     = s(p.get('cs_efg_pct'))
    pu_fga     = s(p.get('pull_up_fga'))
    cs_fga_tot = s(p.get('cs_fga'))

    creation_premium = None
    if pu_fga >= MIN_PULL_UP_FGA and cs_fga_tot >= MIN_CS_FGA:
        creation_premium = pu_efg - cs_efg

    pts_paint          = s(p.get('pts_paint'))
    paint_touches      = s(p.get('paint_touches'))
    paint_scoring_rate = div(pts_paint, paint_touches)

    # ── Shot making vs expectation ────────────────────────────
    def _efg(fgm_, fg3m_, fga_):
        if not fga_ or fga_ == 0: return None
        return (fgm_ + 0.5 * fg3m_) / fga_

    cd_fga_vt  = s(p.get('cd_fga_vt'));  cd_fgm_vt  = s(p.get('cd_fgm_vt'))
    cd_fg3m_vt = s(p.get('cd_fg3m_vt'))
    cd_fga_tg  = s(p.get('cd_fga_tg'));  cd_fgm_tg  = s(p.get('cd_fgm_tg'))
    cd_fg3m_tg = s(p.get('cd_fg3m_tg'))
    cd_fga_op  = s(p.get('cd_fga_op'));  cd_fgm_op  = s(p.get('cd_fgm_op'))
    cd_fg3m_op = s(p.get('cd_fg3m_op'))
    cd_fga_wo  = s(p.get('cd_fga_wo'));  cd_fgm_wo  = s(p.get('cd_fgm_wo'))
    cd_fg3m_wo = s(p.get('cd_fg3m_wo'))

    cont_fga  = cd_fga_vt + cd_fga_tg
    cont_fgm  = cd_fgm_vt + cd_fgm_tg
    cont_fg3m = cd_fg3m_vt + cd_fg3m_tg
    open_fga  = cd_fga_op + cd_fga_wo
    open_fgm  = cd_fgm_op + cd_fgm_wo
    open_fg3m = cd_fg3m_op + cd_fg3m_wo

    player_cont_efg = _efg(cont_fgm, cont_fg3m, cont_fga)
    player_open_efg = _efg(open_fgm, open_fg3m, open_fga)

    contested_fg_making = None
    if player_cont_efg is not None and cont_fga >= 50:
        contested_fg_making = round(player_cont_efg - LG_CONTESTED_EFG, 4)

    open_fg_making = None
    if player_open_efg is not None and open_fga >= 50:
        open_fg_making = round(player_open_efg - LG_OPEN_EFG, 4)

    # ── Playmaking ────────────────────────────────────────────
    ast_pts_created = s(p.get('ast_pts_created'))
    potential_ast   = s(p.get('potential_ast'))
    secondary_ast   = s(p.get('secondary_ast'))
    passes_made     = s(p.get('passes_made'))
    ast_pg          = s(p.get('ast'))
    time_of_poss    = s(p.get('time_of_poss'))
    touches         = s(p.get('touches'))
    drives          = s(p.get('drives'))
    drive_passes    = s(p.get('drive_passes'))
    drive_fga       = s(p.get('drive_fga'))
    drive_tov       = s(p.get('drive_tov'))
    drive_pf        = s(p.get('drive_pf'))
    drive_ast_raw   = s(p.get('drive_ast'))

    potential_ast_pg   = potential_ast   / gp if gp > 0 else 0
    secondary_ast_pg   = secondary_ast   / gp if gp > 0 else 0
    passes_made_pg     = passes_made     / gp if gp > 0 else 0
    ast_pts_created_pg = ast_pts_created / gp if gp > 0 else 0
    drive_passes_pg    = drive_passes    / gp if gp > 0 else drive_passes
    drive_ast_pg       = drive_ast_raw   / gp if gp > 0 else 0
    drive_pg           = drives          / gp if gp > 0 else 0

    potential_ast_per75 = potential_ast_pg * per75 if potential_ast_pg > 0 else None
    ast_conversion_rate = div(ast_pg, potential_ast_pg)
    poss_used_passing   = div(time_of_poss, min_pg) if min_pg > 0 else None
    playmaking_gravity  = div(ast_pts_created_pg, poss_used_passing) if poss_used_passing else None
    secondary_ast_per75 = secondary_ast_pg * per75 if secondary_ast_pg > 0 else None
    pass_to_score_pct   = div(ast_pg, passes_made_pg) if passes_made_pg > 0 else None
    ball_handler_load   = div(time_of_poss, min_pg)
    drive_total         = drive_fga + drive_tov + drive_passes_pg
    drive_and_dish_rate = div(drive_passes_pg, drive_total) if drive_pg >= 2.0 else None
    pass_quality_index  = div(ast_pts_created_pg, passes_made_pg)
    drive_ast_per75     = drive_ast_pg * per75 if drive_ast_pg > 0 and drive_pg >= 2.0 else None
    drive_passes_per75  = drive_passes_pg * per75 if drive_passes_pg > 0 and drive_pg >= 2.0 else None

    ft_ast_raw   = s(p.get('ft_ast'))
    ft_ast_pg    = ft_ast_raw / gp if gp > 0 else 0
    ft_ast_per75 = ft_ast_pg * per75 if ft_ast_pg > 0 else None

    bad_pass_total = safe(p.get('bad_pass_tov'))
    bad_pass_pg    = bad_pass_total / gp if bad_pass_total is not None and gp > 0 else None
    if potential_ast_pg >= 3.0:
        if bad_pass_pg is not None and bad_pass_pg > 0:
            pot_ast_per_tov = div(potential_ast_pg, bad_pass_pg)
        else:
            pot_ast_per_tov = div(potential_ast_pg, tov_pg) if tov_pg and tov_pg > 0 else None
    else:
        pot_ast_per_tov = None

    touches_pg = s(p.get('touches'), 0) / gp if gp > 0 else 0

    bad_pass_tov_pg = (bad_pass_pg / touches_pg * 100
                       if bad_pass_pg is not None and touches_pg > 0 else None)

    lost_ball_total = safe(p.get('lost_ball_tov'))
    if lost_ball_total is not None and gp > 0:
        lost_ball_raw_pg = lost_ball_total / gp
    elif bad_pass_total is not None and gp > 0:
        lost_ball_raw_pg = max(0.0, tov_pg - (bad_pass_total / gp))
    elif tov_pg > 0:
        lost_ball_raw_pg = tov_pg
    else:
        lost_ball_raw_pg = None

    lost_ball_tov_pg = (lost_ball_raw_pg / touches_pg * 100
                        if lost_ball_raw_pg is not None and touches_pg > 0 else None)

    drive_foul_rate     = div(drive_pf, drives) if drive_pg >= 2.0 else None
    drive_pts_raw       = s(p.get('drive_pts'))
    drive_pts_per_drive = div(drive_pts_raw, drives) if drive_pg >= 2.0 else None
    tov_pct             = div(tov_pg, fga + 0.44 * fta + tov_pg) if (fga + fta + tov_pg) > 0 else None

    d_fg_pct_overall = s(p.get('d_fg_pct_overall'))
    normal_fg_pct    = s(p.get('normal_fg_pct'))
    d_fga_overall    = s(p.get('d_fga_overall'))
    d_fg_pct_2pt     = s(p.get('d_fg_pct_2pt'))
    ns_fg2_pct       = s(p.get('ns_fg2_pct'))
    d_fga_2pt        = s(p.get('d_fga_2pt'))
    d_fg_pct_3pt     = s(p.get('d_fg_pct_3pt'))
    ns_fg3_pct       = s(p.get('ns_fg3_pct'))
    d_fga_3pt        = s(p.get('d_fga_3pt'))

    def_delta_overall = (normal_fg_pct - d_fg_pct_overall) if d_fga_overall >= MIN_DEF_FGA and normal_fg_pct > 0 else None
    def_delta_2pt     = (ns_fg2_pct - d_fg_pct_2pt)       if d_fga_2pt >= MIN_DEF_FGA * 0.6 and ns_fg2_pct > 0 else None
    def_delta_3pt     = (ns_fg3_pct - d_fg_pct_3pt)       if d_fga_3pt >= MIN_DEF_FGA * 0.4 and ns_fg3_pct > 0 else None

    def_rim_fga       = s(p.get('def_rim_fga'))
    def_rim_fg_pct    = s(p.get('def_rim_fg_pct'))
    rim_protection_score = (RIM_XFG_BASELINE - def_rim_fg_pct) * def_rim_fga * 2 if def_rim_fga >= MIN_RIM_FGA else None

    stl         = s(p.get('stl'))
    deflections = s(p.get('deflections')) / gp if gp > 0 else 0
    charges     = s(p.get('charges_drawn')) / gp if gp > 0 else 0
    def_disruption_rate = (stl + deflections * 0.35 + charges * 0.80) * per75 if (stl + deflections + charges) > 0 else None

    box_outs_pg  = s(p.get('box_outs')) / gp if gp > 0 else 0
    box_out_rate = box_outs_pg * per75 if box_outs_pg > 0 else None

    screen_ast_pts = s(p.get('screen_ast_pts')) / gp if gp > 0 else 0
    loose_balls    = s(p.get('loose_balls'))    / gp if gp > 0 else 0
    dist_miles_off = s(p.get('dist_miles_off'))
    dist_miles_def = s(p.get('dist_miles_def'))
    deflections_pg = s(p.get('deflections'))    / gp if gp > 0 else 0
    charges_pg     = s(p.get('charges_drawn'))  / gp if gp > 0 else 0

    screen_assist_rate = screen_ast_pts * per75 if screen_ast_pts > 0 else None
    loose_ball_rate    = loose_balls    * per75 if loose_balls    > 0 else None

    hustle_raw       = deflections_pg * 0.35 + charges_pg * 0.80 + loose_balls * 0.65 + screen_ast_pts * 0.10
    hustle_composite = hustle_raw * per75 if hustle_raw > 0 else None

    motor_score = None
    if min_total >= MIN_MINUTES_TOTAL and (dist_miles_off + dist_miles_def) > 0 and min_pg > 0:
        motor_score = ((dist_miles_off + dist_miles_def) / min_pg) * 36

    post_touches  = s(p.get('post_touches'))
    elbow_touches = s(p.get('elbow_touches'))

    creation_load        = div(drives + post_touches + elbow_touches, touches)
    avg_drib_per_touch   = s(p.get('avg_drib_per_touch'))
    dribble_pressure_idx = avg_drib_per_touch if avg_drib_per_touch > 0 else None
    cs_fga_rate          = div(cs_fga_tot, fga) if fga >= MIN_FGA_PER_GAME * gp else None

    ast_pct  = s(p.get('ast_pct'))
    reb_pct  = s(p.get('reb_pct'))
    stl_pct  = stl / poss_pg if poss_pg > 0 else 0
    blk      = s(p.get('blk'))
    blk_pct  = blk / poss_pg if poss_pg > 0 else 0
    usg_pct  = s(p.get('usg_pct'))
    ts_pct   = s(p.get('ts_pct'))
    pos_adj  = {'G': 0.0, 'GF': -0.5, 'F': -1.0, 'FC': -1.5, 'C': -2.0}.get(pos_group, -1.0)

    bpm_computed = None
    if min_total >= MIN_MINUTES_TOTAL and usg_pct > 0 and ts_pct > 0:
        bpm_computed = (
            -2.611
            + 0.318 * ast_pct * 100
            + 0.449 * reb_pct * 100 * 0.3
            + 1.064 * (ts_pct - 0.500) * usg_pct * 100 / 25
            + 0.295 * stl_pct * 100
            + 0.396 * blk_pct * 100
            - 0.257 * (tov_pg / (poss_pg + 0.01)) * 100
            + pos_adj
        )

    def r(v, d=4):
        return round(v, d) if v is not None else None

    return {
        'player_id':   pid,
        'season':      p['season'],
        'season_type': p['season_type'],
        'league':      p.get('league', 'NBA'),

        'ts_pct_computed':     r(ts_pct_computed),
        'ft_rate':             r(ft_rate),
        'creation_premium':    r(creation_premium),
        'paint_scoring_rate':  r(paint_scoring_rate),
        'contested_fg_making': r(contested_fg_making, 4),
        'open_fg_making':      r(open_fg_making, 4),
        'drive_foul_rate':     r(drive_foul_rate, 4),
        'drive_pts_per_drive': r(drive_pts_per_drive, 4),
        'tov_pct':             r(tov_pct, 4),
        'ast_pts_created_pg':  r(ast_pts_created_pg, 2),
        'potential_ast_per75': r(potential_ast_per75, 3),
        'ast_conversion_rate': r(ast_conversion_rate),
        'playmaking_gravity':  r(playmaking_gravity),
        'secondary_ast_per75': r(secondary_ast_per75, 3),
        'pass_to_score_pct':   r(pass_to_score_pct),
        'ball_handler_load':   r(ball_handler_load),
        'drive_and_dish_rate': r(drive_and_dish_rate),
        'pot_ast_per_tov':     r(pot_ast_per_tov, 3),
        'pass_quality_index':  r(pass_quality_index, 4),
        'ft_ast_per75':        r(ft_ast_per75, 3),
        'drive_ast_per75':     r(drive_ast_per75, 3),
        'drive_passes_per75':  r(drive_passes_per75, 3),
        'lost_ball_tov_pg':    r(lost_ball_tov_pg, 3),
        'bad_pass_tov_pg':     r(bad_pass_tov_pg, 3),
        'gravity_creation':    r(
            (s(p.get('gravity_onball_perimeter')) * min_pg
             if pos_group not in ('FC', 'C') and s(p.get('gravity_onball_perimeter')) and min_pg
             else None),
            3),
        'def_delta_overall':    r(def_delta_overall),
        'def_delta_2pt':        r(def_delta_2pt),
        'def_delta_3pt':        r(def_delta_3pt),
        'rim_protection_score': r(rim_protection_score, 3),
        'def_disruption_rate':  r(def_disruption_rate, 3),
        'box_out_rate':         r(box_out_rate, 3),
        'screen_assist_rate':   r(screen_assist_rate, 3),
        'loose_ball_rate':      r(loose_ball_rate, 3),
        'hustle_composite':     r(hustle_composite, 3),
        'motor_score':          r(motor_score, 3),
        'creation_load':        r(creation_load),
        'dribble_pressure_idx': r(dribble_pressure_idx, 3),
        'cs_fga_rate':          r(cs_fga_rate),
        'bpm_computed':         r(bpm_computed, 2),
    }


# ── Composite computation ─────────────────────────────────────
def compute_composites(metrics_list, seasons_map, season, season_type,
                       subcomp_weights=None, catcomp_weights=None):
    """
    Compute all sub-composite and category composite scores.
    Weights are applied as normalized win-correlation multipliers.

    subcomp_weights: {comp_name: {stat_key: weight 0.1-1.0}}
    catcomp_weights: {cat_name:  {sub_key:  weight 0.1-1.0}}
    Falls back to flat (equal-weight) average if weights are None/empty.
    """
    subcomp_weights = subcomp_weights or {}
    catcomp_weights = catcomp_weights or {}

    pos_groups = {}
    for m in metrics_list:
        pid   = m['player_id']
        ps    = seasons_map.get(pid, {})
        pos_g = ps.get('position_group', 'F')
        pos_groups.setdefault(pos_g, []).append(pid)

    all_qualifying = [pid for pids in pos_groups.values() for pid in pids]
    metrics_by_pid = {m['player_id']: m for m in metrics_list}

    def get_val(pid, col, src):
        if src == 'm':
            return safe(metrics_by_pid.get(pid, {}).get(col))
        return safe(seasons_map.get(pid, {}).get(col))

    VW_METRICS = {'all3_efg_vw', 'midrange_efg_vw', 'corner3_efg_vw',
                  'above_break3_efg_vw', 'paint_efg_vw'}

    def percentile_map(pids, col, src):
        vals = [(pid, get_val(pid, col, src)) for pid in pids]
        if col in VW_METRICS:
            vals = [(pid, v) for pid, v in vals if v is not None and v != 0.0]
        else:
            vals = [(pid, v) for pid, v in vals if v is not None]
        if not vals: return {}
        sorted_vals = sorted(vals, key=lambda x: x[1])
        n = len(sorted_vals)
        return {pid: round((i / (n - 1)) * 100, 1) if n > 1 else 50.0
                for i, (pid, _) in enumerate(sorted_vals)}

    ALL_METRICS_LG = [
        ('net_pts100',    's'), ('o_net_pts100',  's'), ('d_net_pts100',  's'),
        ('leverage_full', 's'),
        ('ts_pct', 's'), ('spotup_efg_pct', 's'),
        ('all3_efg_vw', 'm'), ('midrange_efg_vw', 'm'),
        ('sq_fg_pct_above_expected', 's'),
        ('pct_uast_fgm', 's'), ('iso_ppp', 's'), ('pull_up_efg_pct', 's'),
        ('drive_fg_pct', 's'), ('usg_pct', 's'), ('tov_pct', 'm'),
        ('leverage_shooting', 's'),
        ('pot_ast_per_tov', 'm'), ('ast_pct', 's'), ('pass_quality_index', 'm'),
        ('leverage_creation', 's'), ('ast_pts_created_pg', 'm'), ('ft_ast_per75', 'm'),
        ('lost_ball_tov_pg', 'm'), ('pnr_bh_ppp', 's'),
        ('def_iso_ppp', 's'), ('def_pnr_bh_ppp', 's'),
        ('def_post_ppp', 's'), ('def_spotup_ppp', 's'), ('def_pnr_roll_ppp', 's'),
        ('transition_ppp', 's'),
    ]
    ALL_METRICS_POS = [
        ('paint_efg_vw', 'm'), ('paint_scoring_rate', 'm'),
        ('post_ppp', 's'), ('drive_foul_rate', 'm'),
        ('drive_pts_per_drive', 'm'), ('pnr_roll_ppp', 's'),
        ('def_delta_3pt', 'm'), ('def_delta_overall', 'm'),
        ('def_disruption_rate', 'm'), ('contested_shots', 's'),
        ('def_iso_ppp', 's'), ('def_pnr_bh_ppp', 's'),
        ('stl', 's'), ('leverage_defense', 's'),
        ('def_ws', 's'), ('matchup_def_fg_pct', 's'),
        ('rim_protection_score', 'm'), ('def_delta_2pt', 'm'),
        ('dreb_pct', 's'), ('blk', 's'), ('box_out_rate', 'm'),
        ('motor_score', 'm'), ('hustle_composite', 'm'), ('screen_assist_rate', 'm'),
        ('oreb_pct', 's'), ('reb_pct', 's'),
        ('def_spotup_ppp', 's'), ('def_post_ppp', 's'), ('def_pnr_roll_ppp', 's'),
    ]

    LG_CS_EFG_AVG = 0.535

    def pnr_bh_qualified(pid):
        ps = seasons_map.get(pid, {})
        # pnr_bh_poss is stored as a per-game average from the Synergy endpoint
        return s(ps.get('pnr_bh_poss'), 0) >= 5.0

    def transition_qualified(pid):
        ps = seasons_map.get(pid, {})
        gp = max(s(ps.get('gp'), 1), 1)
        return s(ps.get('transition_fga'), 0) / gp >= 0.5

    def def_playtype_qualified(pid, poss_col, min_poss=1.5):
        ps = seasons_map.get(pid, {})
        # poss columns from Synergy are per-game values
        return s(ps.get(poss_col), 0) >= min_poss

    # Apply volume gates (NULL non-qualifying players before percentile ranking)
    for pid in all_qualifying:
        ps = seasons_map.get(pid, {})
        gp = max(s(ps.get('gp'), 1), 1)
        # spotup_efg_pct / C&S EFG%: display only, not in composite.

        # pull_up_efg_pct: requires pull_up_fga/gp >= 1.5/g to qualify.
        if s(ps.get('pull_up_fga'), 0) / gp < 1.5:
            seasons_map[pid]['_pullup_orig'] = ps.get('pull_up_efg_pct')
            seasons_map[pid]['pull_up_efg_pct'] = None
        if not pnr_bh_qualified(pid):
            seasons_map[pid]['_pnr_bh_orig'] = ps.get('pnr_bh_ppp')
            seasons_map[pid]['pnr_bh_ppp'] = None
        if not transition_qualified(pid):
            seasons_map[pid]['_trans_orig'] = ps.get('transition_ppp')
            seasons_map[pid]['transition_ppp'] = None
        if s(ps.get('paint_touches'), 0) / gp < 10.0:
            seasons_map[pid]['_psr_orig'] = ps.get('paint_scoring_rate')
            seasons_map[pid]['paint_scoring_rate'] = None
        if s(ps.get('pnr_roll_poss'), 0) < 2.0:  # per-game value from Synergy
            seasons_map[pid]['_roll_orig'] = ps.get('pnr_roll_ppp')
            seasons_map[pid]['pnr_roll_ppp'] = None
        if s(ps.get('post_poss'), 0) < 2.0:  # per-game value from Synergy
            seasons_map[pid]['_post_orig'] = ps.get('post_ppp')
            seasons_map[pid]['post_ppp'] = None
        for ppp_col, poss_col in [
            ('def_post_ppp',     'def_post_poss'),
            ('def_spotup_ppp',   'def_spotup_poss'),
            ('def_pnr_roll_ppp', 'def_pnr_roll_poss'),
        ]:
            if not def_playtype_qualified(pid, poss_col):
                seasons_map[pid][f'_{ppp_col}_orig'] = ps.get(ppp_col)
                seasons_map[pid][ppp_col] = None

    pct_lg = {col: percentile_map(all_qualifying, col, src)
              for col, src in ALL_METRICS_LG}

    # Restore gated values
    for pid in all_qualifying:
        for orig_key, restore_key in [
            ('_pullup_orig',   'pull_up_efg_pct'),
            ('_pnr_bh_orig',    'pnr_bh_ppp'),
            ('_trans_orig',     'transition_ppp'),
            ('_psr_orig',       'paint_scoring_rate'),
            ('_roll_orig',      'pnr_roll_ppp'),
            ('_post_orig',      'post_ppp'),
        ]:
            if orig_key in seasons_map.get(pid, {}):
                seasons_map[pid][restore_key] = seasons_map[pid].pop(orig_key)
        for ppp_col in ['def_post_ppp', 'def_spotup_ppp', 'def_pnr_roll_ppp']:
            key = f'_{ppp_col}_orig'
            if key in seasons_map.get(pid, {}):
                seasons_map[pid][ppp_col] = seasons_map[pid].pop(key)

    pct_pos = {col: percentile_map(all_qualifying, col, src)
               for col, src in ALL_METRICS_POS}

    # Inverted percentile maps (lower raw = better = higher percentile)
    pct_lg['tov_pct_inv']          = {pid: round(100 - v, 1) for pid, v in pct_lg['tov_pct'].items()}
    pct_lg['lost_ball_tov_pg_inv'] = {pid: round(100 - v, 1) for pid, v in pct_lg['lost_ball_tov_pg'].items()}
    if 'matchup_def_fg_pct' in pct_pos:
        pct_pos['matchup_def_fg_pct_inv'] = {pid: round(100 - v, 1) for pid, v in pct_pos['matchup_def_fg_pct'].items()}
    for col in ['def_iso_ppp', 'def_pnr_bh_ppp', 'def_post_ppp', 'def_spotup_ppp', 'def_pnr_roll_ppp']:
        if col in pct_lg:
            pct_lg[f'{col}_inv'] = {pid: round(100 - v, 1) for pid, v in pct_lg[col].items()}
        if col in pct_pos:
            pct_pos[f'{col}_inv'] = {pid: round(100 - v, 1) for pid, v in pct_pos[col].items()}

    def weighted_avg_pct(pid, cols_srcs, pct_maps, comp_name, min_metrics=1):
        """
        Weighted average of percentiles.
        Resolves inverted col names (e.g. tov_pct_inv) to base key for weight lookup.
        Falls back to weight=1.0 if no weight found.
        """
        comp_w = subcomp_weights.get(comp_name, {})
        vals_weights = []
        for col, src in cols_srcs:
            pmap = pct_maps.get(col, {})
            v    = pmap.get(pid)
            if v is not None:
                base_col = INVERT_MAP.get(col, col)
                raw_w = comp_w.get(base_col)
                w = raw_w if raw_w is not None else 1.0
                vals_weights.append((v, w))
        if len(vals_weights) < min_metrics:
            return None
        total_w = sum(w for _, w in vals_weights)
        if total_w == 0:
            return None
        return round(sum(v * w for v, w in vals_weights) / total_w, 1)

    def passes_gate(pid, gate_key):
        """Sub-composite qualifying gates — volume per game only, no GP floor.
        Storage conventions: ast, fga, pts = per-game averages (no /gp needed).
        potential_ast, drives, touches, paint_touches, def_rim_fga = season totals (need /gp).
        """
        ps = seasons_map.get(pid, {})
        gp = max(s(ps.get('gp'), 1), 1)
        if gate_key == 'finishing':
            return s(ps.get('paint_touches'), 0) / gp >= 3.0
        if gate_key == 'shooting':
            return s(ps.get('fga'), 0) >= 3.0
        if gate_key == 'shot_creation':
            return s(ps.get('drives'), 0) / gp >= 2.0
        if gate_key == 'passing':
            return (s(ps.get('ast'), 0) >= 1.5 and
                    s(ps.get('potential_ast'), 0) / gp >= 3.0)
        if gate_key == 'pm_creation':
            return (s(ps.get('potential_ast'), 0) / gp >= 3.0 and
                    s(ps.get('touches'), 0) / gp >= 40.0)
        if gate_key == 'ball_handling':
            return (s(ps.get('drives'), 0) / gp >= 4.0 and
                    s(ps.get('touches'), 0) / gp >= 40.0)
        if gate_key == 'interior_def':
            return s(ps.get('def_rim_fga'), 0) / gp >= 2.5
        return True

    # ── Sub-composite definitions ─────────────────────────────
    SUB_COMPOSITES = [
        ('finishing_score', None,
         [('paint_efg_vw', 'm'), ('paint_scoring_rate', 'm'),
          ('drive_pts_per_drive', 'm'), ('drive_foul_rate', 'm'),
          ('pnr_roll_ppp', 's'), ('post_ppp', 's'), ('transition_ppp', 's')],
         'pos'),

        ('shooting_score', 'shooting',
         [('all3_efg_vw', 'm'),
          ('midrange_efg_vw', 'm'), ('sq_fg_pct_above_expected', 's')],
         'lg'),

        ('shot_creation_score', 'shot_creation',
         [('pct_uast_fgm', 's'), ('iso_ppp', 's'), ('pull_up_efg_pct', 's'),
          ('drive_fg_pct', 's'), ('usg_pct', 's'), ('tov_pct_inv', 'm'),
          ('leverage_shooting', 's')],
         'lg'),

        ('passing_score', 'passing',
         [('pot_ast_per_tov', 'm'), ('ast_pct', 's'), ('pass_quality_index', 'm')],
         'lg'),

        ('creation_score', 'pm_creation',
         [('leverage_creation', 's'), ('ast_pts_created_pg', 'm'), ('ft_ast_per75', 'm')],
         'lg'),

        ('decision_making_score', 'ball_handling',
         [('lost_ball_tov_pg_inv', 'm'), ('pnr_bh_ppp', 's')],
         'lg'),

        ('perimeter_def_score', None,
         [('def_delta_3pt', 'm'), ('def_delta_overall', 'm'),
          ('def_disruption_rate', 'm'), ('contested_shots', 's'),
          ('stl', 's'), ('def_spotup_ppp_inv', 's')],
         'pos'),

        ('interior_def_score', 'interior_def',
         [('rim_protection_score', 'm'), ('def_delta_2pt', 'm'),
          ('dreb_pct', 's'), ('blk', 's'),
          ('def_post_ppp_inv', 's'), ('def_pnr_roll_ppp_inv', 's')],
         'pos'),

        ('activity_score', None,
         [('motor_score', 'm'), ('hustle_composite', 'm'), ('screen_assist_rate', 'm')],
         'pos'),

        ('rebounding_score', None,
         [('dreb_pct', 's'), ('oreb_pct', 's'), ('box_out_rate', 'm')],
         'pos'),
    ]

    # ── Score each player ─────────────────────────────────────
    for m in metrics_list:
        pid   = m['player_id']
        min_t = s(seasons_map.get(pid, {}).get('min'), 0)

        # No minimum-minutes hard cutoff — stat-level volume gates handle eligibility.


        # Sub-composites
        for comp_name, gate_key, cols_srcs, pct_key in SUB_COMPOSITES:
            if gate_key and not passes_gate(pid, gate_key):
                m[comp_name] = None
                continue
            pct_maps = pct_lg if pct_key == 'lg' else pct_pos
            # Require min(2, n_stats-1) so losing one individually-gated
            # stat never nulls the entire sub-composite.
            # 2-stat composites need 1; 3+-stat composites need 2.
            n_stats = len(cols_srcs)
            min_m = 1 if n_stats <= 2 else 2

            score = weighted_avg_pct(pid, cols_srcs, pct_maps, comp_name, min_metrics=min_m)

            if comp_name == 'finishing_score' and score is not None:
                has_paint = (pct_pos.get('paint_efg_vw', {}).get(pid) is not None or
                             pct_pos.get('paint_scoring_rate', {}).get(pid) is not None)
                if not has_paint:
                    score = None

            if comp_name == 'passing_score' and score is not None:
                if pct_lg.get('pot_ast_per_tov', {}).get(pid) is None:
                    score = None

            m[comp_name] = score

        # ── Category composites (win-correlation weighted) ────
        def flat_cat_score(sub_names):
            """Flat unweighted average of available sub-composite scores.
            Sub-composites already encode win-correlation via raw |r| weights."""
            available = [safe(m.get(sn)) for sn in sub_names if safe(m.get(sn)) is not None]
            if not available:
                return None
            return round(sum(available) / len(available), 1)

        # Playmaker — best 2 of the 3 available sub-composites.
        # Requires passing_score (the core signal). If all 3 are present,
        # the lowest is dropped so ball handling never penalises a player
        # who simply does not run PnR-BH as their primary role.
        _pm_all = [(v, n) for n, v in [
            ('passing',    safe(m.get('passing_score'))),
            ('creation',   safe(m.get('creation_score'))),
            ('ball_handling', safe(m.get('decision_making_score'))),
        ] if v is not None]
        # Must have passing_score
        if not any(n == 'passing' for _, n in _pm_all):
            m['playmaker_score'] = None
        elif len(_pm_all) == 1:
            m['playmaker_score'] = round(_pm_all[0][0], 1)
        else:
            # Take best 2 by value
            top2 = sorted(_pm_all, key=lambda x: x[0], reverse=True)[:2]
            m['playmaker_score'] = round(sum(v for v, _ in top2) / 2, 1)

        # Creator — requires shot_creation_score
        # Creator — best 2 of available sub-scores, requires shot_creation_score.
        # If all 3 qualify, the lowest is dropped so a weak dimension never
        # penalises a specialist (e.g. a pure shooter with low finishing).
        _cr_subs = [(safe(m.get(sn)), sn) for sn in
                    ['shot_creation_score', 'finishing_score', 'shooting_score']]
        _cr_avail = [(v, n) for v, n in _cr_subs if v is not None]
        if not any(n == 'shot_creation_score' for _, n in _cr_avail) or len(_cr_avail) < 2:
            m['creator_score'] = None
        else:
            top2 = sorted(_cr_avail, key=lambda x: x[0], reverse=True)[:2]
            m['creator_score'] = round(sum(v for v, _ in top2) / 2, 1)

        # Defender — flat average of sub-scores + extra signals, no anchors
        def_vals = []
        for sub in ['perimeter_def_score', 'interior_def_score']:
            v = safe(m.get(sub))
            if v is not None: def_vals.append(v)
        for extra_col in ['leverage_defense', 'def_ws']:
            pct = pct_pos.get(extra_col, {}).get(pid)
            if pct is not None: def_vals.append(pct)
        matchup_pct = pct_pos.get('matchup_def_fg_pct_inv', {}).get(pid)
        if matchup_pct is not None: def_vals.append(matchup_pct)
        m['defender_score'] = round(sum(def_vals) / len(def_vals), 1) if def_vals else None

        # Hustle
        m['hustle_score'] = flat_cat_score(['activity_score', 'rebounding_score'])

        # Three-and-D
        pdef  = safe(m.get('perimeter_def_score'))
        shoot = safe(m.get('shooting_score'))
        m['three_and_d_score'] = (round((pdef + shoot) / 2, 1)
                                  if pdef is not None and shoot is not None else None)

        # ── Impact Score ──────────────────────────────────────
        cat_scores = [safe(m.get(cn)) for cn in
                      ['creator_score', 'playmaker_score', 'defender_score', 'hustle_score']]
        cat_avail  = [v for v in cat_scores if v is not None]
        m['impact_score'] = round(sum(cat_avail) / len(cat_avail), 1) if cat_avail else None

    # ── Percentile rank columns ───────────────────────────────
    pctile_cols = [
        ('ts_pct',      'ts_pct',              's', False),
        ('usg_pct',     'usg_pct',             's', False),
        ('ast_pct',     'ast_pct',             's', False),
        ('net_rating',  'net_rating',          's', False),
        ('def_delta',   'def_delta_overall',   'm', False),
        ('rim_prot',    'rim_protection_score', 'm', False),
        ('playmaker',   'playmaker_score',     'm', False),
        ('creator',     'creator_score',       'm', False),
        ('defender',    'defender_score',      'm', True),
        ('three_and_d', 'three_and_d_score',   'm', False),
        ('hustle',      'hustle_score',        'm', False),
    ]
    for pctile_name, col, src, pos_norm in pctile_cols:
        if pos_norm:
            rank_map = {}
            for pos_g, pids in pos_groups.items():
                grp_vals = []
                for pid in pids:
                    v = safe(metrics_by_pid.get(pid, {}).get(col)) if src == 'm' \
                        else safe(seasons_map.get(pid, {}).get(col))
                    if v is not None: grp_vals.append((pid, v))
                if not grp_vals: continue
                sorted_grp = sorted(grp_vals, key=lambda x: x[1])
                n = len(sorted_grp)
                for i, (pid, _) in enumerate(sorted_grp):
                    rank_map[pid] = round((i / (n - 1)) * 100, 1) if n > 1 else 50.0
            for m in metrics_list:
                pid = m['player_id']
                m[f'{pctile_name}_pctile'] = rank_map.get(pid)
        else:
            all_vals = []
            for m in metrics_list:
                pid = m['player_id']
                v = safe(m.get(col)) if src == 'm' else safe(seasons_map.get(pid, {}).get(col))
                if v is not None: all_vals.append((pid, v))
            if not all_vals: continue
            sorted_vals = sorted(all_vals, key=lambda x: x[1])
            rank_map    = {pid: i for i, (pid, _) in enumerate(sorted_vals)}
            n           = len(sorted_vals)
            for m in metrics_list:
                pid = m['player_id']
                m[f'{pctile_name}_pctile'] = (round((rank_map[pid] / n) * 100, 1)
                                              if pid in rank_map else None)

    return metrics_list


# ── Upsert metrics ────────────────────────────────────────────
def upsert_metrics(conn, rows):
    if not rows: return
    cols        = list(rows[0].keys())
    col_str     = ', '.join(cols)
    placeholder = ', '.join(['%s'] * len(cols))
    update_str  = ', '.join([f"{c} = EXCLUDED.{c}" for c in cols
                             if c not in ('player_id','season','season_type','league')])
    sql = f"""
        INSERT INTO player_metrics ({col_str}, updated_at)
        VALUES ({placeholder}, NOW())
        ON CONFLICT (player_id, season, season_type, league) DO UPDATE SET
            {update_str}, updated_at = NOW()
    """
    def clean(val):
        if val is None: return None
        if isinstance(val, (np.floating, np.integer)): return val.item()
        if isinstance(val, float) and math.isnan(val): return None
        return val

    cleaned = [tuple(clean(r.get(c)) for c in cols) for r in rows]

    CHUNK = 25
    for i in range(0, len(cleaned), CHUNK):
        batch = cleaned[i:i+CHUNK]
        for attempt in range(3):
            try:
                chunk_conn = psycopg2.connect(os.getenv('DATABASE_URL'))
                cur = chunk_conn.cursor()
                cur.executemany(sql, batch)
                chunk_conn.commit()
                cur.close()
                chunk_conn.close()
                break
            except Exception as e:
                if attempt < 2:
                    import time; time.sleep(3)
                else:
                    raise e
        print(f"  ... {min(i+CHUNK, len(cleaned))}/{len(cleaned)}")
    print(f"  ✅ Upserted {len(rows)} metric rows")


# ── Zone metrics ──────────────────────────────────────────────
def compute_zone_metrics(conn, season, season_type):
    from collections import defaultdict
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT sz.player_id, sz.zone, sz.fga, sz.fgm, sz.fg_pct, sz.league_fg_pct, ps.gp
        FROM player_shot_zones sz
        JOIN player_seasons ps ON sz.player_id = ps.player_id
            AND ps.season = sz.season AND ps.season_type = %s
        WHERE sz.season = %s
    """, (season_type, season))
    rows = cur.fetchall()
    cur.close()

    if not rows:
        print(f"  ⚠️  No shot zone data found")
        return

    player_zones = defaultdict(dict)
    league       = {}
    for r in rows:
        player_zones[r['player_id']][r['zone']] = dict(r)
        z = r['zone']
        if z not in league: league[z] = {'fgm': 0, 'fga': 0}
        league[z]['fgm'] += r['fgm'] or 0
        league[z]['fga'] += r['fga'] or 0

    league_efg = {}
    for z, t in league.items():
        if t['fga'] > 0:
            is3 = z in ('Corner 3', 'Left Corner 3', 'Right Corner 3', 'Above the Break 3')
            league_efg[z] = (t['fgm'] * 1.5 / t['fga']) if is3 else (t['fgm'] / t['fga'])

    print(f"  League EFG% by zone:")
    for z, efg in sorted(league_efg.items()):
        print(f"    {z:<25} {efg:.3f}")

    updates = []
    for pid, zones in player_zones.items():
        gp = next((z['gp'] for z in zones.values() if z.get('gp')), 1) or 1

        def ze(zone_name, is3=False):
            z   = zones.get(zone_name, {})
            fga = int(z.get('fga') or 0)
            fgm = int(z.get('fgm') or 0)
            if fga == 0: return None, None, 0
            efg   = (fgm * 1.5 / fga) if is3 else (fgm / fga)
            delta = efg - league_efg.get(zone_name, 0)
            return round(efg, 4), round(delta, 4), fga

        def cze(zone_names, is3=False):
            tfga = sum(int(zones.get(z, {}).get('fga') or 0) for z in zone_names)
            tfgm = sum(int(zones.get(z, {}).get('fgm') or 0) for z in zone_names)
            if tfga == 0: return None, None, 0
            efg    = (tfgm * 1.5 / tfga) if is3 else (tfgm / tfga)
            lg_fga = sum(league.get(z, {}).get('fga', 0) for z in zone_names)
            lg_fgm = sum(league.get(z, {}).get('fgm', 0) for z in zone_names)
            lg_efg = (lg_fgm * 1.5 / lg_fga) if (is3 and lg_fga > 0) else (lg_fgm / lg_fga if lg_fga > 0 else 0)
            return round(efg, 4), round(efg - lg_efg, 4), tfga

        pe, pd_, pf = cze(['Restricted Area', 'In The Paint (Non-RA)'])
        me, md, mf  = ze('Mid-Range')
        ce, cd, cf  = ze('Corner 3', is3=True)
        ae, ad, af  = ze('Above the Break 3', is3=True)

        pf_pg = round(pf / gp, 2); mf_pg = round(mf / gp, 2)
        cf_pg = round(cf / gp, 2); af_pg = round(af / gp, 2)

        if pf_pg < ZONE_MIN_FGA_PG['paint']:        pe = pd_ = None
        if mf_pg < ZONE_MIN_FGA_PG['midrange']:     me = md  = None
        if cf_pg < ZONE_MIN_FGA_PG['corner3']:      ce = cd  = None
        if af_pg < ZONE_MIN_FGA_PG['above_break3']: ae = ad  = None

        paint_vw = round(pd_ * pf_pg, 4) if pd_ is not None else None
        mid_vw   = round(md  * mf_pg, 4) if md  is not None else None
        c3_vw    = round(cd  * cf_pg, 4) if cd  is not None else None
        ab3_vw   = round(ad  * af_pg, 4) if ad  is not None else None

        all3_efg, all3_delta, all3_fga = cze(['Corner 3', 'Above the Break 3'], is3=True)
        all3_fga_pg = round(all3_fga / gp, 2)
        all3_vw = round(all3_delta * all3_fga_pg, 4) if all3_delta is not None else None

        updates.append((
            pe, pd_, pf_pg, paint_vw, me, md, mf_pg, mid_vw,
            ce, cd, cf_pg, c3_vw, ae, ad, af_pg, ab3_vw,
            all3_efg, all3_delta, all3_fga_pg, all3_vw,
            pid, season, season_type
        ))

    upd = conn.cursor()
    upd.executemany("""
        UPDATE player_metrics SET
            paint_efg=%s,        paint_efg_delta=%s,        paint_fga_pg=%s,        paint_efg_vw=%s,
            midrange_efg=%s,     midrange_efg_delta=%s,     midrange_fga_pg=%s,     midrange_efg_vw=%s,
            corner3_efg=%s,      corner3_efg_delta=%s,      corner3_fga_pg=%s,      corner3_efg_vw=%s,
            above_break3_efg=%s, above_break3_efg_delta=%s, above_break3_fga_pg=%s, above_break3_efg_vw=%s,
            all3_efg=%s,         all3_efg_delta=%s,         all3_fga_pg=%s,         all3_efg_vw=%s,
            updated_at=NOW()
        WHERE player_id=%s AND season=%s AND season_type=%s
    """, updates)
    conn.commit()
    upd.close()
    print(f"  ✅ Zone metrics updated for {len(updates)} players")

    chk = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    for title, col_efg, col_delta, col_fga in [
        ('Top 10 Paint Finishers',       'paint_efg',    'paint_efg_delta',    'paint_fga_pg'),
        ('Top 10 Mid-Range Shooters',    'midrange_efg', 'midrange_efg_delta', 'midrange_fga_pg'),
        ('Top 10 All 3PT Shooters (VW)', 'all3_efg',     'all3_efg_vw',        'all3_fga_pg'),
    ]:
        chk.execute(f"""
            SELECT p.player_name, p.position_group, pm.{col_efg}, pm.{col_delta}, pm.{col_fga}
            FROM player_metrics pm
            JOIN players p ON pm.player_id = p.player_id
            JOIN player_seasons ps ON pm.player_id = ps.player_id
                AND pm.season = ps.season AND pm.season_type = ps.season_type
            WHERE pm.season = %s AND pm.season_type = %s
              AND ps.min >= 1000 AND pm.{col_efg} IS NOT NULL
            ORDER BY pm.{col_delta} DESC NULLS LAST LIMIT 10
        """, (season, season_type))
        print(f"\n  {title}:")
        print(f"  {'Player':<22} {'POS':<4} {'EFG%':>8} {'Delta/VW':>9} {'FGA/G':>7}")
        print(f"  {'─'*54}")
        for r in chk.fetchall():
            print(f"  {r['player_name']:<22} {r['position_group'] or '':<4} "
                  f"{r[col_efg] or 0:>8.3f} {r[col_delta] or 0:>+9.3f} {r[col_fga] or 0:>7.1f}")
    chk.close()


# ── Spot check ────────────────────────────────────────────────
def spot_check(conn, season, season_type):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    print(f"\n{'='*90}")
    print(f"Top 15 Playmakers — {season}")
    print(f"{'='*90}")
    cur.execute("""
        SELECT p.player_name, p.position_group, ps.min, ps.pts, ps.ast,
               ps.net_rating, pm.bpm_computed, pm.playmaker_score,
               pm.creator_score, pm.pot_ast_per_tov, pm.pass_quality_index
        FROM player_metrics pm
        JOIN player_seasons ps ON pm.player_id = ps.player_id
            AND pm.season = ps.season AND pm.season_type = ps.season_type
        JOIN players p ON pm.player_id = p.player_id
        WHERE pm.season = %s AND pm.season_type = %s
          AND pm.playmaker_score IS NOT NULL
        ORDER BY pm.playmaker_score DESC NULLS LAST LIMIT 15
    """, (season, season_type))
    print(f"{'Player':<22} {'Pos':<4} {'MIN':>5} {'PTS':>5} {'AST':>5} {'NET':>6} "
          f"{'BPM':>5} {'PLYMK':>6} {'CREAT':>6} {'POT/TOV':>8} {'PASS_Q':>7}")
    print("─" * 90)
    for r in cur.fetchall():
        print(f"{r['player_name']:<22} {r['position_group'] or '':<4} "
              f"{int(r['min'] or 0):>5} {r['pts'] or 0:>5.1f} {r['ast'] or 0:>5.1f} "
              f"{r['net_rating'] or 0:>6.1f} {r['bpm_computed'] or 0:>5.1f} "
              f"{r['playmaker_score'] or 0:>6.1f} {r['creator_score'] or 0:>6.1f} "
              f"{r['pot_ast_per_tov'] or 0:>8.2f} {r['pass_quality_index'] or 0:>7.3f}")

    print(f"\nTop 10 Defenders:")
    print("─" * 70)
    cur.execute("""
        SELECT p.player_name, p.position_group, ps.stl, ps.blk,
               pm.defender_score, pm.def_delta_overall, pm.rim_protection_score
        FROM player_metrics pm
        JOIN player_seasons ps ON pm.player_id = ps.player_id
            AND pm.season = ps.season AND pm.season_type = ps.season_type
        JOIN players p ON pm.player_id = p.player_id
        WHERE pm.season = %s AND pm.season_type = %s
          AND pm.defender_score IS NOT NULL
        ORDER BY pm.defender_score DESC NULLS LAST LIMIT 10
    """, (season, season_type))
    for r in cur.fetchall():
        print(f"  {r['player_name']:<22} {r['position_group'] or '':<4} "
              f"STL={r['stl'] or 0:.1f}  BLK={r['blk'] or 0:.1f}  "
              f"DEF={r['defender_score'] or 0:.1f}  Δ={r['def_delta_overall'] or 0:+.3f}  "
              f"RIM={r['rim_protection_score'] or 0:.1f}")

    print(f"\nTop 10 Shooters:")
    print("─" * 70)
    cur.execute("""
        SELECT p.player_name, p.position_group, ps.ts_pct,
               pm.shooting_score, pm.contested_fg_making, pm.open_fg_making,
               pm.all3_efg_vw, pm.midrange_efg_vw
        FROM player_metrics pm
        JOIN player_seasons ps ON pm.player_id = ps.player_id
            AND pm.season = ps.season AND pm.season_type = ps.season_type
        JOIN players p ON pm.player_id = p.player_id
        WHERE pm.season = %s AND pm.season_type = %s
          AND pm.shooting_score IS NOT NULL
        ORDER BY pm.shooting_score DESC NULLS LAST LIMIT 10
    """, (season, season_type))
    for r in cur.fetchall():
        print(f"  {r['player_name']:<22} {r['position_group'] or '':<4} "
              f"TS={r['ts_pct'] or 0:.3f}  SHOOT={r['shooting_score'] or 0:.1f}  "
              f"CONT={r['contested_fg_making'] or 0:+.3f}  OPEN={r['open_fg_making'] or 0:+.3f}")
    cur.close()


# ── Main ──────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--season',      default=SEASON)
    parser.add_argument('--season-type', default=SEASON_TYPE)
    args = parser.parse_args()

    season      = args.season
    season_type = args.season_type

    print(f"\nThe Impact Board — Metrics Computation")
    print(f"Season: {season} | Type: {season_type}")
    print(f"Min minutes (composites):    {MIN_MINUTES_TOTAL}")
    print(f"Min minutes (correlations):  {MIN_MINUTES_CORR}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")

    # Pass 1: derived metrics
    conn = psycopg2.connect(DATABASE_URL)
    print("Loading season data...")
    seasons = load_seasons(conn, season, season_type)
    conn.close()

    seasons_map = {s_['player_id']: s_ for s_ in seasons}

    print("Computing derived metrics (pass 1)...")
    metrics_list = [compute_player_metrics(ps) for ps in seasons]
    print(f"  {len(metrics_list)} players")

    print("\nWriting derived metrics (pass 1)...")
    conn = psycopg2.connect(DATABASE_URL)
    upsert_metrics(conn, metrics_list)

    print("\nComputing zone efficiency metrics...")
    compute_zone_metrics(conn, season, season_type)
    conn.close()

    # Pass 2: reload zone data
    print("\nReloading with zone data for composites (pass 2)...")
    conn = psycopg2.connect(DATABASE_URL)
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        'SELECT player_id, paint_efg_vw, midrange_efg_vw, all3_efg_vw,'
        ' corner3_efg_vw, above_break3_efg_vw, paint_fga_pg, midrange_fga_pg, all3_fga_pg'
        ' FROM player_metrics WHERE season = %s AND season_type = %s',
        (season, season_type)
    )
    zone_rows = {r['player_id']: dict(r) for r in cur.fetchall()}
    cur.close()
    conn.close()

    for m in metrics_list:
        pid = m['player_id']
        if pid in zone_rows:
            m.update({k: v for k, v in zone_rows[pid].items()
                      if k != 'player_id' and v is not None})

    # ── Win correlations + weights (computed BEFORE composites) ──
    print("\nComputing win correlations and composite weights...")
    raw_correlations, subcomp_weights, catcomp_weights, flat_weights = \
        compute_win_correlations(metrics_list, seasons_map, season, season_type)

    # ── Pass 2: composites with weights applied ───────────────
    print("\nComputing composites and percentiles (pass 2, weighted)...")
    metrics_list = compute_composites(
        metrics_list, seasons_map, season, season_type,
        subcomp_weights=subcomp_weights,
        catcomp_weights=catcomp_weights,
    )
    qualifying = sum(1 for m in metrics_list if m.get('shooting_score') is not None)
    print(f"  {qualifying} players qualify (>={MIN_MINUTES_TOTAL} min)")

    print("\nWriting final metrics with composites...")
    ZONE_COLS = {
        'paint_efg_vw', 'midrange_efg_vw', 'all3_efg_vw',
        'corner3_efg_vw', 'above_break3_efg_vw',
        'paint_fga_pg', 'midrange_fga_pg', 'all3_fga_pg',
        'paint_efg', 'paint_efg_delta', 'midrange_efg', 'midrange_efg_delta',
        'corner3_efg', 'corner3_efg_delta', 'above_break3_efg', 'above_break3_efg_delta',
        'all3_efg', 'all3_efg_delta',
    }
    metrics_no_zone = [{k: v for k, v in m.items() if k not in ZONE_COLS}
                       for m in metrics_list]
    conn = psycopg2.connect(DATABASE_URL)
    upsert_metrics(conn, metrics_no_zone)

    spot_check(conn, season, season_type)
    conn.close()

    print(f"\n{'='*60}")
    print(f"✅ Done — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()