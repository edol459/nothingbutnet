"""
The Impact Board — Metrics Computation
========================================
python backend/ingest/compute_metrics.py

Computes derived metrics and percentile-based composites
for all players in a given season.

Usage:
    python backend/ingest/compute_metrics.py
    python backend/ingest/compute_metrics.py --season 2023-24
"""

import os
import sys
import math
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
MIN_FGA_PER_GAME   = 2.0
MIN_PULL_UP_FGA    = 50
MIN_CS_FGA         = 50
MIN_DEF_FGA        = 150
MIN_RIM_FGA        = 20
RIM_XFG_BASELINE   = 0.650

# League average EFG% by defender distance — stable year-over-year estimates
LG_CONTESTED_EFG   = 0.398   # 0-4ft (very tight + tight combined)
LG_OPEN_EFG        = 0.548   # 4ft+ (open + wide open combined)

ZONE_MIN_FGA_PG = {
    'paint':        2.0,
    'midrange':     1.0,
    'corner3':      0.5,
    'above_break3': 1.5,
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

    pts  = s(p.get('pts'))
    fga  = s(p.get('fga'))
    fta  = s(p.get('fta'))
    fgm  = s(p.get('fgm'))
    fg3m = s(p.get('fg3m'))
    fg3a = s(p.get('fg3a'))
    tov_pg = s(p.get('tov'))

    # ── Shooting ──────────────────────────────────────────────
    ts_denom        = 2 * (fga + 0.44 * fta)
    ts_pct_computed = div(pts, ts_denom)
    ft_rate         = div(fta, fga)
    efg_pct         = s(p.get('efg_pct'))
    # Keep for legacy/sidebar use
    shot_quality_delta = None  # removed — not true shot quality, use contested/open_fg_making instead

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

    # ── Shot making vs expectation (contested / open) ─────────
    # Contested = Very Tight (0-2ft) + Tight (2-4ft)
    # Open      = Open (4-6ft) + Wide Open (6ft+)
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

    potential_ast_pg   = potential_ast   / gp if gp > 0 else 0
    secondary_ast_pg   = secondary_ast   / gp if gp > 0 else 0
    passes_made_pg     = passes_made     / gp if gp > 0 else 0
    ast_pts_created_pg = ast_pts_created / gp if gp > 0 else 0
    drive_passes_pg    = drive_passes    / gp if gp > 0 else drive_passes

    potential_ast_per75 = potential_ast_pg * per75 if potential_ast_pg > 0 else None
    ast_conversion_rate = div(ast_pg, potential_ast_pg)
    poss_used_passing   = div(time_of_poss, min_pg) if min_pg > 0 else None
    playmaking_gravity  = div(ast_pts_created_pg, poss_used_passing) if poss_used_passing else None
    secondary_ast_per75 = secondary_ast_pg * per75 if secondary_ast_pg > 0 else None
    pass_to_score_pct   = div(ast_pg, passes_made_pg) if passes_made_pg > 0 else None
    ball_handler_load   = div(time_of_poss, min_pg)
    drive_total         = drive_fga + drive_tov + drive_passes_pg
    drive_and_dish_rate = div(drive_passes_pg, drive_total)
    pass_quality_index  = div(ast_pts_created_pg, passes_made_pg)

    bad_pass_total = safe(p.get('bad_pass_tov'))
    if bad_pass_total is not None and bad_pass_total > 0 and gp > 0:
        pot_ast_per_tov = div(potential_ast_pg, bad_pass_total / gp)
    else:
        pot_ast_per_tov = div(potential_ast_pg, tov_pg) if tov_pg and tov_pg > 0 else None

    # ── New derived metrics ───────────────────────────────────
    # Drive foul rate — how often drives result in fouls drawn
    drive_foul_rate = div(drive_pf, drives) if drives > 0 else None

    # TOV% — standard turnover rate formula
    tov_pct = div(tov_pg, fga + 0.44 * fta + tov_pg) if (fga + fta + tov_pg) > 0 else None

    # ── Defense ───────────────────────────────────────────────
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

    def_rim_fga    = s(p.get('def_rim_fga'))
    def_rim_fg_pct = s(p.get('def_rim_fg_pct'))
    rim_protection_score = (RIM_XFG_BASELINE - def_rim_fg_pct) * def_rim_fga * 2 if def_rim_fga >= MIN_RIM_FGA else None

    stl         = s(p.get('stl'))
    deflections = s(p.get('deflections')) / gp if gp > 0 else 0
    charges     = s(p.get('charges_drawn')) / gp if gp > 0 else 0
    def_disruption_rate = (stl + deflections * 0.35 + charges * 0.80) * per75 if (stl + deflections + charges) > 0 else None

    box_outs_pg  = s(p.get('box_outs')) / gp if gp > 0 else 0
    box_out_rate = box_outs_pg * per75 if box_outs_pg > 0 else None

    # ── Hustle ────────────────────────────────────────────────
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

    # ── Role ──────────────────────────────────────────────────
    post_touches  = s(p.get('post_touches'))
    elbow_touches = s(p.get('elbow_touches'))

    creation_load        = div(drives + post_touches + elbow_touches, touches)
    avg_drib_per_touch   = s(p.get('avg_drib_per_touch'))
    dribble_pressure_idx = avg_drib_per_touch if avg_drib_per_touch > 0 else None
    cs_fga_rate          = div(cs_fga_tot, fga) if fga >= MIN_FGA_PER_GAME * gp else None

    # ── BPM ───────────────────────────────────────────────────
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

        # Shooting
        'ts_pct_computed':     r(ts_pct_computed),
        'ft_rate':             r(ft_rate),
        'creation_premium':    r(creation_premium),
        'paint_scoring_rate':  r(paint_scoring_rate),

        # New shot making
        'contested_fg_making': r(contested_fg_making, 4),
        'open_fg_making':      r(open_fg_making, 4),

        # New derived
        'drive_foul_rate':     r(drive_foul_rate, 4),
        'tov_pct':             r(tov_pct, 4),
        'ast_pts_created_pg':  r(ast_pts_created_pg, 2),

        # Playmaking
        'potential_ast_per75':  r(potential_ast_per75, 3),
        'ast_conversion_rate':  r(ast_conversion_rate),
        'playmaking_gravity':   r(playmaking_gravity),
        'secondary_ast_per75':  r(secondary_ast_per75, 3),
        'pass_to_score_pct':    r(pass_to_score_pct),
        'ball_handler_load':    r(ball_handler_load),
        'drive_and_dish_rate':  r(drive_and_dish_rate),
        'pot_ast_per_tov':      r(pot_ast_per_tov, 3),
        'pass_quality_index':   r(pass_quality_index, 4),

        # Defense
        'def_delta_overall':    r(def_delta_overall),
        'def_delta_2pt':        r(def_delta_2pt),
        'def_delta_3pt':        r(def_delta_3pt),
        'rim_protection_score': r(rim_protection_score, 3),
        'def_disruption_rate':  r(def_disruption_rate, 3),
        'box_out_rate':         r(box_out_rate, 3),

        # Hustle
        'screen_assist_rate':   r(screen_assist_rate, 3),
        'loose_ball_rate':      r(loose_ball_rate, 3),
        'hustle_composite':     r(hustle_composite, 3),
        'motor_score':          r(motor_score, 3),

        # Role / misc
        'creation_load':        r(creation_load),
        'dribble_pressure_idx': r(dribble_pressure_idx, 3),
        'cs_fga_rate':          r(cs_fga_rate),
        'bpm_computed':         r(bpm_computed, 2),
    }


# ── Composite computation ─────────────────────────────────────
def compute_composites(metrics_list, seasons_map):

    # Build position groups for position-normalized percentiles
    pos_groups = {}
    for m in metrics_list:
        pid   = m['player_id']
        ps    = seasons_map.get(pid, {})
        pos_g = ps.get('position_group', 'F')
        min_t = s(ps.get('min'), 0)
        if min_t < MIN_MINUTES_TOTAL: continue
        pos_groups.setdefault(pos_g, []).append(pid)

    all_qualifying = [pid for pids in pos_groups.values() for pid in pids]
    metrics_by_pid = {m['player_id']: m for m in metrics_list}

    def get_val(pid, col, src):
        """Get metric value from metrics dict (src='m') or seasons dict (src='s')."""
        if src == 'm':
            return safe(metrics_by_pid.get(pid, {}).get(col))
        return safe(seasons_map.get(pid, {}).get(col))

    def percentile_map(pids, col, src):
        """Return {pid: 0-100 percentile} for a metric over a set of players."""
        vals = [(pid, get_val(pid, col, src)) for pid in pids]
        vals = [(pid, v) for pid, v in vals if v is not None]
        if not vals: return {}
        sorted_vals = sorted(vals, key=lambda x: x[1])
        n = len(sorted_vals)
        return {pid: round((i / (n - 1)) * 100, 1) if n > 1 else 50.0
                for i, (pid, _) in enumerate(sorted_vals)}

    # ── Pre-compute all percentile maps ───────────────────────
    # League-wide maps (scoring, playmaking categories)
    ALL_METRICS_LG = [
        # Shooting (league-wide)
        ('ts_pct',              's'),
        ('spotup_efg_pct',      's'),
        ('all3_efg_vw',         'm'),
        ('midrange_efg_vw',     'm'),
        # Creation (league-wide)
        ('pct_uast_fgm',        's'),
        ('iso_ppp',             's'),
        ('pull_up_efg_pct',     's'),
        ('drive_fg_pct',        's'),
        ('usg_pct',             's'),
        ('tov_pct',             'm'),
        # Passing (league-wide)
        ('pot_ast_per_tov',     'm'),
        ('pass_quality_index',  'm'),
        ('ast_pct',             's'),
        ('secondary_ast_per75', 'm'),
        ('ast_pts_created_pg',  'm'),
        # Ball handling (league-wide)
        ('playmaking_gravity',  'm'),
        ('ball_handler_load',   'm'),
        ('drive_and_dish_rate', 'm'),
        ('pnr_bh_ppp',          's'),
        ('transition_ppp',      's'),
        ('avg_drib_per_touch',  's'),
        # Needed for inverted maps
        ('def_iso_ppp',         's'),
        ('def_pnr_bh_ppp',      's'),
    ]
    ALL_METRICS_POS = [
        # Finishing (position-normalized)
        ('paint_efg_vw',        'm'),
        ('paint_scoring_rate',  'm'),
        ('post_ppp',            's'),
        ('drive_foul_rate',     'm'),
        # Perimeter defense (position-normalized)
        ('def_delta_3pt',        'm'),
        ('def_delta_overall',    'm'),
        ('def_disruption_rate',  'm'),
        ('contested_shots',      's'),
        ('def_iso_ppp',          's'),
        ('def_pnr_bh_ppp',       's'),
        # Interior defense (position-normalized)
        ('rim_protection_score', 'm'),
        ('def_delta_2pt',        'm'),
        ('dreb_pct',             's'),
        ('blk',                  's'),
        ('box_out_rate',         'm'),
        # Activity (position-normalized)
        ('motor_score',          'm'),
        ('hustle_composite',     'm'),
        ('screen_assist_rate',   'm'),
        # Rebounding (position-normalized)
        ('oreb_pct',             's'),
        ('reb_pct',              's'),
    ]

    pct_lg = {col: percentile_map(all_qualifying, col, src)
              for col, src in ALL_METRICS_LG}

    pct_pos = {}
    for col, src in ALL_METRICS_POS:
        merged_pos = {}
        for pos_g, pids in pos_groups.items():
            merged_pos.update(percentile_map(pids, col, src))
        pct_pos[col] = merged_pos

    # Inverted metrics: lower raw = better = higher percentile
    # tov_pct — lower turnover rate is better
    pct_lg['tov_pct_inv'] = {pid: round(100 - v, 1) for pid, v in pct_lg['tov_pct'].items()}

    # def_iso_ppp, def_pnr_bh_ppp — lower PPP allowed = better
    # Build both league-wide and position-normalized inverted maps
    for col in ['def_iso_ppp', 'def_pnr_bh_ppp']:
        pct_lg[f'{col}_inv']  = {pid: round(100 - v, 1) for pid, v in pct_lg[col].items()}
        inv_pos = {}
        for pos_g, pids in pos_groups.items():
            raw_pos = percentile_map(pids, col, 's')
            for pid, v in raw_pos.items():
                inv_pos[pid] = round(100 - v, 1)
        pct_pos[f'{col}_inv'] = inv_pos

    def avg_pct(pid, cols_srcs, pct_maps, min_metrics=1):
        """Average percentile across metrics, skipping NULLs.
        min_metrics: minimum non-NULL metrics required to return a score."""
        vals = []
        for col, src in cols_srcs:
            pmap = pct_maps.get(col, {})
            v = pmap.get(pid)
            if v is not None:
                vals.append(v)
        if len(vals) < min_metrics:
            return None
        return round(sum(vals) / len(vals), 1) if vals else None

    # ── Volume gates ──────────────────────────────────────────
    # Stats from game logs (fga, ast, pts etc.) are per-game averages.
    # Stats from tracking endpoints (drives, touches, paint_touches, def_rim_fga)
    # are season totals — divide by gp to get per-game rate.
    def passes_gate(pid, gate_key):
        ps    = seasons_map.get(pid, {})
        pos_g = ps.get('position_group', 'F')
        gp    = max(s(ps.get('gp'), 1), 1)
        if gate_key == 'finishing':
            return s(ps.get('paint_touches'), 0) / gp >= 3.0
        if gate_key == 'shooting':
            return s(ps.get('fga'), 0) >= 3.0          # per-game avg
        if gate_key == 'creation':
            return s(ps.get('drives'), 0) / gp >= 2.0
        if gate_key == 'passing':
            return s(ps.get('ast'), 0) >= 2.0           # per-game avg
        if gate_key == 'ballhandling':
            return (pos_g in ('G', 'GF') and
                    s(ps.get('touches'), 0) / gp >= 40.0)
        if gate_key == 'interior_def':
            return s(ps.get('def_rim_fga'), 0) >= 50   # season total
        return True  # no gate

    # ── Sub-composite definitions ─────────────────────────────
    # (output_col, gate_key, [(metric_col, src), ...], pct_map_key)
    # pct_map_key: 'lg' = league-wide, 'pos' = position-normalized
    SUB_COMPOSITES = [

        # FINISHING — position-normalized, no hard gate
        # paint_efg_vw and drive_foul_rate are volume-weighted so low-volume players
        # naturally score low without needing a gate. post_ppp requires min touches
        # via avg_pct NULL-skipping. Removing gate allows guards to get finishing scores.
        ('finishing_score', None,
         [('paint_efg_vw',        'm'),
          ('paint_scoring_rate',  'm'),
          ('post_ppp',            's'),
          ('drive_foul_rate',     'm')],
         'pos'),

        # SHOOTING — league-wide, min FGA gate
        # All three metrics are purely perimeter-based — no rim shooting included.
        # spotup_efg_pct: catch-and-shoot quality (everyone has this data)
        # all3_efg_vw: 3PT volume-weighted delta (volume penalizes low-attempt bigs)
        # midrange_efg_vw: mid-range volume-weighted delta (KD, Booker, SGA)
        # ts_pct excluded: rewards rim finishers, not a perimeter shooting metric.
        # NULLs skipped in avg_pct so a player needs at least 1 qualifying metric,
        # but the FGA gate + volume-weighting on VW metrics naturally suppresses bigs.
        ('shooting_score', 'shooting',
         [('spotup_efg_pct',  's'),
          ('all3_efg_vw',     'm'),
          ('midrange_efg_vw', 'm')],
         'lg'),

        # CREATION — league-wide, drives/g gate
        # Captures: self-creation frequency, ISO efficiency, pull-up shooting,
        #           drive finishing, usage volume, turnover penalty
        ('creation_score', 'creation',
         [('pct_uast_fgm',    's'),
          ('iso_ppp',         's'),
          ('pull_up_efg_pct', 's'),
          ('drive_fg_pct',    's'),
          ('usg_pct',         's'),
          ('tov_pct_inv',     'm')],
         'lg'),

        # PASSING — league-wide, AST/g gate
        # Captures: decision quality, pass value, volume, ball movement, pts created
        ('passing_score', 'passing',
         [('pot_ast_per_tov',    'm'),
          ('pass_quality_index', 'm'),
          ('ast_pct',            's'),
          ('secondary_ast_per75','m'),
          ('ast_pts_created_pg', 'm')],
         'lg'),

        # BALL HANDLING — league-wide, G/GF + touches gate
        # Captures: gravity/efficiency, usage time, drive+dish decisions,
        #           PnR BH efficiency, transition offense, dribble creation style
        ('ballhandling_score', 'ballhandling',
         [('playmaking_gravity',  'm'),
          ('ball_handler_load',   'm'),
          ('drive_and_dish_rate', 'm'),
          ('pnr_bh_ppp',          's'),
          ('transition_ppp',      's'),
          ('avg_drib_per_touch',  's')],
         'lg'),

        # PERIMETER DEFENSE — position-normalized, no gate
        # Captures: 3PT defense, overall shot defense, disruption, contesting volume.
        # def_iso_ppp and def_pnr_bh_ppp removed — these are guard-centric metrics.
        # For bigs, samples are tiny and selection-biased (only best shots taken vs
        # elite rim protectors), which punishes great defenders like Wemby.
        # def_delta_overall and def_delta_3pt already capture their perimeter impact.
        ('perimeter_def_score', None,
         [('def_delta_3pt',       'm'),
          ('def_delta_overall',   'm'),
          ('def_disruption_rate', 'm'),
          ('contested_shots',     's')],
         'pos'),

        # INTERIOR DEFENSE — position-normalized, rim FGA gate
        # Captures: rim protection score, 2PT defense, dreb, blocking, boxing out
        ('interior_def_score', 'interior_def',
         [('rim_protection_score', 'm'),
          ('def_delta_2pt',        'm'),
          ('dreb_pct',             's'),
          ('blk',                  's'),
          ('box_out_rate',         'm')],
         'pos'),

        # ACTIVITY — position-normalized, no gate
        # Captures: motor/effort (distance), combined hustle events, screening value
        ('activity_score', None,
         [('motor_score',        'm'),
          ('hustle_composite',   'm'),
          ('screen_assist_rate', 'm')],
         'pos'),

        # REBOUNDING — position-normalized, no gate
        # Captures: defensive rebounding, offensive rebounding, total rebounding, boxing out
        ('rebounding_score', None,
         [('dreb_pct',    's'),
          ('oreb_pct',    's'),
          ('reb_pct',     's'),
          ('box_out_rate','m')],
         'pos'),
    ]

    # ── Category composite definitions ────────────────────────
    # Scoring/Playmaking: average available sub-scores (player qualifies for what they do)
    # Playmaking: REQUIRES both passing + ball handling (otherwise NULL)
    # Defense/Hustle: average available sub-scores
    CAT_COMPOSITES = [
        ('creator_score',   ['finishing_score', 'shooting_score', 'creation_score']),
        # Note: creator_score requires creation_score non-NULL (enforced below)
        ('playmaker_score', ['passing_score', 'ballhandling_score']),
        ('defender_score',  ['perimeter_def_score', 'interior_def_score']),
        ('hustle_score',    ['activity_score', 'rebounding_score']),
    ]

    # ── Score each player ─────────────────────────────────────
    for m in metrics_list:
        pid   = m['player_id']
        min_t = s(seasons_map.get(pid, {}).get('min'), 0)

        if min_t < MIN_MINUTES_TOTAL:
            for name, _, _, _ in SUB_COMPOSITES:
                m[name] = None
            for name, _ in CAT_COMPOSITES:
                m[name] = None
            m['three_and_d_score'] = None
            continue

        # Sub-composites
        # shooting_score requires at least 2 of 3 perimeter metrics non-NULL
        # to prevent bigs with only spotup data from ranking as shooters
        for comp_name, gate_key, cols_srcs, pct_key in SUB_COMPOSITES:
            if gate_key and not passes_gate(pid, gate_key):
                m[comp_name] = None
                continue
            pct_maps = pct_lg if pct_key == 'lg' else pct_pos
            min_m = 2 if comp_name == 'shooting_score' else 1
            m[comp_name] = avg_pct(pid, cols_srcs, pct_maps, min_metrics=min_m)

        # Category composites
        for comp_name, sub_names in CAT_COMPOSITES:
            sub_vals  = [safe(m.get(sn)) for sn in sub_names]
            available = [v for v in sub_vals if v is not None]

            if comp_name == 'playmaker_score':
                # Require BOTH passing and ball handling — no partial credit
                m[comp_name] = (round(sum(sub_vals) / len(sub_vals), 1)
                                if all(v is not None for v in sub_vals) else None)
            elif comp_name == 'creator_score':
                # Require creation_score — pure shooters without creation don't rank
                # on overall scoring. Finishing is optional (not all scorers post up).
                creation_val = safe(m.get('creation_score'))
                if creation_val is None:
                    m[comp_name] = None
                else:
                    m[comp_name] = round(sum(available) / len(available), 1) if available else None
            else:
                # Average whatever sub-scores the player qualifies for
                m[comp_name] = round(sum(available) / len(available), 1) if available else None

        # Three-and-D bonus score
        pdef  = safe(m.get('perimeter_def_score'))
        shoot = safe(m.get('shooting_score'))
        m['three_and_d_score'] = (round((pdef + shoot) / 2, 1)
                                  if (pdef is not None and shoot is not None) else None)

    # ── Drawer/sort percentiles ───────────────────────────────
    pctile_cols = [
        ('ts_pct',      'ts_pct',             's',  False),
        ('usg_pct',     'usg_pct',            's',  False),
        ('ast_pct',     'ast_pct',            's',  False),
        ('net_rating',  'net_rating',         's',  False),
        ('def_delta',   'def_delta_overall',  'm',  False),
        ('rim_prot',    'rim_protection_score','m', False),
        ('playmaker',   'playmaker_score',    'm',  False),
        ('creator',     'creator_score',      'm',  False),
        ('defender',    'defender_score',     'm',  True),   # position-normalized
        ('three_and_d', 'three_and_d_score',  'm',  False),
        ('hustle',      'hustle_score',       'm',  False),
    ]
    for pctile_name, col, src, pos_norm in pctile_cols:
        if pos_norm:
            # Position-normalized: rank within position group, then merge
            rank_map = {}
            for pos_g, pids in pos_groups.items():
                grp_vals = []
                for pid in pids:
                    min_t = s(seasons_map.get(pid, {}).get('min'), 0)
                    if min_t < MIN_MINUTES_TOTAL: continue
                    v = safe(metrics_by_pid.get(pid, {}).get(col)) if src == 'm' else safe(seasons_map.get(pid, {}).get(col))
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
                pid   = m['player_id']
                min_t = s(seasons_map.get(pid, {}).get('min'), 0)
                if min_t < MIN_MINUTES_TOTAL: continue
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

    # Reconnect per chunk to avoid Railway connection timeouts
    import os
    from dotenv import load_dotenv
    load_dotenv()
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
                    import time
                    time.sleep(3)
                else:
                    raise e
        print(f"  ... {min(i+CHUNK, len(cleaned))}/{len(cleaned)}")
    print(f"  ✅ Upserted {len(rows)} metric rows")


# ── Zone metrics (shot location EFG+ VW) ─────────────────────
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
            is3 = z in ('Corner 3','Left Corner 3','Right Corner 3','Above the Break 3')
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

        pf_pg = round(pf / gp, 2)
        mf_pg = round(mf / gp, 2)
        cf_pg = round(cf / gp, 2)
        af_pg = round(af / gp, 2)

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
            pe, pd_, pf_pg, paint_vw,
            me, md, mf_pg, mid_vw,
            ce, cd, cf_pg, c3_vw,
            ae, ad, af_pg, ab3_vw,
            all3_efg, all3_delta, all3_fga_pg, all3_vw,
            pid, season, season_type
        ))

    upd = conn.cursor()
    upd.executemany("""
        UPDATE player_metrics SET
            paint_efg=%s,         paint_efg_delta=%s,        paint_fga_pg=%s,        paint_efg_vw=%s,
            midrange_efg=%s,      midrange_efg_delta=%s,     midrange_fga_pg=%s,     midrange_efg_vw=%s,
            corner3_efg=%s,       corner3_efg_delta=%s,      corner3_fga_pg=%s,      corner3_efg_vw=%s,
            above_break3_efg=%s,  above_break3_efg_delta=%s, above_break3_fga_pg=%s, above_break3_efg_vw=%s,
            all3_efg=%s,          all3_efg_delta=%s,         all3_fga_pg=%s,         all3_efg_vw=%s,
            updated_at=NOW()
        WHERE player_id=%s AND season=%s AND season_type=%s
    """, updates)
    conn.commit()
    upd.close()
    print(f"  ✅ Zone metrics updated for {len(updates)} players")

    # Spot check
    chk = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    for title, col_efg, col_delta, col_fga in [
        ('Top 10 Paint Finishers',      'paint_efg',   'paint_efg_delta',   'paint_fga_pg'),
        ('Top 10 Mid-Range Shooters',   'midrange_efg','midrange_efg_delta','midrange_fga_pg'),
        ('Top 10 All 3PT Shooters (VW)','all3_efg',    'all3_efg_vw',       'all3_fga_pg'),
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
        JOIN player_seasons ps ON pm.player_id = ps.player_id AND pm.season = ps.season AND pm.season_type = ps.season_type
        JOIN players p ON pm.player_id = p.player_id
        WHERE pm.season = %s AND pm.season_type = %s AND ps.min >= %s
        ORDER BY pm.playmaker_score DESC NULLS LAST LIMIT 15
    """, (season, season_type, MIN_MINUTES_TOTAL))
    print(f"{'Player':<22} {'Pos':<4} {'MIN':>5} {'PTS':>5} {'AST':>5} {'NET':>6} {'BPM':>5} {'PLYMK':>6} {'CREAT':>6} {'POT/TOV':>8} {'PASS_Q':>7}")
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
        JOIN player_seasons ps ON pm.player_id = ps.player_id AND pm.season = ps.season AND pm.season_type = ps.season_type
        JOIN players p ON pm.player_id = p.player_id
        WHERE pm.season = %s AND pm.season_type = %s AND ps.min >= %s
        ORDER BY pm.defender_score DESC NULLS LAST LIMIT 10
    """, (season, season_type, MIN_MINUTES_TOTAL))
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
        JOIN player_seasons ps ON pm.player_id = ps.player_id AND pm.season = ps.season AND pm.season_type = ps.season_type
        JOIN players p ON pm.player_id = p.player_id
        WHERE pm.season = %s AND pm.season_type = %s AND ps.min >= %s
        ORDER BY pm.shooting_score DESC NULLS LAST LIMIT 10
    """, (season, season_type, MIN_MINUTES_TOTAL))
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
    print(f"Min minutes for composites: {MIN_MINUTES_TOTAL}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")

    # Pass 1: derived metrics + zone metrics
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

    # Pass 2: reload zone metrics, then compute composites
    print("\nReloading with zone data for composites (pass 2)...")
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        'SELECT player_id, paint_efg_vw, midrange_efg_vw, all3_efg_vw,'
        ' corner3_efg_vw, above_break3_efg_vw, paint_fga_pg, midrange_fga_pg,'
        ' all3_fga_pg'
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

    print("Computing composites and percentiles (pass 2)...")
    metrics_list = compute_composites(metrics_list, seasons_map)
    qualifying   = sum(1 for m in metrics_list if m.get('shooting_score') is not None)
    print(f"  {qualifying} players qualify (>={MIN_MINUTES_TOTAL} min)")

    print("\nWriting final metrics with composites...")
    # Strip zone cols from metrics_list before upserting — compute_zone_metrics
    # owns those columns and already wrote them. Re-upserting would overwrite
    # them with potentially stale values from zone_rows merge.
    ZONE_COLS = {'paint_efg_vw', 'midrange_efg_vw', 'all3_efg_vw',
                 'corner3_efg_vw', 'above_break3_efg_vw',
                 'paint_fga_pg', 'midrange_fga_pg', 'all3_fga_pg',
                 'paint_efg', 'paint_efg_delta',
                 'midrange_efg', 'midrange_efg_delta',
                 'corner3_efg', 'corner3_efg_delta',
                 'above_break3_efg', 'above_break3_efg_delta',
                 'all3_efg', 'all3_efg_delta'}
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