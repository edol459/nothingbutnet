"""
scoring_engine.py — The Impact Board
Single source of truth for all scoring methodology.

Imported by:
  - compute_metrics.py  (offline batch computation)
  - server.py           (live /api/builder endpoint)

Nothing in here touches the database or filesystem.
All functions take plain dicts of player/season data and percentile maps.
"""

import math

# ── Top-level stat groupings ──────────────────────────────────────────────────

SUBCOMP_STATS = {
    'finishing_score': [
        'paint_efg_vw', 'drive_pts_per_drive', 'pnr_roll_ppp', 'pts_2nd_chance',
    ],
    'shooting_score': [
        'all3_efg_vw', 'midrange_efg_vw', 'sq_fg_pct_above_expected',
    ],
    'shot_creation_score': [
        'pct_uast_fgm', 'iso_ppp', 'pull_up_efg_pct', 'drive_fg_pct',
        'usg_pct', 'leverage_shooting', 'fta',
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
        'def_delta_3pt', 'def_disruption_rate',
        'contested_shots', 'stl', 'def_spotup_ppp',
    ],
    'interior_def_score': [
        'rim_protection_score', 'dreb_pct', 'blk',
        'def_post_ppp', 'def_pnr_roll_ppp',
    ],
    'activity_score': [
        'hustle_composite', 'screen_assist_rate',
        'leverage_rebounds', 'leverage_turnovers',
    ],
    'rebounding_score': [
        'dreb_pct', 'oreb_pct', 'box_out_rate',
    ],
    'gravity_perimeter_score': [
        'gravity_onball_perimeter', 'gravity_offball_perimeter',
    ],
    'gravity_interior_score': [
        'gravity_onball_interior', 'gravity_offball_interior',
    ],
    'gravity_score': [
        'gravity_onball_perimeter', 'gravity_offball_perimeter',
        'gravity_onball_interior', 'gravity_offball_interior',
    ],
    'defender_extras_score': [
        'leverage_defense', 'def_ws', 'matchup_def_fg_pct_adj',
    ],
}

CATCOMP_STATS = {
    'creator_score':   ['finishing_score', 'shooting_score', 'shot_creation_score'],
    'playmaker_score': ['passing_score', 'creation_score', 'decision_making_score'],
    'defender_score':  ['perimeter_def_score', 'interior_def_score'],
    'intangibles_score': ['activity_score', 'rebounding_score', 'gravity_perimeter_score', 'gravity_interior_score'],
}

# Stats where lower raw = better; stored in pct maps as _inv (already flipped)
LOWER_BETTER = {
    'tov_pct', 'lost_ball_tov_pg', 'bad_pass_tov_pg',
    'def_iso_ppp', 'def_pnr_bh_ppp', 'def_post_ppp',
    'def_spotup_ppp', 'def_pnr_roll_ppp',
    'leverage_turnovers', 'matchup_def_fg_pct_adj',
}

# Maps _inv key → base stat key (for weight lookup)
INVERT_MAP = {
    'tov_pct_inv':          'tov_pct',
    'lost_ball_tov_pg_inv': 'lost_ball_tov_pg',
    'def_spotup_ppp_inv':   'def_spotup_ppp',
    'def_post_ppp_inv':     'def_post_ppp',
    'def_pnr_roll_ppp_inv': 'def_pnr_roll_ppp',
}

# Maps user-facing stat key → the key stored in the percentile JSON.
# Lower-better stats are pre-inverted in the JSON so higher pctile = better.
SERVER_KEY_MAP = {
    'lost_ball_tov_pg':   'lost_ball_tov_pg_inv',
    'tov_pct':            'tov_pct_inv',
    'def_spotup_ppp':     'def_spotup_ppp_inv',
    'def_post_ppp':       'def_post_ppp_inv',
    'def_pnr_roll_ppp':   'def_pnr_roll_ppp_inv',
    'def_iso_ppp':        'def_iso_ppp_inv',
    'def_pnr_bh_ppp':     'def_pnr_bh_ppp_inv',
}

# ── Sub-composite definitions ─────────────────────────────────────────────────
# Each entry: (comp_name, gate_key, [(stat_key_in_pctmap, src)], pct_pool)
#   gate_key  — None means no composite-level gate
#   src       — 'm' = player_metrics table, 's' = player_seasons table
#   pct_pool  — 'lg' = league-wide percentile map, 'pos' = position-normalized
#
# IMPORTANT: stat keys here are the keys AS STORED in the percentile maps,
# i.e. lower-better stats already use their _inv key (tov_pct_inv, etc.)

SUB_COMPOSITES = [
    ('finishing_score', None,
     [('paint_efg_vw', 'm'), ('drive_pts_per_drive', 'm'),
      ('pnr_roll_ppp', 's'), ('pts_2nd_chance', 's')],
     'pos'),

    ('shooting_score', 'shooting',
     [('all3_efg_vw', 'm'), ('midrange_efg_vw', 'm'),
      ('sq_fg_pct_above_expected', 's')],
     'pos'),

    ('shot_creation_score', None,
     [('pct_uast_fgm', 's'), ('iso_ppp', 's'), ('pull_up_efg_pct', 's'),
      ('drive_fg_pct', 's'), ('usg_pct', 's'), ('leverage_shooting', 's'),
      ('fta', 's')],
     'lg'),

    ('passing_score', 'passing',
     [('pot_ast_per_tov', 'm'), ('ast_pct', 's'), ('pass_quality_index', 'm')],
     'pos'),

    ('creation_score', 'pm_creation',
     [('leverage_creation', 's'), ('ast_pts_created_pg', 'm'), ('ft_ast_per75', 'm')],
     'lg'),

    ('decision_making_score', 'ball_handling',
     [('lost_ball_tov_pg_inv', 'm'), ('pnr_bh_ppp', 's')],
     'lg'),

    ('perimeter_def_score', None,
     [('def_delta_3pt', 'm'),
      ('def_disruption_rate', 'm'), ('contested_shots', 's'),
      ('stl', 's'), ('def_spotup_ppp_inv', 's')],
     'lg'),

    ('interior_def_score', 'interior_def',
     [('rim_protection_score', 'm'),
      ('dreb_pct', 's'), ('blk', 's'),
      ('def_post_ppp_inv', 's'), ('def_pnr_roll_ppp_inv', 's')],
     'lg'),

    ('activity_score', None,
     [('hustle_composite', 'm'), ('screen_assist_rate', 'm'),
      ('leverage_rebounds', 's'), ('leverage_turnovers', 'm')],
     'pos'),

    ('rebounding_score', None,
     [('dreb_pct', 's'), ('oreb_pct', 's'), ('box_out_rate', 'm')],
     'pos'),

    # Defender extras — outcome-based signals (pos-normalized, no gate)
    # These get proper win-correlation weights via the subcomp system.
    ('defender_extras_score', None,
     [('leverage_defense',        's'),
      ('def_ws',                  's'),
      ('matchup_def_fg_pct_adj',  'm')],
     'lg'),

    # Gravity overall — all 4 stats, league-wide, no position gate
    # Win-weighted avg percentile of all 4 gravity dimensions.
    # Replaces the raw NBA gravity_score with our computed composite.
    ('gravity_score', None,
     [('gravity_onball_perimeter',  's'),
      ('gravity_offball_perimeter', 's'),
      ('gravity_onball_interior',   's'),
      ('gravity_offball_interior',  's')],
     'lg'),

    # Gravity split by position role:
    # Perimeter gravity — G/GF/F only (gate_key='perimeter_player')
    # Interior gravity — FC/C only (gate_key='interior_player')
    ('gravity_perimeter_score', 'perimeter_player',
     [('gravity_onball_perimeter', 's'), ('gravity_offball_perimeter', 's')],
     'lg'),

    ('gravity_interior_score', 'interior_player',
     [('gravity_onball_interior', 's'), ('gravity_offball_interior', 's')],
     'lg'),
]

# Defender extra signals beyond the two sub-composites
DEFENDER_EXTRAS = [
    ('leverage_defense', 'pos'),
    ('def_ws',           'pos'),
]

# ── Gate functions ────────────────────────────────────────────────────────────

def safe(val, default=None):
    if val is None: return default
    try:
        f = float(val)
        return default if math.isnan(f) else f
    except (TypeError, ValueError):
        return default

def s(val, default=0.0):
    return safe(val, default)

def passes_gate(ps, gate_key):
    """
    Check whether a player qualifies for a sub-composite gate.

    ps: dict of player_seasons row (season totals + per-game avgs).
        ast, fga = per-game averages.
        drives, touches, paint_touches, potential_ast, def_rim_fga = season totals.
    """
    if not gate_key:
        return True
    gp = max(s(ps.get('gp'), 1), 1)
    if gate_key == 'finishing':
        return s(ps.get('paint_touches'), 0) / gp >= 3.0
    if gate_key == 'shooting':
        return s(ps.get('fga'), 0) >= 3.0
    if gate_key == 'shot_creation':
        return s(ps.get('drives'), 0) / gp >= 2.0
    if gate_key == 'passing':
        return (s(ps.get('ast'), 0) >= 0.5 and
                s(ps.get('potential_ast'), 0) / gp >= 1.0)
    if gate_key == 'pm_creation':
        return (s(ps.get('potential_ast'), 0) / gp >= 3.0 and
                s(ps.get('touches'), 0) / gp >= 40.0)
    if gate_key == 'ball_handling':
        return (s(ps.get('drives'), 0) / gp >= 4.0 and
                s(ps.get('touches'), 0) / gp >= 40.0)
    if gate_key == 'interior_def':
        return s(ps.get('def_rim_fga'), 0) / gp >= 3.5
    # Gravity position gates — based on position_group
    pos = ps.get('position_group', '')
    if gate_key == 'perimeter_player':
        return pos in ('G', 'GF', 'F')
    if gate_key == 'interior_player':
        return pos in ('FC', 'C')
    return True

# ── Core scoring primitives ───────────────────────────────────────────────────

def weighted_avg_pct(pid, cols_srcs, pct_maps, comp_name, subcomp_weights, min_metrics=1):
    """
    Weighted average of percentiles for one sub-composite.

    pid            — player id (int or str, will be matched against pct_maps keys)
    cols_srcs      — list of (pct_map_key, src) from SUB_COMPOSITES
    pct_maps       — {'lg': {stat: {pid: pctile}}, 'pos': {stat: {pid: pctile}}}
                     OR a single flat {stat: {pid: pctile}} dict
    comp_name      — sub-composite name, for weight lookup
    subcomp_weights— {comp_name: {stat_key: weight}}
    min_metrics    — minimum non-null stats required (1 for ≤2 stats, 2 for 3+)
    """
    comp_w = subcomp_weights.get(comp_name, {})
    vals_weights = []

    # pct_maps can be a nested dict {'lg':..., 'pos':...} or a flat dict
    def _lookup(col, pool):
        if isinstance(pct_maps, dict) and ('lg' in pct_maps or 'pos' in pct_maps):
            flat = pct_maps.get(pool, {})
        else:
            flat = pct_maps
        pmap = flat.get(col, {})
        # Try int pid and str pid
        v = pmap.get(pid) if pmap.get(pid) is not None else pmap.get(str(pid))
        return v

    for col, src in cols_srcs:
        # Determine which pool this col belongs to by looking up the parent subcomp
        pool = _get_pool_for_col(col, comp_name)
        v = _lookup(col, pool)
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

def _get_pool_for_col(col, comp_name):
    """Return 'lg' or 'pos' for a col within a named sub-composite."""
    for name, _gate, cols_srcs, pool in SUB_COMPOSITES:
        if name == comp_name:
            return pool
    return 'lg'

# ── Sub-composite scoring ─────────────────────────────────────────────────────

def score_subcomposites(pid, ps, pct_maps, subcomp_weights):
    """
    Compute all sub-composite scores for one player.

    pid             — player id
    ps              — player_seasons dict for this player
    pct_maps        — {'lg': {stat: {pid: pctile}}, 'pos': {stat: {pid: pctile}}}
    subcomp_weights — {comp_name: {stat_key: weight}}

    Returns dict of {comp_name: score_or_None}
    """
    scores = {}

    for comp_name, gate_key, cols_srcs, pool in SUB_COMPOSITES:
        if not passes_gate(ps, gate_key):
            scores[comp_name] = None
            continue

        pct_pool = pct_maps.get(pool, {})
        n_stats  = len(cols_srcs)
        min_m    = 1 if n_stats <= 2 else 2

        score = _weighted_avg_from_pool(pid, cols_srcs, pct_pool, comp_name, subcomp_weights, min_m)

        # finishing_score: require at least one paint stat (efg or scoring rate)
        if comp_name == 'finishing_score' and score is not None:
            pos_pool = pct_maps.get('pos', {})
            has_paint = (pos_pool.get('paint_efg_vw', {}).get(pid) is not None or
                         pos_pool.get('paint_scoring_rate', {}).get(pid) is not None or
                         pos_pool.get('paint_efg_vw', {}).get(str(pid)) is not None or
                         pos_pool.get('paint_scoring_rate', {}).get(str(pid)) is not None)
            if not has_paint:
                score = None

        # passing_score: pot_ast_per_tov is preferred but not required
        # min_metrics=2 of 3 handles missing stats gracefully

        scores[comp_name] = score

    return scores

def _weighted_avg_from_pool(pid, cols_srcs, pct_pool, comp_name, subcomp_weights, min_metrics):
    """
    Inner: weighted avg using a single flat pct_pool dict {stat: {pid: pctile}}.
    """
    comp_w = subcomp_weights.get(comp_name, {})
    vals_weights = []
    for col, _src in cols_srcs:
        pmap = pct_pool.get(col, {})
        v = pmap.get(pid) if pmap.get(pid) is not None else pmap.get(str(pid))
        if v is not None:
            base_col = INVERT_MAP.get(col, col)
            w = comp_w.get(base_col, 1.0)
            vals_weights.append((v, w))
    if len(vals_weights) < min_metrics:
        return None
    total_w = sum(w for _, w in vals_weights)
    if total_w == 0:
        return None
    return round(sum(v * w for v, w in vals_weights) / total_w, 1)

# ── Category scoring ──────────────────────────────────────────────────────────

def score_categories(subcomp_scores, pid, pct_maps, subcomp_weights=None):
    """
    Compute category scores from sub-composite scores.

    subcomp_scores  — {comp_name: score_or_None}  from score_subcomposites()
    pid             — player id
    pct_maps        — {'lg': ..., 'pos': ...}  needed for defender extras

    Returns dict of {cat_name: score_or_None}
    """
    cat = {}

    def g(name):
        return safe(subcomp_scores.get(name))

    # ── Creator: best 2 of (finishing, shooting, shot_creation)
    # No required anchor — any sub-score qualifies.
    # A big who finishes at the rim and shoots efficiently gets a creator score
    # even without drives. Best-2-of-3 drops the weakest dimension.
    _cr = [(v, n) for n, v in [
        ('finishing_score',     g('finishing_score')),
        ('shooting_score',      g('shooting_score')),
        ('shot_creation_score', g('shot_creation_score')),
    ] if v is not None]
    if not _cr:
        cat['creator_score'] = None
    elif len(_cr) == 1:
        cat['creator_score'] = round(_cr[0][0], 1)
    else:
        top2 = sorted(_cr, key=lambda x: x[0], reverse=True)[:2]
        cat['creator_score'] = round(sum(v for v, _ in top2) / 2, 1)

    # ── Playmaker: best 2 of (passing, creation, decision_making)
    # No required anchor — any sub-score qualifies.
    # A big with screen assists and ft assists gets a creation score
    # even without passing volume. Best-2-of-3 drops the weakest dimension.
    _pm = [(v, n) for n, v in [
        ('passing_score',         g('passing_score')),
        ('creation_score',        g('creation_score')),
        ('decision_making_score', g('decision_making_score')),
    ] if v is not None]
    if not _pm:
        cat['playmaker_score'] = None
    elif len(_pm) == 1:
        cat['playmaker_score'] = round(_pm[0][0], 1)
    else:
        top2 = sorted(_pm, key=lambda x: x[0], reverse=True)[:2]
        cat['playmaker_score'] = round(sum(v for v, _ in top2) / 2, 1)

    # ── Defender: flat avg of sub-composites + extras sub-composite + matchup signal
    # defender_extras_score = leverage_defense + def_ws (properly win-weighted)
    def_vals = []
    for sub in ['perimeter_def_score', 'interior_def_score', 'defender_extras_score']:
        v = g(sub)
        if v is not None: def_vals.append(v)
    cat['defender_score'] = round(sum(def_vals) / len(def_vals), 1) if def_vals else None

    # ── Intangibles: best 2 of available sub-scores (activity, rebounding, gravity)
    # A player only ever has one gravity score (perimeter OR interior based on position).
    # Dropping the weakest dimension mirrors Creator/Playmaker logic — a player who
    # doesn't produce hustle stats isn't penalised if they rebound and create gravity.
    _int_gravity = g('gravity_perimeter_score') or g('gravity_interior_score')
    _int = [(v, n) for n, v in [
        ('activity',    g('activity_score')),
        ('rebounding',  g('rebounding_score')),
        ('gravity',     _int_gravity),
    ] if v is not None]
    if not _int:
        cat['intangibles_score'] = None
    elif len(_int) <= 2:
        cat['intangibles_score'] = round(sum(v for v, _ in _int) / len(_int), 1)
    else:
        top2 = sorted(_int, key=lambda x: x[0], reverse=True)[:2]
        cat['intangibles_score'] = round(sum(v for v, _ in top2) / 2, 1)

    return cat

# ── Builder: run a custom composite from a list of stat keys ─────────────────

def run_builder(selected_keys, players, pct_maps, subcomp_weights, mode='impact'):
    """
    Run a builder composite using the exact same methodology as compute_metrics.py.

    selected_keys  — list of user-facing stat keys (e.g. ['paint_efg_vw', 'all3_efg_vw'])
    players        — list of player dicts: must include player_id and all season fields
                     needed by passes_gate()
    pct_maps       — {'lg': {stat: {pid: pctile}}, 'pos': {stat: {pid: pctile}}}
    subcomp_weights— {comp_name: {stat_key: weight}}
    mode           — 'impact' (win-weighted) or 'flat'

    Returns list of result dicts sorted by score desc.
    """
    if not selected_keys:
        return []

    # Resolve user-facing keys → pct map keys (lower-better → _inv)
    # Build a reverse lookup: pct_map_key → user_key
    resolved = {}  # user_key → pct_map_key
    for uk in selected_keys:
        resolved[uk] = SERVER_KEY_MAP.get(uk, uk)

    # Which sub-composites are touched by the selected stats?
    # Build map: sub_comp_name → {gate, pool, [(pct_map_key, src)]}
    # Only include stats from SUB_COMPOSITES that the user actually selected.
    selected_subcomps = {}
    for comp_name, gate_key, cols_srcs, pool in SUB_COMPOSITES:
        # Map pct_map_key back to user_key to check selection
        matching = []
        for pct_key, src in cols_srcs:
            # pct_key is the _inv version; find the user key
            user_key = INVERT_MAP.get(pct_key, pct_key)  # reverse INVERT_MAP
            if user_key in selected_keys:
                matching.append((pct_key, src))
        if matching:
            selected_subcomps[comp_name] = {
                'gate':     gate_key,
                'pool':     pool,
                'cols_srcs': matching,
                'n_total':  len(cols_srcs),  # total stats in this subcomp (for min_metrics)
            }

    results = []
    for p in players:
        pid    = p['player_id']
        pid_s  = str(pid)
        ps     = p  # passes_gate reads from the same dict

        subcomp_scores = {}
        breakdown = []

        for comp_name, sc_info in selected_subcomps.items():
            if not passes_gate(ps, sc_info['gate']):
                continue

            pct_pool = pct_maps.get(sc_info['pool'], {})
            n_selected = len(sc_info['cols_srcs'])
            # min_metrics mirrors backend: 1 if ≤2 total stats in subcomp, else 2
            min_m = 1 if sc_info['n_total'] <= 2 else 2
            # But also respect: need at least 1 if only 1 selected, etc.
            min_m = min(min_m, n_selected)

            comp_w = subcomp_weights.get(comp_name, {}) if mode == 'impact' else {}
            vals_weights = []
            for pct_key, _src in sc_info['cols_srcs']:
                pmap = pct_pool.get(pct_key, {})
                v = pmap.get(pid) if pmap.get(pid) is not None else pmap.get(pid_s)
                if v is not None:
                    base_col = INVERT_MAP.get(pct_key, pct_key)
                    w = comp_w.get(base_col, 1.0) if mode == 'impact' else 1.0
                    vals_weights.append((pct_key, v, w))

            if len(vals_weights) < min_m:
                continue

            total_w = sum(w for _, _, w in vals_weights)
            if total_w == 0: continue
            sc_score = round(sum(v * w for _, v, w in vals_weights) / total_w, 1)

            # finishing_score extra check: require paint_efg_vw (primary paint anchor)
            if comp_name == 'finishing_score':
                pos_pool = pct_maps.get('pos', {})
                has_paint = (pos_pool.get('paint_efg_vw', {}).get(pid) is not None or
                             pos_pool.get('paint_efg_vw', {}).get(pid_s) is not None)
                if not has_paint:
                    continue

            # passing_score: pot_ast_per_tov is preferred but not required
            # min_metrics=2 of 3 handles missing stats gracefully

            subcomp_scores[comp_name] = sc_score
            for pct_key, v, w in vals_weights:
                user_key = INVERT_MAP.get(pct_key, pct_key)
                breakdown.append({
                    'stat':    user_key,
                    'pct_key': pct_key,
                    'pctile':  v,
                    'weight':  round(w, 4),
                    'subcomp': comp_name,
                })

        if not subcomp_scores:
            continue

        # ── Final score ───────────────────────────────────────────
        # Apply category rules when the selected sub-composites match a known
        # category exactly — this makes "select all scoring" == creator_score.
        # Otherwise fall back to flat average of sub-composite scores.
        cat_scores = score_categories(subcomp_scores, pid, pct_maps, subcomp_weights)

        # Determine which categories are fully represented by selected subcomps
        active_subcomps = set(subcomp_scores.keys())
        CATCOMP_SUBCOMPS = {
            'creator_score':     {'finishing_score', 'shooting_score', 'shot_creation_score'},
            'playmaker_score':   {'passing_score', 'creation_score', 'decision_making_score'},
            'defender_score':    {'perimeter_def_score', 'interior_def_score'},
            'intangibles_score': {'activity_score', 'rebounding_score', 'gravity_perimeter_score', 'gravity_interior_score'},
        }
        # A category is "triggered" if ANY of its sub-composites are selected.
        # Use the category score (with best-2-of-3 etc.) when the selected
        # subcomps are a subset of a single category. If spanning multiple
        # categories, average the triggered category scores.
        triggered_cats = [
            cat for cat, scs in CATCOMP_SUBCOMPS.items()
            if active_subcomps & scs  # at least one subcomp from this category
        ]
        cat_scores_active = [cat_scores[c] for c in triggered_cats if cat_scores.get(c) is not None]

        # How many of the selected subcomps belong to each triggered category?
        # If ALL selected subcomps belong to a single category → strict category rules apply.
        # If subcomps span multiple categories → average the category scores.
        # If only a subset of a category is selected → flat subcomp average (sub-category view).
        selected_subcomps_set = set(selected_subcomps.keys())
        single_category = None
        for cat, scs in CATCOMP_SUBCOMPS.items():
            if selected_subcomps_set <= scs:  # all selected subcomps are within this one category
                single_category = cat
                break

        if single_category and cat_scores.get(single_category) is not None:
            # All selected subcomps belong to one category → use its score (best-2-of-3 etc.)
            final_score = cat_scores[single_category]
        elif single_category and cat_scores.get(single_category) is None:
            # Category rules disqualify this player (e.g. no shot_creation_score)
            # Only show them if they have a sub-composite score (partial select)
            if len(selected_subcomps_set) < len(CATCOMP_SUBCOMPS[single_category]):
                # Partial selection of category — use flat subcomp avg
                final_score = round(sum(subcomp_scores.values()) / len(subcomp_scores), 1)
            else:
                # Full category selected but player disqualified — skip
                continue
        elif cat_scores_active:
            # Spans multiple categories — average active category scores
            final_score = round(sum(cat_scores_active) / len(cat_scores_active), 1)
        else:
            # No category applies — flat average of sub-composite scores
            final_score = round(sum(subcomp_scores.values()) / len(subcomp_scores), 1)

        results.append({
            'player_id':      pid,
            'player_name':    p.get('player_name'),
            'position_group': p.get('position_group'),
            'team_abbr':      p.get('team_abbr') or p.get('team_abbreviation'),
            'min':            p.get('min'),
            'score':          final_score,
            'subcomp_scores': subcomp_scores,
            'cat_scores':     {k: v for k, v in cat_scores.items() if v is not None},
            'breakdown':      breakdown,
            'covered':        len(breakdown),
            'total':          len(selected_keys),
        })

    results.sort(key=lambda x: x['score'], reverse=True)
    return results


# ── ASAP — Advanced Stat Average Percentile ──────────────────────────────────
# Every non-extra composite stat, win-weighted, with coverage penalty.
# The more stats a player qualifies for, the higher their potential score.

# All user-facing stat keys that feed into composites (no extras)
ASAP_STATS = [s for stats in SUBCOMP_STATS.values() for s in stats]

def compute_asap(players, pct_maps, subcomp_weights):
    """
    Compute ASAP score for each player.

    ASAP = impact_score = flat avg of available category scores
    (Creator + Playmaker + Defender + Intangibles).

    The category-level best-2-of-3 rules already handle specialization
    gracefully — no coverage penalty needed. A player who doesn't qualify
    for ball handling isn't penalised because playmaker_score uses the
    best 2 of 3 available sub-composites.

    Returns {player_id: asap_score}
    """
    asap_map = {}
    for p in players:
        pid = p['player_id']
        ps  = p
        subcomp_scores = score_subcomposites(pid, ps, pct_maps, subcomp_weights)
        cat_scores     = score_categories(subcomp_scores, pid, pct_maps, subcomp_weights)
        active = [v for v in cat_scores.values() if v is not None]
        if active:
            asap_map[pid] = round(sum(active) / len(active), 1)
    return asap_map