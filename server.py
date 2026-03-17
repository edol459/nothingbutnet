"""
The Impact Board — Flask API
python server.py
"""

import os
import math
from flask import Flask, jsonify, request
from flask_cors import CORS
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras

load_dotenv()

app = Flask(__name__, static_folder='frontend', static_url_path='')
CORS(app)

DATABASE_URL        = os.getenv('DATABASE_URL')
DEFAULT_SEASON      = os.getenv('NBA_SEASON',      '2024-25')
DEFAULT_SEASON_TYPE = os.getenv('NBA_SEASON_TYPE', 'Regular Season')


def get_conn():
    return psycopg2.connect(DATABASE_URL)


def clean(val):
    if val is None: return None
    try:
        if isinstance(val, float) and math.isnan(val): return None
        return val
    except: return val


def clean_row(row):
    return {k: clean(v) for k, v in dict(row).items()}


# ── Columns returned in player list ──────────────────────────
# Includes everything any category view needs
BASE_COLS = """
    p.player_id,
    p.player_name,
    p.position,
    p.position_group,
    p.height_inches,
    ps.team_abbr,
    ps.season,
    ps.season_type,
    ps.gp,
    ps.min,
    ps.min_per_game,

    -- Box score
    ps.pts, ps.ast, ps.reb, ps.oreb, ps.dreb,
    ps.stl, ps.blk, ps.tov, ps.pf,
    ps.fgm, ps.fga, ps.fg_pct,
    ps.fg3m, ps.fg3a, ps.fg3_pct,
    ps.ftm, ps.fta, ps.ft_pct,
    ps.plus_minus,

    -- Advanced
    ps.ts_pct, ps.efg_pct, ps.usg_pct,
    ps.ast_pct, ps.ast_to,
    ps.oreb_pct, ps.dreb_pct, ps.reb_pct,
    ps.off_rating, ps.def_rating, ps.net_rating,
    ps.pie, ps.pace,
    ps.on_off_diff,
    ps.clutch_net_rating,

    -- Scoring breakdown
    ps.pct_pts_paint, ps.pct_pts_3pt, ps.pct_pts_mid2, ps.pct_pts_ft,
    ps.pct_uast_fgm, ps.pct_ast_fgm,

    -- Tracking (season totals used in some category views)
    ps.pull_up_efg_pct, ps.cs_efg_pct,
    ps.pull_up_fga, ps.cs_fga,
    ps.iso_ppp, ps.pnr_bh_ppp, ps.transition_ppp,
    ps.def_iso_ppp, ps.def_pnr_bh_ppp,
    ps.drives, ps.drive_fga, ps.drive_fg_pct, ps.drive_passes, ps.drive_tov,
    ps.passes_made, ps.potential_ast, ps.ast_pts_created, ps.secondary_ast,
    ps.touches, ps.time_of_poss,

    -- Hustle (season totals)
    ps.deflections, ps.charges_drawn, ps.screen_assists,
    ps.screen_ast_pts, ps.loose_balls, ps.box_outs,
    ps.contested_shots,

    -- Metrics (derived + composites)
    pm.ts_pct_computed,
    pm.ft_rate,
    pm.shot_quality_delta,
    pm.creation_premium,
    pm.paint_scoring_rate,
    pm.potential_ast_per75,
    pm.ast_conversion_rate,
    pm.playmaking_gravity,
    pm.secondary_ast_per75,
    pm.pass_to_score_pct,
    pm.ball_handler_load,
    pm.drive_and_dish_rate,
    pm.pot_ast_per_tov,
    pm.pass_quality_index,
    pm.def_delta_overall,
    pm.def_delta_2pt,
    pm.def_delta_3pt,
    pm.rim_protection_score,
    pm.def_disruption_rate,
    pm.box_out_rate,
    pm.screen_assist_rate,
    pm.loose_ball_rate,
    pm.hustle_composite,
    pm.motor_score,
    pm.creation_load,
    pm.dribble_pressure_idx,
    pm.cs_fga_rate,
    pm.bpm_computed,
    pm.playmaker_score,
    pm.creator_score,
    pm.defender_score,
    pm.three_and_d_score,
    pm.hustle_score,
    pm.playmaker_pctile,
    pm.creator_pctile,
    pm.defender_pctile,
    pm.three_and_d_pctile,
    pm.hustle_pctile,
    pm.ts_pct_pctile,
    pm.net_rating_pctile
"""


def get_sort_col(sort_key):
    """Map a sort key to a SQL column expression."""
    # pm.* columns
    pm_cols = {
        'ts_pct_computed', 'ft_rate', 'shot_quality_delta', 'creation_premium',
        'paint_scoring_rate', 'potential_ast_per75', 'ast_conversion_rate',
        'playmaking_gravity', 'secondary_ast_per75', 'pass_to_score_pct',
        'ball_handler_load', 'drive_and_dish_rate', 'pot_ast_per_tov',
        'pass_quality_index', 'def_delta_overall', 'def_delta_2pt', 'def_delta_3pt',
        'rim_protection_score', 'def_disruption_rate', 'box_out_rate',
        'screen_assist_rate', 'loose_ball_rate', 'hustle_composite', 'motor_score',
        'creation_load', 'dribble_pressure_idx', 'cs_fga_rate', 'bpm_computed',
        'playmaker_score', 'creator_score', 'defender_score', 'three_and_d_score',
        'hustle_score',
    }
    if sort_key in pm_cols:
        return f'pm.{sort_key}'
    return f'ps.{sort_key}'


@app.route('/')
def index():
    return app.send_static_file('index.html')


@app.route('/api/health')
def health():
    return jsonify({'status': 'ok', 'season': DEFAULT_SEASON})


@app.route('/api/players')
def get_players():
    sort        = request.args.get('sort', 'playmaker_score')
    sort_dir    = request.args.get('dir', 'desc')
    pos         = request.args.get('pos', 'ALL')
    season      = request.args.get('season', DEFAULT_SEASON)
    season_type = request.args.get('season_type', DEFAULT_SEASON_TYPE)
    min_min     = float(request.args.get('min', 1000))
    search      = request.args.get('search', '').strip()
    page        = max(1, int(request.args.get('page', 1)))
    per_page    = min(100, int(request.args.get('per_page', 50)))
    offset      = (page - 1) * per_page

    sort_col  = get_sort_col(sort)
    direction = 'DESC' if sort_dir != 'asc' else 'ASC'
    nulls     = 'NULLS LAST' if direction == 'DESC' else 'NULLS FIRST'

    where  = ["ps.season = %s", "ps.season_type = %s", "ps.min >= %s", "ps.league = 'NBA'"]
    params = [season, season_type, min_min]

    if pos and pos != 'ALL':
        where.append("p.position_group = %s")
        params.append(pos)
    if search:
        where.append("LOWER(p.player_name) LIKE %s")
        params.append(f'%{search.lower()}%')

    where_str = ' AND '.join(where)

    try:
        conn = get_conn()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cur.execute(f"""
            SELECT {BASE_COLS}
            FROM player_seasons ps
            JOIN players p ON ps.player_id = p.player_id
            LEFT JOIN player_metrics pm
                ON ps.player_id = pm.player_id
                AND ps.season = pm.season
                AND ps.season_type = pm.season_type
            WHERE {where_str}
            ORDER BY {sort_col} {direction} {nulls}
            LIMIT %s OFFSET %s
        """, params + [per_page, offset])

        rows = [clean_row(r) for r in cur.fetchall()]

        cur.execute(f"""
            SELECT COUNT(*) FROM player_seasons ps
            JOIN players p ON ps.player_id = p.player_id
            LEFT JOIN player_metrics pm
                ON ps.player_id = pm.player_id
                AND ps.season = pm.season
                AND ps.season_type = pm.season_type
            WHERE {where_str}
        """, params)
        total = cur.fetchone()['count']

        cur.close()
        conn.close()

        return jsonify({
            'players':  rows,
            'total':    total,
            'page':     page,
            'per_page': per_page,
            'pages':    math.ceil(total / per_page),
            'season':   season,
            'sort':     sort,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/players/<int:player_id>')
def get_player(player_id):
    season      = request.args.get('season', DEFAULT_SEASON)
    season_type = request.args.get('season_type', DEFAULT_SEASON_TYPE)

    try:
        conn = get_conn()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cur.execute("""
            SELECT ps.*, p.player_name, p.position, p.position_group,
                   p.height_inches, p.weight, p.draft_year, p.draft_round,
                   p.draft_number, p.college, p.country
            FROM player_seasons ps
            JOIN players p ON ps.player_id = p.player_id
            WHERE ps.player_id = %s AND ps.season = %s AND ps.season_type = %s
        """, (player_id, season, season_type))
        s_row = cur.fetchone()
        if not s_row:
            return jsonify({'error': 'Player not found'}), 404

        cur.execute("""
            SELECT * FROM player_metrics
            WHERE player_id = %s AND season = %s AND season_type = %s
        """, (player_id, season, season_type))
        m_row = cur.fetchone()

        cur.execute("""
            SELECT zone, fga, fgm, fg_pct, league_fg_pct
            FROM player_shot_zones
            WHERE player_id = %s AND season = %s ORDER BY zone
        """, (player_id, season))
        zones = [clean_row(r) for r in cur.fetchall()]

        cur.close()
        conn.close()

        return jsonify({
            'season_stats': clean_row(s_row),
            'metrics':      clean_row(m_row) if m_row else {},
            'shot_zones':   zones,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/seasons')
def get_seasons():
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT DISTINCT season, season_type, COUNT(*) player_count
            FROM player_seasons
            GROUP BY season, season_type
            ORDER BY season DESC
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify([{'season': r[0], 'season_type': r[1], 'player_count': r[2]} for r in rows])
    except Exception as e:
        return jsonify({'error': str(e)}), 500


def get_current_season():
    """Return the most recent season in the DB."""
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT season, season_type FROM player_seasons
            ORDER BY season DESC, season_type
            LIMIT 1
        """)
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row:
            return row[0], row[1]
    except:
        pass
    return '2024-25', 'Regular Season'  # fallback

DEFAULT_SEASON, DEFAULT_SEASON_TYPE = get_current_season()

@app.route('/api/leaders')
def get_leaders():
    season      = request.args.get('season', DEFAULT_SEASON)
    season_type = request.args.get('season_type', DEFAULT_SEASON_TYPE)
    min_min     = float(request.args.get('min', 1000))

    composites = [
        ('playmaker_score', 'pm'), ('creator_score', 'pm'),
        ('defender_score',  'pm'), ('three_and_d_score', 'pm'),
        ('hustle_score',    'pm'),
    ]
    result = {}
    try:
        conn = get_conn()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        for col, tbl in composites:
            cur.execute(f"""
                SELECT p.player_id, p.player_name, p.position_group,
                       ps.team_abbr, ps.pts, ps.ast, ps.reb, ps.min_per_game,
                       {tbl}.{col} AS score
                FROM player_seasons ps
                JOIN players p ON ps.player_id = p.player_id
                LEFT JOIN player_metrics pm ON ps.player_id = pm.player_id
                    AND ps.season = pm.season AND ps.season_type = pm.season_type
                WHERE ps.season = %s AND ps.season_type = %s
                  AND ps.min >= %s AND ps.league = 'NBA'
                  AND {tbl}.{col} IS NOT NULL
                ORDER BY {tbl}.{col} DESC LIMIT 5
            """, (season, season_type, min_min))
            result[col] = [clean_row(r) for r in cur.fetchall()]
        cur.close()
        conn.close()
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/compare')
def compare_players():
    ids_str     = request.args.get('ids', '')
    season      = request.args.get('season', DEFAULT_SEASON)
    season_type = request.args.get('season_type', DEFAULT_SEASON_TYPE)
    try:
        ids = [int(i.strip()) for i in ids_str.split(',') if i.strip()]
    except:
        return jsonify({'error': 'Invalid IDs'}), 400
    if not ids or len(ids) > 4:
        return jsonify({'error': 'Provide 2-4 player IDs'}), 400
    try:
        conn = get_conn()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        placeholders = ','.join(['%s'] * len(ids))
        cur.execute(f"""
            SELECT ps.*, pm.*,
                   p.player_name, p.position, p.position_group,
                   p.height_inches, p.weight
            FROM player_seasons ps
            JOIN players p ON ps.player_id = p.player_id
            LEFT JOIN player_metrics pm ON ps.player_id = pm.player_id
                AND ps.season = pm.season AND ps.season_type = pm.season_type
            WHERE ps.player_id IN ({placeholders})
              AND ps.season = %s AND ps.season_type = %s
        """, ids + [season, season_type])
        rows = [clean_row(r) for r in cur.fetchall()]
        cur.close()
        conn.close()
        return jsonify({'players': rows, 'season': season})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    port  = int(os.getenv('PORT', 5000))
    debug = os.getenv('FLASK_ENV') == 'development'
    app.run(host='0.0.0.0', port=port, debug=debug)