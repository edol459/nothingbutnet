"""
ydkball — API Server
============================
python backend/server.py

Endpoints:
  GET  /api/seasons                    — available seasons in DB
  GET  /api/players?season=&q=&pos=    — player list for stats table
  GET  /api/stats?season=&player_id=   — single player full stat row
  GET  /api/stat-keys?season=          — all stats available for Builder
  POST /api/builder                    — run Builder composite
       body: { season, selected:[stat_keys], min_minutes }
"""

import os
import json
import math
from datetime import date
from flask import Flask, jsonify, request
from flask_cors import CORS
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras

load_dotenv()

# Serve frontend/index.html at / so `python backend/server.py` is the only command needed
FRONTEND_DIR = os.path.join(os.path.dirname(__file__), '..', 'frontend')
app = Flask(__name__, static_folder=FRONTEND_DIR, static_url_path='')
CORS(app)

@app.route('/')
def index():
    return app.send_static_file('index.html')

DATABASE_URL = os.getenv("DATABASE_URL")


def get_current_season() -> str:
    """Returns the active NBA season string, e.g. '2025-26'.
    October–December → the season that just started.
    January–September → the season that started last October.
    """
    today = date.today()
    y, m = today.year, today.month
    if m >= 10:
        return f"{y}-{str(y + 1)[2:]}"
    return f"{y - 1}-{str(y)[2:]}"


def get_current_season_type() -> str:
    """Returns 'Playoffs' during late April–June, else 'Regular Season'."""
    today = date.today()
    m, d = today.month, today.day
    if (m == 4 and d >= 20) or m in (5, 6):
        return "Playoffs"
    return "Regular Season"


DEFAULT_SEASON      = os.getenv("NBA_SEASON",      get_current_season())
DEFAULT_SEASON_TYPE = os.getenv("NBA_SEASON_TYPE", get_current_season_type())


def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)

def _fmt_game_time(val) -> str:
    """Return gameTimeUTC as a proper ISO 8601 UTC string (e.g. '2026-04-25T01:30:00Z').
    PostgreSQL returns naive datetimes via psycopg2 as Python datetime objects; str() gives
    '2026-04-25 01:30:00' which JS parses as local time instead of UTC."""
    if not val:
        return ""
    from datetime import datetime, timezone
    if isinstance(val, datetime):
        # If naive, assume it was stored as UTC
        if val.tzinfo is None:
            val = val.replace(tzinfo=timezone.utc)
        return val.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    s = str(val).strip()
    # Already has Z or offset — normalize to Z form
    if s.endswith("Z"):
        return s.replace(" ", "T")
    if "+" in s[10:] or (s[10:].count("-") > 0):
        # Has offset, parse and re-emit as Z
        try:
            dt = datetime.fromisoformat(s)
            return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception:
            pass
    # Naive string — assume UTC
    return s.replace(" ", "T") + "Z"



from auth import auth_bp, init_oauth, login_required, current_user
from datetime import timedelta

app.secret_key = os.getenv("SECRET_KEY")
app.permanent_session_lifetime = timedelta(days=60)
init_oauth(app)
app.register_blueprint(auth_bp)

def _ensure_tables():
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS review_likes (
                user_id    INTEGER REFERENCES users(id)        ON DELETE CASCADE,
                review_id  INTEGER REFERENCES game_reviews(id) ON DELETE CASCADE,
                created_at TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (user_id, review_id)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS favorite_games (
                user_id    INTEGER  REFERENCES users(id) ON DELETE CASCADE,
                game_id    TEXT     NOT NULL,
                position   SMALLINT NOT NULL CHECK (position BETWEEN 1 AND 4),
                created_at TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (user_id, game_id),
                UNIQUE (user_id, position)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS review_replies (
                id          SERIAL  PRIMARY KEY,
                review_id   INTEGER REFERENCES game_reviews(id) ON DELETE CASCADE,
                user_id     INTEGER REFERENCES users(id)        ON DELETE CASCADE,
                reply_text  TEXT    NOT NULL,
                created_at  TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_review_replies_review_id
            ON review_replies(review_id)
        """)
        # Fix playoff games that were incorrectly stored as 'Regular Season'
        # due to the _season_type_from_game_id bug (was using game_id[2:4] instead of game_id[2])
        cur.execute("""
            UPDATE games
            SET season_type = 'Playoffs'
            WHERE LEFT(game_id, 3) = '004'
              AND season_type != 'Playoffs'
        """)
        # Add night_mode preference column if it doesn't exist yet
        cur.execute("""
            ALTER TABLE users ADD COLUMN IF NOT EXISTS night_mode BOOLEAN DEFAULT FALSE
        """)
        conn.commit()
        cur.close(); conn.close()
    except Exception as e:
        print(f"[startup] _ensure_tables warning: {e}")

_ensure_tables()

# ── /api/seasons ─────────────────────────────────────────────

@app.route("/api/seasons")
def get_seasons():
    try:
        source = request.args.get("source", "stats")  # "stats" | "games"
        conn = get_conn()
        cur  = conn.cursor()
        if source == "games":
            cur.execute("""
                SELECT DISTINCT season, season_type
                FROM games
                ORDER BY season DESC, season_type
            """)
        else:
            cur.execute("""
                SELECT DISTINCT season, season_type
                FROM player_seasons
                ORDER BY season DESC, season_type
            """)
        rows = [dict(r) for r in cur.fetchall()]
        cur.close(); conn.close()
        return jsonify({"seasons": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── /api/current-season ───────────────────────────────────────

@app.route("/api/current-season")
def current_season():
    """Returns the active season and season type derived from today's date."""
    return jsonify({
        "season":      get_current_season(),
        "season_type": get_current_season_type(),
    })


# ── /api/players ─────────────────────────────────────────────

@app.route("/api/players")
def get_players():
    season      = request.args.get("season",      DEFAULT_SEASON)
    season_type = request.args.get("season_type", DEFAULT_SEASON_TYPE)
    q           = request.args.get("q",           "").strip()
    pos         = request.args.get("pos",         "ALL")
    sort_col    = request.args.get("sort",        "pts")
    sort_dir    = request.args.get("dir",         "desc").lower()
    min_min     = int(request.args.get("min_min", 0))
    limit       = min(int(request.args.get("limit", 500)), 500)

    # Whitelist sortable columns
    SORTABLE = {
        "player_name", "pts", "ast", "reb", "stl", "blk", "tov",
        "fg_pct", "fg3_pct", "ft_pct", "ts_pct", "efg_pct", "usg_pct",
        "off_rating", "def_rating", "net_rating", "min_per_game",
        "oreb_pct", "dreb_pct", "reb_pct", "ast_pct", "ast_to", "plus_minus", "gp", "pie",
        "drives", "drive_pts", "drive_ast", "drive_fga", "drive_fg_pct",
        "passes_made", "ast_pts_created", "potential_ast", "secondary_ast",
        "touches", "time_of_poss", "pull_up_efg_pct", "cs_efg_pct",
        "pull_up_fga", "cs_fga", "dist_miles", "avg_speed",
        "contested_shots", "deflections", "charges_drawn", "screen_assists",
        "loose_balls", "box_outs", "bad_pass_tov", "lost_ball_tov",
        "pct_uast_fgm", "pct_pts_paint", "pct_pts_3pt", "pct_pts_ft", "pts_paint",
        "def_rim_fga", "def_rim_fg_pct", "oreb", "dreb", "fga", "fta",
        "fgm", "fg3m", "ftm", "pf", "pfd",
        "post_touches", "paint_touches", "elbow_touches",
        "iso_ppp", "iso_fga", "iso_efg_pct", "iso_tov_pct",
        "pnr_bh_ppp", "pnr_bh_fga", "pnr_roll_ppp", "pnr_roll_poss",
        "post_ppp", "post_poss", "spotup_ppp", "spotup_efg_pct",
        "transition_ppp", "transition_fga",
        "def_iso_ppp", "def_pnr_bh_ppp", "def_post_ppp",
        "def_spotup_ppp", "def_pnr_roll_ppp",
        "drive_fgm", "pull_up_fgm", "cs_fgm",
        "clutch_net_rating", "clutch_ts_pct", "clutch_fgm", "def_ws",
        "gravity_score", "gravity_onball_perimeter", "gravity_offball_perimeter",
        "gravity_onball_interior", "gravity_offball_interior",
        "leverage_full", "leverage_offense", "leverage_defense",
        "leverage_shooting", "leverage_creation", "leverage_turnovers",
        "leverage_rebounds", "leverage_onball_def",
        "sq_avg_shot_quality", "sq_fg_pct_above_expected",
        "sq_avg_defender_distance", "sq_avg_defender_pressure",
        "sq_avg_shooter_speed", "sq_avg_made_quality", "sq_avg_missed_quality",
        "darko_dpm", "darko_odpm", "darko_ddpm", "darko_box",
        "lebron", "o_lebron", "d_lebron", "war",
        "net_pts100", "o_net_pts100", "d_net_pts100",
        "min", "post_touch_fga", "pull_up_fg3a", "pull_up_fg3_pct",
        "cs_fg3a", "cs_fg3_pct", "contested_2pt", "contested_3pt",
        "screen_ast_pts", "def_rim_fgm",
        "cd_fga_vt", "cd_fga_tg", "cd_fga_op", "cd_fga_wo",
    }
    if sort_col not in SORTABLE:
        sort_col = "pts"
    dir_sql = "ASC" if sort_dir == "asc" else "DESC"

    filters = ["ps.season = %s", "ps.season_type = %s"]
    params  = [season, season_type]

    if q:
        filters.append("p.player_name ILIKE %s")
        params.append(f"%{q}%")
    if pos and pos != "ALL":
        filters.append("p.position_group = %s")
        params.append(pos)
    if min_min > 0:
        filters.append("ps.min >= %s")
        params.append(min_min)

    where = " AND ".join(filters)
    params.append(limit)

    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute(f"""
            SELECT
                p.player_id, p.player_name, p.position, p.position_group,
                ps.team_abbr, ps.gp, ps.min, ps.min_per_game,
                ps.pts, ps.ast, ps.reb, ps.oreb, ps.dreb,
                ps.stl, ps.blk, ps.tov, ps.pf, ps.pfd,
                ps.fgm, ps.fga, ps.fg_pct,
                ps.fg3m, ps.fg3a, ps.fg3_pct,
                ps.ftm, ps.fta, ps.ft_pct, ps.plus_minus,
                ps.off_rating, ps.def_rating, ps.net_rating,
                ps.ts_pct, ps.efg_pct, ps.usg_pct,
                ps.ast_pct, ps.oreb_pct, ps.dreb_pct, ps.reb_pct,
                ps.ast_to, ps.pie,
                ps.pct_uast_fgm, ps.pct_pts_paint, ps.pct_pts_3pt, ps.pct_pts_ft, ps.pts_paint,
                ps.bad_pass_tov, ps.lost_ball_tov,
                ps.def_ws,
                ps.drives, ps.drive_fga, ps.drive_fgm, ps.drive_fg_pct,
                ps.drive_pts, ps.drive_ast, ps.drive_tov, ps.drive_passes, ps.drive_pf,
                ps.passes_made, ps.passes_received, ps.ast_pts_created,
                ps.potential_ast, ps.secondary_ast, ps.ft_ast,
                ps.touches, ps.time_of_poss, ps.avg_sec_per_touch, ps.avg_drib_per_touch,
                ps.elbow_touches, ps.post_touches, ps.paint_touches,
                ps.pull_up_fga, ps.pull_up_fgm, ps.pull_up_fg_pct,
                ps.pull_up_fg3a, ps.pull_up_fg3_pct, ps.pull_up_efg_pct,
                ps.cs_fga, ps.cs_fgm, ps.cs_fg_pct, ps.cs_fg3a, ps.cs_fg3_pct, ps.cs_efg_pct,
                ps.post_touch_fga, ps.post_touch_fg_pct, ps.post_touch_pts,
                ps.post_touch_ast, ps.post_touch_tov,
                ps.dist_miles, ps.dist_miles_off, ps.dist_miles_def,
                ps.avg_speed, ps.avg_speed_off, ps.avg_speed_def,
                ps.def_rim_fga, ps.def_rim_fgm, ps.def_rim_fg_pct,
                ps.contested_shots, ps.contested_2pt, ps.contested_3pt,
                ps.deflections, ps.charges_drawn, ps.screen_assists, ps.screen_ast_pts,
                ps.loose_balls, ps.box_outs, ps.off_box_outs, ps.def_box_outs,
                ps.cd_fga_vt, ps.cd_fgm_vt, ps.cd_fg3a_vt, ps.cd_fg3m_vt,
                ps.cd_fga_tg, ps.cd_fgm_tg, ps.cd_fg3a_tg, ps.cd_fg3m_tg,
                ps.cd_fga_op, ps.cd_fgm_op, ps.cd_fg3a_op, ps.cd_fg3m_op,
                ps.cd_fga_wo, ps.cd_fgm_wo, ps.cd_fg3a_wo, ps.cd_fg3m_wo,
                ps.iso_ppp, ps.iso_fga, ps.iso_efg_pct, ps.iso_tov_pct,
                ps.pnr_bh_ppp, ps.pnr_bh_fga,
                ps.pnr_roll_ppp, ps.pnr_roll_poss, ps.post_ppp, ps.post_poss,
                ps.spotup_ppp, ps.spotup_efg_pct,
                ps.transition_ppp, ps.transition_fga,
                ps.def_iso_ppp, ps.def_pnr_bh_ppp, ps.def_post_ppp,
                ps.def_spotup_ppp, ps.def_pnr_roll_ppp,
                ps.clutch_net_rating, ps.clutch_ts_pct, ps.clutch_usg_pct, ps.clutch_min, ps.clutch_fgm,
                ps.gravity_score, ps.gravity_onball_perimeter, ps.gravity_offball_perimeter,
                ps.gravity_onball_interior, ps.gravity_offball_interior,
                ps.leverage_full, ps.leverage_offense, ps.leverage_defense,
                ps.leverage_shooting, ps.leverage_creation, ps.leverage_turnovers,
                ps.leverage_rebounds, ps.leverage_onball_def,
                ps.sq_avg_shot_quality, ps.sq_fg_pct_above_expected,
                ps.sq_avg_defender_distance, ps.sq_avg_defender_pressure,
                ps.sq_avg_shooter_speed, ps.sq_avg_made_quality, ps.sq_avg_missed_quality,
                ps.darko_dpm, ps.darko_odpm, ps.darko_ddpm, ps.darko_box,
                ps.lebron, ps.o_lebron, ps.d_lebron, ps.war,
                ps.net_pts100, ps.o_net_pts100, ps.d_net_pts100
            FROM player_seasons ps
            JOIN players p ON ps.player_id = p.player_id
            WHERE {where}
            ORDER BY ps.{sort_col} {dir_sql} NULLS LAST
            LIMIT %s
        """, params)
        rows = [dict(r) for r in cur.fetchall()]
        cur.close(); conn.close()
        return jsonify({"players": rows, "season": season, "count": len(rows)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── /api/stats ────────────────────────────────────────────────

@app.route("/api/stats")
def get_stats():
    """Full stat row for a single player."""
    player_id   = request.args.get("player_id")
    season      = request.args.get("season",      DEFAULT_SEASON)
    season_type = request.args.get("season_type", DEFAULT_SEASON_TYPE)

    if not player_id:
        return jsonify({"error": "player_id required"}), 400

    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT ps.*, p.player_name, p.position, p.position_group
            FROM player_seasons ps
            JOIN players p ON ps.player_id = p.player_id
            WHERE ps.player_id = %s AND ps.season = %s AND ps.season_type = %s
        """, (player_id, season, season_type))
        row = cur.fetchone()
        cur.close(); conn.close()
        if not row:
            return jsonify({"error": "Not found"}), 404
        return jsonify({"stats": dict(row)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── /api/stat-keys ────────────────────────────────────────────

@app.route("/api/stat-keys")
def get_stat_keys():
    """Return stat keys available in player_pctiles for the Builder."""
    season      = request.args.get("season",      DEFAULT_SEASON)
    season_type = request.args.get("season_type", DEFAULT_SEASON_TYPE)
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT stat_key FROM player_pctiles
            WHERE season = %s AND season_type = %s
            ORDER BY stat_key
        """, (season, season_type))
        keys = [r["stat_key"] for r in cur.fetchall()]
        cur.close(); conn.close()
        return jsonify({"stat_keys": keys, "season": season})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── /api/builder ──────────────────────────────────────────────

@app.route("/api/builder", methods=["POST"])
def run_builder():
    """
    Rank players by average percentile across selected stats.

    Body:
      {
        "season": "2024-25",
        "season_type": "Regular Season",
        "selected": ["pts", "ast", "ts_pct"],   // stat keys
        "min_minutes": 500,
        "pos": "ALL"    // optional position filter
      }

    Response:
      {
        "results": [
          {
            "rank": 1,
            "player_id": 123,
            "player_name": "...",
            "position_group": "G",
            "team_abbr": "GSW",
            "score": 87.3,      // average percentile (0–100)
            "covered": 3,       // stats with percentile data
            "total": 3,
            "breakdown": [
              { "stat": "pts", "pctile": 91.2 },
              ...
            ]
          }
        ]
      }
    """
    body        = request.get_json() or {}
    season      = body.get("season",      DEFAULT_SEASON)
    season_type = body.get("season_type", DEFAULT_SEASON_TYPE)
    selected    = body.get("selected",    [])
    min_minutes = int(body.get("min_minutes", 500))
    pos_filter  = body.get("pos", "ALL")
    mode        = body.get("mode", "flat")  # 'flat' or 'impact'

    if not selected:
        return jsonify({"error": "No stats selected"}), 400
    if len(selected) > 150:
        return jsonify({"error": "Max 150 stats at a time"}), 400

    # Load win-correlation weights for impact mode
    impact_weights = {}
    if mode == 'impact':
        season_key = season.replace('-', '_')
        corr_path  = os.path.join(
            os.path.dirname(__file__), 'ingest', 'data',
            f'win_correlations_{season_key}.json'
        )
        if os.path.exists(corr_path):
            with open(corr_path) as f:
                corr_data = json.load(f)
            impact_weights = corr_data.get('weights', {})

    try:
        conn = get_conn()
        cur  = conn.cursor()

        # Load percentile maps for selected stats
        cur.execute("""
            SELECT stat_key, pctile_map
            FROM player_pctiles
            WHERE season = %s AND season_type = %s
              AND stat_key = ANY(%s)
        """, (season, season_type, selected))
        pctile_rows = cur.fetchall()

        if not pctile_rows:
            cur.close(); conn.close()
            return jsonify({"error": "No percentile data found. Run compute_pctiles.py first."}), 404

        # Build stat→{player_id: pctile} lookup
        pct_maps = {r["stat_key"]: r["pctile_map"] for r in pctile_rows}

        # Fetch qualifying players with all raw stat columns
        pos_clause = "AND p.position_group = %s" if pos_filter != "ALL" else ""
        pos_params = [pos_filter] if pos_filter != "ALL" else []

        cur.execute(f"""
            SELECT ps.*, p.player_name, p.position_group
            FROM player_seasons ps
            JOIN players p ON ps.player_id = p.player_id
            WHERE ps.season = %s AND ps.season_type = %s
              AND ps.min >= %s
              {pos_clause}
        """, [season, season_type, min_minutes] + pos_params)
        players = cur.fetchall()

        cur.close(); conn.close()

        # Stats stored as season totals in player_seasons — divide by GP for per-game value
        TOTAL_KEYS = {
            'drives', 'drive_fga', 'drive_fgm', 'drive_pts', 'drive_passes', 'drive_pf', 'drive_tov',
            'bad_pass_tov', 'lost_ball_tov', 'passes_made', 'passes_received', 'ast_pts_created',
            'potential_ast', 'touches', 'paint_touches', 'elbow_touches',
            'pull_up_fga', 'pull_up_fgm', 'pull_up_fg3a', 'cs_fga', 'cs_fgm', 'cs_fg3a',
            'contested_shots', 'contested_2pt', 'contested_3pt', 'deflections',
            'def_rim_fga', 'def_rim_fgm', 'screen_ast_pts',
            'cd_fga_vt', 'cd_fga_tg', 'cd_fga_op', 'cd_fga_wo',
            'cd_fgm_vt', 'cd_fgm_tg', 'cd_fgm_op', 'cd_fgm_wo',
            'iso_fga', 'pnr_bh_fga', 'transition_fga', 'pts_paint',
        }

        def get_raw_value(row, stat):
            """Return the display value for a stat from a player_seasons row."""
            if stat == 'pot_ast_per_bad_pass_tov':
                pa  = row.get('potential_ast')
                bpt = row.get('bad_pass_tov')
                if pa is not None and bpt and float(bpt) > 0:
                    return round(float(pa) / float(bpt), 2)
                return None
            val = row.get(stat)
            if val is None:
                return None
            val = float(val)
            if stat in TOTAL_KEYS:
                gp = row.get('gp')
                if gp and float(gp) > 0:
                    val = val / float(gp)
                else:
                    return None
            return round(val, 2)

        # Score each player
        results = []
        for p in players:
            pid = str(p["player_id"])
            breakdown = []
            total_wgt = 0.0
            total_wpct = 0.0
            covered   = 0

            for stat in selected:
                pmap = pct_maps.get(stat, {})
                pct  = pmap.get(pid) or pmap.get(int(pid))
                if pct is not None:
                    w = impact_weights.get(stat, 1.0) if mode == 'impact' else 1.0
                    raw_val = get_raw_value(p, stat)
                    breakdown.append({"stat": stat, "pctile": round(float(pct), 1), "weight": round(w, 4), "value": raw_val})
                    total_wpct += float(pct) * w
                    total_wgt  += w
                    covered    += 1

            if covered == 0:
                continue
            # Require at least 80% stat coverage to avoid severely skewed scores
            # (e.g. playtypes missing for low-usage players, PBP stats for some)
            if covered < len(selected) * 0.8:
                continue

            score = round(total_wpct / total_wgt, 2)
            results.append({
                "player_id":      int(p["player_id"]),
                "player_name":    p["player_name"],
                "position_group": p["position_group"],
                "team_abbr":      p["team_abbr"],
                "min":            p["min"],
                "score":          score,
                "covered":        covered,
                "total":          len(selected),
                "breakdown":      sorted(breakdown, key=lambda x: -x["pctile"]),
            })

        results.sort(key=lambda r: -r["score"])
        for i, r in enumerate(results):
            r["rank"] = i + 1

        return jsonify({
            "results": results,
            "season":  season,
            "n":       len(results),
            "mode":    mode,
            "stats_found": list(pct_maps.keys()),
            "stats_missing": [s for s in selected if s not in pct_maps],
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── /api/builder/pctiles ──────────────────────────────────────

@app.route("/api/builder/pctiles", methods=["GET"])
def builder_pctiles():
    """
    Return per-player percentile data for client-side matching in the Builder.

    Query params:
      season, season_type, selected (comma-sep stat keys, max 10), min_minutes

    Response:
      {
        "players": [
          { "player_id": 123, "player_name": "...", "position_group": "G",
            "team_abbr": "GSW", "min": 1200, "pctiles": {"pts": 91.2, ...} }
        ],
        "impact_weights": { "pts": 0.62, ... },
        "season": "2024-25",
        "n": 320
      }
    """
    season      = request.args.get("season",      DEFAULT_SEASON)
    season_type = request.args.get("season_type", DEFAULT_SEASON_TYPE)
    raw_sel     = request.args.get("selected",    "")
    selected    = [s.strip() for s in raw_sel.split(",") if s.strip()]
    min_minutes = int(request.args.get("min_minutes", 500))
    raw_pos     = request.args.get("positions", "")
    positions   = [p.strip() for p in raw_pos.split(",") if p.strip()]

    if not selected:
        return jsonify({"error": "No stats selected"}), 400

    # Load win-correlation weights for impact mode
    impact_weights = {}
    season_key = season.replace('-', '_')
    corr_path  = os.path.join(
        os.path.dirname(__file__), 'ingest', 'data',
        f'win_correlations_{season_key}.json'
    )
    if os.path.exists(corr_path):
        with open(corr_path) as f:
            corr_data = json.load(f)
        raw_w = corr_data.get('weights', corr_data.get('correlations', {}))
        for stat in selected:
            if stat in raw_w:
                impact_weights[stat] = round(abs(float(raw_w[stat])), 4)

    try:
        conn = get_conn()
        cur  = conn.cursor()

        cur.execute("""
            SELECT stat_key, pctile_map
            FROM player_pctiles
            WHERE season = %s AND season_type = %s AND stat_key = ANY(%s)
        """, (season, season_type, selected))
        pct_maps = {r["stat_key"]: r["pctile_map"] for r in cur.fetchall()}

        # Use substring matching so compound groups (GF, FC) are included
        if positions:
            pos_conditions = " OR ".join(["p.position_group ILIKE %s"] * len(positions))
            pos_clause = f"AND ({pos_conditions})"
            pos_param  = [f"%{p}%" for p in positions]
        else:
            pos_clause = ""
            pos_param  = []
        cur.execute(f"""
            SELECT ps.*, p.player_name, p.position_group
            FROM player_seasons ps
            JOIN players p ON ps.player_id = p.player_id
            WHERE ps.season = %s AND ps.season_type = %s AND ps.min >= %s
            {pos_clause}
        """, [season, season_type, min_minutes] + pos_param)
        players = cur.fetchall()
        cur.close(); conn.close()

        # Stats stored as season totals — divide by GP for per-game display value
        TOTAL_KEYS = {
            'drives', 'drive_fga', 'drive_fgm', 'drive_pts', 'drive_passes', 'drive_pf', 'drive_tov',
            'bad_pass_tov', 'lost_ball_tov', 'passes_made', 'passes_received', 'ast_pts_created',
            'potential_ast', 'touches', 'paint_touches', 'elbow_touches',
            'pull_up_fga', 'pull_up_fgm', 'pull_up_fg3a', 'cs_fga', 'cs_fgm', 'cs_fg3a',
            'contested_shots', 'contested_2pt', 'contested_3pt', 'deflections',
            'def_rim_fga', 'def_rim_fgm', 'screen_ast_pts',
            'cd_fga_vt', 'cd_fga_tg', 'cd_fga_op', 'cd_fga_wo',
            'cd_fgm_vt', 'cd_fgm_tg', 'cd_fgm_op', 'cd_fgm_wo',
            'iso_fga', 'pnr_bh_fga', 'transition_fga', 'pts_paint',
        }

        def get_raw_value(row, stat):
            if stat == 'pot_ast_per_bad_pass_tov':
                pa  = row.get('potential_ast')
                bpt = row.get('bad_pass_tov')
                if pa is not None and bpt and float(bpt) > 0:
                    return round(float(pa) / float(bpt), 2)
                return None
            val = row.get(stat)
            if val is None:
                return None
            val = float(val)
            if stat in TOTAL_KEYS:
                gp = row.get('gp')
                if gp and float(gp) > 0:
                    val = val / float(gp)
            return round(val, 3)

        result = []
        for p in players:
            pid = str(p["player_id"])
            pctiles = {}
            values  = {}
            covered = 0
            for stat in selected:
                pmap = pct_maps.get(stat, {})
                pct  = pmap.get(pid) or pmap.get(int(pid))
                if pct is not None:
                    pctiles[stat] = round(float(pct), 1)
                    covered += 1
                raw = get_raw_value(p, stat)
                if raw is not None:
                    values[stat] = raw
            if covered < len(selected) * 0.8:
                continue
            result.append({
                "player_id":      int(p["player_id"]),
                "player_name":    p["player_name"],
                "position_group": p["position_group"],
                "team_abbr":      p["team_abbr"],
                "min":            float(p["min"]),
                "pctiles":        pctiles,
                "values":         values,
            })

        return jsonify({
            "players":        result,
            "impact_weights": impact_weights,
            "season":         season,
            "n":              len(result),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


"""
ADD THESE ROUTES TO backend/server.py
Paste them before the `

if __name__ == "__main__":` block.
"""

import requests as _requests
import time as _time
import threading as _threading
from datetime import datetime as _dt
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── Past-date cache ───────────────────────────────────────────────
# Scores never change once Final — cache forever after first ScoreboardV3 fetch.
# Cleared on server restart, which forces a fresh ScoreboardV3 call.
_past_sb_cache: dict = {}   # date -> {"payload": dict, "ts": float}

# ── Future-date cache (schedule can change — TTL 60 min) ──────────
_future_sb_cache: dict = {}  # date -> {"payload": dict, "ts": float}

# ── ESPN injury report cache (TTL 30 min) ────────────────────────
_injury_cache: dict = {"data": {}, "ts": 0.0}

# ── Full season schedule from CDN (cached 2 h — used for future dates) ──
_schedule_cache: dict = {"data": None, "ts": 0.0}


def _fetch_nba_schedule() -> dict | None:
    """Fetch the NBA season schedule from the CDN (not rate-limited on cloud IPs).
    Cached in memory for 2 hours.  Returns the raw JSON dict or None on failure."""
    if _schedule_cache["data"] and _time.time() - _schedule_cache["ts"] < 7200:
        return _schedule_cache["data"]
    try:
        url  = "https://cdn.nba.com/static/json/staticData/scheduleLeagueV2_1.json"
        resp = _requests.get(url, headers=_CDN_HEADERS, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        _schedule_cache["data"] = data
        _schedule_cache["ts"]   = _time.time()
        return data
    except Exception:
        return None

# ── Today's scoreboard — kept fresh by the background poller ─────────────────
_today_sb_cache: dict = {}   # {"payload": dict, "ts": float, "date": str}

# ── Background scoreboard poller ──────────────────────────────────────────────
# Polls the NBA CDN every 30 s when games are live, 5 min when idle.
# Writes directly into _today_sb_cache so the endpoint never blocks on a live
# CDN call.  Falls back to the season schedule when CDN is unreachable, so the
# correct games still show (without scores) instead of "No games today."

_sb_poller_stop   = _threading.Event()
_sb_poller_thread = None
_POLL_LIVE_S      = 30
_POLL_IDLE_S      = 300


def _parse_cdn_scoreboard(cdn_data: dict, game_today: str) -> dict | None:
    """Parse CDN scoreboard JSON into payload format.
    Returns None if the CDN gameDate doesn't match game_today (CDN still on prior date)."""
    cdn_games = cdn_data.get("scoreboard", {}).get("games", [])
    cdn_date  = cdn_data.get("scoreboard", {}).get("gameDate", "")
    if cdn_date != game_today:
        return None
    games = []
    for g in cdn_games:
        away = g.get("awayTeam", {}); home = g.get("homeTeam", {})
        games.append({
            "gameId":         g.get("gameId", ""),
            "gameStatus":     g.get("gameStatus", 1),
            "gameStatusText": g.get("gameStatusText", ""),
            "period":         g.get("period", 0),
            "gameClock":      g.get("gameClock", ""),
            "gameTimeUTC":    g.get("gameTimeUTC", ""),
            "away": {"abbr": away.get("teamTricode", ""), "score": int(away.get("score", 0) or 0),
                     "wins": away.get("wins"), "losses": away.get("losses")},
            "home": {"abbr": home.get("teamTricode", ""), "score": int(home.get("score", 0) or 0),
                     "wins": home.get("wins"), "losses": home.get("losses")},
        })
        if int(g.get("gameStatus", 1) or 1) == 3 and g.get("gameId"):
            _upsert_game_from_boxscore(g["gameId"], g)
    _enrich_games_with_records(games)
    return {"games": games, "date": cdn_date}


def _sb_poller_tick() -> tuple[bool, bool]:
    """One poll iteration. Returns (has_live_game, cdn_succeeded)."""
    game_today = _compute_game_today()

    # ── Primary: NBA live CDN ─────────────────────────────────────────────────
    try:
        r = _requests.get(
            "https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json",
            headers=_CDN_HEADERS, timeout=8,
        )
        r.raise_for_status()
        payload = _parse_cdn_scoreboard(r.json(), game_today)
        if payload:
            _today_sb_cache.update({"payload": payload, "ts": _time.time(), "date": game_today})
            return any(g["gameStatus"] == 2 for g in payload["games"]), True
        # CDN returned a past date — fall through to schedule
    except Exception:
        pass

    # ── Fallback: season schedule (games shown without scores) ───────────────
    # Only seeds the cache if we don't already have live/final data for today,
    # so we never overwrite real scores with schedule stubs.
    try:
        sched = _fetch_nba_schedule()
        if sched:
            dt_obj    = _dt.strptime(game_today, "%Y-%m-%d")
            sched_key = f"{dt_obj.month:02d}/{dt_obj.day:02d}/{dt_obj.year} 00:00:00"
            target    = next(
                (gd for gd in sched.get("leagueSchedule", {}).get("gameDates", [])
                 if gd.get("gameDate") == sched_key),
                None,
            )
            if target:
                games = []
                for g in target.get("games", []):
                    away = g.get("awayTeam", {}); home = g.get("homeTeam", {})
                    games.append({
                        "gameId": g.get("gameId", ""), "gameStatus": 1,
                        "gameStatusText": g.get("gameStatusText", ""),
                        "period": 0, "gameClock": "",
                        "gameTimeUTC": g.get("gameTimeUTC", ""),
                        "away": {"abbr": away.get("teamTricode", ""), "score": 0,
                                 "wins": None, "losses": None},
                        "home": {"abbr": home.get("teamTricode", ""), "score": 0,
                                 "wins": None, "losses": None},
                    })
                if games:
                    _enrich_games_with_records(games)
                    cached = _today_sb_cache.get("payload", {})
                    has_real_data = (
                        _today_sb_cache.get("date") == game_today
                        and any(g["gameStatus"] in (2, 3) for g in cached.get("games", []))
                    )
                    if not has_real_data:
                        _today_sb_cache.update({
                            "payload": {"games": games, "date": game_today},
                            "ts": _time.time(), "date": game_today,
                        })
    except Exception:
        pass

    return False, False


def _sb_poller_loop():
    import logging
    log = logging.getLogger("sb_poller")
    log.info("[POLLER] Scoreboard background poller started")
    while not _sb_poller_stop.is_set():
        try:
            has_live, cdn_ok = _sb_poller_tick()
        except Exception:
            has_live, cdn_ok = False, False
        # 30 s when live, 5 min when idle, 60 s when CDN is failing (retry sooner)
        interval = _POLL_LIVE_S if has_live else (_POLL_IDLE_S if cdn_ok else 60)
        _sb_poller_stop.wait(interval)
    log.info("[POLLER] Scoreboard background poller stopped")


def start_sb_poller():
    global _sb_poller_thread
    if _sb_poller_thread and _sb_poller_thread.is_alive():
        return
    _sb_poller_stop.clear()
    _sb_poller_thread = _threading.Thread(
        target=_sb_poller_loop, daemon=True, name="ScoreboardPoller",
    )
    _sb_poller_thread.start()


def _compute_game_today():
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        from backports.zoneinfo import ZoneInfo
    from datetime import datetime, timedelta
    now_et = datetime.now(ZoneInfo('America/New_York'))
    if now_et.hour < 6:
        return (now_et - timedelta(days=1)).strftime('%Y-%m-%d')
    return now_et.strftime('%Y-%m-%d')

# Headers for NBA CDN (live boxscore proxy)
_CDN_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Referer":    "https://www.nba.com/",
    "Origin":     "https://www.nba.com",
    "Accept":     "application/json, text/plain, */*",
}


def _fetch_boxscores_parallel(game_ids, timeout=8):
    """Fetch CDN boxscores for multiple game IDs in parallel.
    Returns a dict mapping game_id -> box dict (or None on failure)."""
    def _fetch_one(gid):
        try:
            url  = f"https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{gid}.json"
            resp = _requests.get(url, headers=_CDN_HEADERS, timeout=timeout)
            if resp.status_code == 200:
                return gid, resp.json().get("game", {})
        except Exception:
            pass
        return gid, None

    results = {}
    with ThreadPoolExecutor(max_workers=min(len(game_ids), 12)) as pool:
        futures = {pool.submit(_fetch_one, gid): gid for gid in game_ids}
        for fut in as_completed(futures):
            gid, box = fut.result()
            results[gid] = box
    return results


def _enrich_games_with_records(games):
    """
    Enrich scoreboard game dicts in-place with W-L / series records.
    - Sets g['is_playoffs'] = True for playoff games (gameId starts with '004').
    - Playoff games: adds g['away']['series_wins'] and g['home']['series_wins'].
    - Regular season games: fills in wins/losses from DB if currently None.
    Silently swallows errors so it never breaks the scoreboard response.
    """
    if not games:
        return
    try:
        season = os.getenv("NBA_SEASON", "2025-26")
        all_abbrs = set()
        playoff_pairs = set()

        for g in games:
            away_abbr = g.get("away", {}).get("abbr", "")
            home_abbr = g.get("home", {}).get("abbr", "")
            if away_abbr:
                all_abbrs.add(away_abbr)
            if home_abbr:
                all_abbrs.add(home_abbr)
            game_id = str(g.get("gameId", ""))
            is_po = game_id.startswith("004")
            g["is_playoffs"] = is_po
            if is_po and away_abbr and home_abbr:
                playoff_pairs.add(tuple(sorted([away_abbr, home_abbr])))

        conn = get_conn()
        cur  = conn.cursor()

        # ── Regular season W-L (fills in Nones from DB-first path) ──
        reg_records = {}
        if all_abbrs:
            abbr_list = list(all_abbrs)
            cur.execute("""
                SELECT team_abbr,
                       SUM(CASE WHEN won THEN 1 ELSE 0 END)::int AS wins,
                       SUM(CASE WHEN NOT won THEN 1 ELSE 0 END)::int AS losses
                FROM (
                    SELECT home_team_abbr AS team_abbr,
                           home_score > away_score AS won
                    FROM games
                    WHERE season = %s AND season_type = 'Regular Season'
                      AND status = 'Final'
                      AND home_team_abbr = ANY(%s)
                    UNION ALL
                    SELECT away_team_abbr AS team_abbr,
                           away_score > home_score AS won
                    FROM games
                    WHERE season = %s AND season_type = 'Regular Season'
                      AND status = 'Final'
                      AND away_team_abbr = ANY(%s)
                ) sub
                GROUP BY team_abbr
            """, (season, abbr_list, season, abbr_list))
            for r in cur.fetchall():
                reg_records[r["team_abbr"]] = (int(r["wins"]), int(r["losses"]))

        # ── Playoff series records ──
        series_records = {}
        for pair in playoff_pairs:
            t1, t2 = pair
            cur.execute("""
                SELECT home_team_abbr, away_team_abbr,
                       home_score, away_score
                FROM games
                WHERE season = %s AND season_type = 'Playoffs'
                  AND status = 'Final'
                  AND ((home_team_abbr = %s AND away_team_abbr = %s)
                    OR (home_team_abbr = %s AND away_team_abbr = %s))
            """, (season, t1, t2, t2, t1))
            wins = {t1: 0, t2: 0}
            for sg in cur.fetchall():
                h, a = sg["home_team_abbr"], sg["away_team_abbr"]
                if int(sg["home_score"] or 0) > int(sg["away_score"] or 0):
                    wins[h] = wins.get(h, 0) + 1
                else:
                    wins[a] = wins.get(a, 0) + 1
            series_records[pair] = wins

        cur.close()
        conn.close()

        # ── Apply to games ──
        for g in games:
            away = g.get("away", {})
            home = g.get("home", {})
            away_abbr = away.get("abbr", "")
            home_abbr = home.get("abbr", "")
            game_id   = str(g.get("gameId", ""))

            if game_id.startswith("004") and away_abbr and home_abbr:
                pair = tuple(sorted([away_abbr, home_abbr]))
                sr   = series_records.get(pair, {})
                away["series_wins"] = sr.get(away_abbr, 0)
                home["series_wins"] = sr.get(home_abbr, 0)
            else:
                if away.get("wins") is None and away_abbr in reg_records:
                    away["wins"], away["losses"] = reg_records[away_abbr]
                if home.get("wins") is None and home_abbr in reg_records:
                    home["wins"], home["losses"] = reg_records[home_abbr]

    except Exception:
        pass


# ── /api/scoreboard?date=YYYY-MM-DD ──────────────────────────────
@app.route("/api/scoreboard")
def get_scoreboard():
    """
    No ?date  → today (with 6 AM ET cutoff).
                Primary: NBA live CDN (fast, works on cloud IPs).
                Fallback: ScoreboardV3 (works locally, may be blocked on production).
    ?date=YYYY-MM-DD → DB first for past dates, then ScoreboardV3.
    Results are cached: past dates forever (after first ScoreboardV3 fetch),
    today for 30 s, future for 60 min.
    """
    date = request.args.get("date", "").strip()
    _game_today = _compute_game_today()

    if not date:
        date = _game_today

    is_past   = date < _game_today
    is_today  = date == _game_today

    # Past dates — cache forever once fetched from ScoreboardV3
    if is_past and date in _past_sb_cache:
        return jsonify(_past_sb_cache[date]["payload"])

    # Past dates — DB-first path (games table is populated by fetch_games.py and
    # _upsert_game_from_boxscore).  ScoreboardV3 is rate-limited on cloud IPs, so
    # the DB is the only reliable source for historical dates on production.
    if is_past:
        try:
            conn = get_conn()
            cur  = conn.cursor()
            cur.execute("""
                SELECT game_id, home_team_abbr, away_team_abbr,
                       home_score, away_score, status
                FROM games
                WHERE game_date = %s
                ORDER BY game_id
            """, (date,))
            db_rows = cur.fetchall()
            cur.close(); conn.close()

            if db_rows:
                games = []
                for g in db_rows:
                    games.append({
                        "gameId":         g["game_id"],
                        "gameStatus":     3,
                        "gameStatusText": "Final",
                        "period":         0,
                        "gameClock":      "",
                        "gameTimeUTC":    "",
                        "away": {"abbr": g["away_team_abbr"], "score": int(g["away_score"] or 0),
                                 "wins": None, "losses": None},
                        "home": {"abbr": g["home_team_abbr"], "score": int(g["home_score"] or 0),
                                 "wins": None, "losses": None},
                    })
                _enrich_games_with_records(games)
                payload = {"games": games, "date": date}
                _past_sb_cache[date] = {"payload": payload, "ts": _time.time()}
                return jsonify(payload)
            # DB has nothing for this date — fall through to ScoreboardV3
        except Exception:
            pass  # DB error — fall through to ScoreboardV3

    # Today — serve from poller-maintained cache (refreshed every 30 s when live,
    # 5 min when idle). Falls back to on-demand CDN fetch if cache is cold or
    # the poller has been silent for more than 5 minutes.
    if is_today and _today_sb_cache.get("date") == _game_today:
        if _time.time() - _today_sb_cache.get("ts", 0) < 300:
            return jsonify(_today_sb_cache["payload"])

    # Today — try the NBA live CDN first (not rate-limited on cloud IPs)
    if is_today:
        try:
            cdn_url  = "https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json"
            cdn_resp = _requests.get(cdn_url, headers=_CDN_HEADERS, timeout=8)
            cdn_resp.raise_for_status()
            cdn_data  = cdn_resp.json()
            cdn_games = cdn_data.get("scoreboard", {}).get("games", [])
            cdn_date  = cdn_data.get("scoreboard", {}).get("gameDate", "")
            if cdn_date == _game_today and cdn_games:
                away_k = "awayTeam"; home_k = "homeTeam"
                games = []
                for g in cdn_games:
                    away = g.get(away_k, {}); home = g.get(home_k, {})
                    games.append({
                        "gameId":         g.get("gameId", ""),
                        "gameStatus":     g.get("gameStatus", 1),
                        "gameStatusText": g.get("gameStatusText", ""),
                        "period":         g.get("period", 0),
                        "gameClock":      g.get("gameClock", ""),
                        "gameTimeUTC":    g.get("gameTimeUTC", ""),
                        "away": {"abbr": away.get("teamTricode",""), "score": int(away.get("score",0) or 0),
                                 "wins": away.get("wins"), "losses": away.get("losses")},
                        "home": {"abbr": home.get("teamTricode",""), "score": int(home.get("score",0) or 0),
                                 "wins": home.get("wins"), "losses": home.get("losses")},
                    })
                    # Persist Final games to DB so they're available tomorrow via the DB-first path
                    if int(g.get("gameStatus", 1) or 1) == 3 and g.get("gameId"):
                        _upsert_game_from_boxscore(g["gameId"], g)
                _enrich_games_with_records(games)
                payload = {"games": games, "date": cdn_date}
                _today_sb_cache.update({"payload": payload, "ts": _time.time(), "date": _game_today})
                return jsonify(payload)
            # CDN is still on a past date — fall through to ScoreboardV3
        except Exception:
            pass  # CDN failed — fall through to ScoreboardV3

    # Future dates — cache for 60 min
    if not is_past and not is_today and date in _future_sb_cache:
        entry = _future_sb_cache[date]
        if _time.time() - entry["ts"] < 3600:
            return jsonify(entry["payload"])

    # Future dates — CDN season schedule (not rate-limited on cloud IPs)
    if not is_past and not is_today:
        try:
            sched = _fetch_nba_schedule()
            if sched:
                dt = _dt.strptime(date, "%Y-%m-%d")
                # Schedule uses zero-padded "MM/DD/YYYY 00:00:00"
                sched_key  = f"{dt.month:02d}/{dt.day:02d}/{dt.year} 00:00:00"
                game_dates = sched.get("leagueSchedule", {}).get("gameDates", [])
                target     = next((gd for gd in game_dates if gd.get("gameDate") == sched_key), None)
                if target is not None:
                    games = []
                    for g in target.get("games", []):
                        away = g.get("awayTeam", {})
                        home = g.get("homeTeam", {})
                        games.append({
                            "gameId":         g.get("gameId", ""),
                            "gameStatus":     1,
                            "gameStatusText": g.get("gameStatusText", ""),
                            "period":         0,
                            "gameClock":      "",
                            "gameTimeUTC":    g.get("gameTimeUTC", ""),
                            "away": {"abbr": away.get("teamTricode", ""), "score": 0,
                                     "wins": None, "losses": None},
                            "home": {"abbr": home.get("teamTricode", ""), "score": 0,
                                     "wins": None, "losses": None},
                        })
                    _enrich_games_with_records(games)
                    payload = {"games": games, "date": date}
                    _future_sb_cache[date] = {"payload": payload, "ts": _time.time()}
                    return jsonify(payload)
        except Exception:
            pass  # Fall through to ScoreboardV3

    try:
        from nba_api.stats.endpoints import scoreboardv3

        dt    = _dt.strptime(date, "%Y-%m-%d")
        board = scoreboardv3.ScoreboardV3(
            game_date=dt.strftime("%Y-%m-%d"),
            league_id="00",
            timeout=15,
        )
        gh_df = board.game_header.get_data_frame()

        if gh_df.empty:
            payload = {"games": [], "date": date}
            if is_past:
                _past_sb_cache[date] = {"payload": payload, "ts": _time.time()}
            elif is_today:
                _today_sb_cache.update({"payload": payload, "ts": _time.time(), "date": _game_today})
            else:
                _future_sb_cache[date] = {"payload": payload, "ts": _time.time()}
            return jsonify(payload)

        rows = [(str(row.get("gameId", "") or row.get("GAME_ID", "")), row)
                for _, row in gh_df.iterrows()
                if row.get("gameId") or row.get("GAME_ID")]

        gids = [gid for gid, _ in rows]
        boxscores = _fetch_boxscores_parallel(gids) if gids else {}

        games = []
        for gid, row in rows:
            box = boxscores.get(gid)

            away_abbr = home_abbr = ""
            away_score = home_score = 0
            away_wins = away_losses = home_wins = home_losses = None

            if box:
                away        = box.get("awayTeam", {})
                home        = box.get("homeTeam", {})
                away_abbr   = away.get("teamTricode", "")
                home_abbr   = home.get("teamTricode", "")
                away_score  = int(away.get("score", 0) or 0)
                home_score  = int(home.get("score", 0) or 0)
                away_wins   = away.get("wins")
                away_losses = away.get("losses")
                home_wins   = home.get("wins")
                home_losses = home.get("losses")
                if is_past:
                    _upsert_game_from_boxscore(gid, box)
            else:
                code = str(row.get("gameCode", "") or row.get("GAMECODE", "") or "")
                if "/" in code:
                    teams = code.split("/")[1]
                    away_abbr = teams[:3] if len(teams) >= 6 else ""
                    home_abbr = teams[3:6] if len(teams) >= 6 else ""

            if is_past:
                game_status_id   = 3
                game_status_text = "Final"
            else:
                raw_status = row.get("gameStatus", row.get("GAME_STATUS_ID", 1))
                game_status_id   = int(raw_status or 1)
                game_status_text = str(row.get("gameStatusText", row.get("GAME_STATUS_TEXT", "")) or "")

            games.append({
                "gameId":         gid,
                "gameStatus":     game_status_id,
                "gameStatusText": game_status_text,
                "period":         box.get("period", 0) if box else 0,
                "gameClock":      box.get("gameClock", "") if box else "",
                "gameTimeUTC":    _fmt_game_time(row.get("gameTimeUTC", row.get("GAME_TIME_UTC", ""))),
                "away": {"abbr": away_abbr, "score": away_score,
                         "wins": away_wins, "losses": away_losses},
                "home": {"abbr": home_abbr, "score": home_score,
                         "wins": home_wins, "losses": home_losses},
            })

        _enrich_games_with_records(games)
        payload = {"games": games, "date": date}
        if is_past:
            _past_sb_cache[date] = {"payload": payload, "ts": _time.time()}
        elif is_today:
            _today_sb_cache.update({"payload": payload, "ts": _time.time(), "date": _game_today})
        else:
            _future_sb_cache[date] = {"payload": payload, "ts": _time.time()}
        return jsonify(payload)

    except Exception as e:
        # ScoreboardV3 failed (rate-limited on cloud) — last resort: CDN season schedule
        if not is_past:
            try:
                sched = _fetch_nba_schedule()
                if sched:
                    dt_obj   = _dt.strptime(date, "%Y-%m-%d")
                    sched_key = f"{dt_obj.month:02d}/{dt_obj.day:02d}/{dt_obj.year} 00:00:00"
                    game_dates = sched.get("leagueSchedule", {}).get("gameDates", [])
                    target = next((gd for gd in game_dates if gd.get("gameDate") == sched_key), None)
                    if target is not None:
                        games = []
                        for g in target.get("games", []):
                            away = g.get("awayTeam", {})
                            home = g.get("homeTeam", {})
                            games.append({
                                "gameId":         g.get("gameId", ""),
                                "gameStatus":     1,
                                "gameStatusText": g.get("gameStatusText", ""),
                                "period":         0,
                                "gameClock":      "",
                                "gameTimeUTC":    g.get("gameTimeUTC", ""),
                                "away": {"abbr": away.get("teamTricode", ""), "score": 0,
                                         "wins": None, "losses": None},
                                "home": {"abbr": home.get("teamTricode", ""), "score": 0,
                                         "wins": None, "losses": None},
                            })
                        _enrich_games_with_records(games)
                        if games:
                            return jsonify({"games": games, "date": date})
            except Exception:
                pass
        return jsonify({"error": str(e), "games": [], "date": date}), 200


# ── /api/news ─────────────────────────────────────────────────────
_news_cache: dict = {}  # {"payload": list, "ts": float}

@app.route("/api/news")
def get_news():
    import xml.etree.ElementTree as ET
    if _news_cache.get("payload") and _time.time() - _news_cache.get("ts", 0) < 300:
        return jsonify({"status": "ok", "items": _news_cache["payload"]})
    try:
        resp = _requests.get(
            "https://www.espn.com/espn/rss/nba/news",
            headers={"User-Agent": "Mozilla/5.0 (compatible; NothingButNet/1.0)"},
            timeout=10,
        )
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        items = []
        for item in root.iter("item"):
            title    = (item.findtext("title") or "").strip()
            link     = (item.findtext("link") or "").strip()
            pub_date = (item.findtext("pubDate") or "").strip()
            if title:
                items.append({"title": title, "link": link, "pubDate": pub_date})
            if len(items) >= 10:
                break
        _news_cache["payload"] = items
        _news_cache["ts"] = _time.time()
        return jsonify({"status": "ok", "items": items})
    except Exception as ex:
        return jsonify({"status": "error", "message": str(ex)}), 200


# ── Injury helpers ───────────────────────────────────────────────
def _norm_name(name: str) -> str:
    n = name.lower().strip()
    for suffix in (" jr.", " sr.", " ii", " iii", " iv", " v"):
        n = n.replace(suffix, "")
    return n.strip()


def _fetch_injury_report() -> dict:
    """Fetch player injury statuses from ESPN. Returns {norm_name: status_lower}.
    Cached 30 minutes; returns stale data on error."""
    now = _time.time()
    if now - _injury_cache["ts"] < 1800:
        return _injury_cache["data"]
    try:
        url  = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/injuries"
        resp = _requests.get(url, timeout=8)
        resp.raise_for_status()
        injured = {}
        for team_entry in resp.json().get("injuries", []):
            for inj in team_entry.get("injuries", []):
                name   = inj.get("athlete", {}).get("displayName", "").strip()
                status = inj.get("status", "").lower()
                if name:
                    injured[_norm_name(name)] = status
        _injury_cache["data"] = injured
        _injury_cache["ts"]   = now
        return injured
    except Exception:
        return _injury_cache["data"]


def _is_out(player_name: str, injury_report: dict) -> bool:
    """True if player is Out / Doubtful / Injured Reserve / Suspension."""
    if not injury_report or not player_name:
        return False
    status = injury_report.get(_norm_name(player_name), "")
    return status in ("out", "doubtful", "injured reserve", "out for season",
                      "suspension", "not with team", "inactive")


def _box_star(team_data: dict):
    """Top P+R+A player ID from a CDN boxscore team dict (must have played ≥1 min)."""
    best_id, best_total = None, -1
    for p in team_data.get("players", []):
        s = p.get("statistics", {})
        min_str = s.get("minutes", "PT0M0.00S") or "PT0M0.00S"
        try:
            mins = float(min_str.replace("PT", "").replace("S", "").split("M")[0])
        except Exception:
            mins = 0
        if mins < 1:
            continue
        total = (int(s.get("points", 0) or 0)
                 + int(s.get("reboundsTotal", 0) or 0)
                 + int(s.get("assists", 0) or 0))
        if total > best_total:
            best_total = total
            best_id    = p.get("personId")
    return best_id


# ── /api/game-posters ────────────────────────────────────────────
@app.route("/api/game-posters", methods=["POST"])
def get_game_posters():
    """
    Returns best-fit player headshot IDs for each team per game.

    Final games   → actual P+R+A leader from CDN boxscore.
    Upcoming/live → highest season P+R+A among non-injured players.

    Body:    {"games": [{"gameId":"...","away":"LAL","home":"BOS","status":3}, ...]}
    Returns: {"posters": {"<gameId>": {"away": <playerId>, "home": <playerId>}}}
    """
    body  = request.get_json(force=True, silent=True) or {}
    games = body.get("games", [])
    if not games:
        return jsonify({"posters": {}})

    posters: dict = {}

    # ── Final games: CDN boxscore actual leaders ──────────────────
    final_games    = [g for g in games if int(g.get("status", 1) or 1) == 3]
    nonfinal_games = [g for g in games if int(g.get("status", 1) or 1) != 3]

    if final_games:
        boxscores = _fetch_boxscores_parallel([g["gameId"] for g in final_games])
        for g in final_games:
            gid = g.get("gameId", "")
            box = boxscores.get(gid)
            if box:
                posters[gid] = {
                    "away": _box_star(box.get("awayTeam", {})),
                    "home": _box_star(box.get("homeTeam", {})),
                }
            else:
                nonfinal_games.append(g)  # CDN miss → fall back to season stats

    # ── Upcoming / live: season stats + ESPN injury filter ────────
    if nonfinal_games:
        now    = _dt.utcnow()
        season = (f"{now.year}-{str(now.year + 1)[2:]}"
                  if now.month >= 10
                  else f"{now.year - 1}-{str(now.year)[2:]}")

        teams_needed = {(g.get("away") or "").upper() for g in nonfinal_games} | \
                       {(g.get("home") or "").upper() for g in nonfinal_games}
        teams_needed.discard("")

        team_candidates: dict[str, list] = {}
        try:
            conn = get_conn()
            cur  = conn.cursor()
            for abbr in teams_needed:
                cur.execute("""
                    SELECT ps.player_id, p.player_name,
                           COALESCE(ps.pts,0)+COALESCE(ps.ast,0)+COALESCE(ps.reb,0) AS total
                    FROM player_seasons ps
                    JOIN players p ON p.player_id = ps.player_id
                    WHERE ps.team_abbr = %s
                      AND ps.season = %s
                      AND ps.season_type = 'Regular Season'
                      AND COALESCE(ps.gp,0) >= 5
                    ORDER BY total DESC
                    LIMIT 5
                """, (abbr, season))
                team_candidates[abbr] = cur.fetchall()
            cur.close(); conn.close()
        except Exception as e:
            return jsonify({"error": str(e), "posters": posters}), 500

        injury_report = _fetch_injury_report()

        def best_season_id(abbr):
            for row in team_candidates.get(abbr, []):
                if not _is_out(row["player_name"], injury_report):
                    return row["player_id"]
            rows = team_candidates.get(abbr, [])
            return rows[0]["player_id"] if rows else None

        for g in nonfinal_games:
            gid = g.get("gameId", "")
            if not gid:
                continue
            posters[gid] = {
                "away": best_season_id((g.get("away") or "").upper()),
                "home": best_season_id((g.get("home") or "").upper()),
            }

    return jsonify({"posters": posters})


# ── /api/top-performers?date=YYYY-MM-DD ──────────────────────────
@app.route("/api/top-performers")
def get_top_performers():
    """
    Returns top 5 players by PTS+REB+AST for a given date.
    Uses the same CDN boxscores as the scoreboard — no extra API calls
    if the scoreboard was already fetched (browser hits this separately).
    No ?date = today via live CDN scoreboard.
    ?date = historical via ScoreboardV2 game IDs + CDN boxscores.
    """
    date = request.args.get("date", "").strip()

    # Resolve actual date string for labeling
    if not date:
        try:
            url  = "https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json"
            resp = _requests.get(url, headers=_CDN_HEADERS, timeout=12)
            resp.raise_for_status()
            sb_data    = resp.json()
            raw_games  = sb_data.get("scoreboard", {}).get("games", [])
            actual_date = sb_data.get("scoreboard", {}).get("gameDate", "")
        except Exception as e:
            return jsonify({"error": str(e), "players": [], "date": ""}), 200
    else:
        actual_date = date
        raw_games = []
        # DB-first for past dates (ScoreboardV3 is rate-limited on cloud IPs)
        _game_today = _compute_game_today()
        if date < _game_today:
            try:
                conn = get_conn(); cur = conn.cursor()
                cur.execute("SELECT game_id FROM games WHERE game_date = %s AND status = 'Final'", (date,))
                raw_games = [{"gameId": r["game_id"]} for r in cur.fetchall()]
                cur.close(); conn.close()
            except Exception:
                pass
        if not raw_games:
            try:
                from nba_api.stats.endpoints import scoreboardv3
                dt = _dt.strptime(date, "%Y-%m-%d")
                board = scoreboardv3.ScoreboardV3(
                    game_date=dt.strftime("%Y-%m-%d"),
                    league_id="00",
                    timeout=30,
                )
                gh_df = board.game_header.get_data_frame()
                raw_games = [{"gameId": str(r.get("gameId", "") or r.get("GAME_ID", ""))}
                             for _, r in gh_df.iterrows()
                             if r.get("gameId") or r.get("GAME_ID")]
            except Exception as e:
                return jsonify({"error": str(e), "players": [], "date": date}), 200

    def get_gid(g):
        return g.get("gameId") or g.get("GAME_ID") or ""

    game_ids = [get_gid(g) for g in raw_games if get_gid(g)]
    if not game_ids:
        return jsonify({"players": [], "date": actual_date})

    # Fetch all boxscores in parallel, then collect player lines
    boxscores = _fetch_boxscores_parallel(game_ids)
    all_players = []
    game_stars  = {}  # gameId → {away: playerId, home: playerId} — top scorer per team

    def _top_scorer_id(team_data):
        best_id, best_pts = None, -1
        for p in team_data.get("players", []):
            s = p.get("statistics", {})
            min_str = s.get("minutes", "PT0M0.00S") or "PT0M0.00S"
            try:
                mins = float(min_str.replace("PT","").replace("S","").split("M")[0])
            except Exception:
                mins = 0
            if mins < 1:
                continue
            pts = int(s.get("points", 0) or 0)
            if pts > best_pts:
                best_pts = pts
                best_id  = p.get("personId")
        return best_id

    for gid, box in boxscores.items():
        if not box:
            continue
        away = box.get("awayTeam", {})
        home = box.get("homeTeam", {})
        away_abbr = away.get("teamTricode", "")
        home_abbr = home.get("teamTricode", "")
        matchup   = f"{away_abbr} @ {home_abbr}"

        game_status = box.get("gameStatus", 1)
        is_live     = game_status == 2

        game_stars[gid] = {
            "away": _top_scorer_id(away),
            "home": _top_scorer_id(home),
        }

        for team, abbr in [(away, away_abbr), (home, home_abbr)]:
            for p in team.get("players", []):
                s       = p.get("statistics", {})
                min_str = s.get("minutes", "PT0M0.00S")
                try:
                    mins = float(min_str.replace("PT","").replace("S","").split("M")[0]) if min_str else 0
                except Exception:
                    mins = 0
                if mins < 1:
                    continue

                pts = int(s.get("points", 0) or 0)
                reb = int(s.get("reboundsTotal", 0) or 0)
                ast = int(s.get("assists", 0) or 0)
                all_players.append({
                    "player_id": p.get("personId"),
                    "name":      p.get("name", ""),
                    "team":      abbr,
                    "matchup":   matchup,
                    "game_id":   gid,
                    "is_live":   is_live,
                    "pts":       pts,
                    "reb":       reb,
                    "ast":       ast,
                    "total":     pts + reb + ast,
                })

    # Sort by total desc, take top 5
    all_players.sort(key=lambda x: x["total"], reverse=True)
    top5 = all_players[:5]

    return jsonify({"players": top5, "date": actual_date, "game_stars": game_stars})


# ── /api/preview/records/<away>/<home> ───────────────────────────
@app.route("/api/preview/records/<away>/<home>")
def preview_records(away, home):
    """Returns regular-season W/L records for both teams from the games table."""
    away = away.upper()
    home = home.upper()
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT
                team_abbr,
                COUNT(*) FILTER (WHERE won) AS wins,
                COUNT(*) FILTER (WHERE NOT won) AS losses
            FROM (
                SELECT
                    home_team_abbr AS team_abbr,
                    home_score > away_score AS won
                FROM games
                WHERE season_type = 'Regular Season'
                  AND status = 'Final'
                  AND season = %s
                  AND home_team_abbr = ANY(%s)
                UNION ALL
                SELECT
                    away_team_abbr AS team_abbr,
                    away_score > home_score AS won
                FROM games
                WHERE season_type = 'Regular Season'
                  AND status = 'Final'
                  AND season = %s
                  AND away_team_abbr = ANY(%s)
            ) t
            GROUP BY team_abbr
        """, (get_current_season(), [away, home], get_current_season(), [away, home]))
        rows = {r["team_abbr"]: r for r in cur.fetchall()}
        conn.close()

        def rec(abbr):
            r = rows.get(abbr)
            return {"wins": r["wins"], "losses": r["losses"]} if r else {"wins": None, "losses": None}

        return jsonify({"away": rec(away), "home": rec(home)})
    except Exception as e:
        return jsonify({"away": {"wins": None, "losses": None},
                        "home": {"wins": None, "losses": None},
                        "error": str(e)}), 200


# ── /api/preview/team-stats/<abbr> ───────────────────────────────
@app.route("/api/preview/team-stats/<abbr>")
def preview_team_stats(abbr):
    abbr = abbr.upper()
    try:
        conn = get_conn()   # ← was get_db()
        cur  = conn.cursor()
        cur.execute("""
            SELECT
              SUM(pts * gp)  AS tot_pts,
              SUM(reb * gp)  AS tot_reb,
              SUM(ast * gp)  AS tot_ast,
              SUM(tov * gp)  AS tot_tov,
              SUM(fgm * gp)  AS tot_fgm,
              SUM(fga * gp)  AS tot_fga,
              SUM(fg3m * gp) AS tot_fg3m,
              SUM(fg3a * gp) AS tot_fg3a,
              MAX(gp)        AS max_gp
            FROM player_seasons ps
            JOIN players p ON ps.player_id = p.player_id
            WHERE ps.team_abbr = %s          -- ← was p.team_abbreviation
              AND ps.season = %s
              AND ps.season_type = %s
              AND ps.gp >= 5
        """, (abbr, get_current_season(), "Regular Season"))
        row = cur.fetchone()
        cur.close()
        conn.close()   # ← also close the connection

        if not row or not row["max_gp"]:
            return jsonify({"error": "no data", "abbr": abbr})

        max_gp = float(row["max_gp"])
        def safe_div(a, b): return round(a / b, 4) if b else None

        return jsonify({
            "abbr":    abbr,
            "ppg":     round(row["tot_pts"]  / max_gp, 1) if row["tot_pts"]  else None,
            "rpg":     round(row["tot_reb"]  / max_gp, 1) if row["tot_reb"]  else None,
            "apg":     round(row["tot_ast"]  / max_gp, 1) if row["tot_ast"]  else None,
            "topg":    round(row["tot_tov"]  / max_gp, 1) if row["tot_tov"]  else None,
            "fg_pct":  safe_div(row["tot_fgm"], row["tot_fga"]),
            "fg3_pct": safe_div(row["tot_fg3m"], row["tot_fg3a"]),
        })
    except Exception as e:
        return jsonify({"error": str(e), "abbr": abbr}), 200

# ── /api/preview/h2h/<away>/<home> ───────────────────────────────
@app.route("/api/preview/h2h/<away>/<home>")
def preview_h2h(away, home):
    """
    Returns last 5 head-to-head games between two teams from the local DB.
    """
    away = away.upper()
    home = home.upper()

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT game_id, game_date, home_team_abbr, away_team_abbr,
                   home_score, away_score
            FROM games
            WHERE status = 'Final'
              AND (
                    (home_team_abbr = %s AND away_team_abbr = %s)
                 OR (home_team_abbr = %s AND away_team_abbr = %s)
              )
            ORDER BY game_date DESC
            LIMIT 5
        """, (home, away, away, home))
        rows = cur.fetchall()
        conn.close()

        games_out = []
        for row in rows:
            game_date = row["game_date"].strftime("%Y-%m-%d") if hasattr(row["game_date"], "strftime") else str(row["game_date"])
            games_out.append({
                "game_id":   row["game_id"],
                "date":      game_date,
                "away_abbr": row["away_team_abbr"],
                "home_abbr": row["home_team_abbr"],
                "away_pts":  row["away_score"],
                "home_pts":  row["home_score"],
            })

        return jsonify({"games": games_out, "away": away, "home": home})

    except Exception as e:
        return jsonify({"games": [], "error": str(e)}), 200


# ── Serve preview.html ────────────────────────────────────────────
@app.route("/preview")
@app.route("/preview.html")
def preview_page():
    return app.send_static_file("preview.html")


# ── /api/live/boxscore/<game_id> ──────────────────────────────────
@app.route("/api/live/boxscore/<game_id>")
@app.route("/api/live/boxscore/<game_id>")
def get_live_boxscore(game_id):
    """Proxy NBA CDN live boxscore + auto-upsert completed games.
    Falls back to nba_api BoxScoreTraditionalV3 for historical games."""
    # Try CDN first (works for current season)
    try:
        url  = f"https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json"
        resp = _requests.get(url, headers=_CDN_HEADERS, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        game = data.get("game", data)
        if game.get("gameStatus") == 3:
            _upsert_game_from_boxscore(game_id, game)
        return jsonify(game)
    except Exception:
        pass

    # CDN failed — fall back to nba_api for historical games
    try:
        from nba_api.stats.endpoints import boxscoretraditionalv3
        box = boxscoretraditionalv3.BoxScoreTraditionalV3(game_id=game_id, timeout=30)
        raw = box.get_dict()
        # Normalise to the same shape the frontend expects
        bd = raw.get("boxScoreTraditional", {})
        home_team = bd.get("homeTeam", {})
        away_team = bd.get("awayTeam", {})

        def norm_player(p):
            s = p.get("statistics", {})
            return {
                "personId": p.get("personId"),
                "name": f"{p.get('firstName','')} {p.get('familyName','')}".strip(),
                "nameI": p.get("nameI", ""),
                "jerseyNum": p.get("jerseyNum", ""),
                "position": p.get("position", ""),
                "starter": p.get("starter", "0"),
                "played": p.get("played", "1"),
                "statistics": s,
            }

        def norm_team(t):
            players_raw = t.get("players", [])
            score = t.get("score", 0) or 0
            if not score:
                score = sum(int(p.get("statistics", {}).get("points", 0) or 0) for p in players_raw)
            return {
                "teamId": t.get("teamId"),
                "teamCity": t.get("teamCity", ""),
                "teamName": t.get("teamName", ""),
                "teamTricode": t.get("teamTricode", ""),
                "score": score,
                "players": [norm_player(p) for p in players_raw],
            }

        game_meta = bd.get("game", {})
        result = {
            "gameId": game_id,
            "gameStatus": 3,
            "gameStatusText": "Final",
            "homeTeam": norm_team(home_team),
            "awayTeam": norm_team(away_team),
            "gameTimeUTC": game_meta.get("gameTimeUTC", ""),
            "period": game_meta.get("period", 4),
            "gameClock": "",
        }
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 404
 
 
def _season_type_from_game_id(game_id: str) -> str:
    """
    Derive season type from the NBA game ID.
    Format: 00TYYYYYY where T is a single digit at position [2]:
      1 = Pre-Season, 2 = Regular Season, 4 = Playoffs, 5 = Play-In
    e.g. 0022400001 → Regular Season, 0042400001 → Playoffs
    """
    prefix = game_id[2] if len(game_id) >= 3 else ""
    return {
        "1": "Pre Season",
        "2": "Regular Season",
        "4": "Playoffs",
        "5": "PlayIn",
    }.get(prefix, os.getenv("NBA_SEASON_TYPE", "Regular Season"))


def _upsert_game_from_boxscore(game_id: str, game: dict):
    """
    Upsert a completed game into the games table from CDN boxscore data.
    Silently swallows errors so it never breaks the main response.
    """
    try:
        away = game.get("awayTeam", {})
        home = game.get("homeTeam", {})
        away_abbr  = away.get("teamTricode", "")
        home_abbr  = home.get("teamTricode", "")
        away_score = int(away.get("score", 0) or 0)
        home_score = int(home.get("score", 0) or 0)

        # Derive season type from game ID prefix (002=Regular, 004=Playoffs, 005=PlayIn)
        season_type = _season_type_from_game_id(game_id)

        # Parse game date from gameTimeUTC
 
        # Parse game date from gameTimeUTC, converted to ET.
        # NBA game dates are defined in ET — a 10 PM PT tip-off is still "that day"
        # in ET, but its UTC timestamp flips to the next calendar day, so we must
        # localise to ET before extracting the date.
        game_time_utc = game.get("gameTimeUTC", "")
        if game_time_utc:
            from datetime import datetime as _dt2
            try:
                from zoneinfo import ZoneInfo as _ZI
            except ImportError:
                from backports.zoneinfo import ZoneInfo as _ZI
            utc_dt    = _dt2.fromisoformat(game_time_utc.replace("Z", "+00:00"))
            game_date = utc_dt.astimezone(_ZI("America/New_York")).date()
        else:
            from datetime import date as _date2
            game_date = _date2.today()

        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            INSERT INTO games (
                game_id, season, season_type, game_date,
                home_team_abbr, away_team_abbr,
                home_score, away_score, status
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'Final')
            ON CONFLICT (game_id) DO UPDATE SET
                home_score     = EXCLUDED.home_score,
                away_score     = EXCLUDED.away_score,
                season_type    = EXCLUDED.season_type,
                status         = 'Final',
                updated_at     = NOW()
            WHERE games.status != 'Final'
               OR games.home_score IS NULL
               OR games.season_type != EXCLUDED.season_type
        """, (
            game_id,
            os.getenv("NBA_SEASON", "2025-26"),
            season_type,
            game_date,
            home_abbr, away_abbr,
            home_score, away_score,
        ))
        conn.commit()
        cur.close()
        conn.close()
    except Exception:
        pass  # Never break the main response

# ── /api/live/pbp/<game_id> ───────────────────────────────────────
@app.route("/api/live/pbp/<game_id>")
def get_live_pbp(game_id):
    """Proxy NBA CDN live play-by-play.
    Falls back to nba_api PlayByPlayV3 for historical games."""
    # Try CDN first
    try:
        url = f"https://cdn.nba.com/static/json/liveData/playbyplay/playbyplay_{game_id}.json"
        resp = _requests.get(url, headers=_CDN_HEADERS, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        return jsonify(data.get("game", data))
    except Exception:
        pass

    # Fall back to nba_api for historical games
    try:
        from nba_api.stats.endpoints import playbyplayv3
        pbp = playbyplayv3.PlayByPlayV3(game_id=game_id, timeout=30)
        raw = pbp.get_dict()
        actions = raw.get("game", {}).get("actions", [])
        return jsonify({"gameId": game_id, "actions": actions})
    except Exception as e:
        return jsonify({"error": str(e)}), 404


# ── Serve game.html ───────────────────────────────────────────────
@app.route("/game")
def game_page():
    return app.send_static_file("game.html")

# ── Serve builder.html ────────────────────────────────────────────
@app.route("/builder.html")
@app.route("/builder")
def builder_page():
    return app.send_static_file("builder.html")

@app.route("/stats")
@app.route("/stats-hub")
@app.route("/stats-hub.html")
def stats_hub():
    return app.send_static_file("stats-hub.html")

@app.route("/leaderboard")
@app.route("/stats.html")
def stats_page():
    return app.send_static_file("stats.html")

_PBPSTATS_TEAM_IDS = {
    "ATL":1610612737,"BOS":1610612738,"BKN":1610612751,"CHA":1610612766,
    "CHI":1610612741,"CLE":1610612739,"DAL":1610612742,"DEN":1610612743,
    "DET":1610612765,"GSW":1610612744,"HOU":1610612745,"IND":1610612754,
    "LAC":1610612746,"LAL":1610612747,"MEM":1610612763,"MIA":1610612748,
    "MIL":1610612749,"MIN":1610612750,"NOP":1610612740,"NYK":1610612752,
    "OKC":1610612760,"ORL":1610612753,"PHI":1610612755,"PHX":1610612756,
    "POR":1610612757,"SAC":1610612758,"SAS":1610612759,"TOR":1610612761,
    "UTA":1610612762,"WAS":1610612764,
}

# Cache pbpstats lineup responses: key=(team_abbr, season, leverage) → (fetched_at, lineups)
# Past seasons never change so they're kept indefinitely; current season TTL is 1 hour.
import time as _time
_pbp_cache: dict = {}
_PBP_CURRENT_TTL = 3600  # 1 hour for live season

_ALL_LEV = {"Low", "Medium", "High", "VeryHigh"}

def _fetch_pbp_lineups(team_abbr, season, leverage):
    """Return parsed lineup list from pbpstats, using in-memory cache.

    leverage: comma-separated string of leverage types to include,
              e.g. "Medium,High,VeryHigh". Pass all four (or empty) for no filter.
    """
    # Normalise to a frozenset for a stable cache key
    lev_set = frozenset(v.strip() for v in leverage.split(",") if v.strip()) if leverage else _ALL_LEV
    cache_key = (team_abbr, season, lev_set)
    current_season = get_current_season()
    now = _time.monotonic()

    if cache_key in _pbp_cache:
        fetched_at, cached = _pbp_cache[cache_key]
        if season != current_season or (now - fetched_at) < _PBP_CURRENT_TTL:
            return cached

    team_id = _PBPSTATS_TEAM_IDS[team_abbr]
    params = {
        "TeamId":     team_id,
        "Season":     season,
        "SeasonType": "Regular Season",
        "Type":       "Team",
    }

    # Build URL manually so commas in Leverage are NOT percent-encoded —
    # pbpstats expects literal commas and rejects %2C.
    import urllib.parse as _urlparse
    base_url = "https://api.pbpstats.com/get-wowy-stats/nba?" + _urlparse.urlencode(params)
    if lev_set and lev_set != _ALL_LEV:
        base_url += "&Leverage=" + ",".join(lev_set)

    print(f"[pbpstats] GET {base_url}")
    try:
        resp = _requests.get(base_url, timeout=25)
        resp.raise_for_status()
    except _requests.exceptions.Timeout:
        print(f"[pbpstats] TIMEOUT after 50s")
        raise
    except Exception as _e:
        print(f"[pbpstats] ERROR {type(_e).__name__}: {_e}")
        raise

    lineups = []
    for row in resp.json().get("multi_row_table_data", []):
        if not row or not row.get("EntityId") or not row.get("Minutes"):
            continue
        pids     = [p for p in row["EntityId"].split("-") if p.strip()]
        names    = [n.strip() for n in row.get("Name", "").split(",")]
        off_poss = row.get("OffPoss") or 0
        def_poss = row.get("DefPoss") or 0
        points   = row.get("Points") or 0
        opp_pts  = row.get("OpponentPoints") or 0
        ortg = round(points  / off_poss * 100, 1) if off_poss else None
        drtg = round(opp_pts / def_poss * 100, 1) if def_poss else None
        net  = round(ortg - drtg, 1) if ortg is not None and drtg is not None else None
        lineups.append({"pids": pids, "_ids": pids, "_names": names,
                        "min": round(row["Minutes"]),
                        "ortg": ortg, "drtg": drtg, "net": net})

    _pbp_cache[cache_key] = (now, lineups)
    return lineups

@app.route("/api/wowy/roster")
def get_wowy_roster():
    """Fast endpoint: returns only roster from DB, no pbpstats call."""
    team_abbr = request.args.get("team", "").upper()
    season    = request.args.get("season", get_current_season())

    if not team_abbr:
        return jsonify({"error": "team param required"}), 400

    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT player_id, player_name, number, position
            FROM team_rosters
            WHERE team_abbr = %s AND season = %s
            ORDER BY player_name
        """, (team_abbr, season))
        rows = cur.fetchall()
        cur.close()
        conn.close()

        return jsonify({
            "roster": [
                {"player_id": r["player_id"], "player_name": r["player_name"],
                 "number": r["number"] or "", "position": r["position"] or ""}
                for r in rows
            ]
        })
    except Exception as exc:
        import traceback
        return jsonify({"error": str(exc), "trace": traceback.format_exc()}), 500

@app.route("/api/wowy/lineups")
def get_wowy_lineups():
    """Returns leverage-filtered lineup data from wowy_lineups table (pre-fetched locally)."""
    team_abbr = request.args.get("team", "").upper()
    season    = request.args.get("season", get_current_season())

    if not team_abbr:
        return jsonify({"error": "team param required"}), 400

    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT group_id, player_ids, player_names, min, ortg, drtg, net
            FROM wowy_lineups
            WHERE team_abbr = %s AND season = %s
            ORDER BY "min" DESC
        """, (team_abbr, season))
        rows = cur.fetchall()
        cur.close()
        conn.close()

        lineups = [
            {
                "pids":   list(r["player_ids"]),
                "_ids":   list(r["player_ids"]),
                "_names": list(r["player_names"]),
                "min":    float(r["min"]) if r["min"] is not None else None,
                "ortg":   float(r["ortg"]) if r["ortg"] is not None else None,
                "drtg":   float(r["drtg"]) if r["drtg"] is not None else None,
                "net":    float(r["net"])  if r["net"]  is not None else None,
            }
            for r in rows
        ]
        return jsonify({"team": team_abbr, "season": season, "lineups": lineups})
    except Exception as exc:
        import traceback
        return jsonify({"error": str(exc), "trace": traceback.format_exc()}), 500

@app.route("/api/wowy")
def get_wowy():
    team_abbr = request.args.get("team", "").upper()
    season    = request.args.get("season", get_current_season())
    leverage  = request.args.get("leverage", "Low,Medium,High,VeryHigh")

    if not team_abbr:
        return jsonify({"error": "team param required"}), 400

    if team_abbr not in _PBPSTATS_TEAM_IDS:
        return jsonify({"error": f"Unknown team: {team_abbr}"}), 400

    try:
        # ── Roster from DB ────────────────────────────────────────
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT player_id, player_name, number, position
            FROM team_rosters
            WHERE team_abbr = %s AND season = %s
            ORDER BY player_name
        """, (team_abbr, season))
        roster_rows = cur.fetchall()
        cur.close()
        conn.close()

        roster = [
            {"player_id": r["player_id"], "player_name": r["player_name"],
             "number": r["number"] or "", "position": r["position"] or ""}
            for r in roster_rows
        ]

        # ── Lineups from pbpstats (cached) ────────────────────────
        lineups = _fetch_pbp_lineups(team_abbr, season, leverage)

        if not roster and not lineups:
            return jsonify({"error": f"No data found for {team_abbr} {season}."}), 404

        return jsonify({
            "team":     team_abbr,
            "season":   season,
            "leverage": leverage,
            "roster":   roster,
            "lineups":  lineups,
        })

    except _requests.exceptions.Timeout:
        return jsonify({"error": "pbpstats API timed out. Try again."}), 504
    except Exception as exc:
        import traceback
        return jsonify({"error": str(exc), "trace": traceback.format_exc()}), 500

# ── Serve wowy.html ──────────────────────────────────────────
@app.route("/wowy")
@app.route("/wowy.html")
def wowy_page():
    return app.send_static_file("wowy.html")



"""
ydkball — Reviews API Routes (v2)
=========================================
Replaces the original reviews_routes.py paste-in in server.py.

Changes from v1:
- Profanity/slur filter on review submit
- Admin endpoints (delete any review, list all reviews)
- Admin check reads ADMIN_GOOGLE_IDS from .env
- /api/games/<id>/reviews supports offset for load-more
- GET /api/reviews/recent supports offset for load-more
"""

import re as _re
import os as _os

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ADMIN — read from env
# ADMIN_GOOGLE_IDS=id1,id2,id3  (comma-separated Google sub IDs)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _get_admin_ids():
    raw = _os.getenv("ADMIN_GOOGLE_IDS", "")
    return {s.strip() for s in raw.split(",") if s.strip()}

def _is_admin(user: dict) -> bool:
    if not user:
        return False
    return user.get("google_id") in _get_admin_ids()

def _admin_required(f):
    from functools import wraps
    @wraps(f)
    def decorated(*args, **kwargs):
        user = current_user()
        if not user:
            return jsonify({"error": "Authentication required"}), 401
        if not _is_admin(user):
            return jsonify({"error": "Admin access required"}), 403
        return f(*args, **kwargs)
    return decorated


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PROFANITY FILTER
# Loose filter — blocks slurs and hate speech, not general profanity.
# Add terms as lowercase; checked as whole words and substrings of
# compound words (e.g. "xxxword" in "xxxwordhere").
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Core list — racial/ethnic/sexual slurs and hate speech terms.
# Stored as a tuple so it's not trivially enumerable in source.
_BLOCKED = (
    "nigger","nigga","chink","spic","wetback","kike","faggot","fag",
    "dyke","tranny","retard","cunt","gook","towelhead","sandnigger",
    "raghead","beaner","zipperhead","cracker","honky","cripple",
    "spastic","mongoloid","trannies","shemale","ladyboy","fags",
    "kikes","niggers","chinks","spics","wetbacks","faggots","dykes",
    "retards","cunts","gooks",
)

_BLOCKED_PATTERN = _re.compile(
    r'(' + '|'.join(_re.escape(w) for w in _BLOCKED) + r')',
    _re.IGNORECASE
)

def _contains_slur(text: str) -> bool:
    """Return True if text contains a blocked term."""
    if not text:
        return False
    # Normalise: collapse repeated chars (e.g. "niiiigger" → "nigger")
    normalised = _re.sub(r'(.)\1{2,}', r'\1\1', text.lower())
    # Strip common leet substitutions
    normalised = normalised.replace('3', 'e').replace('0', 'o').replace('1', 'i').replace('@', 'a')
    return bool(_BLOCKED_PATTERN.search(normalised))


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# HELPERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _format_review(r: dict) -> dict:
    return {
        "id":             r["id"],
        "game_id":        r["game_id"],
        "user_id":        r["user_id"],
        "display_name":   r.get("display_name", ""),
        "avatar_url":     r.get("avatar_url", ""),
        "favorite_team":  r.get("favorite_team") or "",
        "rating":         r["rating"],
        "stars":          r["rating"] / 2,
        "review_text":    r.get("review_text"),
        "created_at":     str(r.get("created_at", "")),
        "updated_at":     str(r.get("updated_at", "")),
        "like_count":     int(r.get("like_count", 0)),
        "liked_by_me":    bool(r.get("liked_by_me", False)),
        "reply_count":    int(r.get("reply_count", 0)),
        "tags":           r.get("tags") or [],
        "attended":       bool(r.get("attended", False)),
    }


def _format_game(g: dict) -> dict:
    avg_stars = None
    if g.get("review_count", 0) > 0:
        avg_stars = round(g["rating_sum"] / g["review_count"] / 2, 2)
    return {
        "game_id":        g["game_id"],
        "season":         g["season"],
        "season_type":    g["season_type"],
        "game_date":      str(g["game_date"]),
        "home_team_abbr": g["home_team_abbr"],
        "away_team_abbr": g["away_team_abbr"],
        "home_score":     g["home_score"],
        "away_score":     g["away_score"],
        "status":         g["status"],
        "review_count":   g.get("review_count", 0),
        "avg_stars":      avg_stars,
        "bayesian_rating": g.get("bayesian_rating"),
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GET /api/games
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/games")
def get_games():
    season      = request.args.get("season",      get_current_season())
    season_type = request.args.get("season_type", "").strip()
    team        = request.args.get("team",        "").upper().strip()
    sort        = request.args.get("sort",        "date")
    direction   = "ASC" if request.args.get("dir", "desc").lower() == "asc" else "DESC"
    limit       = min(int(request.args.get("limit", 50)), 100)
    offset      = int(request.args.get("offset", 0))
    reviewed_by = request.args.get("reviewed_by")

    SORT_MAP = {
        "date":    "g.game_date",
        "rating":  "(g.rating_sum::float / NULLIF(g.review_count, 0))",
        "reviews": "g.review_count",
    }
    order_col = SORT_MAP.get(sort, "g.game_date")

    filters = ["g.season = %s", "g.status = 'Final'"]
    params  = [season]
    if season_type:
        filters.append("g.season_type = %s")
        params.append(season_type)

    if team:
        filters.append("(g.home_team_abbr = %s OR g.away_team_abbr = %s)")
        params += [team, team]

    if reviewed_by:
        filters.append("""
            EXISTS (
                SELECT 1 FROM game_reviews gr
                WHERE gr.game_id = g.game_id AND gr.user_id = %s
            )
        """)
        params.append(int(reviewed_by))

    where = " AND ".join(filters)

    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute(f"""
            SELECT g.*
            FROM games g
            WHERE {where}
            ORDER BY {order_col} {direction} NULLS LAST
            LIMIT %s OFFSET %s
        """, params + [limit, offset])
        games = [_format_game(dict(r)) for r in cur.fetchall()]

        cur.execute(f"SELECT COUNT(*) FROM games g WHERE {where}", params)
        total = cur.fetchone()["count"]

        cur.close(); conn.close()
        return jsonify({"games": games, "total": total, "limit": limit, "offset": offset})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GET /api/games/<game_id>
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/games/<game_id>")
def get_game(game_id):
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("SELECT * FROM games WHERE game_id = %s", (game_id,))
        row = cur.fetchone()
        cur.close(); conn.close()
        if not row:
            return jsonify({"error": "Game not found"}), 404
        return jsonify({"game": _format_game(dict(row))})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GET /api/games/<game_id>/reviews
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/games/<game_id>/reviews")
def get_game_reviews(game_id):
    limit  = min(int(request.args.get("limit", 20)), 100)
    offset = int(request.args.get("offset", 0))
    sort   = request.args.get("sort", "date")  # "date" | "likes"
    order_sql = "like_count DESC, gr.created_at DESC" if sort == "likes" else "gr.created_at DESC"
    user    = current_user()
    user_id = user["id"] if user else None
    try:
        conn = get_conn()
        cur  = conn.cursor()
        if user_id:
            cur.execute(f"""
                SELECT gr.*, u.display_name, u.avatar_url, u.favorite_team,
                       COUNT(rl.review_id)                                   AS like_count,
                       BOOL_OR(rl_me.user_id IS NOT NULL)                    AS liked_by_me,
                       (SELECT COUNT(*) FROM review_replies rr WHERE rr.review_id = gr.id) AS reply_count
                FROM game_reviews gr
                JOIN users u ON gr.user_id = u.id
                LEFT JOIN review_likes rl    ON rl.review_id    = gr.id
                LEFT JOIN review_likes rl_me ON rl_me.review_id = gr.id
                                            AND rl_me.user_id   = %s
                WHERE gr.game_id = %s
                GROUP BY gr.id, u.display_name, u.avatar_url, u.favorite_team
                ORDER BY {order_sql}
                LIMIT %s OFFSET %s
            """, (user_id, game_id, limit, offset))
        else:
            cur.execute(f"""
                SELECT gr.*, u.display_name, u.avatar_url, u.favorite_team,
                       COUNT(rl.review_id) AS like_count,
                       FALSE               AS liked_by_me,
                       (SELECT COUNT(*) FROM review_replies rr WHERE rr.review_id = gr.id) AS reply_count
                FROM game_reviews gr
                JOIN users u ON gr.user_id = u.id
                LEFT JOIN review_likes rl ON rl.review_id = gr.id
                WHERE gr.game_id = %s
                GROUP BY gr.id, u.display_name, u.avatar_url, u.favorite_team
                ORDER BY {order_sql}
                LIMIT %s OFFSET %s
            """, (game_id, limit, offset))
        reviews = [_format_review(dict(r)) for r in cur.fetchall()]
        cur.execute("SELECT COUNT(*) FROM game_reviews WHERE game_id = %s", (game_id,))
        total = cur.fetchone()["count"]
        cur.close(); conn.close()
        return jsonify({"reviews": reviews, "total": total, "has_more": offset + len(reviews) < total})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# POST /api/games/<game_id>/reviews  — submit/update
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/games/<game_id>/reviews", methods=["POST"])
@login_required
def submit_review(game_id):
    user = current_user()
    body = request.get_json() or {}

    rating = body.get("rating")
    if rating is None or not isinstance(rating, int) or not (1 <= rating <= 10):
        return jsonify({"error": "rating must be an integer 1–10"}), 400

    review_text = (body.get("review_text") or "").strip() or None

    # ── Profanity filter ──────────────────────────────────────────
    if review_text and _contains_slur(review_text):
        return jsonify({"error": "Your review contains language that isn't allowed. Please edit and resubmit."}), 400

    # ── Sanitize tags ─────────────────────────────────────────────
    import json as _json
    raw_tags = body.get("tags") or []
    if not isinstance(raw_tags, list):
        raw_tags = []
    clean_tags = []
    for t in raw_tags[:5]:
        if isinstance(t, dict):
            clean_tags.append({
                "player_id":    str(t.get("player_id", ""))[:20],
                "player_name":  str(t.get("player_name", ""))[:60],
                "team_abbr":    str(t.get("team_abbr", ""))[:5],
                "stat_label":   str(t.get("stat_label", ""))[:10],
                "stat_display": str(t.get("stat_display", ""))[:20],
            })

    attended = bool(body.get("attended", False))

    try:
        conn = get_conn()
        cur  = conn.cursor()

        # One-time idempotent migrations
        cur.execute("ALTER TABLE game_reviews ADD COLUMN IF NOT EXISTS tags JSONB DEFAULT '[]'")
        cur.execute("ALTER TABLE game_reviews ADD COLUMN IF NOT EXISTS attended BOOLEAN DEFAULT FALSE")

        cur.execute("SELECT game_id FROM games WHERE game_id = %s", (game_id,))
        if not cur.fetchone():
            cur.close(); conn.close()
            return jsonify({"error": "Game not found"}), 404

        cur.execute("""
            INSERT INTO game_reviews (user_id, game_id, rating, review_text, tags, attended)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (user_id, game_id) DO UPDATE SET
                rating      = EXCLUDED.rating,
                review_text = EXCLUDED.review_text,
                tags        = EXCLUDED.tags,
                attended    = EXCLUDED.attended,
                updated_at  = NOW()
            RETURNING *
        """, (user["id"], game_id, rating, review_text, _json.dumps(clean_tags), attended))

        review = dict(cur.fetchone())
        cur.execute("SELECT avatar_url, favorite_team FROM users WHERE id = %s", (user["id"],))
        user_row = cur.fetchone()
        review["display_name"]  = user["display_name"]
        review["avatar_url"]    = (user_row["avatar_url"] if user_row else None) or ""
        review["favorite_team"] = (user_row["favorite_team"] if user_row else None) or ""
        conn.commit()
        cur.close(); conn.close()
        return jsonify({"review": _format_review(review)}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DELETE /api/games/<game_id>/reviews  — user deletes own review
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/games/<game_id>/reviews", methods=["DELETE"])
@login_required
def delete_review(game_id):
    user = current_user()
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            DELETE FROM game_reviews
            WHERE user_id = %s AND game_id = %s
            RETURNING id
        """, (user["id"], game_id))
        deleted = cur.fetchone()
        conn.commit()
        cur.close(); conn.close()
        if not deleted:
            return jsonify({"error": "Review not found"}), 404
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# POST /api/reviews/<review_id>/like  — toggle like on a review
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/reviews/<int:review_id>/like", methods=["POST"])
@login_required
def toggle_review_like(review_id):
    user = current_user()
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS review_likes (
                user_id    INTEGER REFERENCES users(id)        ON DELETE CASCADE,
                review_id  INTEGER REFERENCES game_reviews(id) ON DELETE CASCADE,
                created_at TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (user_id, review_id)
            )
        """)
        cur.execute(
            "SELECT 1 FROM review_likes WHERE user_id = %s AND review_id = %s",
            (user["id"], review_id)
        )
        if cur.fetchone():
            cur.execute(
                "DELETE FROM review_likes WHERE user_id = %s AND review_id = %s",
                (user["id"], review_id)
            )
            liked = False
        else:
            cur.execute(
                "INSERT INTO review_likes (user_id, review_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (user["id"], review_id)
            )
            liked = True
        cur.execute("SELECT COUNT(*) FROM review_likes WHERE review_id = %s", (review_id,))
        like_count = cur.fetchone()["count"]
        conn.commit()
        cur.close(); conn.close()
        return jsonify({"liked": liked, "like_count": int(like_count)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GET /api/reviews/<review_id>/replies
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/reviews/<int:review_id>/replies")
def get_review_replies(review_id):
    limit  = min(int(request.args.get("limit", 3)), 100)
    offset = int(request.args.get("offset", 0))
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT rr.id, rr.reply_text, rr.created_at,
                   u.id AS user_id, u.display_name, u.avatar_url, u.favorite_team
            FROM review_replies rr
            JOIN users u ON rr.user_id = u.id
            WHERE rr.review_id = %s
            ORDER BY rr.created_at ASC
            LIMIT %s OFFSET %s
        """, (review_id, limit, offset))
        replies = [dict(r) for r in cur.fetchall()]
        cur.execute("SELECT COUNT(*) FROM review_replies WHERE review_id = %s", (review_id,))
        total = cur.fetchone()["count"]
        cur.close(); conn.close()
        for r in replies:
            r["created_at"] = str(r["created_at"])
        return jsonify({"replies": replies, "total": int(total)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# POST /api/reviews/<review_id>/replies
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/reviews/<int:review_id>/replies", methods=["POST"])
@login_required
def post_review_reply(review_id):
    user = current_user()
    body = request.get_json(silent=True) or {}
    text = (body.get("reply_text") or "").strip()
    if not text:
        return jsonify({"error": "reply_text is required"}), 400
    if len(text) > 1000:
        return jsonify({"error": "Reply must be 1000 characters or fewer"}), 400
    try:
        conn = get_conn()
        cur  = conn.cursor()
        # Verify review exists
        cur.execute("SELECT id FROM game_reviews WHERE id = %s", (review_id,))
        if not cur.fetchone():
            cur.close(); conn.close()
            return jsonify({"error": "Review not found"}), 404
        cur.execute("""
            INSERT INTO review_replies (review_id, user_id, reply_text)
            VALUES (%s, %s, %s)
            RETURNING id, created_at
        """, (review_id, user["id"], text))
        row = cur.fetchone()
        conn.commit()
        cur.close(); conn.close()
        return jsonify({
            "id":           row["id"],
            "review_id":    review_id,
            "user_id":      user["id"],
            "display_name": user["display_name"],
            "avatar_url":   user.get("avatar_url"),
            "favorite_team": user.get("favorite_team"),
            "reply_text":   text,
            "created_at":   str(row["created_at"]),
        }), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DELETE /api/reviews/<review_id>/replies/<reply_id>
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/reviews/<int:review_id>/replies/<int:reply_id>", methods=["DELETE"])
@login_required
def delete_review_reply(review_id, reply_id):
    user = current_user()
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute(
            "DELETE FROM review_replies WHERE id = %s AND review_id = %s AND user_id = %s RETURNING id",
            (reply_id, review_id, user["id"])
        )
        deleted = cur.fetchone()
        conn.commit()
        cur.close(); conn.close()
        if not deleted:
            return jsonify({"error": "Reply not found or not yours"}), 404
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DELETE /api/admin/reviews/<review_id>  — admin deletes any review
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/admin/reviews/<int:review_id>", methods=["DELETE"])
@_admin_required
def admin_delete_review(review_id):
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("DELETE FROM game_reviews WHERE id = %s RETURNING id", (review_id,))
        deleted = cur.fetchone()
        conn.commit()
        cur.close(); conn.close()
        if not deleted:
            return jsonify({"error": "Review not found"}), 404
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GET /api/admin/reviews  — paginated list of all reviews
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/admin/reviews")
@_admin_required
def admin_list_reviews():
    limit  = min(int(request.args.get("limit", 50)), 200)
    offset = int(request.args.get("offset", 0))
    q      = request.args.get("q", "").strip()   # search review text

    filters = []
    params  = []
    if q:
        filters.append("(gr.review_text ILIKE %s OR u.display_name ILIKE %s)")
        params += [f"%{q}%", f"%{q}%"]

    where = ("WHERE " + " AND ".join(filters)) if filters else ""

    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute(f"""
            SELECT
                gr.id, gr.game_id, gr.rating, gr.review_text,
                gr.created_at,
                u.id AS user_id, u.display_name, u.email,
                g.game_date, g.home_team_abbr, g.away_team_abbr,
                g.home_score, g.away_score
            FROM game_reviews gr
            JOIN users u ON gr.user_id = u.id
            JOIN games g ON gr.game_id = g.game_id
            {where}
            ORDER BY gr.created_at DESC
            LIMIT %s OFFSET %s
        """, params + [limit, offset])
        rows = cur.fetchall()

        cur.execute(f"""
            SELECT COUNT(*) FROM game_reviews gr
            JOIN users u ON gr.user_id = u.id
            JOIN games g ON gr.game_id = g.game_id
            {where}
        """, params)
        total = cur.fetchone()["count"]

        cur.close(); conn.close()

        result = []
        for r in rows:
            d = dict(r)
            result.append({
                "id":           d["id"],
                "game_id":      d["game_id"],
                "rating":       d["rating"],
                "stars":        d["rating"] / 2,
                "review_text":  d["review_text"],
                "created_at":   str(d["created_at"]),
                "user_id":      d["user_id"],
                "display_name": d["display_name"],
                "email":        d["email"],
                "game_date":    str(d["game_date"]),
                "home_team_abbr": d["home_team_abbr"],
                "away_team_abbr": d["away_team_abbr"],
                "home_score":   d["home_score"],
                "away_score":   d["away_score"],
                "matchup":      f"{d['away_team_abbr']} @ {d['home_team_abbr']}",
            })

        return jsonify({"reviews": result, "total": total,
                        "has_more": offset + len(result) < total})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GET /api/reviews/top-games
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/reviews/top-games")
def get_top_rated_games():
    season      = request.args.get("season",      get_current_season()).strip()
    season_type = request.args.get("season_type", "").strip()
    min_reviews = int(request.args.get("min_reviews", 1))
    limit       = min(int(request.args.get("limit", 25)), 100)

    all_seasons = season.lower() in ("all", "")

    try:
        conn = get_conn()
        cur  = conn.cursor()
        s_filter  = "" if all_seasons else "AND season = %s"
        s_params  = [] if all_seasons else [season]
        st_filter = "AND season_type = %s" if season_type else ""
        st_params = [season_type] if season_type else []
        cur.execute(f"""
            SELECT *
            FROM games
            WHERE status = 'Final'
              {s_filter}
              {st_filter}
              AND review_count >= %s
            ORDER BY (rating_sum::float / NULLIF(review_count, 0)) DESC NULLS LAST
            LIMIT %s
        """, s_params + st_params + [min_reviews, limit])
        games = [_format_game(dict(r)) for r in cur.fetchall()]
        cur.close(); conn.close()
        return jsonify({"games": games})
    except Exception as e:
        return jsonify({"error": str(e)}), 500



# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GET /api/reviews/most-liked
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/reviews/most-liked")
def get_most_liked_reviews():
    limit   = min(int(request.args.get("limit", 20)), 100)
    offset  = int(request.args.get("offset", 0))
    user    = current_user()
    user_id = user["id"] if user else None
    try:
        conn = get_conn()
        cur  = conn.cursor()
        if user_id:
            cur.execute("""
                SELECT
                    gr.id, gr.game_id, gr.rating, gr.review_text,
                    gr.created_at, gr.updated_at,
                    COALESCE(gr.tags, '[]'::jsonb) AS tags,
                    gr.attended,
                    u.id AS user_id, u.display_name, u.avatar_url, u.favorite_team,
                    g.game_date, g.home_team_abbr, g.away_team_abbr,
                    g.home_score, g.away_score,
                    COUNT(rl.review_id)                AS like_count,
                    BOOL_OR(rl_me.user_id IS NOT NULL) AS liked_by_me,
                    (SELECT COUNT(*) FROM review_replies rr WHERE rr.review_id = gr.id) AS reply_count
                FROM game_reviews gr
                JOIN users u  ON gr.user_id = u.id
                JOIN games g  ON gr.game_id = g.game_id
                LEFT JOIN review_likes rl    ON rl.review_id    = gr.id
                LEFT JOIN review_likes rl_me ON rl_me.review_id = gr.id
                                            AND rl_me.user_id   = %s
                GROUP BY gr.id, u.id, g.game_id
                ORDER BY like_count DESC, gr.created_at DESC
                LIMIT %s OFFSET %s
            """, (user_id, limit, offset))
        else:
            cur.execute("""
                SELECT
                    gr.id, gr.game_id, gr.rating, gr.review_text,
                    gr.created_at, gr.updated_at,
                    COALESCE(gr.tags, '[]'::jsonb) AS tags,
                    gr.attended,
                    u.id AS user_id, u.display_name, u.avatar_url, u.favorite_team,
                    g.game_date, g.home_team_abbr, g.away_team_abbr,
                    g.home_score, g.away_score,
                    COUNT(rl.review_id) AS like_count,
                    FALSE               AS liked_by_me,
                    (SELECT COUNT(*) FROM review_replies rr WHERE rr.review_id = gr.id) AS reply_count
                FROM game_reviews gr
                JOIN users u  ON gr.user_id = u.id
                JOIN games g  ON gr.game_id = g.game_id
                LEFT JOIN review_likes rl ON rl.review_id = gr.id
                GROUP BY gr.id, u.id, g.game_id
                ORDER BY like_count DESC, gr.created_at DESC
                LIMIT %s OFFSET %s
            """, (limit, offset))
        rows = cur.fetchall()
        cur.execute("SELECT COUNT(*) FROM game_reviews")
        total = cur.fetchone()["count"]
        cur.close(); conn.close()
        result = []
        for r in rows:
            d = dict(r)
            result.append({
                **_format_review(d),
                "game_date":      str(d["game_date"]),
                "home_team_abbr": d["home_team_abbr"],
                "away_team_abbr": d["away_team_abbr"],
                "home_score":     d["home_score"],
                "away_score":     d["away_score"],
            })
        return jsonify({"reviews": result, "total": total,
                        "has_more": offset + len(result) < total})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GET /api/reviews/recent
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/reviews/recent")
def get_recent_reviews():
    limit   = min(int(request.args.get("limit", 20)), 100)
    offset  = int(request.args.get("offset", 0))
    user    = current_user()
    user_id = user["id"] if user else None
    try:
        conn = get_conn()
        cur  = conn.cursor()
        if user_id:
            cur.execute("""
                SELECT
                    gr.id, gr.game_id, gr.rating, gr.review_text,
                    gr.created_at, gr.updated_at,
                    COALESCE(gr.tags, '[]'::jsonb) AS tags,
                    gr.attended,
                    u.id AS user_id, u.display_name, u.avatar_url, u.favorite_team,
                    g.game_date, g.home_team_abbr, g.away_team_abbr,
                    g.home_score, g.away_score,
                    COUNT(rl.review_id)                        AS like_count,
                    BOOL_OR(rl_me.user_id IS NOT NULL)         AS liked_by_me,
                    (SELECT COUNT(*) FROM review_replies rr WHERE rr.review_id = gr.id) AS reply_count
                FROM game_reviews gr
                JOIN users u  ON gr.user_id = u.id
                JOIN games g  ON gr.game_id = g.game_id
                LEFT JOIN review_likes rl    ON rl.review_id    = gr.id
                LEFT JOIN review_likes rl_me ON rl_me.review_id = gr.id
                                            AND rl_me.user_id   = %s
                GROUP BY gr.id, u.id, g.game_id
                ORDER BY gr.created_at DESC
                LIMIT %s OFFSET %s
            """, (user_id, limit, offset))
        else:
            cur.execute("""
                SELECT
                    gr.id, gr.game_id, gr.rating, gr.review_text,
                    gr.created_at, gr.updated_at,
                    COALESCE(gr.tags, '[]'::jsonb) AS tags,
                    gr.attended,
                    u.id AS user_id, u.display_name, u.avatar_url, u.favorite_team,
                    g.game_date, g.home_team_abbr, g.away_team_abbr,
                    g.home_score, g.away_score,
                    COUNT(rl.review_id) AS like_count,
                    FALSE               AS liked_by_me,
                    (SELECT COUNT(*) FROM review_replies rr WHERE rr.review_id = gr.id) AS reply_count
                FROM game_reviews gr
                JOIN users u  ON gr.user_id = u.id
                JOIN games g  ON gr.game_id = g.game_id
                LEFT JOIN review_likes rl ON rl.review_id = gr.id
                GROUP BY gr.id, u.id, g.game_id
                ORDER BY gr.created_at DESC
                LIMIT %s OFFSET %s
            """, (limit, offset))
        rows = cur.fetchall()
        cur.execute("SELECT COUNT(*) FROM game_reviews")
        total = cur.fetchone()["count"]
        cur.close(); conn.close()
        result = []
        for r in rows:
            d = dict(r)
            result.append({
                **_format_review(d),
                "game_date":      str(d["game_date"]),
                "home_team_abbr": d["home_team_abbr"],
                "away_team_abbr": d["away_team_abbr"],
                "home_score":     d["home_score"],
                "away_score":     d["away_score"],
            })
        return jsonify({"reviews": result, "total": total,
                        "has_more": offset + len(result) < total})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GET /api/users/<user_id>/reviews
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/users/<int:user_id>/reviews")
def get_user_reviews(user_id):
    limit       = min(int(request.args.get("limit", 20)), 100)
    offset      = int(request.args.get("offset", 0))
    sort        = request.args.get("sort", "date_desc")
    team        = request.args.get("team", "").strip()
    attended    = request.args.get("attended", "")
    season      = request.args.get("season", "").strip()
    season_type = request.args.get("season_type", "").strip()

    conditions = ["gr.user_id = %s"]
    params: list = [user_id]

    if team:
        conditions.append("(g.home_team_abbr = %s OR g.away_team_abbr = %s)")
        params += [team, team]
    if attended == "true":
        conditions.append("gr.attended = TRUE")
    if season:
        conditions.append("g.season = %s")
        params.append(season)
    if season_type:
        conditions.append("g.season_type = %s")
        params.append(season_type)

    where = " AND ".join(conditions)

    order_map = {
        "date_desc":   "g.game_date DESC",
        "date_asc":    "g.game_date ASC",
        "rating_desc": "gr.rating DESC, g.game_date DESC",
        "rating_asc":  "gr.rating ASC, g.game_date DESC",
    }
    order = order_map.get(sort, "g.game_date DESC")

    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute(f"""
            SELECT
                gr.*, u.display_name, u.avatar_url, u.favorite_team,
                g.game_date, g.home_team_abbr, g.away_team_abbr,
                g.home_score, g.away_score, g.season, g.season_type,
                (SELECT COUNT(*) FROM review_replies rr WHERE rr.review_id = gr.id) AS reply_count
            FROM game_reviews gr
            JOIN users u ON gr.user_id = u.id
            JOIN games g ON gr.game_id = g.game_id
            WHERE {where}
            ORDER BY {order}
            LIMIT %s OFFSET %s
        """, params + [limit, offset])
        rows = cur.fetchall()
        cur.execute(f"""
            SELECT COUNT(*) FROM game_reviews gr
            JOIN games g ON gr.game_id = g.game_id
            WHERE {where}
        """, params)
        total = cur.fetchone()["count"]
        cur.close(); conn.close()
        result = []
        for r in rows:
            d = dict(r)
            result.append({
                **_format_review(d),
                "game_date":      str(d["game_date"]),
                "home_team_abbr": d["home_team_abbr"],
                "away_team_abbr": d["away_team_abbr"],
                "home_score":     d["home_score"],
                "away_score":     d["away_score"],
                "season":         d["season"],
                "season_type":    d["season_type"],
            })
        return jsonify({"reviews": result, "total": total})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# get_user_profile moved to profile_routes.py


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GET /api/admin/check  — confirm admin status (for frontend)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/admin/check")
def admin_check():
    user = current_user()
    return jsonify({"is_admin": _is_admin(user)})


# ── Page routes ───────────────────────────────────────────────────
@app.route("/reviews")
@app.route("/reviews.html")
def reviews_page():
    return app.send_static_file("reviews.html")

@app.route("/admin")
@app.route("/admin.html")
def admin_page():
    return app.send_static_file("admin.html")

# ── Run ───────────────────────────────────────────────────────


# ── Profile & Friends routes ──────────────────────────────────
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PATCH /api/me/display-name
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/me/display-name", methods=["PATCH"])
@login_required
def update_display_name():
    user = current_user()
    body = request.get_json() or {}
    name = body.get("display_name", "").strip()

    if not name:
        return jsonify({"error": "display_name is required"}), 400
    if len(name) > 40:
        return jsonify({"error": "Display name must be 40 characters or fewer"}), 400
    # Basic sanity: printable chars only
    if not all(c.isprintable() for c in name):
        return jsonify({"error": "Invalid characters in display name"}), 400

    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            UPDATE users SET display_name = %s, display_name_set = TRUE, updated_at = NOW()
            WHERE id = %s
            RETURNING id, display_name, display_name_set
        """, (name, user["id"]))
        row = dict(cur.fetchone())
        conn.commit()
        cur.close(); conn.close()

        # Update session so nav shows new name immediately
        from flask import session
        if "user" in session:
            session["user"]["display_name"] = name
            session.modified = True

        return jsonify({"ok": True, "display_name": row["display_name"]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PATCH /api/me/night-mode  — toggle dark mode preference
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/me/night-mode", methods=["PATCH"])
@login_required
def set_night_mode():
    user    = current_user()
    body    = request.get_json(silent=True) or {}
    enabled = bool(body.get("enabled", False))
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("UPDATE users SET night_mode = %s WHERE id = %s", (enabled, user["id"]))
        conn.commit()
        cur.close(); conn.close()
        return jsonify({"ok": True, "night_mode": enabled})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PATCH /api/me/favorite-team  — set or clear favorite NBA team
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
_NBA_ABBRS = {
    "ATL","BOS","BKN","CHA","CHI","CLE","DAL","DEN","DET","GSW",
    "HOU","IND","LAC","LAL","MEM","MIA","MIL","MIN","NOP","NYK",
    "OKC","ORL","PHI","PHX","POR","SAC","SAS","TOR","UTA","WAS",
}

@app.route("/api/me/favorite-team", methods=["PATCH"])
@login_required
def update_favorite_team():
    user = current_user()
    body = request.get_json() or {}
    team = (body.get("favorite_team") or "").strip().upper() or None

    if team and team not in _NBA_ABBRS:
        return jsonify({"error": "Invalid team abbreviation"}), 400

    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            UPDATE users SET favorite_team = %s, updated_at = NOW()
            WHERE id = %s
        """, (team, user["id"]))
        conn.commit()
        cur.close(); conn.close()

        from flask import session
        if "user" in session:
            session["user"]["favorite_team"] = team or ""
            session.modified = True

        return jsonify({"ok": True, "favorite_team": team or ""})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# POST /api/me/avatar  — upload / replace profile picture
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/me/avatar", methods=["POST"])
@login_required
def update_avatar():
    user = current_user()
    body = request.get_json() or {}
    data = body.get("avatar_data", "").strip()

    if not data:
        return jsonify({"error": "avatar_data is required"}), 400

    # Must be a data URL with an image MIME type
    if not data.startswith("data:image/"):
        return jsonify({"error": "Invalid image format"}), 400

    # Limit size: base64-encoded ~200 KB image → ~270 KB string
    if len(data) > 300_000:
        return jsonify({"error": "Image too large (max ~200 KB after resize)"}), 400

    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            UPDATE users SET avatar_url = %s, updated_at = NOW()
            WHERE id = %s
        """, (data, user["id"]))
        conn.commit()
        cur.close(); conn.close()

        return jsonify({"ok": True, "avatar_url": data})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PUT /api/me/favorites  — set a game at a position (1–4)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/me/favorites", methods=["PUT"])
@login_required
def set_favorite():
    me   = current_user()
    body = request.get_json() or {}
    game_id  = body.get("game_id", "").strip()
    position = body.get("position")

    if not game_id:
        return jsonify({"error": "game_id is required"}), 400
    if position not in (1, 2, 3, 4):
        return jsonify({"error": "position must be 1–4"}), 400

    try:
        conn = get_conn()
        cur  = conn.cursor()

        # Verify game exists
        cur.execute("SELECT game_id FROM games WHERE game_id = %s", (game_id,))
        if not cur.fetchone():
            cur.close(); conn.close()
            return jsonify({"error": "Game not found"}), 404

        # Remove any existing entry for this game (in case it was in another slot)
        cur.execute("DELETE FROM favorite_games WHERE user_id = %s AND game_id = %s",
                    (me["id"], game_id))
        # Remove whatever was at this position
        cur.execute("DELETE FROM favorite_games WHERE user_id = %s AND position = %s",
                    (me["id"], position))
        # Insert new
        cur.execute("""
            INSERT INTO favorite_games (user_id, game_id, position)
            VALUES (%s, %s, %s)
        """, (me["id"], game_id, position))
        conn.commit()
        cur.close(); conn.close()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DELETE /api/me/favorites/<game_id>  — remove a favorite
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/me/favorites/<game_id>", methods=["DELETE"])
@login_required
def remove_favorite(game_id):
    me = current_user()
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("DELETE FROM favorite_games WHERE user_id = %s AND game_id = %s",
                    (me["id"], game_id))
        conn.commit()
        cur.close(); conn.close()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GET /api/users/<user_id>/profile  (public)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/users/<int:user_id>/profile")
def get_user_profile(user_id):
    viewer = current_user()  # may be None

    try:
        conn = get_conn()
        cur  = conn.cursor()

        cur.execute("""
            SELECT id, display_name, avatar_url, favorite_team, display_name_set, created_at
            FROM users WHERE id = %s
        """, (user_id,))
        user = cur.fetchone()
        if not user:
            cur.close(); conn.close()
            return jsonify({"error": "User not found"}), 404

        cur.execute("""
            SELECT
                COUNT(*)                             AS total_reviews,
                ROUND(AVG(rating)::numeric, 2)       AS avg_rating,
                COUNT(*) FILTER (WHERE rating = 10)  AS five_star_count,
                COUNT(*) FILTER (WHERE rating <= 2)  AS half_star_count
            FROM game_reviews WHERE user_id = %s
        """, (user_id,))
        stats = dict(cur.fetchone())

        # Rating distribution (1–10 buckets → displayed as ½–5 stars)
        cur.execute("""
            SELECT rating, COUNT(*) AS cnt
            FROM game_reviews WHERE user_id = %s
            GROUP BY rating ORDER BY rating
        """, (user_id,))
        dist = {r["rating"]: r["cnt"] for r in cur.fetchall()}

        # Friend status relative to viewer
        friend_status = None
        if viewer and viewer["id"] != user_id:
            cur.execute("""
                SELECT status, sender_id FROM friendships
                WHERE (sender_id = %s AND receiver_id = %s)
                   OR (sender_id = %s AND receiver_id = %s)
            """, (viewer["id"], user_id, user_id, viewer["id"]))
            fs = cur.fetchone()
            if fs:
                if fs["status"] == "accepted":
                    friend_status = "friends"
                elif fs["sender_id"] == viewer["id"]:
                    friend_status = "request_sent"
                else:
                    friend_status = "request_received"

        # Friend count
        cur.execute("""
            SELECT COUNT(*) FROM friendships
            WHERE (sender_id = %s OR receiver_id = %s) AND status = 'accepted'
        """, (user_id, user_id))
        friend_count = cur.fetchone()["count"]

        # Favorite games (up to 4, ordered by position)
        cur.execute("""
            SELECT fg.position, fg.game_id,
                   g.home_team_abbr, g.away_team_abbr,
                   g.home_score, g.away_score, g.game_date
            FROM favorite_games fg
            LEFT JOIN games g ON g.game_id = fg.game_id
            WHERE fg.user_id = %s
            ORDER BY fg.position
        """, (user_id,))
        favorites = [dict(r) for r in cur.fetchall()]

        cur.close(); conn.close()

        return jsonify({
            "user": {
                "id":               user["id"],
                "display_name":     user["display_name"],
                "avatar_url":       user["avatar_url"],
                "favorite_team":    user["favorite_team"] or "",
                "display_name_set": user["display_name_set"],
                "member_since":     str(user["created_at"]),
            },
            "stats": {
                "total_reviews":   int(stats["total_reviews"] or 0),
                "avg_rating":      round(float(stats["avg_rating"] or 0) / 2, 2),
                "five_star_count": int(stats["five_star_count"] or 0),
                "half_star_count": int(stats["half_star_count"] or 0),
                "distribution":    dist,
            },
            "favorites":     favorites,
            "friend_count":  friend_count,
            "friend_status": friend_status,  # null if viewing own profile or not logged in
            "is_own":        viewer and viewer["id"] == user_id,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500





# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GET /api/users/search?q=<name>
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/users/search")
@login_required
def search_users():
    q     = request.args.get("q", "").strip()
    limit = min(int(request.args.get("limit", 10)), 20)
    me    = current_user()

    if not q:
        return jsonify({"users": []})

    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT id, display_name, avatar_url
            FROM users
            WHERE display_name ILIKE %s AND id != %s
            ORDER BY display_name
            LIMIT %s
        """, (q, me["id"], limit))
        users = [dict(r) for r in cur.fetchall()]
        cur.close(); conn.close()
        return jsonify({"users": users})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GET /api/friends  — my friends + pending requests
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/friends")
@login_required
def get_friends():
    me = current_user()
    try:
        conn = get_conn()
        cur  = conn.cursor()

        # Accepted friends
        cur.execute("""
            SELECT u.id, u.display_name, u.avatar_url,
                   f.created_at AS friends_since
            FROM friendships f
            JOIN users u ON u.id = CASE
                WHEN f.sender_id = %s THEN f.receiver_id
                ELSE f.sender_id END
            WHERE (f.sender_id = %s OR f.receiver_id = %s)
              AND f.status = 'accepted'
            ORDER BY u.display_name
        """, (me["id"], me["id"], me["id"]))
        friends = [dict(r) for r in cur.fetchall()]

        # Pending — received (I need to accept/decline)
        cur.execute("""
            SELECT u.id, u.display_name, u.avatar_url, f.id AS friendship_id
            FROM friendships f
            JOIN users u ON u.id = f.sender_id
            WHERE f.receiver_id = %s AND f.status = 'pending'
            ORDER BY f.created_at DESC
        """, (me["id"],))
        received = [dict(r) for r in cur.fetchall()]

        # Pending — sent (waiting on them)
        cur.execute("""
            SELECT u.id, u.display_name, u.avatar_url, f.id AS friendship_id
            FROM friendships f
            JOIN users u ON u.id = f.receiver_id
            WHERE f.sender_id = %s AND f.status = 'pending'
            ORDER BY f.created_at DESC
        """, (me["id"],))
        sent = [dict(r) for r in cur.fetchall()]

        cur.close(); conn.close()
        return jsonify({
            "friends":          friends,
            "requests_received": received,
            "requests_sent":    sent,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# POST /api/friends/<user_id>  — send friend request
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/friends/<int:target_id>", methods=["POST"])
@login_required
def send_friend_request(target_id):
    me = current_user()
    if me["id"] == target_id:
        return jsonify({"error": "Can't friend yourself"}), 400
    try:
        conn = get_conn()
        cur  = conn.cursor()
        # Check they exist
        cur.execute("SELECT id FROM users WHERE id = %s", (target_id,))
        if not cur.fetchone():
            return jsonify({"error": "User not found"}), 404
        # Check no existing relationship
        cur.execute("""
            SELECT id, status FROM friendships
            WHERE (sender_id = %s AND receiver_id = %s)
               OR (sender_id = %s AND receiver_id = %s)
        """, (me["id"], target_id, target_id, me["id"]))
        existing = cur.fetchone()
        if existing:
            if existing["status"] == "accepted":
                return jsonify({"error": "Already friends"}), 409
            return jsonify({"error": "Request already exists"}), 409

        cur.execute("""
            INSERT INTO friendships (sender_id, receiver_id, status)
            VALUES (%s, %s, 'pending') RETURNING id
        """, (me["id"], target_id))
        conn.commit()
        cur.close(); conn.close()
        return jsonify({"ok": True, "status": "request_sent"}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PATCH /api/friends/<user_id>  — accept friend request
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/friends/<int:target_id>", methods=["PATCH"])
@login_required
def accept_friend_request(target_id):
    me = current_user()
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            UPDATE friendships SET status = 'accepted', updated_at = NOW()
            WHERE sender_id = %s AND receiver_id = %s AND status = 'pending'
            RETURNING id
        """, (target_id, me["id"]))
        updated = cur.fetchone()
        conn.commit()
        cur.close(); conn.close()
        if not updated:
            return jsonify({"error": "No pending request found"}), 404
        return jsonify({"ok": True, "status": "friends"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DELETE /api/friends/<user_id>  — remove friend or decline/cancel request
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/friends/<int:target_id>", methods=["DELETE"])
@login_required
def remove_friend(target_id):
    me = current_user()
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            DELETE FROM friendships
            WHERE (sender_id = %s AND receiver_id = %s)
               OR (sender_id = %s AND receiver_id = %s)
            RETURNING id
        """, (me["id"], target_id, target_id, me["id"]))
        deleted = cur.fetchone()
        conn.commit()
        cur.close(); conn.close()
        if not deleted:
            return jsonify({"error": "No relationship found"}), 404
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Page routes ───────────────────────────────────────────────────
@app.route("/profile")
@app.route("/profile/")
def own_profile():
    return app.send_static_file("profile.html")

@app.route("/profile/<int:user_id>")
def user_profile(user_id):
    return app.send_static_file("profile.html")

@app.route("/compare")
@app.route("/compare.html")
def compare_page():
    return app.send_static_file("compare.html")


# ── Matchups API ──────────────────────────────────────────────────

_MATCHUP_SORT_LEADERS  = {'adj_delta', 'impact', 'possessions', 'min', 'avg_opp_fg_pct', 'avg_matchup_fg_pct'}
_MATCHUP_SORT_PAIRINGS = {'adj_delta', 'possessions', 'opp_season_fg_pct', 'fg_pct'}

@app.route("/api/matchups/leaders")
def matchups_leaders():
    """Top defenders ranked by opponent-adjusted FG% allowed, computed from player_matchups."""
    season      = request.args.get("season",      DEFAULT_SEASON)
    season_type = request.args.get("season_type", DEFAULT_SEASON_TYPE)
    min_poss    = max(0, int(request.args.get("min_poss", 200)))
    sort_col    = request.args.get("sort", "impact")
    sort_dir    = request.args.get("dir",  "desc").lower()
    limit       = min(int(request.args.get("limit", 150)), 300)
    pos_filter  = request.args.get("pos",  "ALL").strip().upper()
    team_filter = request.args.get("team", "ALL").strip().upper()

    if sort_col not in _MATCHUP_SORT_LEADERS:
        sort_col = "adj_delta"
    dir_sql = "ASC" if sort_dir == "asc" else "DESC"

    col_map = {
        "adj_delta":          "adj_delta",
        "impact":             "impact",
        "possessions":        "possessions",
        "min":                "ps.min",
        "avg_opp_fg_pct":     "avg_opp_fg_pct",
        "avg_matchup_fg_pct": "avg_matchup_fg_pct",
    }

    extra_where  = []
    extra_params = []
    if pos_filter  != "ALL":
        extra_where.append("p.position_group = %s")
        extra_params.append(pos_filter)
    if team_filter != "ALL":
        extra_where.append("ps.team_abbr = %s")
        extra_params.append(team_filter)
    extra_sql = ("AND " + " AND ".join(extra_where)) if extra_where else ""

    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute(f"""
            SELECT
                p.player_id,
                p.player_name,
                p.position_group,
                ps.team_abbr,
                SUM(pm.adj_delta * pm.possessions)         / NULLIF(SUM(pm.possessions), 0) AS adj_delta,
                SUM(pm.adj_delta * pm.possessions)                                           AS impact,
                SUM(pm.opp_season_fg_pct * pm.possessions) / NULLIF(SUM(pm.possessions), 0) AS avg_opp_fg_pct,
                SUM(pm.fg_pct * pm.possessions)            / NULLIF(SUM(pm.possessions), 0) AS avg_matchup_fg_pct,
                SUM(pm.possessions) AS possessions,
                ps.min
            FROM player_matchups pm
            JOIN players p ON pm.defender_id = p.player_id
            LEFT JOIN player_seasons ps ON pm.defender_id = ps.player_id
                AND pm.season = ps.season AND pm.season_type = ps.season_type
            WHERE pm.season = %s AND pm.season_type = %s
              {extra_sql}
            GROUP BY p.player_id, p.player_name, p.position_group, ps.team_abbr, ps.min
            HAVING SUM(pm.possessions) >= %s
            ORDER BY {col_map[sort_col]} {dir_sql} NULLS LAST
            LIMIT %s
        """, [season, season_type] + extra_params + [min_poss, limit])
        rows = [dict(r) for r in cur.fetchall()]
        cur.close(); conn.close()
        return jsonify({"defenders": rows, "season": season, "n": len(rows)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/matchups/pairings")
def matchups_pairings():
    """Individual defender×attacker pairing results."""
    season      = request.args.get("season",      DEFAULT_SEASON)
    season_type = request.args.get("season_type", DEFAULT_SEASON_TYPE)
    min_poss    = max(0, int(request.args.get("min_poss", 100)))
    sort_col    = request.args.get("sort", "adj_delta")
    sort_dir    = request.args.get("dir",  "desc").lower()
    limit       = min(int(request.args.get("limit", 150)), 300)
    pos_filter  = request.args.get("pos",  "ALL").strip().upper()
    team_filter = request.args.get("team", "ALL").strip().upper()

    if sort_col not in _MATCHUP_SORT_PAIRINGS:
        sort_col = "adj_delta"
    dir_sql = "ASC" if sort_dir == "asc" else "DESC"

    col_map = {
        "adj_delta":         "pm.adj_delta",
        "possessions":       "pm.possessions",
        "opp_season_fg_pct": "pm.opp_season_fg_pct",
        "fg_pct":            "pm.fg_pct",
    }

    extra_where  = []
    extra_params = []
    if pos_filter  != "ALL":
        extra_where.append("dp.position_group = %s")
        extra_params.append(pos_filter)
    if team_filter != "ALL":
        extra_where.append("dps.team_abbr = %s")
        extra_params.append(team_filter)
    extra_sql = ("AND " + " AND ".join(extra_where)) if extra_where else ""

    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute(f"""
            SELECT
                dp.player_id   AS defender_id,
                dp.player_name AS defender_name,
                dps.team_abbr  AS defender_team,
                op.player_id   AS attacker_id,
                op.player_name AS attacker_name,
                ops.team_abbr  AS attacker_team,
                pm.possessions,
                pm.fg_pct,
                pm.opp_season_fg_pct,
                pm.adj_delta,
                pm.fga,
                pm.fgm
            FROM player_matchups pm
            JOIN players dp ON pm.defender_id = dp.player_id
            JOIN players op ON pm.offensive_player_id = op.player_id
            LEFT JOIN player_seasons dps ON pm.defender_id = dps.player_id
                AND dps.season = pm.season AND dps.season_type = pm.season_type
            LEFT JOIN player_seasons ops ON pm.offensive_player_id = ops.player_id
                AND ops.season = pm.season AND ops.season_type = pm.season_type
            WHERE pm.season = %s AND pm.season_type = %s
              AND pm.possessions >= %s
              {extra_sql}
            ORDER BY {col_map[sort_col]} {dir_sql} NULLS LAST
            LIMIT %s
        """, [season, season_type, min_poss] + extra_params + [limit])
        rows = [dict(r) for r in cur.fetchall()]
        cur.close(); conn.close()
        return jsonify({"pairings": rows, "season": season, "n": len(rows)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/matchups/defender/<int:player_id>")
def matchups_defender(player_id):
    """Full matchup card for a specific defender."""
    season      = request.args.get("season",      DEFAULT_SEASON)
    season_type = request.args.get("season_type", DEFAULT_SEASON_TYPE)

    try:
        conn = get_conn()
        cur  = conn.cursor()

        cur.execute("""
            SELECT
                p.player_id,
                p.player_name,
                p.position_group,
                ps.team_abbr,
                ps.matchup_def_fg_pct_adj AS adj_delta,
                ps.matchup_poss           AS possessions,
                ps.min
            FROM player_seasons ps
            JOIN players p ON ps.player_id = p.player_id
            WHERE ps.player_id = %s AND ps.season = %s AND ps.season_type = %s
        """, (player_id, season, season_type))
        defender = cur.fetchone()

        cur.execute("""
            SELECT
                op.player_id       AS attacker_id,
                op.player_name     AS attacker_name,
                ops.team_abbr      AS attacker_team,
                op.position_group  AS attacker_pos,
                pm.possessions,
                pm.fg_pct,
                pm.opp_season_fg_pct,
                pm.adj_delta,
                pm.fga,
                pm.fgm
            FROM player_matchups pm
            JOIN players op ON pm.offensive_player_id = op.player_id
            LEFT JOIN player_seasons ops ON pm.offensive_player_id = ops.player_id
                AND ops.season = pm.season AND ops.season_type = pm.season_type
            WHERE pm.defender_id = %s AND pm.season = %s AND pm.season_type = %s
            ORDER BY pm.possessions DESC NULLS LAST
        """, (player_id, season, season_type))
        matchups = [dict(r) for r in cur.fetchall()]

        cur.close(); conn.close()
        if not defender:
            return jsonify({"error": "Player not found"}), 404
        return jsonify({"defender": dict(defender), "matchups": matchups, "season": season, "n": len(matchups)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/matchups")
@app.route("/matchups.html")
def matchups_page():
    return app.send_static_file("matchups.html")


# ── /api/trends ───────────────────────────────────────────────
# Returns risers and fallers for each of the 5 tracked stats.
# delta = avg of last N games  minus  avg of all prior games
# Only players with >= 10 mpg in their last N games are included.
# Players must have more than N games played so there are prior games to compare.

TREND_STATS = [
    {"key": "pts",    "label": "PPG"},
    {"key": "ts_pct", "label": "TS%"},
    {"key": "fg3m",   "label": "3PM"},
    {"key": "ast",    "label": "APG"},
    {"key": "reb",    "label": "RPG"},
]

def _safe(v):
    """Convert float-like values to JSON-safe Python float. NaN/Inf → None."""
    if v is None:
        return None
    try:
        f = float(v)
        return None if (math.isnan(f) or math.isinf(f)) else f
    except (TypeError, ValueError):
        return v


@app.route("/api/trends")
def get_trends():
    season = request.args.get("season", DEFAULT_SEASON)
    n      = int(request.args.get("n", 5))
    if n not in (5, 10, 15):
        n = 5

    try:
        conn = get_conn()
        cur  = conn.cursor()

        results = {}
        all_player_ids = set()

        for stat in TREND_STATS:
            col = stat["key"]
            # ts_pct and pts require a minimum FGA to be a valid data point
            fga_gate = "AND fga >= 5" if col in ("ts_pct", "pts") else ""
            cur.execute(f"""
                WITH ranked AS (
                    SELECT
                        player_id,
                        player_name,
                        game_date,
                        {col},
                        min,
                        ROW_NUMBER() OVER (
                            PARTITION BY player_id
                            ORDER BY game_date DESC
                        ) AS rn,
                        COUNT(*) OVER (PARTITION BY player_id) AS total_games
                    FROM player_gamelogs
                    WHERE season = %s
                      AND {col} IS NOT NULL
                      AND min IS NOT NULL
                      AND NOT ({col} = 'NaN'::real)
                      {fga_gate}
                ),
                last_n AS (
                    SELECT
                        player_id,
                        player_name,
                        AVG({col})::numeric(7,4) AS last_n_avg,
                        AVG(min)::numeric(6,2)   AS last_n_mpg,
                        total_games
                    FROM ranked
                    WHERE rn <= %s
                    GROUP BY player_id, player_name, total_games
                    HAVING AVG(min) >= 10
                ),
                prior AS (
                    SELECT
                        player_id,
                        AVG({col})::numeric(7,4) AS prior_avg
                    FROM ranked
                    WHERE rn > %s
                    GROUP BY player_id
                    HAVING COUNT(*) >= %s
                )
                SELECT
                    l.player_id,
                    l.player_name,
                    l.last_n_avg,
                    l.last_n_mpg,
                    p.prior_avg,
                    (l.last_n_avg - p.prior_avg)::numeric(7,4) AS delta
                FROM last_n l
                JOIN prior p ON p.player_id = l.player_id
                ORDER BY delta DESC
            """, (season, n, n, n))

            rows = [dict(r) for r in cur.fetchall()]
            for r in rows:
                for k in ("last_n_avg", "last_n_mpg", "prior_avg", "delta"):
                    r[k] = _safe(r[k])
                all_player_ids.add(r["player_id"])

            risers  = [r for r in rows if r["delta"] is not None and r["delta"] >= 0]
            fallers = sorted(
                [r for r in rows if r["delta"] is not None and r["delta"] < 0],
                key=lambda x: x["delta"]
            )
            results[col] = {"label": stat["label"], "risers": risers, "fallers": fallers}

        # Add most-recent team_abbr to every player row (parsed from matchup)
        if all_player_ids:
            cur.execute("""
                SELECT DISTINCT ON (player_id)
                    player_id,
                    SUBSTRING(matchup FROM 1 FOR 3) AS team_abbr
                FROM player_gamelogs
                WHERE player_id = ANY(%s)
                  AND season = %s
                  AND matchup IS NOT NULL
                ORDER BY player_id, game_date DESC
            """, (list(all_player_ids), season))
            team_map = {r["player_id"]: r["team_abbr"] for r in cur.fetchall()}
            for stat_data in results.values():
                for r in stat_data["risers"] + stat_data["fallers"]:
                    r["team_abbr"] = team_map.get(r["player_id"])

        cur.close()
        conn.close()
        return jsonify({"n": n, "season": season, "stats": results})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── /api/trends/gamelog ───────────────────────────────────────
# Returns the full game log for a single player (for the line graph).

@app.route("/api/trends/gamelog")
def get_trends_gamelog():
    player_id = request.args.get("player_id", type=int)
    season    = request.args.get("season",    DEFAULT_SEASON)

    if not player_id:
        return jsonify({"error": "player_id required"}), 400

    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT game_id, game_date, matchup, wl, min, fga, pts, ast, reb, fg3m, ts_pct
            FROM player_gamelogs
            WHERE player_id = %s
              AND season = %s
            ORDER BY game_date ASC
        """, (player_id, season))
        rows = [dict(r) for r in cur.fetchall()]
        float_cols = ("min", "fga", "pts", "ast", "reb", "fg3m", "ts_pct")
        for r in rows:
            if r["game_date"]:
                r["game_date"] = r["game_date"].strftime("%Y-%m-%d")
            for k in float_cols:
                r[k] = _safe(r[k])
            # Parse team from matchup (first 3 chars: "BOS vs. MIA" → "BOS")
            r["team_abbr"] = r["matchup"][:3] if r.get("matchup") else None
        cur.close()
        conn.close()
        return jsonify({"games": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/trends")
@app.route("/trends.html")
def trends_page():
    return "", 404


# ══════════════════════════════════════════════════════════════════════════════
# PVA (Possession Value Added)
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/pva/leaders")
def pva_leaders():
    """
    Return the PVA leaderboard for a season.

    Query params:
      season       — e.g. "2024-25"  (defaults to current season)
      season_type  — "Regular Season" | "Playoffs"
      min_poss     — minimum offensive possessions (default 200)
      sort         — column to sort by (default "total_pva_per_100")
      dir          — "desc" | "asc" (default "desc")
      limit        — max rows (default 200)
    """
    season      = request.args.get("season",      DEFAULT_SEASON)
    season_type = request.args.get("season_type", "Regular Season")
    min_poss    = request.args.get("min_poss",    200,  type=int)
    sort        = request.args.get("sort",        "total_pva_per_100")
    direction   = request.args.get("dir",         "desc").lower()
    limit       = request.args.get("limit",       200,  type=int)

    allowed_sorts = {
        "total_pva_per_100", "off_pva_per_100", "def_pva_per_100",
        "total_pva", "off_pva", "def_pva",
        "off_possessions", "total_possessions",
        "pva_from_makes", "pva_from_misses", "pva_from_turnovers",
        "avg_actual_pts", "avg_expected_pts",
    }
    if sort not in allowed_sorts:
        sort = "total_pva_per_100"
    order = "DESC" if direction != "asc" else "ASC"

    try:
        conn = get_conn()
        cur  = conn.cursor()

        cur.execute(f"""
            SELECT
                pv.player_id,
                pv.player_name,
                pv.off_possessions,
                pv.def_possessions,
                pv.total_possessions,
                pv.off_pva,
                pv.def_pva,
                pv.total_pva,
                pv.off_pva_per_100,
                pv.def_pva_per_100,
                pv.total_pva_per_100,
                pv.pva_from_makes,
                pv.pva_from_misses,
                pv.pva_from_turnovers,
                pv.avg_expected_pts,
                pv.avg_actual_pts,
                ps.team_abbr
            FROM player_pva_season pv
            LEFT JOIN player_seasons ps
                   ON ps.player_id = pv.player_id
                  AND ps.season     = pv.season
                  AND ps.season_type = pv.season_type
            WHERE pv.season      = %s
              AND pv.season_type = %s
              AND pv.off_possessions >= %s
            ORDER BY {sort} {order}
            LIMIT %s
        """, (season, season_type, min_poss, limit))

        rows = cur.fetchall()
        float_cols = (
            "off_pva", "def_pva", "total_pva",
            "off_pva_per_100", "def_pva_per_100", "total_pva_per_100",
            "pva_from_makes", "pva_from_misses", "pva_from_turnovers",
            "avg_expected_pts", "avg_actual_pts",
        )
        result = []
        for r in rows:
            row = dict(r)
            for k in float_cols:
                row[k] = _safe(row[k])
            result.append(row)

        cur.close(); conn.close()
        return jsonify({
            "season": season,
            "season_type": season_type,
            "min_poss": min_poss,
            "players": result,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/pva/player/<int:player_id>")
def pva_player(player_id):
    """
    Return all seasons of PVA data for a single player, plus their last
    10 possessions (for game-log flavour context).
    """
    season      = request.args.get("season",      DEFAULT_SEASON)
    season_type = request.args.get("season_type", "Regular Season")

    try:
        conn = get_conn()
        cur  = conn.cursor()

        # Career PVA rows (all seasons)
        cur.execute("""
            SELECT season, season_type,
                   off_possessions, def_possessions, total_possessions,
                   off_pva, def_pva, total_pva,
                   off_pva_per_100, def_pva_per_100, total_pva_per_100,
                   pva_from_makes, pva_from_misses, pva_from_turnovers,
                   avg_expected_pts, avg_actual_pts,
                   computed_at
            FROM player_pva_season
            WHERE player_id = %s
            ORDER BY season DESC, season_type
        """, (player_id,))
        seasons = []
        float_cols = (
            "off_pva", "def_pva", "total_pva",
            "off_pva_per_100", "def_pva_per_100", "total_pva_per_100",
            "pva_from_makes", "pva_from_misses", "pva_from_turnovers",
            "avg_expected_pts", "avg_actual_pts",
        )
        for r in cur.fetchall():
            row = dict(r)
            for k in float_cols:
                row[k] = _safe(row[k])
            if row.get("computed_at"):
                row["computed_at"] = row["computed_at"].isoformat()
            seasons.append(row)

        # Per-game PVA: join possession outcomes with game log for context
        # (gives actual vs expected for games where this player was primary actor)
        cur.execute("""
            SELECT
                p.game_id,
                p.period,
                p.points_scored,
                p.expected_points,
                p.points_scored - p.expected_points AS pva,
                p.end_reason,
                p.score_margin_offense,
                p.start_clock_seconds
            FROM possessions p
            JOIN possession_events pe
                ON pe.possession_id = p.id
               AND pe.action_type IN ('2pt', '3pt', 'turnover', 'freethrow')
               AND pe.player_id = %s
            WHERE p.season      = %s
              AND p.expected_points IS NOT NULL
            ORDER BY p.game_seconds_start DESC
            LIMIT 50
        """, (player_id, season))
        recent = []
        for r in cur.fetchall():
            row = dict(r)
            row["expected_points"] = _safe(row["expected_points"])
            row["pva"]             = _safe(row["pva"])
            recent.append(row)

        cur.close(); conn.close()
        return jsonify({
            "player_id": player_id,
            "season": season,
            "season_type": season_type,
            "career": seasons,
            "recent_possessions": recent,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/pva/seasons")
def pva_seasons():
    """Return seasons that have computed PVA data."""
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT DISTINCT season, season_type,
                   COUNT(DISTINCT player_id) AS player_count
            FROM player_pva_season
            GROUP BY season, season_type
            ORDER BY season DESC
        """)
        rows = [dict(r) for r in cur.fetchall()]
        cur.close(); conn.close()
        return jsonify({"seasons": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/pva")
@app.route("/pva.html")
def pva_page():
    return app.send_static_file("pva.html")


# ── Adjusted WoWY ─────────────────────────────────────────────────────────────

@app.route("/api/adjusted-wowy/leaders")
def adjusted_wowy_leaders():
    season      = request.args.get("season", get_current_season())
    season_type = request.args.get("season_type", "Regular Season")
    min_poss    = int(request.args.get("min_poss", 500))
    sort_col    = request.args.get("sort", "adj_wowy")
    sort_dir    = request.args.get("dir", "desc")

    allowed_sorts = {"adj_wowy", "on_net_adj", "off_net_adj", "raw_wowy",
                     "on_net_raw", "off_net_raw", "on_poss"}
    if sort_col not in allowed_sorts:
        sort_col = "adj_wowy"
    order = "DESC" if sort_dir != "asc" else "ASC"

    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute(f"""
            SELECT w.player_id, w.player_name, w.team_abbr,
                   w.on_poss, w.off_poss,
                   w.on_net_adj, w.off_net_adj, w.adj_wowy,
                   w.on_net_raw, w.off_net_raw, w.raw_wowy
            FROM player_adjusted_wowy w
            WHERE w.season      = %s
              AND w.season_type = %s
              AND w.on_poss    >= %s
            ORDER BY {sort_col} {order}
        """, (season, season_type, min_poss))
        rows = [dict(r) for r in cur.fetchall()]
        cur.close(); conn.close()
        return jsonify({"season": season, "min_poss": min_poss, "players": rows})
    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500


@app.route("/api/adjusted-wowy/seasons")
def adjusted_wowy_seasons():
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT DISTINCT season, season_type, COUNT(DISTINCT player_id) AS player_count
            FROM player_adjusted_wowy
            GROUP BY season, season_type
            ORDER BY season DESC
        """)
        rows = [dict(r) for r in cur.fetchall()]
        cur.close(); conn.close()
        return jsonify({"seasons": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/adjusted-wowy/by-players")
def adjusted_wowy_by_players():
    """Return adjusted WoWY stats for specific player IDs (used by WoWY page toggle)."""
    season      = request.args.get("season", get_current_season())
    season_type = request.args.get("season_type", "Regular Season")
    players_raw = request.args.get("players", "")

    if not players_raw:
        return jsonify({"error": "players param required"}), 400

    try:
        player_ids = [int(p) for p in players_raw.split(",") if p.strip()]
    except ValueError:
        return jsonify({"error": "players must be comma-separated integers"}), 400

    if not player_ids:
        return jsonify({"error": "no valid player IDs"}), 400

    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT w.player_id, w.player_name, w.team_abbr,
                   w.on_poss, w.off_poss,
                   w.on_net_adj, w.off_net_adj, w.adj_wowy,
                   w.on_net_raw, w.off_net_raw, w.raw_wowy
            FROM player_adjusted_wowy w
            WHERE w.season      = %s
              AND w.season_type = %s
              AND w.player_id   = ANY(%s)
            ORDER BY w.adj_wowy DESC
        """, (season, season_type, player_ids))
        rows = [dict(r) for r in cur.fetchall()]
        cur.close(); conn.close()
        return jsonify({"season": season, "season_type": season_type, "players": rows})
    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500


@app.route("/impact")
@app.route("/impact.html")
def impact_page():
    return app.send_static_file("impact.html")


# ── WoWY possession-data modes ─────────────────────────────────────────────────

def _wowy_team_id(cur, player_ids: list, season: str):
    """Return the offense_team_id most associated with these players this season."""
    cur.execute("""
        SELECT p.offense_team_id, COUNT(*) AS cnt
        FROM possessions p
        JOIN possession_lineups pl ON pl.possession_id = p.id
        WHERE pl.player_id = ANY(%s) AND pl.side = 'offense' AND p.season = %s
        GROUP BY p.offense_team_id
        ORDER BY cnt DESC LIMIT 1
    """, (player_ids, season))
    row = cur.fetchone()
    return row["offense_team_id"] if row else None


@app.route("/api/wowy/shot-profile")
def wowy_shot_profile():
    """Shot zone distribution per lineup combination, derived from possession data."""
    season     = request.args.get("season", get_current_season())
    players_raw = request.args.get("players", "")
    try:
        player_ids = [int(x) for x in players_raw.split(",") if x.strip()]
    except ValueError:
        return jsonify({"error": "Invalid player IDs"}), 400
    if not player_ids:
        return jsonify({"error": "No players specified"}), 400

    try:
        conn = get_conn()
        cur  = conn.cursor()

        team_id = _wowy_team_id(cur, player_ids, season)
        if not team_id:
            cur.close(); conn.close()
            return jsonify({"error": "No possession data for these players this season"}), 404

        # All team offensive possessions: lineup + shot zone
        cur.execute("""
            SELECT p.id AS pid, p.shot_zone,
                   array_agg(pl.player_id) AS lineup
            FROM possessions p
            JOIN possession_lineups pl
              ON pl.possession_id = p.id AND pl.side = 'offense'
            WHERE p.offense_team_id = %s AND p.season = %s
            GROUP BY p.id, p.shot_zone
        """, (team_id, season))
        rows = cur.fetchall()
        cur.close(); conn.close()

        ZONES = {1: "ra", 2: "paint", 3: "mid", 4: "c3", 5: "ab3"}
        player_set = set(player_ids)
        combos = {}  # frozenset(on_selected) → {total, ra, paint, mid, c3, ab3}

        for r in rows:
            on_sel = frozenset(set(r["lineup"]) & player_set)
            if on_sel not in combos:
                combos[on_sel] = {"total": 0, "ra": 0, "paint": 0, "mid": 0, "c3": 0, "ab3": 0}
            z = r["shot_zone"]
            if z and z > 0:
                combos[on_sel]["total"] += 1
                if z in ZONES:
                    combos[on_sel][ZONES[z]] += 1

        results = []
        for on_sel, s in combos.items():
            total = s["total"]
            pct = lambda k: round(s[k] / total * 100, 1) if total > 0 else None
            results.append({
                "on_players": sorted(on_sel),
                "fga":        total,
                "ra_pct":     pct("ra"),
                "paint_pct":  pct("paint"),
                "mid_pct":    pct("mid"),
                "c3_pct":     pct("c3"),
                "ab3_pct":    pct("ab3"),
            })

        return jsonify({"combos": results, "season": season})

    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500


@app.route("/api/wowy/shot-locations")
def wowy_shot_locations():
    """Raw FGA coordinates split by anchor player on/off court."""
    season = request.args.get("season", get_current_season())
    try:
        anchor_id = int(request.args.get("anchor", ""))
    except (ValueError, TypeError):
        return jsonify({"error": "anchor param required (player_id)"}), 400

    try:
        conn = get_conn()
        cur  = conn.cursor()

        team_id = _wowy_team_id(cur, [anchor_id], season)
        if not team_id:
            cur.close(); conn.close()
            return jsonify({"error": "No possession data for this player this season"}), 404

        cur.execute("""
            WITH anchor_on_poss AS (
                SELECT possession_id FROM possession_lineups
                WHERE player_id = %(anchor)s AND side = 'offense'
            )
            SELECT
                pe.x_legacy                         AS x,
                pe.y_legacy                         AS y,
                (pe.sub_type = 'made')              AS made,
                pe.action_type                      AS shot_type,
                (ao.possession_id IS NOT NULL)      AS anchor_on
            FROM possessions p
            JOIN possession_events pe ON pe.possession_id = p.id
            LEFT JOIN anchor_on_poss ao ON ao.possession_id = p.id
            WHERE p.offense_team_id = %(team_id)s
              AND p.season          = %(season)s
              AND pe.is_field_goal
              AND pe.x_legacy IS NOT NULL
              AND pe.y_legacy IS NOT NULL
        """, {"anchor": anchor_id, "team_id": team_id, "season": season})
        shot_rows = cur.fetchall()

        cur.execute("""
            WITH anchor_on_poss AS (
                SELECT possession_id FROM possession_lineups
                WHERE player_id = %(anchor)s AND side = 'offense'
            )
            SELECT
                COUNT(*) FILTER (WHERE ao.possession_id IS NOT NULL) AS on_poss,
                COUNT(*) FILTER (WHERE ao.possession_id IS NULL)     AS off_poss
            FROM possessions p
            LEFT JOIN anchor_on_poss ao ON ao.possession_id = p.id
            WHERE p.offense_team_id = %(team_id)s
              AND p.season          = %(season)s
        """, {"anchor": anchor_id, "team_id": team_id, "season": season})
        counts = cur.fetchone()
        cur.close(); conn.close()

        on_shots, off_shots = [], []
        for r in shot_rows:
            shot = {
                "x":    float(r["x"]),
                "y":    float(r["y"]),
                "made": bool(r["made"]),
                "is3":  r["shot_type"] == "3pt",
            }
            if r["anchor_on"]:
                on_shots.append(shot)
            else:
                off_shots.append(shot)

        return jsonify({
            "on_shots":  on_shots,
            "off_shots": off_shots,
            "on_poss":   int(counts["on_poss"]  or 0),
            "off_poss":  int(counts["off_poss"] or 0),
            "season":    season,
        })

    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500


@app.route("/api/wowy/stat-line")
def wowy_stat_line():
    """Per-teammate stat shifts for an anchor player.

    For each teammate on the same team, returns their individual per-100
    stats split by whether the anchor player is on the floor (ON) or off (OFF).
    """
    season     = request.args.get("season", get_current_season())
    try:
        anchor_id = int(request.args.get("anchor", ""))
    except (ValueError, TypeError):
        return jsonify({"error": "anchor param required (player_id)"}), 400

    try:
        conn = get_conn()
        cur  = conn.cursor()

        team_id = _wowy_team_id(cur, [anchor_id], season)
        if not team_id:
            cur.close(); conn.close()
            return jsonify({"error": "No possession data for this player this season"}), 404

        # ── Per-teammate stats split by anchor on/off ──────────────
        # CTE 1: mark every team offensive possession as anchor_on or not.
        # CTE 2: for each (teammate, possession), get their event stats.
        # CTE 3: assists where the teammate is the assister (separate field).
        # Separate CTEs avoid the N×M cross-product from joining lineups + events.
        cur.execute("""
            WITH anchor_on_poss AS (
                SELECT possession_id
                FROM possession_lineups
                WHERE player_id = %(anchor)s AND side = 'offense'
            ),
            teammate_poss AS (
                SELECT pl.player_id,
                       pl.possession_id,
                       (ao.possession_id IS NOT NULL) AS anchor_on
                FROM possession_lineups pl
                JOIN possessions p ON p.id = pl.possession_id
                LEFT JOIN anchor_on_poss ao ON ao.possession_id = pl.possession_id
                WHERE p.offense_team_id = %(team_id)s
                  AND p.season          = %(season)s
                  AND pl.side           = 'offense'
                  AND pl.player_id     != %(anchor)s
            ),
            teammate_events AS (
                SELECT tp.player_id, tp.anchor_on,
                    SUM(CASE WHEN pe.is_field_goal THEN 1 ELSE 0 END)                                          AS fga,
                    SUM(CASE WHEN pe.is_field_goal AND pe.action_type = '2pt'
                                  AND pe.sub_type = 'made' THEN 2 ELSE 0 END)
                  + SUM(CASE WHEN pe.is_field_goal AND pe.action_type = '3pt'
                                  AND pe.sub_type = 'made' THEN 3 ELSE 0 END)                                  AS fg_pts,
                    SUM(CASE WHEN pe.is_field_goal AND pe.sub_type = 'made' THEN 1 ELSE 0 END)                 AS fgm,
                    SUM(CASE WHEN pe.is_field_goal AND pe.action_type = '3pt' THEN 1 ELSE 0 END)               AS fg3a,
                    SUM(CASE WHEN pe.is_field_goal AND pe.action_type = '3pt'
                                  AND pe.sub_type = 'made' THEN 1 ELSE 0 END)                                  AS fg3m,
                    SUM(CASE WHEN pe.action_type = 'freethrow' AND pe.sub_type = 'made' THEN 1 ELSE 0 END)     AS ftm,
                    SUM(CASE WHEN pe.action_type = 'rebound' THEN 1 ELSE 0 END)                                AS reb,
                    SUM(CASE WHEN pe.action_type = 'turnover' THEN 1 ELSE 0 END)                               AS tov
                FROM teammate_poss tp
                JOIN possession_events pe
                  ON pe.possession_id = tp.possession_id AND pe.player_id = tp.player_id
                GROUP BY tp.player_id, tp.anchor_on
            ),
            teammate_ast AS (
                SELECT tp.player_id, tp.anchor_on, COUNT(*) AS ast
                FROM teammate_poss tp
                JOIN possession_events pe ON pe.possession_id = tp.possession_id
                WHERE pe.assist_player_id = tp.player_id
                  AND pe.is_field_goal AND pe.sub_type = 'made'
                GROUP BY tp.player_id, tp.anchor_on
            ),
            poss_counts AS (
                SELECT player_id, anchor_on, COUNT(*) AS poss
                FROM teammate_poss
                GROUP BY player_id, anchor_on
            )
            SELECT pc.player_id, pc.anchor_on, pc.poss,
                   COALESCE(te.fga,    0) AS fga,
                   COALESCE(te.fg_pts, 0) AS fg_pts,
                   COALESCE(te.fgm,    0) AS fgm,
                   COALESCE(te.fg3a,   0) AS fg3a,
                   COALESCE(te.fg3m,   0) AS fg3m,
                   COALESCE(te.ftm,    0) AS ftm,
                   COALESCE(te.reb,    0) AS reb,
                   COALESCE(te.tov,    0) AS tov,
                   COALESCE(ta.ast,    0) AS ast
            FROM poss_counts pc
            LEFT JOIN teammate_events te USING (player_id, anchor_on)
            LEFT JOIN teammate_ast    ta USING (player_id, anchor_on)
            ORDER BY pc.player_id, pc.anchor_on
        """, {"anchor": anchor_id, "team_id": team_id, "season": season})
        rows = cur.fetchall()

        # Resolve player names
        teammate_ids = list({r["player_id"] for r in rows})
        cur.execute(
            "SELECT player_id, player_name FROM players WHERE player_id = ANY(%s)",
            (teammate_ids,)
        )
        name_map = {r["player_id"]: r["player_name"] for r in cur.fetchall()}

        # Resolve anchor name
        cur.execute("SELECT player_name FROM players WHERE player_id = %s", (anchor_id,))
        anc = cur.fetchone()
        anchor_name = anc["player_name"] if anc else str(anchor_id)

        cur.close(); conn.close()

        # Pivot ON / OFF rows per teammate
        by_player = {}
        for r in rows:
            pid = r["player_id"]
            if pid not in by_player:
                by_player[pid] = {}
            side = "on" if r["anchor_on"] else "off"
            by_player[pid][side] = r

        def p100(n, poss):
            return round(n / poss * 100, 1) if poss > 0 else None

        def efg(fgm, fg3m, fga):
            return round((fgm + 0.5 * fg3m) / fga * 100, 1) if fga > 0 else None

        def fg3pct(fg3m, fg3a):
            return round(fg3m / fg3a * 100, 1) if fg3a > 0 else None

        def diff(a, b):
            return round(a - b, 1) if a is not None and b is not None else None

        teammates = []
        for pid, sides in by_player.items():
            on  = sides.get("on",  {})
            off = sides.get("off", {})

            on_poss  = int(on.get("poss", 0)  or 0)
            off_poss = int(off.get("poss", 0) or 0)
            if on_poss + off_poss < 50:
                continue  # skip players with almost no shared minutes

            def stat(key, poss, row):
                return p100(int(row.get(key, 0) or 0), poss)

            pts_on   = p100(int(on.get("fg_pts",0) or 0) + int(on.get("ftm",0) or 0), on_poss)
            pts_off  = p100(int(off.get("fg_pts",0) or 0) + int(off.get("ftm",0) or 0), off_poss)
            efg_on   = efg(int(on.get("fgm",0) or 0),  int(on.get("fg3m",0) or 0),  int(on.get("fga",0) or 0))
            efg_off  = efg(int(off.get("fgm",0) or 0), int(off.get("fg3m",0) or 0), int(off.get("fga",0) or 0))
            ast_on   = stat("ast",  on_poss,  on)
            ast_off  = stat("ast",  off_poss, off)
            reb_on   = stat("reb",  on_poss,  on)
            reb_off  = stat("reb",  off_poss, off)
            tov_on   = stat("tov",  on_poss,  on)
            tov_off  = stat("tov",  off_poss, off)
            fg3a_on  = stat("fg3a", on_poss,  on)
            fg3a_off = stat("fg3a", off_poss, off)
            fg3p_on  = fg3pct(int(on.get("fg3m",0) or 0),  int(on.get("fg3a",0) or 0))
            fg3p_off = fg3pct(int(off.get("fg3m",0) or 0), int(off.get("fg3a",0) or 0))

            teammates.append({
                "player_id":   pid,
                "player_name": name_map.get(pid, str(pid)),
                "on_poss":     on_poss,
                "off_poss":    off_poss,
                "pts_on":  pts_on,  "pts_off":  pts_off,  "pts_diff":  diff(pts_on,  pts_off),
                "efg_on":  efg_on,  "efg_off":  efg_off,  "efg_diff":  diff(efg_on,  efg_off),
                "ast_on":  ast_on,  "ast_off":  ast_off,  "ast_diff":  diff(ast_on,  ast_off),
                "reb_on":  reb_on,  "reb_off":  reb_off,  "reb_diff":  diff(reb_on,  reb_off),
                "tov_on":  tov_on,  "tov_off":  tov_off,  "tov_diff":  diff(tov_on,  tov_off),
                "fg3a_on": fg3a_on, "fg3a_off": fg3a_off, "fg3a_diff": diff(fg3a_on, fg3a_off),
                "fg3p_on": fg3p_on, "fg3p_off": fg3p_off, "fg3p_diff": diff(fg3p_on, fg3p_off),
            })

        # Sort by on_poss descending (most shared minutes first)
        teammates.sort(key=lambda t: t["on_poss"], reverse=True)

        return jsonify({
            "anchor_id":   anchor_id,
            "anchor_name": anchor_name,
            "teammates":   teammates,
            "season":      season,
        })

    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500


start_sb_poller()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=False)