"""
NothingButNet — API Server
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

DATABASE_URL       = os.getenv("DATABASE_URL")
DEFAULT_SEASON     = os.getenv("NBA_SEASON",      "2024-25")
DEFAULT_SEASON_TYPE = os.getenv("NBA_SEASON_TYPE", "Regular Season")


def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)


# ── /api/seasons ─────────────────────────────────────────────

@app.route("/api/seasons")
def get_seasons():
    try:
        conn = get_conn()
        cur  = conn.cursor()
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
        "clutch_net_rating", "clutch_ts_pct", "def_ws",
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
                ps.clutch_net_rating, ps.clutch_ts_pct, ps.clutch_usg_pct, ps.clutch_min,
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

    if not selected:
        return jsonify({"error": "No stats selected"}), 400
    if len(selected) > 150:
        return jsonify({"error": "Max 150 stats at a time"}), 400

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

        # Fetch qualifying players
        pos_clause = "AND p.position_group = %s" if pos_filter != "ALL" else ""
        pos_params = [pos_filter] if pos_filter != "ALL" else []

        cur.execute(f"""
            SELECT ps.player_id, p.player_name, p.position_group, ps.team_abbr, ps.min
            FROM player_seasons ps
            JOIN players p ON ps.player_id = p.player_id
            WHERE ps.season = %s AND ps.season_type = %s
              AND ps.min >= %s
              {pos_clause}
        """, [season, season_type, min_minutes] + pos_params)
        players = cur.fetchall()

        cur.close(); conn.close()

        # Score each player
        results = []
        for p in players:
            pid = str(p["player_id"])
            breakdown = []
            total_pct = 0.0
            covered   = 0

            for stat in selected:
                pmap = pct_maps.get(stat, {})
                pct  = pmap.get(pid) or pmap.get(int(pid))
                if pct is not None:
                    breakdown.append({"stat": stat, "pctile": round(float(pct), 1)})
                    total_pct += float(pct)
                    covered   += 1

            if covered == 0:
                continue
            # Require at least 80% stat coverage to avoid severely skewed scores
            # (e.g. playtypes missing for low-usage players, PBP stats for some)
            if covered < len(selected) * 0.8:
                continue

            score = round(total_pct / covered, 2)
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
            "stats_found": list(pct_maps.keys()),
            "stats_missing": [s for s in selected if s not in pct_maps],
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Run ───────────────────────────────────────────────────────

if __name__ == "__main__":
    port  = int(os.getenv("PORT", 5000))
    debug = os.getenv("FLASK_ENV") == "development"
    app.run(host="0.0.0.0", port=port, debug=debug)