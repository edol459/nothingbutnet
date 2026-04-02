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

"""
ADD THESE ROUTES TO backend/server.py
Paste them before the `if __name__ == "__main__":` block.
"""

import requests as _requests
from datetime import datetime as _dt

# Headers for NBA CDN (live data — boxscore/pbp proxy)
_CDN_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Referer":    "https://www.nba.com/",
    "Origin":     "https://www.nba.com",
    "Accept":     "application/json, text/plain, */*",
}


def _norm_cdn_game(g):
    """Normalise a CDN scoreboard game object to our flat schema."""
    away   = g.get("awayTeam", {})
    home   = g.get("homeTeam", {})
    status = g.get("gameStatus", 1)
    return {
        "gameId":         g.get("gameId", ""),
        "gameStatus":     status,           # 1=upcoming, 2=live, 3=final
        "gameStatusText": g.get("gameStatusText", ""),
        "period":         g.get("period", 0),
        "gameClock":      g.get("gameClock", ""),
        "gameTimeUTC":    g.get("gameTimeUTC", ""),
        "away": {
            "abbr":   away.get("teamTricode", ""),
            "name":   away.get("teamName", ""),
            "score":  int(away.get("score", 0) or 0),
            "wins":   away.get("wins"),
            "losses": away.get("losses"),
        },
        "home": {
            "abbr":   home.get("teamTricode", ""),
            "name":   home.get("teamName", ""),
            "score":  int(home.get("score", 0) or 0),
            "wins":   home.get("wins"),
            "losses": home.get("losses"),
        },
    }


# ── /api/scoreboard?date=YYYY-MM-DD ──────────────────────────────
@app.route("/api/scoreboard")
def get_scoreboard():
    """
    No ?date  → today via NBA live CDN (fast, always current).
    ?date=YYYY-MM-DD → historical via nba_api ScoreboardV2
                       (same library powering the stats pipeline — handles auth).
    """
    date = request.args.get("date", "").strip()

    if not date:
        # ── Today: live CDN ───────────────────────────────────────
        try:
            url  = "https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json"
            resp = _requests.get(url, headers=_CDN_HEADERS, timeout=12)
            resp.raise_for_status()
            data       = resp.json()
            raw_games  = data.get("scoreboard", {}).get("games", [])
            games      = [_norm_cdn_game(g) for g in raw_games]
            board_date = data.get("scoreboard", {}).get("gameDate", "")
            return jsonify({"games": games, "date": board_date})
        except Exception as e:
            return jsonify({"error": str(e), "games": [], "date": ""}), 200

    # ── Historical: get game IDs from ScoreboardV2, scores from CDN boxscore ──
    # ScoreboardV2 gives us the game list but PTS is always null for past games.
    # The CDN boxscore (same source the game detail page uses) has the final scores.
    try:
        from nba_api.stats.endpoints import scoreboardv2

        dt = _dt.strptime(date, "%Y-%m-%d")
        board = scoreboardv2.ScoreboardV2(
            game_date=dt.strftime("%m/%d/%Y"),
            league_id="00",
            day_offset=0,
        )
        gh_df = board.game_header.get_data_frame()

        if gh_df.empty:
            return jsonify({"games": [], "date": date})

        # Fetch CDN boxscore for each game to get final scores
        games = []
        for _, row in gh_df.iterrows():
            gid = str(row.get("GAME_ID", ""))
            if not gid:
                continue

            # Try CDN boxscore for this game
            away_abbr = home_abbr = ""
            away_score = home_score = 0
            away_wins = away_losses = home_wins = home_losses = None

            try:
                box_url = f"https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{gid}.json"
                box_resp = _requests.get(box_url, headers=_CDN_HEADERS, timeout=8)
                if box_resp.status_code == 200:
                    box = box_resp.json().get("game", {})
                    away = box.get("awayTeam", {})
                    home = box.get("homeTeam", {})
                    away_abbr  = away.get("teamTricode", "")
                    home_abbr  = home.get("teamTricode", "")
                    away_score = int(away.get("score", 0) or 0)
                    home_score = int(home.get("score", 0) or 0)
                    away_wins  = away.get("wins")
                    away_losses = away.get("losses")
                    home_wins  = home.get("wins")
                    home_losses = home.get("losses")
                else:
                    # Boxscore not on CDN — derive team abbrs from GAMECODE
                    # GAMECODE format: "20260329/LACMIL"
                    code = str(row.get("GAMECODE", "") or "")
                    if "/" in code:
                        teams = code.split("/")[1]
                        # visitor is first 3 chars, home is last 3
                        away_abbr = teams[:3] if len(teams) >= 6 else ""
                        home_abbr = teams[3:6] if len(teams) >= 6 else ""
            except Exception:
                pass

            # All past-date games are final
            games.append({
                "gameId":         gid,
                "gameStatus":     3,
                "gameStatusText": "Final",
                "period":         0,
                "gameClock":      "",
                "gameTimeUTC":    "",
                "away": {"abbr": away_abbr, "score": away_score, "wins": away_wins, "losses": away_losses},
                "home": {"abbr": home_abbr, "score": home_score, "wins": home_wins, "losses": home_losses},
            })

        return jsonify({"games": games, "date": date})

    except Exception as e:
        return jsonify({"error": str(e), "games": [], "date": date}), 200


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
        # Get game IDs for this historical date
        try:
            from nba_api.stats.endpoints import scoreboardv2
            dt = _dt.strptime(date, "%Y-%m-%d")
            board  = scoreboardv2.ScoreboardV2(
                game_date=dt.strftime("%m/%d/%Y"),
                league_id="00",
                day_offset=0,
            )
            gh_df = board.game_header.get_data_frame()
            raw_games  = [{"gameId": str(r["GAME_ID"]), "gamecode": str(r.get("GAMECODE",""))}
                          for _, r in gh_df.iterrows() if r.get("GAME_ID")]
            actual_date = date
        except Exception as e:
            return jsonify({"error": str(e), "players": [], "date": date}), 200

    # For today, raw_games are CDN dicts; for historical they're our dicts
    # Normalise to just game_id strings
    def get_gid(g):
        return g.get("gameId") or g.get("GAME_ID") or ""

    game_ids = [get_gid(g) for g in raw_games if get_gid(g)]
    if not game_ids:
        return jsonify({"players": [], "date": actual_date})

    # Fetch boxscore for each game and collect player lines
    all_players = []
    for gid in game_ids:
        try:
            box_url  = f"https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{gid}.json"
            box_resp = _requests.get(box_url, headers=_CDN_HEADERS, timeout=8)
            if box_resp.status_code != 200:
                continue
            box  = box_resp.json().get("game", {})
            away = box.get("awayTeam", {})
            home = box.get("homeTeam", {})
            away_abbr = away.get("teamTricode", "")
            home_abbr = home.get("teamTricode", "")
            matchup   = f"{away_abbr} @ {home_abbr}"

            for team, abbr in [(away, away_abbr), (home, home_abbr)]:
                for p in team.get("players", []):
                    s   = p.get("statistics", {})
                    min_str = s.get("minutes", "PT0M0.00S")
                    # Skip DNP / zero-minute players
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
                        "pts":       pts,
                        "reb":       reb,
                        "ast":       ast,
                        "total":     pts + reb + ast,
                    })
        except Exception:
            continue

    # Sort by total desc, take top 5
    all_players.sort(key=lambda x: x["total"], reverse=True)
    top5 = all_players[:5]

    return jsonify({"players": top5, "date": actual_date})


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
              AND ps.season = '2025-26'
              AND ps.season_type = 'Regular Season'
              AND ps.gp >= 5
        """, (abbr,))
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
    Returns last 5 head-to-head games between two teams using nba_api.
    Uses TeamGameLog for the away team and filters for games vs the home team.
    """
    away = away.upper()
    home = home.upper()

    # Build a reverse lookup: abbr → team_id
    _TEAM_IDS = {
        "ATL":1610612737,"BOS":1610612738,"BKN":1610612751,"CHA":1610612766,
        "CHI":1610612741,"CLE":1610612739,"DAL":1610612742,"DEN":1610612743,
        "DET":1610612765,"GSW":1610612744,"HOU":1610612745,"IND":1610612754,
        "LAC":1610612746,"LAL":1610612747,"MEM":1610612763,"MIA":1610612748,
        "MIL":1610612749,"MIN":1610612750,"NOP":1610612740,"NYK":1610612752,
        "OKC":1610612760,"ORL":1610612753,"PHI":1610612755,"PHX":1610612756,
        "POR":1610612757,"SAC":1610612758,"SAS":1610612759,"TOR":1610612761,
        "UTA":1610612762,"WAS":1610612764,
    }

    away_id = _TEAM_IDS.get(away)
    home_id = _TEAM_IDS.get(home)
    if not away_id or not home_id:
        return jsonify({"games": [], "error": "unknown team abbreviation"})

    try:
        from nba_api.stats.endpoints import teamgamelog

        # Pull last 2 seasons to get enough H2H games
        games_out = []
        for season in ["2025-26", "2024-25"]:
            if len(games_out) >= 5:
                break
            try:
                log = teamgamelog.TeamGameLog(
                    team_id=away_id,
                    season=season,
                    season_type_all_star="Regular Season",
                )
                df = log.get_data_frames()[0]
            except Exception:
                continue

            # Filter for matchups vs home team
            # MATCHUP looks like "ATL vs. BOS" or "ATL @ BOS"
            mask = df["MATCHUP"].str.contains(home, na=False)
            filtered = df[mask].head(5 - len(games_out))

            for _, row in filtered.iterrows():
                matchup = str(row.get("MATCHUP", ""))
                is_home = "vs." in matchup  # away team was home if "vs."
                away_abbr = away if not is_home else home
                home_abbr = home if not is_home else away
                away_pts  = int(row.get("PTS", 0) or 0)
                # We only have the away team's score from TeamGameLog
                # Derive home score from win/loss + point diff if available
                # PTS = points scored by the logged team
                # Use WL and plus_minus to get opponent score
                plus_minus = int(row.get("PLUS_MINUS", 0) or 0)
                opp_pts = away_pts - plus_minus  # opponent scored away_pts - plus_minus

                if is_home:
                    # away team was actually playing at home
                    final_away_pts = opp_pts
                    final_home_pts = away_pts
                else:
                    final_away_pts = away_pts
                    final_home_pts = opp_pts

                game_date = str(row.get("GAME_DATE", ""))
                # Convert "DEC 25, 2025" → "2025-12-25"
                try:
                    from datetime import datetime as _dt2
                    parsed = _dt2.strptime(game_date, "%b %d, %Y")
                    game_date = parsed.strftime("%Y-%m-%d")
                except Exception:
                    pass

                games_out.append({
                    "game_id":   str(row.get("Game_ID", "")),
                    "date":      game_date,
                    "away_abbr": away_abbr,
                    "home_abbr": home_abbr,
                    "away_pts":  final_away_pts,
                    "home_pts":  final_home_pts,
                })

        return jsonify({"games": games_out[:5], "away": away, "home": home})

    except Exception as e:
        return jsonify({"games": [], "error": str(e)}), 200


# ── Serve preview.html ────────────────────────────────────────────
@app.route("/preview")
@app.route("/preview.html")
def preview_page():
    return app.send_static_file("preview.html")


# ── /api/live/boxscore/<game_id> ──────────────────────────────────
@app.route("/api/live/boxscore/<game_id>")
def get_live_boxscore(game_id):
    """Proxy NBA CDN live boxscore."""
    try:
        url = f"https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json"
        resp = _requests.get(url, headers=_CDN_HEADERS, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        return jsonify(data.get("game", data))
    except Exception as e:
        return jsonify({"error": str(e)}), 404


# ── /api/live/pbp/<game_id> ───────────────────────────────────────
@app.route("/api/live/pbp/<game_id>")
def get_live_pbp(game_id):
    """Proxy NBA CDN live play-by-play."""
    try:
        url = f"https://cdn.nba.com/static/json/liveData/playbyplay/playbyplay_{game_id}.json"
        resp = _requests.get(url, headers=_CDN_HEADERS, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        return jsonify(data.get("game", data))
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

# ── Serve stats.html (renamed from index.html) ────────────────────
# Add this AFTER renaming your old index.html → stats.html
@app.route("/stats.html")
@app.route("/stats")
def stats_page():
    return app.send_static_file("stats.html")

@app.route("/api/onoff")
def get_onoff():
    import time, math
    import pandas as pd
    from nba_api.stats.endpoints import LeagueDashLineups, CommonTeamRoster

    _TEAM_IDS = {
        "ATL":1610612737,"BOS":1610612738,"BKN":1610612751,"CHA":1610612766,
        "CHI":1610612741,"CLE":1610612739,"DAL":1610612742,"DEN":1610612743,
        "DET":1610612765,"GSW":1610612744,"HOU":1610612745,"IND":1610612754,
        "LAC":1610612746,"LAL":1610612747,"MEM":1610612763,"MIA":1610612748,
        "MIL":1610612749,"MIN":1610612750,"NOP":1610612740,"NYK":1610612752,
        "OKC":1610612760,"ORL":1610612753,"PHI":1610612755,"PHX":1610612756,
        "POR":1610612757,"SAC":1610612758,"SAS":1610612759,"TOR":1610612761,
        "UTA":1610612762,"WAS":1610612764,
    }

    team_abbr = request.args.get("team", "").upper()
    season    = request.args.get("season", "2025-26")

    if not team_abbr:
        return jsonify({"error": "team param required"}), 400
    team_id = _TEAM_IDS.get(team_abbr)
    if not team_id:
        return jsonify({"error": f"Unknown team: {team_abbr}"}), 404

    def safe(v):
        try:
            f = float(v)
            return None if math.isnan(f) or math.isinf(f) else round(f, 1)
        except Exception:
            return None

    try:
        # Roster
        roster = []
        try:
            r_df = CommonTeamRoster(team_id=team_id, season=season).get_data_frames()[0]
            time.sleep(0.6)
            for _, row in r_df.iterrows():
                roster.append({
                    "player_id":   str(int(row["PLAYER_ID"])),
                    "player_name": str(row["PLAYER"]),
                    "number":      str(row.get("NUM", "")),
                    "position":    str(row.get("POSITION", "")),
                })
        except Exception:
            pass

        # Fetch only 5-man lineups — one call, covers everything
        ep = LeagueDashLineups(
            team_id_nullable=team_id,
            group_quantity=5,
            season=season,
            season_type_all_star="Regular Season",
            measure_type_detailed_defense="Advanced",
            per_mode_detailed="Totals",   # Totals so we can weight-average ratings
            timeout=60,
        )
        time.sleep(0.6)
        df = ep.get_data_frames()[0]

        if df.empty:
            return jsonify({"error": "No lineup data returned"}), 502

        # Parse player IDs from GROUP_ID ("-pid1-pid2-pid3-pid4-pid5-")
        def parse_ids(gid):
            return set(p for p in str(gid).split("-") if p.strip())

        lineups = []
        for _, row in df.iterrows():
            pids = parse_ids(row["GROUP_ID"])
            mins = float(row["MIN"]) if pd.notna(row.get("MIN")) else 0.0
            # MIN in Totals mode is already in minutes (decimal)
            lineups.append({
                "pids":  list(pids),
                "min":   mins,
                "ortg":  safe(row.get("OFF_RATING")),
                "drtg":  safe(row.get("DEF_RATING")),
                "net":   safe(row.get("NET_RATING")),
                "gp":    int(row["GP"]) if pd.notna(row.get("GP")) else 0,
            })

        return jsonify({
            "team":     team_abbr,
            "season":   season,
            "roster":   roster,
            "lineups":  lineups,
        })

    except Exception as exc:
        import traceback
        return jsonify({"error": str(exc), "trace": traceback.format_exc()}), 500

# ── Serve onoff.html ──────────────────────────────────────────
@app.route("/onoff")
@app.route("/onoff.html")
def onoff_page():
    return app.send_static_file("onoff.html")

# ── Run ───────────────────────────────────────────────────────

if __name__ == "__main__":
    port  = int(os.getenv("PORT", 5000))
    debug = os.getenv("FLASK_ENV") == "development"
    app.run(host="0.0.0.0", port=port, debug=debug)