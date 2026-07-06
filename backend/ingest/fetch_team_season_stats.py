"""
Build team_season_stats — per-team, per-season averages (Base + Advanced).

Feeds the team pages' stat bar (REB/AST/STL/BLK/TOV, FG%/3P%/FT%, and
ORTG/DRTG/NetRtg/Pace). PPG/OPP/DIFF/record are derived from the games table
in the API, so this script is only needed for the richer box + advanced stats.

Source: NBA stats API LeagueDashTeamStats (Base + Advanced, PerGame).
Blocked from Railway — run on a residential IP (Windows local_daily), same as
the player-season stats.

Usage:
  python backend/ingest/fetch_team_season_stats.py                 # both leagues, all seasons
  python backend/ingest/fetch_team_season_stats.py --league nba
  python backend/ingest/fetch_team_season_stats.py --league wnba --seasons 2025 2026
  python backend/ingest/fetch_team_season_stats.py --seasons 2024-25 2025-26
  python backend/ingest/fetch_team_season_stats.py --dry-run
"""

import os, time, argparse, concurrent.futures
from dotenv import load_dotenv
import psycopg2, psycopg2.extras

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

parser = argparse.ArgumentParser()
parser.add_argument("--league",  choices=["nba", "wnba", "both"], default="both")
parser.add_argument("--seasons",  nargs="*", help="e.g. 2024-25 2025-26  (or 2025 2026 for WNBA)")
parser.add_argument("--current",  action="store_true", help="only the latest season per league (for daily refresh)")
parser.add_argument("--delay",    type=float, default=1.5)
parser.add_argument("--dry-run",  action="store_true")
# Tolerate the shared daily-runner flags without erroring; --current is what
# the daily pipeline actually uses.
parser.add_argument("--season",       help=argparse.SUPPRESS)
parser.add_argument("--season-type",  help=argparse.SUPPRESS)
args = parser.parse_args()

# Current NBA season we have completed data for is 2025-26; WNBA is 2026.
NBA_SEASONS  = [f"{y}-{str(y+1)[-2:]}" for y in range(1996, 2026)]
WNBA_SEASONS = [str(y) for y in range(2018, 2027)]

# WNBA team id -> canonical games abbr (reverse of the CDN logo id map).
WNBA_ID_TO_ABBR = {
    1611661330: "ATL", 1611661329: "CHI", 1611661323: "CON", 1611661321: "DAL",
    1611661331: "GS",  1611661325: "IND", 1611661320: "LA",  1611661319: "LV",
    1611661324: "MIN", 1611661313: "NY",  1611661317: "PHX", 1611661327: "POR",
    1611661328: "SEA", 1611661332: "TOR", 1611661322: "WSH",
}


def get_conn():
    c = psycopg2.connect(DATABASE_URL)
    c.cursor_factory = psycopg2.extras.RealDictCursor
    return c


def ensure_table(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS team_season_stats (
            team_abbr    TEXT NOT NULL,
            team_name    TEXT,
            league       TEXT NOT NULL DEFAULT 'nba',
            season       TEXT NOT NULL,
            season_type  TEXT NOT NULL DEFAULT 'Regular Season',
            gp           INTEGER, wins INTEGER, losses INTEGER,
            pts          REAL, reb REAL, ast REAL, stl REAL, blk REAL, tov REAL,
            oreb         REAL, dreb REAL, pf REAL, plus_minus REAL,
            fg_pct       REAL, fg3_pct REAL, ft_pct REAL,
            off_rating   REAL, def_rating REAL, net_rating REAL, pace REAL,
            PRIMARY KEY (team_abbr, league, season, season_type)
        )
    """)
    # ADD COLUMNs for tables created before wins/losses existed.
    cur.execute("ALTER TABLE team_season_stats ADD COLUMN IF NOT EXISTS wins INTEGER")
    cur.execute("ALTER TABLE team_season_stats ADD COLUMN IF NOT EXISTS losses INTEGER")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_tss_lookup ON team_season_stats(team_abbr, league, season)")
    conn.commit(); cur.close()
    print("✅ team_season_stats table ready")


def _nba_abbr_map():
    from nba_api.stats.static import teams
    return {t["id"]: t["abbreviation"] for t in teams.get_teams()}


def _fetch_measure(league_id, season, measure, delay):
    """LeagueDashTeamStats for one measure type, PerGame, Regular Season."""
    from nba_api.stats.endpoints import leaguedashteamstats
    time.sleep(delay)

    def _go():
        return leaguedashteamstats.LeagueDashTeamStats(
            season=season,
            season_type_all_star="Regular Season",
            per_mode_detailed="PerGame",
            measure_type_detailed_defense=measure,
            league_id_nullable=league_id,
            timeout=45,
        )

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
        s = ex.submit(_go).result(timeout=60)
    return s.get_data_frames()[0]


def _f(row, key):
    try:
        v = row.get(key)
        return float(v) if v is not None else None
    except Exception:
        return None


def run_season(conn, league, season, delay):
    league_id = "10" if league == "wnba" else "00"
    try:
        base = _fetch_measure(league_id, season, "Base", delay)
        adv  = _fetch_measure(league_id, season, "Advanced", delay)
    except concurrent.futures.TimeoutError:
        print(f"  ⚠ {league} {season}: timeout — skipping"); return 0
    except Exception as e:
        print(f"  ⚠ {league} {season}: {e}"); return 0

    adv_by_id = {int(r["TEAM_ID"]): r for _, r in adv.iterrows()}
    abbr_map  = _nba_abbr_map() if league == "nba" else WNBA_ID_TO_ABBR

    rows = []
    for _, r in base.iterrows():
        tid  = int(r["TEAM_ID"])
        abbr = abbr_map.get(tid)
        if not abbr:
            continue
        a = adv_by_id.get(tid, {})
        rows.append({
            "team_abbr": abbr, "team_name": r.get("TEAM_NAME"),
            "league": league, "season": season, "season_type": "Regular Season",
            "gp": int(r.get("GP") or 0),
            "wins": int(r["W"]) if r.get("W") is not None else None,
            "losses": int(r["L"]) if r.get("L") is not None else None,
            "pts": _f(r, "PTS"), "reb": _f(r, "REB"), "ast": _f(r, "AST"),
            "stl": _f(r, "STL"), "blk": _f(r, "BLK"), "tov": _f(r, "TOV"),
            "oreb": _f(r, "OREB"), "dreb": _f(r, "DREB"), "pf": _f(r, "PF"),
            "plus_minus": _f(r, "PLUS_MINUS"),
            "fg_pct": _f(r, "FG_PCT"), "fg3_pct": _f(r, "FG3_PCT"), "ft_pct": _f(r, "FT_PCT"),
            "off_rating": _f(a, "OFF_RATING"), "def_rating": _f(a, "DEF_RATING"),
            "net_rating": _f(a, "NET_RATING"), "pace": _f(a, "PACE"),
        })

    if args.dry_run:
        print(f"  {league} {season}: {len(rows)} teams (dry-run)")
        return len(rows)

    cur = conn.cursor()
    for d in rows:
        cur.execute("""
            INSERT INTO team_season_stats
              (team_abbr, team_name, league, season, season_type, gp, wins, losses,
               pts, reb, ast, stl, blk, tov, oreb, dreb, pf, plus_minus,
               fg_pct, fg3_pct, ft_pct, off_rating, def_rating, net_rating, pace)
            VALUES
              (%(team_abbr)s, %(team_name)s, %(league)s, %(season)s, %(season_type)s, %(gp)s, %(wins)s, %(losses)s,
               %(pts)s, %(reb)s, %(ast)s, %(stl)s, %(blk)s, %(tov)s, %(oreb)s, %(dreb)s, %(pf)s, %(plus_minus)s,
               %(fg_pct)s, %(fg3_pct)s, %(ft_pct)s, %(off_rating)s, %(def_rating)s, %(net_rating)s, %(pace)s)
            ON CONFLICT (team_abbr, league, season, season_type) DO UPDATE SET
              team_name=EXCLUDED.team_name, gp=EXCLUDED.gp,
              wins=EXCLUDED.wins, losses=EXCLUDED.losses,
              pts=EXCLUDED.pts, reb=EXCLUDED.reb, ast=EXCLUDED.ast, stl=EXCLUDED.stl,
              blk=EXCLUDED.blk, tov=EXCLUDED.tov, oreb=EXCLUDED.oreb, dreb=EXCLUDED.dreb,
              pf=EXCLUDED.pf, plus_minus=EXCLUDED.plus_minus,
              fg_pct=EXCLUDED.fg_pct, fg3_pct=EXCLUDED.fg3_pct, ft_pct=EXCLUDED.ft_pct,
              off_rating=EXCLUDED.off_rating, def_rating=EXCLUDED.def_rating,
              net_rating=EXCLUDED.net_rating, pace=EXCLUDED.pace
        """, d)
    conn.commit(); cur.close()
    print(f"  ✅ {league} {season}: {len(rows)} teams")
    return len(rows)


def main():
    conn = get_conn()
    ensure_table(conn)

    leagues = ["nba", "wnba"] if args.league == "both" else [args.league]
    total = 0
    for league in leagues:
        full = NBA_SEASONS if league == "nba" else WNBA_SEASONS
        if args.seasons:
            seasons = args.seasons
        elif args.current:
            seasons = [full[-1]]
        else:
            seasons = full
        print(f"\n── {league.upper()} ({len(seasons)} seasons) ──")
        for season in seasons:
            total += run_season(conn, league, season, args.delay)

    conn.close()
    print(f"\n{'(dry-run) ' if args.dry_run else ''}Done — {total} team-season rows.")


if __name__ == "__main__":
    main()
