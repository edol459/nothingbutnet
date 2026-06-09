"""
Ingest historical NBA player seasons into player_seasons.

Fetches only bio + base + advanced stats (tracking/synergy/hustle didn't
exist for most of this range). All advanced tracking columns stay NULL.

Usage:
  python backend/ingest/fetch_historical_seasons.py
  python backend/ingest/fetch_historical_seasons.py --start 1996-97 --end 2019-20
  python backend/ingest/fetch_historical_seasons.py --start 2015-16 --end 2015-16 --playoffs-only
"""

import os, sys, time, argparse
from dotenv import load_dotenv
import psycopg2

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("❌ DATABASE_URL not set"); sys.exit(1)

parser = argparse.ArgumentParser()
parser.add_argument("--start",         default="1996-97", help="First season e.g. 1996-97")
parser.add_argument("--end",           default="2019-20", help="Last season e.g. 2019-20")
parser.add_argument("--no-playoffs",   action="store_true", help="Skip playoffs")
parser.add_argument("--playoffs-only", action="store_true", help="Only playoffs")
parser.add_argument("--delay",         type=float, default=1.5, help="Seconds between API calls")
args = parser.parse_args()

DELAY = args.delay

def sleep(extra=0): time.sleep(DELAY + extra)
def get_conn():     return psycopg2.connect(DATABASE_URL)

def _safe_num(val):
    if val is None: return None
    try:
        import math; f = float(val)
        return None if math.isnan(f) else f
    except: return None

def _safe_int(val):
    f = _safe_num(val)
    return None if f is None else int(f)

def _get_df(ep):
    try:
        frames = ep.get_data_frames()
        return frames[0] if frames else None
    except: return None

def _idx(df):
    if df is None or len(df) == 0: return {}
    if "PLAYER_ID" not in df.columns: return {}
    if df["PLAYER_ID"].duplicated().any():
        df = df.drop_duplicates(subset=["PLAYER_ID"])
    return df.set_index("PLAYER_ID").to_dict(orient="index")

def _g(row, *cols):
    if row is None: return None
    for c in cols:
        if c in row: return _safe_num(row[c])
    return None

# ── Fetchers ──────────────────────────────────────────────────

def _fetch(label, fn, retries=3):
    print(f"  → {label}...", end=" ", flush=True)
    backoff = [0, 8, 20]  # extra seconds on each retry
    for attempt in range(retries):
        try:
            if attempt > 0:
                time.sleep(backoff[attempt])
            sleep()
            r = fn()
            if r is not None and len(r) > 0:
                print(f"✅ {len(r)} rows"); return r
            print("⚠️  empty"); return None
        except Exception as e:
            if attempt < retries - 1:
                print(f"⚠️  retry {attempt+2}/{retries} (wait {backoff[attempt+1]}s)...", end=" ", flush=True)
            else:
                print(f"❌ {e}")
    return None

def fetch_season_data(season, season_type):
    from nba_api.stats.endpoints import (
        LeagueDashPlayerStats, LeagueDashPlayerBioStats
    )

    totals = _fetch("Totals",  lambda: _get_df(LeagueDashPlayerStats(
        season=season, season_type_all_star=season_type,
        per_mode_detailed="Totals")))

    per_game = _fetch("PerGame", lambda: _get_df(LeagueDashPlayerStats(
        season=season, season_type_all_star=season_type,
        per_mode_detailed="PerGame")))

    bio = _fetch("Bio", lambda: _get_df(LeagueDashPlayerBioStats(
        season=season, season_type_all_star=season_type,
        per_mode_simple="PerGame")))

    # Advanced didn't exist pre-1997 but usually works from ~1997-98+
    adv = _fetch("Advanced", lambda: _get_df(LeagueDashPlayerStats(
        season=season, season_type_all_star=season_type,
        per_mode_detailed="PerGame",
        measure_type_detailed_defense="Advanced")))

    return totals, per_game, bio, adv

# ── Upserts ────────────────────────────────────────────────────

def upsert_players(conn, df, bio_df):
    source = bio_df if bio_df is not None else df
    if source is None: return
    cur = conn.cursor()

    def pg(p):
        if not p: return None
        m = {"PG":"G","SG":"G","G":"G","G-F":"GF","F-G":"GF",
             "GUARD-FORWARD":"GF","FORWARD-GUARD":"GF",
             "SF":"F","PF":"F","F":"F","FORWARD":"F",
             "F-C":"FC","C-F":"FC","FORWARD-CENTER":"FC","CENTER-FORWARD":"FC",
             "C":"C","CENTER":"C"}
        return m.get(str(p).upper())

    count = 0
    for _, row in source.iterrows():
        pid = int(row["PLAYER_ID"])
        pos = row.get("POSITION")
        height = None
        h_str = row.get("PLAYER_HEIGHT_FEET")
        if h_str and str(h_str) not in ("None", "nan", ""):
            try:
                parts = str(h_str).split("-")
                height = int(parts[0]) * 12 + int(parts[1])
            except: pass

        cur.execute("""
            INSERT INTO players (player_id, player_name, position, position_group,
                                 height_inches, weight, is_active, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, FALSE, NOW())
            ON CONFLICT (player_id) DO UPDATE SET
              player_name     = EXCLUDED.player_name,
              position        = COALESCE(players.position, EXCLUDED.position),
              position_group  = COALESCE(players.position_group, EXCLUDED.position_group),
              height_inches   = COALESCE(players.height_inches, EXCLUDED.height_inches),
              weight          = COALESCE(players.weight, EXCLUDED.weight)
        """, (pid, row["PLAYER_NAME"], pos, pg(pos), height,
              _safe_int(row.get("PLAYER_WEIGHT"))))
        count += 1

    conn.commit(); cur.close()
    print(f"  ✅ {count} players upserted")

def upsert_seasons(conn, rows):
    if not rows: return
    cur = conn.cursor()
    for row in rows:
        cur.execute("""
            INSERT INTO player_seasons
              (player_id, season, season_type,
               team_id, team_abbr, gp, min, min_per_game,
               pts, ast, reb, oreb, dreb, stl, blk, tov, pf, pfd,
               fgm, fga, fg_pct, fg3m, fg3a, fg3_pct,
               ftm, fta, ft_pct, plus_minus,
               off_rating, def_rating, net_rating,
               ast_pct, ast_to, oreb_pct, dreb_pct, reb_pct,
               efg_pct, ts_pct, usg_pct, pie,
               updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,NOW())
            ON CONFLICT (player_id, season, season_type) DO UPDATE SET
              team_id=EXCLUDED.team_id, team_abbr=EXCLUDED.team_abbr,
              gp=EXCLUDED.gp, min=EXCLUDED.min, min_per_game=EXCLUDED.min_per_game,
              pts=EXCLUDED.pts, ast=EXCLUDED.ast, reb=EXCLUDED.reb,
              oreb=EXCLUDED.oreb, dreb=EXCLUDED.dreb, stl=EXCLUDED.stl, blk=EXCLUDED.blk,
              tov=EXCLUDED.tov, pf=EXCLUDED.pf, pfd=EXCLUDED.pfd,
              fgm=EXCLUDED.fgm, fga=EXCLUDED.fga, fg_pct=EXCLUDED.fg_pct,
              fg3m=EXCLUDED.fg3m, fg3a=EXCLUDED.fg3a, fg3_pct=EXCLUDED.fg3_pct,
              ftm=EXCLUDED.ftm, fta=EXCLUDED.fta, ft_pct=EXCLUDED.ft_pct,
              plus_minus=EXCLUDED.plus_minus,
              off_rating=COALESCE(EXCLUDED.off_rating, player_seasons.off_rating),
              def_rating=COALESCE(EXCLUDED.def_rating, player_seasons.def_rating),
              net_rating=COALESCE(EXCLUDED.net_rating, player_seasons.net_rating),
              ast_pct=COALESCE(EXCLUDED.ast_pct, player_seasons.ast_pct),
              ast_to=COALESCE(EXCLUDED.ast_to, player_seasons.ast_to),
              oreb_pct=COALESCE(EXCLUDED.oreb_pct, player_seasons.oreb_pct),
              dreb_pct=COALESCE(EXCLUDED.dreb_pct, player_seasons.dreb_pct),
              reb_pct=COALESCE(EXCLUDED.reb_pct, player_seasons.reb_pct),
              efg_pct=COALESCE(EXCLUDED.efg_pct, player_seasons.efg_pct),
              ts_pct=COALESCE(EXCLUDED.ts_pct, player_seasons.ts_pct),
              usg_pct=COALESCE(EXCLUDED.usg_pct, player_seasons.usg_pct),
              pie=COALESCE(EXCLUDED.pie, player_seasons.pie),
              updated_at=NOW()
        """, (row["player_id"], row["season"], row["season_type"],
              row.get("team_id"), row.get("team_abbr"),
              row.get("gp"), row.get("min"), row.get("min_per_game"),
              row.get("pts"), row.get("ast"), row.get("reb"),
              row.get("oreb"), row.get("dreb"), row.get("stl"), row.get("blk"),
              row.get("tov"), row.get("pf"), row.get("pfd"),
              row.get("fgm"), row.get("fga"), row.get("fg_pct"),
              row.get("fg3m"), row.get("fg3a"), row.get("fg3_pct"),
              row.get("ftm"), row.get("fta"), row.get("ft_pct"),
              row.get("plus_minus"),
              row.get("off_rating"), row.get("def_rating"), row.get("net_rating"),
              row.get("ast_pct"), row.get("ast_to"),
              row.get("oreb_pct"), row.get("dreb_pct"), row.get("reb_pct"),
              row.get("efg_pct"), row.get("ts_pct"), row.get("usg_pct"), row.get("pie")))

    conn.commit(); cur.close()
    print(f"  ✅ {len(rows)} season rows upserted")

def build_row(pid, season, season_type, totals_row, pg_row, adv_row):
    g = _g
    return dict(
        player_id=pid, season=season, season_type=season_type,
        team_id=_safe_int(g(totals_row, "TEAM_ID")),
        team_abbr=totals_row.get("TEAM_ABBREVIATION") if totals_row else None,
        gp=_safe_int(g(totals_row, "GP", "G")),
        min=g(totals_row, "MIN"),
        min_per_game=g(pg_row, "MIN"),
        pts=g(pg_row, "PTS"), ast=g(pg_row, "AST"), reb=g(pg_row, "REB"),
        oreb=g(pg_row, "OREB"), dreb=g(pg_row, "DREB"),
        stl=g(pg_row, "STL"), blk=g(pg_row, "BLK"),
        tov=g(pg_row, "TOV"), pf=g(pg_row, "PF"), pfd=g(pg_row, "PFD"),
        fgm=g(pg_row, "FGM"), fga=g(pg_row, "FGA"), fg_pct=g(pg_row, "FG_PCT"),
        fg3m=g(pg_row, "FG3M"), fg3a=g(pg_row, "FG3A"), fg3_pct=g(pg_row, "FG3_PCT"),
        ftm=g(pg_row, "FTM"), fta=g(pg_row, "FTA"), ft_pct=g(pg_row, "FT_PCT"),
        plus_minus=g(pg_row, "PLUS_MINUS"),
        off_rating=g(adv_row, "OFF_RATING"), def_rating=g(adv_row, "DEF_RATING"),
        net_rating=g(adv_row, "NET_RATING"), ast_pct=g(adv_row, "AST_PCT"),
        ast_to=g(adv_row, "AST_TO"), oreb_pct=g(adv_row, "OREB_PCT"),
        dreb_pct=g(adv_row, "DREB_PCT"), reb_pct=g(adv_row, "REB_PCT"),
        efg_pct=g(adv_row, "EFG_PCT"), ts_pct=g(adv_row, "TS_PCT"),
        usg_pct=g(adv_row, "USG_PCT"), pie=g(adv_row, "PIE"),
    )

# ── Season generator ─────────────────────────────────────────

def parse_start_year(s):
    return int(s.split("-")[0])

def season_str(start_year):
    return f"{start_year}-{str(start_year + 1)[-2:]}"

def seasons_between(start, end):
    y = parse_start_year(start)
    end_y = parse_start_year(end)
    result = []
    while y <= end_y:
        result.append(season_str(y))
        y += 1
    return result

# ── Main ──────────────────────────────────────────────────────

def process_season(season, season_type, conn):
    print(f"\n{'─'*60}")
    print(f"  {season}  {season_type}")
    print(f"{'─'*60}")

    totals_df, pg_df, bio_df, adv_df = fetch_season_data(season, season_type)

    if totals_df is None and pg_df is None:
        print("  ⚠️  No base data — skipping")
        return

    upsert_players(conn, totals_df if totals_df is not None else pg_df, bio_df)

    source_df = pg_df if pg_df is not None else totals_df
    totals_map = _idx(totals_df)
    pg_map     = _idx(pg_df)
    adv_map    = _idx(adv_df)

    rows = []
    for _, row in source_df.iterrows():
        pid = int(row["PLAYER_ID"])
        rows.append(build_row(
            pid=pid, season=season, season_type=season_type,
            totals_row=totals_map.get(pid),
            pg_row=pg_map.get(pid),
            adv_row=adv_map.get(pid),
        ))

    upsert_seasons(conn, rows)

def run():
    seasons = seasons_between(args.start, args.end)
    season_types = []
    if not args.playoffs_only:
        season_types.append("Regular Season")
    if not args.no_playoffs:
        season_types.append("Playoffs")

    total = len(seasons) * len(season_types)
    print(f"\n🏀 Historical NBA ingest — {args.start} → {args.end}")
    print(f"   {len(seasons)} seasons × {len(season_types)} types = {total} batches")
    print(f"   Estimated time: ~{total * 4 * DELAY / 60:.0f} min")

    conn = get_conn()
    done = 0
    for season in seasons:
        for st in season_types:
            try:
                process_season(season, st, conn)
                done += 1
                print(f"  [{done}/{total}] complete")
                if done < total:
                    print(f"  Pausing 6s before next batch...")
                    time.sleep(6)
            except Exception as e:
                print(f"  ❌ {season} {st}: {e}")
    conn.close()
    print(f"\n✅ Done. {done}/{total} batches ingested.")

if __name__ == "__main__":
    run()
