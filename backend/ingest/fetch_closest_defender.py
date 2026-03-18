import os, sys, time, argparse, math
from datetime import datetime
from dotenv import load_dotenv
import psycopg2
from nba_api.stats.endpoints import LeagueDashPlayerPtShot

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
SEASON      = os.getenv("NBA_SEASON", "2024-25")
SEASON_TYPE = os.getenv("NBA_SEASON_TYPE", "Regular Season")
DELAY       = 2.0

if not DATABASE_URL:
    print("ERROR: DATABASE_URL not set.")
    sys.exit(1)

def sf(val):
    if val is None:
        return None
    try:
        if isinstance(val, float) and math.isnan(val):
            return None
        return float(val)
    except:
        return None

def fetch_bracket(season, season_type, close_def_range, label):
    print("  Fetching " + label + "...", end=" ", flush=True)
    try:
        time.sleep(DELAY)
        ep = LeagueDashPlayerPtShot(
            season=season,
            season_type_all_star=season_type,
            per_mode_simple="Totals",
            close_def_dist_range_nullable=close_def_range,
        )
        dfs = ep.get_data_frames()
        if dfs and len(dfs[0]) > 0:
            print("OK (" + str(len(dfs[0])) + " rows)")
            return dfs[0]
        print("empty")
        return None
    except Exception as e:
        print("FAILED: " + str(e))
        return None

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", default=SEASON)
    parser.add_argument("--season-type", default=SEASON_TYPE)
    args = parser.parse_args()
    season = args.season
    season_type = args.season_type

    print("\nThe Impact Board - Closest Defender Fetch")
    print("Season: " + season + " | Type: " + season_type)
    print("Started: " + datetime.now().strftime("%Y-%m-%d %H:%M") + "\n")

    brackets = [
        ("0-2 Feet - Very Tight", "vt", "Very Tight (0-2ft)"),
        ("2-4 Feet - Tight",      "tg", "Tight (2-4ft)"),
        ("4-6 Feet - Open",       "op", "Open (4-6ft)"),
        ("6+ Feet - Wide Open",   "wo", "Wide Open (6ft+)"),
    ]

    bracket_data = {}
    for range_str, suffix, label in brackets:
        df = fetch_bracket(season, season_type, range_str, label)
        if df is not None:
            bracket_data[suffix] = df

    if not bracket_data:
        print("\nERROR: No data fetched.")
        sys.exit(1)

    updates = {}
    for suffix, df in bracket_data.items():
        for _, row in df.iterrows():
            pid = row.get("PLAYER_ID")
            if pid is None:
                continue
            try:
                pid = int(pid)
            except:
                continue
            if pid not in updates:
                updates[pid] = {}
            updates[pid]["cd_fga_"  + suffix] = sf(row.get("FGA"))
            updates[pid]["cd_fgm_"  + suffix] = sf(row.get("FGM"))
            updates[pid]["cd_fg3a_" + suffix] = sf(row.get("FG3A"))
            updates[pid]["cd_fg3m_" + suffix] = sf(row.get("FG3M"))

    print("\n  " + str(len(updates)) + " players with closest-defender data")

    all_cd_cols = [
        "cd_fga_vt", "cd_fgm_vt", "cd_fg3a_vt", "cd_fg3m_vt",
        "cd_fga_tg", "cd_fgm_tg", "cd_fg3a_tg", "cd_fg3m_tg",
        "cd_fga_op", "cd_fgm_op", "cd_fg3a_op", "cd_fg3m_op",
        "cd_fga_wo", "cd_fgm_wo", "cd_fg3a_wo", "cd_fg3m_wo",
    ]

    set_clause = ", ".join(col + " = %s" for col in all_cd_cols)
    sql = "UPDATE player_seasons SET " + set_clause + ", updated_at = NOW() WHERE player_id = %s AND season = %s AND season_type = %s AND league = 'NBA'"

    db_rows = []
    for pid, vals in updates.items():
        row_vals = [vals.get(col) for col in all_cd_cols]
        row_vals += [pid, season, season_type]
        db_rows.append(tuple(row_vals))

    conn = psycopg2.connect(DATABASE_URL)
    cur  = conn.cursor()
    cur.executemany(sql, db_rows)
    conn.commit()
    updated = cur.rowcount
    cur.close()
    conn.close()

    print("  Updated " + str(updated) + " player_seasons rows")
    print("\nFinished: " + datetime.now().strftime("%Y-%m-%d %H:%M"))
    print("\nNext step: python backend/ingest/compute_metrics.py --season " + season)

if __name__ == "__main__":
    main()