"""
ydkball — External Metrics Fetcher
==========================================
python backend/ingest/fetch_external.py

Fetches third-party impact metrics and stores them in player_seasons.
Matches players by nba_id where available, name otherwise.

Metrics:
  - LEBRON     (Box BBI via fanspo.com) — overall impact, offense, defense, WAR
  - DARKO DPM  (darko.app API)          — overall, offense, defense, box
  - net_pts100 (ESPN via S3)            — luck-adjusted on/off net rating

Run after fetch_stats.py:
    python backend/ingest/fetch_external.py
    python backend/ingest/fetch_external.py --season 2025-26
"""

import os, sys, time, math, argparse, requests
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("❌ DATABASE_URL not set."); sys.exit(1)

parser = argparse.ArgumentParser()
parser.add_argument("--season",      default=os.getenv("NBA_SEASON", "2025-26"))
parser.add_argument("--season-type", default=os.getenv("NBA_SEASON_TYPE", "Regular Season"))
args = parser.parse_args()
SEASON, SEASON_TYPE = args.season, args.season_type

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "application/json",
}

def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)

def safe_float(val):
    if val is None: return None
    try:
        f = float(val)
        return None if math.isnan(f) else f
    except: return None


# ── LEBRON ────────────────────────────────────────────────────
def fetch_lebron(season):
    """
    Fetches LEBRON from fanspo.com BBI Role Explorer API.
    Matches on nba_id directly.
    """
    api_year = str(int(season.split("-")[0]) + 1)
    url = "https://fanspo.com/bbi-role-explorer/api/lebron_dashboard_data"
    payload = {
        "seasons": [api_year], "positions": [], "offensiveArchetypes": [],
        "defensiveRoles": [], "playerRoles": [], "teams": [],
        "seasonView": "average", "minMinutes": 200, "minMpg": 10,
    }
    print(f"  → LEBRON (fanspo.com, season {api_year})...", end=" ", flush=True)
    try:
        time.sleep(1.5)
        resp = requests.post(
            url,
            headers={**HEADERS, "Content-Type": "application/json", "Referer": "https://fanspo.com/"},
            json=payload,
            timeout=30,
        )
        resp.raise_for_status()
        players = resp.json().get("players", [])
        print(f"✅ {len(players)} players")
        return players
    except Exception as e:
        print(f"❌ {e}")
        return []

def write_lebron(players, season, season_type):
    if not players: return
    conn = get_conn(); cur = conn.cursor()
    for col, typ in [("lebron", "REAL"), ("o_lebron", "REAL"), ("d_lebron", "REAL"), ("war", "REAL")]:
        try:
            cur.execute(f"ALTER TABLE player_seasons ADD COLUMN IF NOT EXISTS {col} {typ}")
            conn.commit()
        except: conn.rollback()

    updated = 0
    for p in players:
        nba_id = p.get("nba_id")
        if not nba_id: continue
        try: nba_id = int(nba_id)
        except (ValueError, TypeError): continue

        cur.execute("""
            UPDATE player_seasons SET
                lebron   = %s,
                o_lebron = %s,
                d_lebron = %s,
                war      = %s,
                updated_at = NOW()
            WHERE player_id = %s AND season = %s AND season_type = %s
        """, (
            safe_float(p.get("LEBRON")),
            safe_float(p.get("O-LEBRON")),
            safe_float(p.get("D-LEBRON")),
            safe_float(p.get("WAR")),
            nba_id, season, season_type,
        ))
        if cur.rowcount: updated += 1
    conn.commit(); cur.close(); conn.close()
    print(f"  ✅ LEBRON: {updated} players updated")


# ── DARKO ─────────────────────────────────────────────────────
def fetch_darko():
    """
    Fetches DARKO DPM from darko.app's active-players API.
    Matches on nba_id directly — no name matching needed.
    """
    url = "https://www.darko.app/api/active-players"
    print(f"  → DARKO (darko.app/api/active-players)...", end=" ", flush=True)
    try:
        time.sleep(1.0)
        resp = requests.get(
            url,
            headers={**HEADERS, "Referer": "https://www.darko.app/"},
            timeout=30,
        )
        resp.raise_for_status()
        players = resp.json()
        print(f"✅ {len(players)} players")
        return players
    except Exception as e:
        print(f"❌ {e}")
        return []

def write_darko(players, season, season_type):
    if not players: return
    conn = get_conn(); cur = conn.cursor()
    for col in ["darko_dpm", "darko_odpm", "darko_ddpm", "darko_box"]:
        try:
            cur.execute(f"ALTER TABLE player_seasons ADD COLUMN IF NOT EXISTS {col} REAL")
            conn.commit()
        except: conn.rollback()

    updated = 0
    for p in players:
        nba_id = p.get("nba_id")
        if not nba_id: continue
        cur.execute("""
            UPDATE player_seasons SET
                darko_dpm  = %s,
                darko_odpm = %s,
                darko_ddpm = %s,
                darko_box  = %s,
                updated_at = NOW()
            WHERE player_id = %s AND season = %s AND season_type = %s
        """, (
            safe_float(p.get("dpm")),
            safe_float(p.get("o_dpm")),
            safe_float(p.get("d_dpm")),
            safe_float(p.get("box_dpm")),
            int(nba_id), season, season_type,
        ))
        if cur.rowcount: updated += 1
    conn.commit(); cur.close(); conn.close()
    print(f"  ✅ DARKO: {updated} players updated")


# ── net_pts100 (ESPN via S3) ──────────────────────────────────
NET_PTS_URL = "https://nfl-player-metrics.s3.amazonaws.com/net-pts/nba_net_pts100_data.json"

NAME_OVERRIDES = {
    "kristaps porzingis": "Kristaps Porziņģis",
    "nikola jokic":       "Nikola Jokić",
    "luka doncic":        "Luka Dončić",
    "alperen sengun":     "Alperen Şengün",
    "bojan bogdanovic":   "Bojan Bogdanović",
    "nikola vucevic":     "Nikola Vučević",
    "bogdan bogdanovic":  "Bogdan Bogdanović",
    "jusuf nurkic":       "Jusuf Nurkić",
    "dario saric":        "Dario Šarić",
    "ivica zubac":        "Ivica Zubac",
    "pacome dadiet":      "Pacôme Dadiet",
    "alexandre sarr":     "Alexandre Sarr",
    "tim hardaway":       "Tim Hardaway Jr.",
}

def normalize_name(name):
    name = name.lower().strip()
    for suffix in [" jr.", " sr.", " iii", " ii", " iv"]:
        name = name.replace(suffix, "")
    for src, dst in {"č":"c","ć":"c","š":"s","ž":"z","đ":"d","á":"a","é":"e","í":"i",
                     "ó":"o","ú":"u","ō":"o","ū":"u","ő":"o","ö":"o","ü":"u","ñ":"n",
                     "ç":"c","ğ":"g","ı":"i","ā":"a","ę":"e","ń":"n","ż":"z"}.items():
        name = name.replace(src, dst)
    return name.strip()

def fetch_net_pts():
    """
    Fetches luck-adjusted on/off net rating from ESPN data hosted on S3.
    """
    print(f"  → net_pts100 (ESPN via S3)...", end=" ", flush=True)
    try:
        time.sleep(1.0)
        resp = requests.get(NET_PTS_URL, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        players = data if isinstance(data, list) else data.get("players", data.get("data", []))
        print(f"✅ {len(players)} total records")
        return players
    except Exception as e:
        print(f"❌ {e}")
        return []

def write_net_pts(all_players, season, season_type):
    if not all_players: return

    target_year = int(season.split("-")[0])

    # Filter to the target season
    season_players = [
        p for p in all_players
        if p.get("max_season") == target_year
        and p.get("seasonType", "Regular Season") == season_type
    ]
    if not season_players:
        season_players = [p for p in all_players if p.get("min_season") == target_year]

    print(f"  Filtered to {len(season_players)} players for {season}")
    if not season_players:
        available = sorted(set(p.get("max_season") for p in all_players if p.get("max_season")))
        print(f"  ⚠️  No data for {target_year}. Available years: {available}")
        return

    conn = get_conn(); cur = conn.cursor()
    for col in ["net_pts100", "o_net_pts100", "d_net_pts100"]:
        try:
            cur.execute(f"ALTER TABLE player_seasons ADD COLUMN IF NOT EXISTS {col} REAL")
            conn.commit()
        except: conn.rollback()

    # Build name lookup from DB
    cur.execute("""
        SELECT p.player_id, p.player_name
        FROM players p
        JOIN player_seasons ps ON p.player_id = ps.player_id
        WHERE ps.season = %s AND ps.season_type = %s
    """, (season, season_type))
    db_players  = cur.fetchall()
    db_by_norm  = {normalize_name(r["player_name"]): r["player_id"] for r in db_players}
    db_by_lower = {r["player_name"].lower(): r["player_id"] for r in db_players}

    updated, skipped, unmatched = 0, 0, []

    for p in season_players:
        full_nm = p.get("full_nm", "").strip()
        if not full_nm: continue

        norm = normalize_name(full_nm)
        pid  = None

        override = NAME_OVERRIDES.get(norm)
        if override:
            pid = db_by_lower.get(override.lower()) or db_by_norm.get(normalize_name(override))
        if not pid:
            pid = db_by_norm.get(norm) or db_by_lower.get(full_nm.lower())
        if not pid:
            parts = norm.split()
            if len(parts) == 2:
                pid = db_by_norm.get(f"{parts[1]} {parts[0]}")

        if not pid:
            unmatched.append(full_nm)
            continue

        cur.execute("""
            UPDATE player_seasons SET
                net_pts100   = %s,
                o_net_pts100 = %s,
                d_net_pts100 = %s,
                updated_at   = NOW()
            WHERE player_id = %s AND season = %s AND season_type = %s
        """, (
            safe_float(p.get("tNet100")),
            safe_float(p.get("oNet100")),
            safe_float(p.get("dNet100")),
            pid, season, season_type,
        ))
        if cur.rowcount: updated += 1
        else: skipped += 1

    conn.commit(); cur.close(); conn.close()
    print(f"  ✅ net_pts100: {updated} players updated"
          + (f" | ⚠️  {len(unmatched)} unmatched" if unmatched else ""))
    if unmatched:
        print(f"     Unmatched (first 10): {unmatched[:10]}")


# ── Main ──────────────────────────────────────────────────────

def run():
    print(f"\n🌐 ydkball — Fetching External Metrics")
    print(f"   Season: {SEASON} {SEASON_TYPE}")
    print("=" * 50)

    print("\n[1/3] LEBRON")
    write_lebron(fetch_lebron(SEASON), SEASON, SEASON_TYPE)

    print("\n[2/3] DARKO")
    write_darko(fetch_darko(), SEASON, SEASON_TYPE)

    print("\n[3/3] net_pts100")
    write_net_pts(fetch_net_pts(), SEASON, SEASON_TYPE)

    print(f"\n✅ External metrics done for {SEASON}.")
    print("Next: python backend/ingest/compute_pctiles.py")

if __name__ == "__main__":
    run()