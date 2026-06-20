"""
Backfill an `awards TEXT[]` column on player_seasons from nba_api PlayerAwards.

Each player-season's Regular Season row gets the awards that player won that season,
e.g. {'MVP','All-NBA','All-Star'}. Source is the NBA's own data — no guessing.

    python backend/ingest/fetch_awards.py            # resumable full backfill
    python backend/ingest/fetch_awards.py --player 2544   # one player (testing)

Resumable: tracks completed player_ids in a progress file, so you can stop/restart.
"""

import os
import re
import json
import time
import argparse

import psycopg2
from dotenv import load_dotenv
from nba_api.stats.endpoints import playerawards

load_dotenv()

PROGRESS = os.path.join(os.path.dirname(__file__), ".awards_progress.json")
SEASON_RE = re.compile(r"^\d{4}-\d{2}$")

# NBA's verbose award names → clean labels we store/query.
AWARD_MAP = {
    "NBA Most Valuable Player":          "MVP",
    "NBA Rookie of the Year":            "ROTY",
    "NBA Defensive Player of the Year":  "DPOY",
    "NBA Sixth Man of the Year":         "6MOY",
    "NBA Most Improved Player":          "MIP",
    "NBA Finals Most Valuable Player":   "Finals MVP",
    "NBA All-Star":                      "All-Star",
}


def connect():
    return psycopg2.connect(os.getenv("DATABASE_URL"))


def ensure_column(conn):
    with conn.cursor() as cur:
        cur.execute("ALTER TABLE player_seasons ADD COLUMN IF NOT EXISTS awards TEXT[]")
    conn.commit()


def player_ids(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT player_id FROM player_seasons ORDER BY player_id")
        return [r[0] for r in cur.fetchall()]


def fetch_player_awards(pid, tries=4):
    """Return {season: set(labels)} for one player."""
    for attempt in range(tries):
        try:
            r = playerawards.PlayerAwards(player_id=pid, timeout=30)
            d = r.get_dict()["resultSets"][0]
            cols = d["headers"]
            di, si = cols.index("DESCRIPTION"), cols.index("SEASON")
            out = {}
            for row in d["rowSet"]:
                label = AWARD_MAP.get(row[di])
                season = row[si]
                if label and isinstance(season, str) and SEASON_RE.match(season):
                    out.setdefault(season, set()).add(label)
            return out
        except Exception as e:
            time.sleep(2 * (attempt + 1))
    print(f"  ✗ giving up on player {pid}")
    return {}


def write_awards(conn, pid, by_season):
    with conn.cursor() as cur:
        for season, labels in by_season.items():
            cur.execute(
                """UPDATE player_seasons SET awards = %s
                   WHERE player_id = %s AND season = %s AND season_type = 'Regular Season'""",
                (sorted(labels), pid, season),
            )
    conn.commit()


def load_done():
    if os.path.exists(PROGRESS):
        with open(PROGRESS) as f:
            return set(json.load(f))
    return set()


def save_done(done):
    with open(PROGRESS, "w") as f:
        json.dump(sorted(done), f)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--player", type=int, default=None)
    ap.add_argument("--pause", type=float, default=0.6)
    args = ap.parse_args()

    conn = connect()
    ensure_column(conn)

    if args.player:
        bs = fetch_player_awards(args.player)
        write_awards(conn, args.player, bs)
        print(f"player {args.player}: {bs}")
        conn.close()
        return

    ids = player_ids(conn)
    done = load_done()
    todo = [p for p in ids if p not in done]
    print(f"Awards backfill: {len(todo)} players to fetch ({len(done)} already done)", flush=True)
    found = 0
    for i, pid in enumerate(todo, 1):
        bs = fetch_player_awards(pid)              # API only — no DB
        if bs:
            for attempt in range(5):               # survive Railway dropping the connection
                try:
                    write_awards(conn, pid, bs)
                    found += 1
                    break
                except psycopg2.OperationalError:
                    print(f"  DB connection dropped — reconnecting ({attempt+1})", flush=True)
                    time.sleep(2 * (attempt + 1))
                    try:
                        conn = connect()
                    except Exception:
                        pass
        done.add(pid)
        if i % 25 == 0:
            save_done(done)
            print(f"  ... {i}/{len(todo)} players · {found} with awards", flush=True)
        time.sleep(args.pause)
    save_done(done)
    conn.close()
    print(f"done — {found} players had awards.", flush=True)


if __name__ == "__main__":
    main()
