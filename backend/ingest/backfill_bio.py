"""
ydkball — Backfill player BIO (height / draft / college / country / position)
=============================================================================
python backend/ingest/backfill_bio.py [--no-position]

The `players` bio columns were only populated for ACTIVE players; retired players are NULL.
This fills them so the "guess the performance" game can use bio clues for all answers.

  Part 1 (bulk, ~20s): LeagueDashPlayerBioStats per season (1996-97→now) → one bulk call each
    → height, weight, draft (year/round/number), college, country. Fills NULLs only.
  Part 2 (~5 min): for notable players STILL missing position, CommonPlayerInfo per-player
    (position isn't in the bulk feed). Skip with --no-position.
"""

import os
import sys
import time
import argparse
from datetime import date
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")


def current_season() -> str:
    t = date.today()
    return f"{t.year}-{str(t.year + 1)[2:]}" if t.month >= 10 else f"{t.year - 1}-{str(t.year)[2:]}"


def seasons_since(start="1996-97"):
    out, y, end = [], int(start[:4]), int(current_season()[:4])
    while y <= end:
        out.append(f"{y}-{str(y + 1)[2:]}"); y += 1
    return out


def _int(v):
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def _short_pos(p):
    m = {"guard": "G", "forward": "F", "center": "C"}
    return "-".join(m.get(t.strip().lower(), t[:1].upper()) for t in p.split("-") if t.strip())


def bulk_bio(conn):
    from nba_api.stats.endpoints import LeagueDashPlayerBioStats
    seasons = seasons_since()
    bio = {}                                   # pid -> bio dict (newest season wins)
    print(f"Part 1 — bulk bio over {len(seasons)} seasons …")
    for season in reversed(seasons):           # newest first
        try:
            df = LeagueDashPlayerBioStats(season=season).get_data_frames()[0]
        except Exception as e:
            print(f"  {season}: ❌ {e}"); continue
        for _, r in df.iterrows():
            pid = int(r["PLAYER_ID"])
            if pid in bio:
                continue
            bio[pid] = {
                "height_inches": _int(r.get("PLAYER_HEIGHT_INCHES")),
                "weight":        _int(r.get("PLAYER_WEIGHT")),
                "draft_year":    _int(r.get("DRAFT_YEAR")),
                "draft_round":   _int(r.get("DRAFT_ROUND")),
                "draft_number":  _int(r.get("DRAFT_NUMBER")),
                "college":       (r.get("COLLEGE") or None),
                "country":       (r.get("COUNTRY") or None),
            }
        time.sleep(0.4)
    print(f"  collected bio for {len(bio)} players; filling NULLs …")

    cur = conn.cursor()
    rows = [{"pid": pid, **b} for pid, b in bio.items()]
    psycopg2.extras.execute_batch(cur, """
        UPDATE players SET
            height_inches = COALESCE(height_inches, %(height_inches)s),
            weight        = COALESCE(weight,        %(weight)s),
            draft_year    = COALESCE(draft_year,    %(draft_year)s),
            draft_round   = COALESCE(draft_round,   %(draft_round)s),
            draft_number  = COALESCE(draft_number,  %(draft_number)s),
            college       = COALESCE(college,       %(college)s),
            country       = COALESCE(country,       %(country)s)
        WHERE player_id = %(pid)s
    """, rows, page_size=500)
    conn.commit()
    cur.close()
    print("  ✅ bulk bio done")


def position_pass(conn):
    from nba_api.stats.endpoints import CommonPlayerInfo
    cur = conn.cursor()
    # notable players (appear as a daily-worthy performance) still missing position
    cur.execute("""
        WITH elig AS (
            SELECT player_id FROM player_seasons GROUP BY player_id
            HAVING SUM(gp) >= 200 OR BOOL_OR(awards IS NOT NULL AND array_length(awards,1) > 0)),
        answers AS (
            SELECT DISTINCT g.player_id FROM player_gamelogs g JOIN elig e ON e.player_id = g.player_id
            WHERE g.pts>=40 OR (g.pts>=10 AND g.reb>=10 AND g.ast>=10) OR g.reb>=20 OR g.ast>=15
               OR g.fg3m>=8 OR (g.season_type='Playoffs' AND g.pts>=35))
        SELECT a.player_id FROM answers a JOIN players p ON p.player_id = a.player_id
        WHERE p.position_group IS NULL
    """)
    pids = [r["player_id"] for r in cur.fetchall()]
    print(f"\nPart 2 — position for {len(pids)} notable players missing it …")
    done = 0
    for i, pid in enumerate(pids):
        try:
            info = CommonPlayerInfo(player_id=pid).get_data_frames()[0]
            pos = str(info["POSITION"].iloc[0] or "").strip()
        except Exception:
            time.sleep(0.6); continue
        if pos:
            grp = _short_pos(pos).split("-")[0]
            cur.execute("UPDATE players SET position = COALESCE(position, %s), "
                        "position_group = COALESCE(position_group, %s) WHERE player_id = %s",
                        (_short_pos(pos), grp, pid))
            done += 1
            if done % 25 == 0:
                conn.commit(); print(f"    …{done}/{len(pids)}")
        time.sleep(0.6)
    conn.commit(); cur.close()
    print(f"  ✅ position filled for {done} players")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-position", action="store_true", help="skip the slower per-player position pass")
    args = ap.parse_args()
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    t0 = time.time()
    bulk_bio(conn)
    if not args.no_position:
        position_pass(conn)
    print(f"\nDone in {(time.time()-t0)/60:.1f} min.")
    conn.close()


if __name__ == "__main__":
    main()
