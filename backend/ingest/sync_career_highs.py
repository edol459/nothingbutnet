"""
sync_career_highs.py — single-game career bests, straight from the NBA.
=======================================================================

`player_gamelogs` has no steals or blocks columns, so a career high in those can't
be derived locally. nba_api's PlayerProfileV2 carries a `CareerHighs` result set
that returns all six at once — one call per player instead of re-pulling 789k
gamelog rows.

Feeds the Higher or Lower "Single game high" boards (backend/games/chain_api.py).

**Why it has to keep running:** a career high is broken by playing, so every active
player's row goes stale the moment they have a big night. Retired players never
change, so the daily pass only touches `players.is_active` — about 160 players, a
minute of calls. Everyone else is fetched once and left alone.

Lives in daily_update_local.py because stats.nba.com blocks Railway IPs.

    python backend/ingest/sync_career_highs.py              # fill in anyone missing
    python backend/ingest/sync_career_highs.py --active     # refresh active players (daily)
    python backend/ingest/sync_career_highs.py --all        # re-pull everyone
"""

import os
import sys
import time
import argparse

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

STATS = ("PTS", "REB", "AST", "STL", "BLK", "FG3M")
# Who is worth holding at all: the Higher or Lower pool gates on 15,000 career
# minutes, and this leaves headroom so a player crossing that line is already here.
MIN_MINUTES = 12_000
PAUSE = 0.35          # stats.nba.com rate limit


def _ensure(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS player_career_highs (
                player_id  INTEGER NOT NULL REFERENCES players(player_id) ON DELETE CASCADE,
                stat       TEXT    NOT NULL,
                value      INTEGER NOT NULL,
                game_id    TEXT,
                game_date  DATE,
                updated_at TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (player_id, stat)
            )
        """)
    conn.commit()


def _targets(conn, mode):
    """Which players to fetch. `missing` is the default so a re-run is cheap."""
    where = {
        "missing": "AND c.player_id IS NULL",
        "active":  "AND p.is_active",
        "all":     "",
    }[mode]
    with conn.cursor() as cur:
        cur.execute(f"""
            WITH mins AS (
                SELECT player_id, SUM(min) AS total FROM player_gamelogs
                WHERE season_type = 'Regular Season' GROUP BY player_id
            ),
            first AS (
                SELECT player_id, MIN(season) AS first_season FROM player_seasons
                WHERE season_type = 'Regular Season' GROUP BY player_id
            )
            SELECT DISTINCT p.player_id, p.player_name
            FROM   players p
            JOIN   mins ON mins.player_id = p.player_id
            JOIN   first ON first.player_id = p.player_id
            LEFT   JOIN player_career_highs c ON c.player_id = p.player_id
            WHERE  mins.total >= %s {where}
              -- Same completeness test the game's pool uses. Without it a third of
              -- the calls went to players who debuted before our data starts and can
              -- never appear on a board.
              AND ((p.draft_year::text ~ '^[0-9]{{4}}$' AND p.draft_year::text::int >= 1996)
                   OR (p.draft_year IS NULL AND first.first_season >= '1997-98'))
            ORDER  BY p.player_id
        """, (MIN_MINUTES,))
        return cur.fetchall()


def _fetch(pid):
    """[(stat, value, game_id, game_date)] — the API lists one row per occurrence,
    so a stat matched on several nights appears more than once; keep the best."""
    from nba_api.stats.endpoints import playerprofilev2
    sets = playerprofilev2.PlayerProfileV2(player_id=pid, timeout=30).get_dict()["resultSets"]
    rs = next((x for x in sets if x["name"] == "CareerHighs"), None)
    if not rs:
        return []
    hdr = rs["headers"]
    best = {}
    for row in rs["rowSet"]:
        rec = dict(zip(hdr, row))
        stat, val = rec.get("STAT"), rec.get("STAT_VALUE")
        if stat not in STATS or val is None:
            continue
        if stat not in best or val > best[stat][0]:
            best[stat] = (val, rec.get("GAME_ID"), rec.get("DATE_EST") or None)
    return [(s, v, gid, gd) for s, (v, gid, gd) in best.items()]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--active", action="store_true", help="refresh active players (the daily pass)")
    ap.add_argument("--all", action="store_true", help="re-pull everyone")
    ap.add_argument("--limit", type=int, default=0, help="stop after N players")
    args = ap.parse_args()
    mode = "all" if args.all else "active" if args.active else "missing"

    conn = psycopg2.connect(os.getenv("DATABASE_URL"),
                            cursor_factory=psycopg2.extras.RealDictCursor)
    _ensure(conn)
    targets = _targets(conn, mode)
    if args.limit:
        targets = targets[:args.limit]
    if not targets:
        print(f"[career-highs] nothing to fetch ({mode})")
        return

    print(f"[career-highs] {mode}: {len(targets)} players")
    wrote = failed = 0
    for i, row in enumerate(targets, 1):
        try:
            highs = _fetch(row["player_id"])
        except Exception as e:
            failed += 1
            if failed <= 3:
                print(f"  {row['player_name']}: {type(e).__name__} {e}")
            time.sleep(PAUSE)
            continue
        if highs:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, """
                    INSERT INTO player_career_highs (player_id, stat, value, game_id, game_date)
                    VALUES %s
                    ON CONFLICT (player_id, stat) DO UPDATE
                      SET value = EXCLUDED.value, game_id = EXCLUDED.game_id,
                          game_date = EXCLUDED.game_date, updated_at = NOW()
                """, [(row["player_id"], s, v, gid, gd) for s, v, gid, gd in highs])
            conn.commit()
            wrote += 1
        if i % 100 == 0:
            print(f"  {i}/{len(targets)}")
        time.sleep(PAUSE)

    print(f"[career-highs] wrote {wrote}, failed {failed}")
    conn.close()


if __name__ == "__main__":
    main()
