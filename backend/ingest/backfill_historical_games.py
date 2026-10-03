"""
ydkball — Backfill historical `games` rows from player_gamelogs
===============================================================
python backend/ingest/backfill_historical_games.py --validate
python backend/ingest/backfill_historical_games.py              # dry-run
python backend/ingest/backfill_historical_games.py --commit

`games` only goes back to 2010-11, but `backfill_gamelogs.py` already filled
`player_gamelogs` from 1996-97 onward — ~17.5k pre-2010 game_ids sit there with
no `games` row, so they can't be opened, rated or logged.

This derives those rows from data we already hold. No network, no NBA API, no
Akamai problem — it is one read and one insert against Postgres:

  team        split_part(matchup, ' ', 1)   -- matchup is from the player's POV
  home/away   matchup LIKE '%@%'            -- "VAN @ CLE" away, "CLE vs. VAN" home
  score       SUM(pts) per side             -- no team points exist outside players
  season_type game_id[2]                    -- same rule as reconcile_games.py

`--validate` re-derives seasons we ALREADY have real scores for and reports any
disagreement. It is the proof the method is sound; run it before --commit.
(Measured at time of writing: 2015-16 matched 1316/1316 on both scores and home
team.)

Never updates or deletes an existing row — ON CONFLICT DO NOTHING. Safe to re-run.

Note on team abbreviations: historical rows keep the abbreviation the game was
played under (VAN, SEA, NJN, CHH, WSB). That is the honest record, but logos and
team pages are keyed to current abbrs, so those will not resolve until a
historical-abbr map exists. See docs/ for the WNBA version of this problem.
"""

import os
import sys
import argparse
from collections import defaultdict

import psycopg2
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    print("❌ DATABASE_URL not set.")
    sys.exit(1)

# game_id[2] -> season type. Matches reconcile_games.py and
# server._season_type_from_game_id. Anything not listed here (preseason "1",
# All-Star "3") is deliberately skipped: those aren't games anyone logs, and
# All-Star rosters would land fake tricodes in the games table.
SEASON_TYPE = {"2": "Regular Season", "4": "Playoffs", "5": "PlayIn"}

# Minimum players a side must have before we trust SUM(pts) as the final score.
# A missing box score row silently understates the score, and there is nothing
# to check it against for pre-2010 games. Measured against the 20,447 games we
# already have real scores for, the derivation is exact at 8+ and the single
# wrong game in the whole set had a 7-player side (0022501198: UTA derived 101,
# actual 107 — six points of missing rows). Sides of 6-7 are ~1% of games and
# are held back rather than guessed; they can be recovered later with a targeted
# boxscore fetch.
MIN_PLAYERS = 8

DERIVE_SQL = """
WITH t AS (
    SELECT game_id,
           game_date,
           season,
           split_part(matchup, ' ', 1) AS team,
           matchup LIKE '%%@%%'        AS is_away,
           pts
    FROM player_gamelogs
    WHERE matchup IS NOT NULL
      AND game_id IS NOT NULL
      AND game_date IS NOT NULL
      {season_filter}
)
SELECT game_id,
       min(game_date)                                   AS game_date,
       min(season)                                      AS season,
       count(DISTINCT season)                           AS n_seasons,
       count(DISTINCT team)                             AS n_teams,
       max(team)   FILTER (WHERE NOT is_away)           AS home_abbr,
       max(team)   FILTER (WHERE is_away)               AS away_abbr,
       sum(pts)    FILTER (WHERE NOT is_away)           AS home_score,
       sum(pts)    FILTER (WHERE is_away)               AS away_score,
       count(*)    FILTER (WHERE NOT is_away)           AS home_players,
       count(*)    FILTER (WHERE is_away)               AS away_players
FROM t
GROUP BY game_id
"""


def derive(cur, start=None, end=None, min_players=MIN_PLAYERS):
    """Derive one row per game_id from player_gamelogs.

    Returns (clean, rejected) where clean rows are ready to insert and rejected
    rows carry a reason. A game is rejected rather than guessed at — a wrong
    score on a game someone might log is worse than a missing game.
    """
    filt, params = "", []
    if start:
        filt += " AND season >= %s"
        params.append(start)
    if end:
        filt += " AND season <= %s"
        params.append(end)

    cur.execute(DERIVE_SQL.format(season_filter=filt), params)

    clean, rejected = [], defaultdict(list)
    for (gid, gdate, season, n_seasons, n_teams, home, away,
         hs, as_, hp, ap) in cur.fetchall():

        if len(gid) < 3 or gid[2] not in SEASON_TYPE:
            rejected["not a loggable season type"].append(gid)
            continue
        if n_teams != 2:
            rejected[f"expected 2 teams, found {n_teams}"].append(gid)
            continue
        if n_seasons != 1:
            rejected["game_id spans multiple seasons"].append(gid)
            continue
        if not home or not away:
            rejected["only one side has box score rows"].append(gid)
            continue
        if hs is None or as_ is None:
            rejected["null points on one side"].append(gid)
            continue
        if hp < min_players or ap < min_players:
            rejected[f"under {min_players} players a side — score not trusted"].append(gid)
            continue

        clean.append((gid, season, SEASON_TYPE[gid[2]], gdate,
                      home, away, int(hs), int(as_)))

    return clean, rejected


def validate(cur, min_players=MIN_PLAYERS):
    """Re-derive seasons we already have and compare. Proves the method."""
    cur.execute("SELECT min(season), max(season) FROM games WHERE league = 'nba'")
    lo, hi = cur.fetchone()
    print(f"Validating derivation against existing NBA games ({lo} → {hi})\n")

    clean, rejected = derive(cur, start=lo, end=hi, min_players=min_players)
    derived = {r[0]: r for r in clean}

    cur.execute("""
        SELECT game_id, season, season_type, game_date,
               home_team_abbr, away_team_abbr, home_score, away_score
        FROM games WHERE league = 'nba'
    """)
    actual = {r[0]: r for r in cur.fetchall()}

    overlap = set(derived) & set(actual)
    stats = defaultdict(int)
    examples = defaultdict(list)

    for gid in overlap:
        d, a = derived[gid], actual[gid]
        stats["compared"] += 1
        for field, di, ai in (("home_abbr", 4, 4), ("away_abbr", 5, 5),
                              ("home_score", 6, 6), ("away_score", 7, 7),
                              ("game_date", 3, 3), ("season_type", 2, 2)):
            if d[di] != a[ai]:
                stats[field] += 1
                if len(examples[field]) < 5:
                    examples[field].append(f"{gid}: derived={d[di]!r} actual={a[ai]!r}")

    print(f"  games compared          {stats['compared']:,}")
    print(f"  in games, not derivable {len(set(actual) - set(derived)):,}")
    print(f"  derivable, not in games {len(set(derived) - set(actual)):,}")
    print()
    ok = True
    for field in ("home_abbr", "away_abbr", "home_score", "away_score",
                  "game_date", "season_type"):
        n = stats[field]
        flag = "✅" if n == 0 else "❌"
        pct = (n / stats["compared"] * 100) if stats["compared"] else 0
        print(f"  {flag} {field:<12} {n:>6,} mismatches ({pct:.2f}%)")
        for ex in examples[field]:
            print(f"        {ex}")
        if n:
            ok = False

    if rejected:
        print("\n  rejected during derivation:")
        for reason, gids in sorted(rejected.items(), key=lambda kv: -len(kv[1])):
            print(f"    {len(gids):>6,}  {reason}   e.g. {', '.join(gids[:3])}")

    print("\n" + ("✅ Derivation is exact. Safe to backfill."
                  if ok else
                  "❌ Derivation disagrees with known data — do NOT commit."))
    return ok


def backfill(conn, cur, commit, start, end, min_players=MIN_PLAYERS):
    clean, rejected = derive(cur, start=start, end=end, min_players=min_players)

    cur.execute("SELECT game_id FROM games")
    have = {r[0] for r in cur.fetchall()}
    missing = [g for g in clean if g[0] not in have]

    by_season = defaultdict(int)
    for g in missing:
        by_season[g[1]] += 1

    print(f"{'' if commit else '(dry-run) '}Backfilling historical NBA games "
          f"from player_gamelogs\n")
    print(f"  derivable games      {len(clean):,}")
    print(f"  already in `games`   {len(clean) - len(missing):,}")
    print(f"  to insert            {len(missing):,}\n")

    for season in sorted(by_season):
        print(f"    {season}   +{by_season[season]:,}")

    if rejected:
        print("\n  skipped:")
        for reason, gids in sorted(rejected.items(), key=lambda kv: -len(kv[1])):
            print(f"    {len(gids):>6,}  {reason}")

    if missing:
        print("\n  sample of what would be inserted:")
        for g in missing[:8]:
            print(f"    {g[0]}  {g[3]}  {g[5]} {g[7]} @ {g[4]} {g[6]}  ({g[2]}, {g[1]})")

    if not missing:
        print("\n✅ Nothing to do — every derivable game already exists.")
        return 0

    if not commit:
        print(f"\n(dry-run) Would insert {len(missing):,} games. "
              f"Re-run with --commit to write.")
        return 0

    cur.executemany("""
        INSERT INTO games (
            game_id, season, season_type, game_date,
            home_team_abbr, away_team_abbr,
            home_score, away_score, status, league
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'Final','nba')
        ON CONFLICT (game_id) DO NOTHING
    """, missing)
    conn.commit()
    print(f"\n✅ Inserted {len(missing):,} historical games.")
    return len(missing)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--validate", action="store_true",
                    help="check the derivation against seasons already in `games`")
    ap.add_argument("--commit", action="store_true",
                    help="actually insert (default is dry-run)")
    ap.add_argument("--start", help="earliest season, e.g. 1996-97")
    ap.add_argument("--end", help="latest season, e.g. 2009-10")
    ap.add_argument("--min-players", type=int, default=MIN_PLAYERS,
                    help=f"players a side required to trust the score "
                         f"(default {MIN_PLAYERS}; see MIN_PLAYERS)")
    args = ap.parse_args()

    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    try:
        if args.validate:
            sys.exit(0 if validate(cur, args.min_players) else 1)
        backfill(conn, cur, args.commit, args.start, args.end, args.min_players)
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
