"""
ydkball — Backfill `games` for 1983-84 → 1995-96 from LeagueGameFinder
======================================================================
python backend/ingest/backfill_pre1996_games.py --validate
python backend/ingest/backfill_pre1996_games.py                    # dry-run
python backend/ingest/backfill_pre1996_games.py --commit

`backfill_historical_games.py` derived games from `player_gamelogs`, which starts
at 1996-97. Nothing older can come from there, so this reads the league's own game
finder instead — team-level rows (date, teams, score, W/L), two per game.

1983-84 is the floor. Every season from 1982-83 back returns zero rows: that is the
NBA's own game-level data boundary, not a rate limit. 1983-84 through 1985-86 each
return exactly 943 games, which is 23 teams x 82 games / 2 — the right answer.

MUST RUN FROM A RESIDENTIAL IP. stats.nba.com blocks Railway, which is why the
local pipeline exists at all (see daily_update_local.py). ~26 requests total, one
per season and season type.

What this does NOT get: player box scores. LeagueGameFinder is team-level only, so
these games land with a date, a matchup and a final score and an empty Boxscore tab.
Filling that in means BoxScoreTraditionalV2 once per game — ~14.5k calls — which is
a separate overnight job.

Never updates or deletes an existing row — ON CONFLICT DO NOTHING. Safe to re-run.
"""

import os
import sys
import time
import argparse
from collections import defaultdict

import psycopg2
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    print("❌ DATABASE_URL not set.")
    sys.exit(1)

try:
    from nba_api.stats.endpoints import LeagueGameFinder
except ImportError:
    print("❌ nba_api not installed (pip install nba_api).")
    sys.exit(1)

FIRST_SEASON = "1983-84"       # earliest season the endpoint actually serves
LAST_SEASON  = "1995-96"       # 1996-97+ already came from player_gamelogs

# game_id[2] -> season type, matching reconcile_games.py and
# server._season_type_from_game_id. Preseason and All-Star are not backfilled.
SEASON_TYPE = {"2": "Regular Season", "4": "Playoffs", "5": "PlayIn"}
FETCH_TYPES = ("Regular Season", "Playoffs")

# Codes this era uses for franchises that still exist unchanged — same team, same
# city, just a different abbreviation in the feed. Normalised so a 1985 Warriors
# game resolves to the Warriors' logo, colors and team page like any other, instead
# of looking like a defunct franchise.
ABBR_ALIASES = {
    "GOS": "GSW",   # Golden State Warriors
    "PHL": "PHI",   # Philadelphia 76ers
    "SAN": "SAS",   # San Antonio Spurs
    "UTH": "UTA",   # Utah Jazz
}

# Genuinely different franchises — kept as played, the way SEA and VAN are. They
# need an entry in the defunct-franchise tables (nba-assets.js, Components.swift)
# or they render without a crest.
#   KCK  Kansas City Kings     -> SAC
#   SDC  San Diego Clippers    -> LAC

DELAY   = 1.5
RETRIES = 3


def fetch_season(season: str, season_type: str):
    """One (season, type) page of the game finder. Returns a list of team-rows."""
    for attempt in range(RETRIES):
        try:
            time.sleep(DELAY * (attempt + 1))
            dfs = LeagueGameFinder(
                season_nullable=season,
                league_id_nullable="00",
                season_type_nullable=season_type,
                timeout=60,
            ).get_data_frames()
            if not dfs or dfs[0].empty:
                return []
            df = dfs[0]
            return df[["GAME_ID", "GAME_DATE", "MATCHUP",
                       "TEAM_ABBREVIATION", "PTS"]].to_dict("records")
        except Exception as e:
            if attempt == RETRIES - 1:
                print(f"    ⚠️  {season} {season_type}: {type(e).__name__} {e}")
                return []
    return []


def pair_rows(rows, season, rejected):
    """Fold two team-rows into one game.

    MATCHUP is written from that row's own perspective: "SAN vs. POR" is the home
    side, "SAN @ POR" the away side. A game needs exactly one of each.
    """
    by_game = defaultdict(list)
    for r in rows:
        by_game[str(r["GAME_ID"])].append(r)

    out = []
    for gid, sides in by_game.items():
        if len(gid) < 3 or gid[2] not in SEASON_TYPE:
            rejected["not a loggable season type"].append(gid)
            continue
        if len(sides) != 2:
            rejected[f"expected 2 team rows, found {len(sides)}"].append(gid)
            continue

        home = next((s for s in sides if "@" not in str(s["MATCHUP"])), None)
        away = next((s for s in sides if "@" in str(s["MATCHUP"])), None)
        if home is None or away is None:
            rejected["could not tell home from away"].append(gid)
            continue
        if home["PTS"] is None or away["PTS"] is None:
            rejected["missing score"].append(gid)
            continue

        h = ABBR_ALIASES.get(home["TEAM_ABBREVIATION"], home["TEAM_ABBREVIATION"])
        a = ABBR_ALIASES.get(away["TEAM_ABBREVIATION"], away["TEAM_ABBREVIATION"])
        if h == a:
            rejected["same team on both sides"].append(gid)
            continue

        out.append((gid, season, SEASON_TYPE[gid[2]],
                    str(home["GAME_DATE"])[:10], h, a,
                    int(home["PTS"]), int(away["PTS"])))
    return out


def seasons_between(start, end):
    out, y = [], int(start[:4])
    while y <= int(end[:4]):
        out.append(f"{y}-{str(y + 1)[2:]}")
        y += 1
    return out


def collect(seasons, rejected):
    games = []
    for s in seasons:
        per_season = []
        for st in FETCH_TYPES:
            rows = fetch_season(s, st)
            per_season += pair_rows(rows, s, rejected)
        print(f"    {s}  {len(per_season):>5} games")
        games += per_season
    return games


def validate(cur, n_seasons):
    """Cross-check the feed against seasons we already hold.

    Those rows came from player_gamelogs by a completely different route, so an
    exact match is real evidence that this path produces the same games rather
    than merely self-consistent ones.
    """
    cur.execute("""
        SELECT DISTINCT season FROM games
         WHERE league = 'nba' AND season > %s AND season < '2010-11'
         ORDER BY season ASC LIMIT %s
    """, (LAST_SEASON, n_seasons))
    seasons = [r[0] for r in cur.fetchall()]
    if not seasons:
        print("No overlapping seasons to validate against.")
        return False

    print(f"Validating the game finder against seasons already in `games`: "
          f"{', '.join(seasons)}\n")
    rejected = defaultdict(list)
    fetched = {g[0]: g for g in collect(seasons, rejected)}

    cur.execute("""
        SELECT game_id, season, season_type, game_date::text,
               home_team_abbr, away_team_abbr, home_score, away_score
        FROM games WHERE league='nba' AND season = ANY(%s)
    """, (seasons,))
    actual = {r[0]: r for r in cur.fetchall()}

    overlap = set(fetched) & set(actual)
    fields = [("season", 1), ("season_type", 2), ("game_date", 3),
              ("home_abbr", 4), ("away_abbr", 5), ("home_score", 6), ("away_score", 7)]
    stats, examples = defaultdict(int), defaultdict(list)
    for gid in overlap:
        f, a = fetched[gid], actual[gid]
        for label, i in fields:
            if f[i] != a[i]:
                stats[label] += 1
                if len(examples[label]) < 4:
                    examples[label].append(f"{gid}: feed={f[i]!r} db={a[i]!r}")

    print(f"\n  compared              {len(overlap):,}")
    print(f"  in db, not in feed    {len(set(actual) - set(fetched)):,}")
    print(f"  in feed, not in db    {len(set(fetched) - set(actual)):,}\n")

    # Structural fields prove THIS script is correct: if pairing, home/away or
    # season-type derivation were wrong they would be wrong in bulk, not in threes.
    STRUCTURAL = {"season", "season_type", "game_date", "home_abbr", "away_abbr"}
    ok = True
    for label, _ in fields:
        n = stats[label]
        structural = label in STRUCTURAL
        mark = "✅" if n == 0 else ("❌" if structural else "⚠️ ")
        print(f"  {mark} {label:<12} {n:>5} mismatches")
        for ex in examples[label]:
            print(f"        {ex}")
        if n and structural:
            ok = False

    score_diffs = stats["home_score"] + stats["away_score"]
    if rejected:
        print("\n  rejected while pairing:")
        for reason, gids in sorted(rejected.items(), key=lambda kv: -len(kv[1])):
            print(f"    {len(gids):>5}  {reason}")

    if not ok:
        print("\n❌ Structural mismatch — the pairing logic is wrong. Do NOT commit.")
        return False

    print("\n✅ Every game pairs to the same teams, date and season type as the "
          "rows derived independently from player_gamelogs.")
    if score_diffs:
        # Not a failure of this script. backfill_historical_games.py computed scores
        # as SUM(player points), which understates a game with a missing box-score
        # row and misplaces points when a row is attributed to the wrong side. The
        # feed carries the league's own team totals, so where they differ the feed
        # is right and the stored value is the defect.
        print(f"\n⚠️  {score_diffs} score field(s) differ from what we stored. The feed "
              f"is the league's own team total and is authoritative here — these are "
              f"errors in the gamelog-derived backfill, not in this script. "
              f"Run --audit-scores to size it across every derived season.")
    return True


def audit_scores(conn, cur, commit):
    """Check every gamelog-derived season's scores against the league's own totals.

    backfill_historical_games.py had to compute scores as SUM(player points) — there
    was no other source for 1996-2010. That is exact only when every box-score row is
    present and attributed to the right side, and the validator found cases where
    neither held. This is the authoritative check, and the only way to fix them.

    Read-only unless --commit. Touches nothing but home_score/away_score on games the
    feed disagrees with; never inserts, never deletes, never touches a user row.
    """
    cur.execute("""
        SELECT DISTINCT season FROM games
         WHERE league='nba' AND season > %s AND season < '2010-11'
         ORDER BY season
    """, (LAST_SEASON,))
    seasons = [r[0] for r in cur.fetchall()]
    print(f"Auditing scores for {len(seasons)} derived seasons "
          f"({seasons[0]} → {seasons[-1]}) against LeagueGameFinder\n")

    rejected = defaultdict(list)
    fetched = {g[0]: g for g in collect(seasons, rejected)}

    cur.execute("""
        SELECT game_id, home_score, away_score, game_date::text,
               home_team_abbr, away_team_abbr
        FROM games WHERE league='nba' AND season = ANY(%s)
    """, (seasons,))
    wrong = []
    checked = 0
    for gid, hs, as_, gdate, h, a in cur.fetchall():
        f = fetched.get(gid)
        if not f:
            continue
        checked += 1
        if f[6] != hs or f[7] != as_:
            wrong.append((gid, gdate, a, as_, f[7], h, hs, f[6]))

    print(f"\n  checked            {checked:,}")
    print(f"  scores disagreeing {len(wrong):,}"
          f"  ({len(wrong) / checked * 100:.2f}%)" if checked else "")
    for gid, gdate, a, as_, fa, h, hs, fh in sorted(wrong, key=lambda r: r[1])[:25]:
        print(f"    {gid}  {gdate}  {a} {as_}→{fa}  @  {h} {hs}→{fh}")
    if len(wrong) > 25:
        print(f"    … and {len(wrong) - 25} more")

    if not wrong:
        print("\n✅ Every derived score matches the league's own totals.")
        return 0
    if not commit:
        print(f"\n(dry-run) Would correct {len(wrong):,} game(s). "
              f"Re-run with --audit-scores --commit to write.")
        return 0

    cur.executemany(
        "UPDATE games SET home_score=%s, away_score=%s, updated_at=NOW() WHERE game_id=%s",
        [(w[7], w[4], w[0]) for w in wrong])
    conn.commit()
    print(f"\n✅ Corrected {len(wrong):,} game score(s).")
    return len(wrong)


def backfill(conn, cur, commit, start, end):
    seasons = seasons_between(start, end)
    print(f"{'' if commit else '(dry-run) '}Backfilling NBA games "
          f"{seasons[0]} → {seasons[-1]} from LeagueGameFinder\n")
    rejected = defaultdict(list)
    games = collect(seasons, rejected)

    cur.execute("SELECT game_id FROM games")
    have = {r[0] for r in cur.fetchall()}
    missing = [g for g in games if g[0] not in have]

    by_season = defaultdict(int)
    for g in missing:
        by_season[g[1]] += 1

    print(f"\n  fetched              {len(games):,}")
    print(f"  already in `games`   {len(games) - len(missing):,}")
    print(f"  to insert            {len(missing):,}")

    seen = sorted({g[4] for g in missing} | {g[5] for g in missing})
    print(f"\n  team abbreviations:  {' '.join(seen)}")
    if rejected:
        print("\n  skipped:")
        for reason, gids in sorted(rejected.items(), key=lambda kv: -len(kv[1])):
            print(f"    {len(gids):>5}  {reason}")

    if missing:
        print("\n  sample:")
        for g in sorted(missing, key=lambda x: x[3])[:8]:
            print(f"    {g[0]}  {g[3]}  {g[5]} {g[7]} @ {g[4]} {g[6]}  ({g[2]}, {g[1]})")

    if not missing:
        print("\n✅ Nothing to do — every game already exists.")
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
    print(f"\n✅ Inserted {len(missing):,} games.")
    return len(missing)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--validate", action="store_true",
                    help="cross-check the feed against seasons already in `games`")
    ap.add_argument("--validate-seasons", type=int, default=2,
                    help="how many overlapping seasons to check (default 2)")
    ap.add_argument("--audit-scores", action="store_true",
                    help="check 1996-2010 derived scores against the league's totals")
    ap.add_argument("--commit", action="store_true",
                    help="actually write (default is dry-run)")
    ap.add_argument("--start", default=FIRST_SEASON)
    ap.add_argument("--end",   default=LAST_SEASON)
    args = ap.parse_args()

    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    try:
        if args.validate:
            sys.exit(0 if validate(cur, args.validate_seasons) else 1)
        if args.audit_scores:
            audit_scores(conn, cur, args.commit)
            return
        backfill(conn, cur, args.commit, args.start, args.end)
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
