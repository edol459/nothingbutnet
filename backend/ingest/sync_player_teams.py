"""
ydkball — Sync every player's current team
=============================================================
    python backend/ingest/sync_player_teams.py --dry-run
    python backend/ingest/sync_player_teams.py
    python backend/ingest/sync_player_teams.py --season 2026-27

Answers "who does this player play for right now", which the stats tables can't:
`player_seasons.team_abbr` only moves when a player takes the floor, so every
offseason trade is invisible until games start in late October.

Source is CommonAllPlayers for the upcoming season — one request for all ~580
players with their team, rather than thirty roster calls that can half-succeed.
The NBA publishes it as soon as teams are assigned, months before any stats
exist, which is exactly the window the ballot lives in.

Safety, because this writes a field the whole app reads:

  * It refuses to write a partial league. A response covering fewer than
    MIN_TEAMS teams or MIN_PLAYERS players is treated as an upstream hiccup and
    abandoned — a season that's only half-published would be worse than a stale
    one, since MAX(season) would then select it.
  * It only ever fills `current_team`. Nothing historical is touched.
  * Players absent from `players` are inserted (identity only, no stats), which
    is how two-way signings and undrafted rookies become pickable at all. Pass
    --no-insert to update in place only.

Runs from a residential IP (stats.nba.com blocks Railway), so this belongs in
daily_update_local.py. Cheap enough to run daily; a trade shows up the next day.
"""
import argparse
import os
import sys

from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from nba_api.stats.endpoints import commonallplayers
import season_util

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("DATABASE_URL not found."); sys.exit(1)

# A real league response is 30 teams / ~580 players. These are floors for
# "clearly complete", not targets.
MIN_TEAMS = 25
MIN_PLAYERS = 400


def fetch(season):
    df = commonallplayers.CommonAllPlayers(
        season=season, is_only_current_season=1, timeout=60).get_data_frames()[0]
    rows = []
    for _, r in df.iterrows():
        abbr = str(r.get("TEAM_ABBREVIATION") or "").strip()
        pid  = r.get("PERSON_ID")
        name = str(r.get("DISPLAY_FIRST_LAST") or "").strip()
        if not pid or not name or len(abbr) < 2:
            continue                      # free agents carry a blank team
        rows.append((int(pid), name, abbr))
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", default=None,
                    help="defaults to the upcoming season from the schedule")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--no-insert", action="store_true",
                    help="update existing players only; don't add missing ones")
    args = ap.parse_args()

    season = args.season or season_util.roster_season()
    print(f"Syncing player teams for {season}…")

    rows = fetch(season)
    teams = {abbr for _, _, abbr in rows}
    print(f"  {len(rows)} players across {len(teams)} teams")

    if len(teams) < MIN_TEAMS or len(rows) < MIN_PLAYERS:
        print(f"  Refusing to write: expected >= {MIN_TEAMS} teams and "
              f">= {MIN_PLAYERS} players. Upstream looks partial for {season}.")
        sys.exit(1)

    conn = psycopg2.connect(DATABASE_URL)
    cur  = conn.cursor(cursor_factory=RealDictCursor)

    # What would actually change, so a dry run is worth reading.
    cur.execute("""
        SELECT p.player_id, p.player_name, p.current_team,
               (SELECT ps.team_abbr FROM player_seasons ps
                 WHERE ps.player_id = p.player_id
                 ORDER BY ps.season DESC LIMIT 1) AS stats_team
        FROM players p
    """)
    known = {r["player_id"]: r for r in cur.fetchall()}

    moved, unchanged, missing = [], 0, []
    for pid, name, abbr in rows:
        row = known.get(pid)
        if row is None:
            missing.append((pid, name, abbr))
        elif (row["current_team"] or row["stats_team"]) != abbr:
            moved.append((name, row["current_team"] or row["stats_team"], abbr))
        else:
            unchanged += 1

    print(f"  {unchanged} unchanged, {len(moved)} moved, {len(missing)} not yet in players")
    for name, was, now in moved[:15]:
        print(f"    {name}: {was or '—'} -> {now}")
    if len(moved) > 15:
        print(f"    … and {len(moved) - 15} more")

    if args.dry_run:
        print("\n[dry run] nothing written.")
        conn.close()
        return

    written = inserted = 0
    with conn.cursor() as w:
        for pid, name, abbr in rows:
            if pid in known:
                w.execute("""
                    UPDATE players
                    SET current_team = %s, current_team_season = %s, current_team_at = NOW()
                    WHERE player_id = %s
                """, (abbr, season, pid))
                written += 1
            elif not args.no_insert:
                # Identity only. No stats are invented, and every stats surface
                # joins player_seasons, so these stay invisible until they play.
                w.execute("""
                    INSERT INTO players (player_id, player_name, is_active,
                                         current_team, current_team_season, current_team_at)
                    VALUES (%s, %s, TRUE, %s, %s, NOW())
                    ON CONFLICT (player_id) DO UPDATE
                        SET current_team = EXCLUDED.current_team,
                            current_team_season = EXCLUDED.current_team_season,
                            current_team_at = NOW()
                """, (pid, name, abbr, season))
                inserted += 1
    conn.commit()
    conn.close()
    print(f"\nUpdated {written} player(s), inserted {inserted}.")


if __name__ == "__main__":
    main()
