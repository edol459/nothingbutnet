"""
ydkball — Possession Data Collector (PostgreSQL)
=======================================================
Fetches play-by-play for every game in a season, runs the possession
pipeline, and writes results directly to your Railway PostgreSQL database.

Reads DATABASE_URL from environment (same as the rest of your project).

Usage:
    python collect_to_db.py --seasons 2024-25
    python collect_to_db.py --seasons 2024-25 --workers 1 --delay 0.8

Resume-safe: already-collected games are skipped via collection_progress table.
"""

import argparse
import logging
import os
import time
from contextlib import contextmanager

import psycopg2
import psycopg2.extras
from nba_api.stats.endpoints import leaguegamelog

from possession_pipeline import (
    build_possessions,
    make_nba_session,
    Possession,
    PbpEvent,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Database connection
# ---------------------------------------------------------------------------

def get_db_url() -> str:
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError(
            "DATABASE_URL environment variable not set.\n"
            "Export it before running:\n"
            "  export DATABASE_URL=postgresql://user:pass@host:port/dbname"
        )
    return url


@contextmanager
def get_conn():
    conn = psycopg2.connect(get_db_url())
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Game ID fetching
# ---------------------------------------------------------------------------

def fetch_game_ids(season: str, delay: float, session) -> list[str]:
    log.info(f"Fetching game IDs for {season}...")
    df = leaguegamelog.LeagueGameLog(
        season=season,
        season_type_all_star="Regular Season",
        league_id="00",
        headers=dict(session.headers),
        timeout=60,
    ).get_data_frames()[0]
    time.sleep(delay)
    game_ids = df["GAME_ID"].unique().tolist()
    log.info(f"  Found {len(game_ids)} games")
    return game_ids


# ---------------------------------------------------------------------------
# Progress tracking
# ---------------------------------------------------------------------------

def get_done_games(season: str) -> set[str]:
    """Return set of game_ids already in collection_progress for this season."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT game_id FROM collection_progress WHERE season = %s",
                (season,)
            )
            return {row[0] for row in cur.fetchall()}


def mark_game(game_id: str, season: str, status: str, error_msg: str = None):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO collection_progress (game_id, season, status, error_msg)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (game_id) DO UPDATE
                    SET status = EXCLUDED.status,
                        error_msg = EXCLUDED.error_msg,
                        collected_at = NOW()
            """, (game_id, season, status, error_msg))


# ---------------------------------------------------------------------------
# DB writer
# ---------------------------------------------------------------------------

def write_possession(cur, possession: Possession, season: str) -> int:
    """
    Insert one Possession into the DB (possessions + events + lineups).
    Returns the new possession.id.
    """
    # 1. Insert possession row
    cur.execute("""
        INSERT INTO possessions (
            game_id, possession_number, season,
            offense_team_id, defense_team_id,
            period, start_clock_seconds, end_clock_seconds,
            game_seconds_start, score_margin_offense,
            points_scored, end_reason
        ) VALUES (
            %s, %s, %s,
            %s, %s,
            %s, %s, %s,
            %s, %s,
            %s, %s
        )
        ON CONFLICT (game_id, possession_number) DO NOTHING
        RETURNING id
    """, (
        possession.game_id,
        possession.possession_number,
        season,
        possession.offense_team_id,
        possession.defense_team_id,
        possession.period,
        possession.start_clock_seconds,
        possession.end_clock_seconds,
        possession.game_seconds_start,
        possession.score_margin_offense,
        possession.points_scored,
        possession.end_reason,
    ))

    row = cur.fetchone()
    if row is None:
        # Already exists (ON CONFLICT DO NOTHING) — fetch the existing id
        cur.execute(
            "SELECT id FROM possessions WHERE game_id = %s AND possession_number = %s",
            (possession.game_id, possession.possession_number)
        )
        row = cur.fetchone()

    possession_id = row[0]

    # 2. Insert events
    event_rows = []
    for idx, event in enumerate(possession.events):
        event_rows.append((
            possession_id,
            idx,
            event.action_number,
            event.action_type,
            event.sub_type or "",
            event.description or "",
            event.player_id,
            event.team_id,
            event.clock_seconds,
            event.game_seconds,
            event.shot_distance,
            event.shot_result,
            event.is_field_goal,
            event.x_legacy,
            event.y_legacy,
        ))

    if event_rows:
        psycopg2.extras.execute_values(cur, """
            INSERT INTO possession_events (
                possession_id, event_index, action_number,
                action_type, sub_type, description,
                player_id, team_id,
                clock_seconds, game_seconds,
                shot_distance, shot_result, is_field_goal,
                x_legacy, y_legacy
            ) VALUES %s
            ON CONFLICT DO NOTHING
        """, event_rows)

    # 3. Insert lineups
    lineup_rows = []
    for player_id in possession.lineup_offense:
        lineup_rows.append((possession_id, player_id, "offense", possession.offense_team_id))
    for player_id in possession.lineup_defense:
        lineup_rows.append((possession_id, player_id, "defense", possession.defense_team_id))

    if lineup_rows:
        psycopg2.extras.execute_values(cur, """
            INSERT INTO possession_lineups (possession_id, player_id, side, team_id)
            VALUES %s
            ON CONFLICT DO NOTHING
        """, lineup_rows)

    return possession_id


def write_game(possessions: list[Possession], season: str):
    """Write all possessions for a game in a single transaction."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            for p in possessions:
                write_possession(cur, p, season)


# ---------------------------------------------------------------------------
# Main collection loop
# ---------------------------------------------------------------------------

def collect(seasons: list[str], delay: float = 0.8):
    session = make_nba_session()

    for season in seasons:
        game_ids = fetch_game_ids(season, delay, session)
        done = get_done_games(season)
        remaining = [g for g in game_ids if g not in done]

        log.info(f"Season {season}: {len(remaining)} games to collect ({len(done)} already done)")

        for i, game_id in enumerate(remaining):
            try:
                possessions = build_possessions(game_id, delay=delay, session=session)
                write_game(possessions, season)
                mark_game(game_id, season, "done")
                log.info(f"  [{i+1}/{len(remaining)}] ✓ {game_id} — {len(possessions)} possessions")

            except Exception as e:
                log.warning(f"  [{i+1}/{len(remaining)}] ✗ {game_id} failed: {e}")
                mark_game(game_id, season, "failed", str(e)[:500])

            time.sleep(delay)

        # Summary
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM possessions WHERE season = %s", (season,)
                )
                total = cur.fetchone()[0]
        log.info(f"Season {season} complete — {total:,} total possessions in DB")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--seasons", nargs="+", default=["2024-25"],
        help='e.g. "2024-25" or "2022-23 2023-24 2024-25"'
    )
    parser.add_argument("--delay", type=float, default=0.8)
    args = parser.parse_args()
    collect(args.seasons, args.delay)