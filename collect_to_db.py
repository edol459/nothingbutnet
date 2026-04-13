"""
ydkball — Possession Data Collector (pbpstats edition)
=======================================================
Uses pbpstats (Darryl Blackport) instead of the custom possession pipeline:
  - One API call per game (pbpstats handles PBP fetching internally)
  - File caching in data/pbp_cache/ — re-runs skip already-downloaded games
  - assist_player_id comes directly from event.player2_id (no description parsing)
  - Lineup info from event.current_players (no separate GameRotation call)

Resume-safe: 'done' games are skipped; 'failed' games ARE retried on next run.

Usage:
    python collect_to_db.py --seasons 2024-25
    python collect_to_db.py --seasons 2023-24 2024-25
    python collect_to_db.py --seasons 2024-25 --delay 1.2
"""

import argparse
import logging
import os
import time
from pathlib import Path
from contextlib import contextmanager

import psycopg2
import psycopg2.extras
from nba_api.stats.endpoints import leaguegamelog
from dotenv import load_dotenv

from pbpstats.data_loader.live.possessions.web    import LivePossessionWebLoader
from pbpstats.data_loader.live.possessions.loader import LivePossessionLoader
from pbpstats.resources.enhanced_pbp.field_goal   import FieldGoal
from pbpstats.resources.enhanced_pbp.free_throw   import FreeThrow
from pbpstats.resources.enhanced_pbp.turnover     import Turnover
from pbpstats.resources.enhanced_pbp.rebound      import Rebound
from pbpstats.resources.enhanced_pbp.foul         import Foul
from pbpstats.resources.enhanced_pbp.substitution import Substitution
from pbpstats.resources.enhanced_pbp.timeout      import Timeout
from pbpstats.resources.enhanced_pbp.jump_ball    import JumpBall
from pbpstats.resources.enhanced_pbp.end_of_period   import EndOfPeriod
from pbpstats.resources.enhanced_pbp.start_of_period import StartOfPeriod

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

CACHE_DIR = Path("data/pbp_cache")


# ── DB helpers ─────────────────────────────────────────────────────────────────

def get_db_url() -> str:
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError(
            "DATABASE_URL not set. Export it first:\n"
            "  export $(grep -v '^#' .env | xargs)"
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


# ── Game ID fetching ───────────────────────────────────────────────────────────

def fetch_game_ids(season: str, delay: float) -> list[str]:
    log.info(f"Fetching game IDs for {season}...")
    df = leaguegamelog.LeagueGameLog(
        season=season,
        season_type_all_star="Regular Season",
        league_id="00",
        timeout=60,
    ).get_data_frames()[0]
    time.sleep(delay)
    game_ids = df["GAME_ID"].unique().tolist()
    log.info(f"  Found {len(game_ids)} games")
    return game_ids


# ── Progress tracking (only 'done' games are skipped; 'failed' are retried) ───

def get_done_games(season: str) -> set[str]:
    """Return game IDs already collected; 'failed' games are retried."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT game_id FROM collection_progress WHERE season = %s AND status = 'done'",
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
                    SET status     = EXCLUDED.status,
                        error_msg  = EXCLUDED.error_msg,
                        collected_at = NOW()
            """, (game_id, season, status, error_msg))


# ── Possession helpers ─────────────────────────────────────────────────────────

def _clock_to_game_seconds(period: int, clock_seconds: float) -> float:
    """Seconds elapsed in game (OT periods are 5 min each)."""
    if period <= 4:
        return (period - 1) * 720 + (720 - clock_seconds)
    return 4 * 720 + (period - 5) * 300 + (300 - clock_seconds)


def _get_action_type(event) -> str:
    if isinstance(event, FieldGoal):
        return "3pt" if event.shot_value == 3 else "2pt"
    if isinstance(event, FreeThrow):
        return "freethrow"
    if isinstance(event, Turnover):
        return "turnover"
    if isinstance(event, Rebound):
        return "rebound"
    if isinstance(event, Foul):
        return "foul"
    if isinstance(event, Substitution):
        return "substitution"
    if isinstance(event, Timeout):
        return "timeout"
    if isinstance(event, JumpBall):
        return "jumpball"
    if isinstance(event, StartOfPeriod):
        return "period"
    if isinstance(event, EndOfPeriod):
        return "period"
    return type(event).__name__.lower()


def _get_sub_type(event) -> str:
    if isinstance(event, FieldGoal):
        return "made" if event.is_made else "missed"
    if isinstance(event, FreeThrow):
        return "made" if event.is_made else "missed"
    if isinstance(event, Rebound):
        return "offensive" if event.oreb else "defensive"
    return ""


def _get_end_reason(possession) -> str:
    """Determine how a possession ended by inspecting events in reverse."""
    for event in reversed(possession.events):
        if isinstance(event, FieldGoal):
            return "made_fg" if event.is_made else "missed_fg"
        if isinstance(event, FreeThrow) and event.is_end_ft:
            return "freethrow"
        if isinstance(event, Turnover) and not event.is_no_turnover:
            return "turnover"
        if isinstance(event, Rebound) and event.is_real_rebound and not event.oreb:
            return "missed_fg"
        if isinstance(event, EndOfPeriod):
            return "end_period"
    return "end_period"


def _count_points(possession) -> int:
    """Sum points scored during this possession (FGs + FTs)."""
    total = 0
    for event in possession.events:
        if isinstance(event, FieldGoal) and event.is_made:
            total += event.shot_value
        elif isinstance(event, FreeThrow) and event.is_made:
            total += 1
    return total


def _get_lineups(possession) -> tuple[list[int], list[int]]:
    """Return (lineup_offense, lineup_defense) as lists of player_ids."""
    if not possession.events:
        return [], []
    players = possession.events[0].current_players   # {team_id: [player_ids]}
    offense_id = possession.offense_team_id
    lineup_off = list(players.get(offense_id, []))
    lineup_def = []
    for team_id, pids in players.items():
        if team_id != offense_id:
            lineup_def = list(pids)
            break
    return lineup_off, lineup_def


# ── DB writer ──────────────────────────────────────────────────────────────────

def write_possession(cur, possession, season: str, possession_number: int) -> int | None:
    """Insert one possession (+ events + lineups). Returns possession_id or None if skipped."""
    if not possession.events:
        return None

    game_id     = possession.events[0].game_id
    period      = possession.events[0].period
    start_clock = possession.events[0].seconds_remaining
    end_clock   = possession.events[-1].seconds_remaining
    game_secs   = _clock_to_game_seconds(period, start_clock)

    offense_id  = possession.offense_team_id
    lineup_off, lineup_def = _get_lineups(possession)

    # Get defense team_id from lineup (or infer from current_players keys)
    players_map = possession.events[0].current_players
    defense_id  = next((t for t in players_map if t != offense_id), 0)

    points    = _count_points(possession)
    end_reason = _get_end_reason(possession)
    margin    = possession.start_score_margin

    # 1. Insert possession row
    cur.execute("""
        INSERT INTO possessions (
            game_id, possession_number, season,
            offense_team_id, defense_team_id,
            period, start_clock_seconds, end_clock_seconds,
            game_seconds_start, score_margin_offense,
            points_scored, end_reason
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (game_id, possession_number) DO NOTHING
        RETURNING id
    """, (
        game_id, possession_number, season,
        offense_id, defense_id,
        period, start_clock, end_clock,
        game_secs, margin,
        points, end_reason,
    ))
    row = cur.fetchone()
    if row is None:
        cur.execute(
            "SELECT id FROM possessions WHERE game_id=%s AND possession_number=%s",
            (game_id, possession_number)
        )
        row = cur.fetchone()
    if row is None:
        return None
    possession_id = row[0]

    # 2. Insert events
    event_rows = []
    for idx, event in enumerate(possession.events):
        action_type = _get_action_type(event)
        sub_type    = _get_sub_type(event)

        shot_result = None
        is_fg       = False
        if isinstance(event, FieldGoal):
            shot_result = "Made" if event.is_made else "Missed"
            is_fg = True

        # Assist player ID is available directly — no description parsing needed
        assist_player_id = None
        if isinstance(event, FieldGoal) and event.is_assisted:
            assist_player_id = getattr(event, "player2_id", None)

        shot_dist = None
        if isinstance(event, FieldGoal):
            shot_dist = getattr(event, "distance", None)

        x_loc = getattr(event, "locX", None)
        y_loc = getattr(event, "locY", None)

        event_rows.append((
            possession_id,
            idx,
            getattr(event, "event_num", idx),
            action_type,
            sub_type,
            getattr(event, "description", ""),
            getattr(event, "player1_id", None),
            getattr(event, "team_id", None),
            event.seconds_remaining,
            _clock_to_game_seconds(event.period, event.seconds_remaining),
            shot_dist,
            shot_result,
            is_fg,
            x_loc,
            y_loc,
            assist_player_id,
        ))

    if event_rows:
        psycopg2.extras.execute_values(cur, """
            INSERT INTO possession_events (
                possession_id, event_index, action_number,
                action_type, sub_type, description,
                player_id, team_id,
                clock_seconds, game_seconds,
                shot_distance, shot_result, is_field_goal,
                x_legacy, y_legacy,
                assist_player_id
            ) VALUES %s
            ON CONFLICT DO NOTHING
        """, event_rows)

    # 3. Insert lineups
    lineup_rows = []
    for pid in lineup_off:
        lineup_rows.append((possession_id, int(pid), "offense", offense_id))
    for pid in lineup_def:
        lineup_rows.append((possession_id, int(pid), "defense", defense_id))

    if lineup_rows:
        psycopg2.extras.execute_values(cur, """
            INSERT INTO possession_lineups (possession_id, player_id, side, team_id)
            VALUES %s
            ON CONFLICT DO NOTHING
        """, lineup_rows)

    return possession_id


def write_game(possessions, season: str):
    """Write all possessions for a single game in one transaction."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            for i, p in enumerate(possessions):
                write_possession(cur, p, season, i)


# ── Per-game loader with retry ────────────────────────────────────────────────

def _load_game(game_id: str, season: str, source_loader, idx: int, total: int, delay: float):
    """Load and write one game. Permanently skips games where the NBA has
    deprecated the playbyplayv2 endpoint (returns empty JSON → KeyError 'resultSets')."""
    tag = f"[{idx+1}/{total}] {game_id}"
    try:
        possession_loader = LivePossessionLoader(game_id, source_loader)
        possessions = possession_loader.items
        write_game(possessions, season)
        mark_game(game_id, season, "done")
        log.info(f"  ✓ {tag} — {len(possessions)} possessions")
    except Exception as e:
        log.warning(f"  ✗ {tag} failed: {e}")
        mark_game(game_id, season, "failed", str(e)[:500])
    time.sleep(delay)


# ── Main collection loop ───────────────────────────────────────────────────────

def collect(seasons: list[str], delay: float = 1.0):
    # LivePossessionWebLoader caches PBP JSON to {dir}/pbp/live_<game_id>.json
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    (CACHE_DIR / "pbp").mkdir(exist_ok=True)

    # Shared loader — pulls from NBA's S3 CDN, caches locally for re-runs
    source_loader = LivePossessionWebLoader(file_directory=str(CACHE_DIR))

    for season in seasons:
        game_ids  = fetch_game_ids(season, delay)
        done      = get_done_games(season)
        remaining = [g for g in game_ids if g not in done]

        log.info(f"Season {season}: {len(remaining)} games to collect ({len(done)} already done)")

        for i, game_id in enumerate(remaining):
            _load_game(game_id, season, source_loader, i, len(remaining), delay)

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM possessions WHERE season = %s", (season,))
                total = cur.fetchone()[0]
        log.info(f"Season {season} complete — {total:,} total possessions in DB")


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--seasons", nargs="+", default=["2024-25"])
    parser.add_argument(
        "--delay", type=float, default=1.0,
        help="Seconds between API calls (increase if you hit rate limits)"
    )
    args = parser.parse_args()
    collect(args.seasons, args.delay)
