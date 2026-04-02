"""
NothingButNet — Possession Stitching Pipeline
==============================================
Converts raw PlayByPlayV3 events + GameRotation data into structured
possession objects, each with:
  - Ordered event sequence
  - Players on floor (both teams)
  - Game state at start/end
  - Outcome (points scored)
  - Per-event EV delta hooks (filled later by the model)

Usage:
    from possession_pipeline import build_possessions
    possessions = build_possessions(game_id="0022300001")
"""

import time
import logging
import requests
from dataclasses import dataclass, field
from typing import Optional
from nba_api.stats.endpoints import playbyplayv3, gamerotation

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# NBA API session (mirrors your existing fetch_nba_stats.py approach)
# ---------------------------------------------------------------------------

NBA_HEADERS = {
    'User-Agent':          'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    'Referer':             'https://www.nba.com/',
    'Origin':              'https://www.nba.com',
    'Accept':              'application/json, text/plain, */*',
    'Accept-Language':     'en-US,en;q=0.9',
    'Accept-Encoding':     'gzip, deflate, br',
    'Connection':          'keep-alive',
    'Host':                'stats.nba.com',
    'x-nba-stats-origin':  'stats',
    'x-nba-stats-token':   'true',
    'sec-ch-ua':           '"Google Chrome";v="123", "Not:A-Brand";v="8"',
    'sec-ch-ua-mobile':    '?0',
    'sec-ch-ua-platform':  '"macOS"',
    'sec-fetch-dest':      'empty',
    'sec-fetch-mode':      'cors',
    'sec-fetch-site':      'same-site',
}


def make_nba_session() -> requests.Session:
    """
    Visit nba.com to establish a session with cookies, then return it.
    Pass this session to build_possessions() so every API call uses it.
    """
    session = requests.Session()
    session.headers.update(NBA_HEADERS)
    try:
        log.info("Warming up NBA session...")
        session.get('https://www.nba.com/', timeout=30)
        time.sleep(2)
        session.get('https://www.nba.com/stats/', timeout=30)
        time.sleep(2)
        log.info(f"Session ready ({len(session.cookies)} cookies)")
    except Exception as e:
        log.warning(f"Session warmup issue (continuing anyway): {e}")
    return session

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class PbpEvent:
    """A single raw play-by-play event, cleaned and typed."""
    action_number: int
    action_id: int
    period: int
    clock_str: str          # "PT08M23.00S"
    clock_seconds: float    # seconds remaining in period
    game_seconds: float     # seconds elapsed in game (for rotation lookup)
    team_id: Optional[int]
    player_id: Optional[int]
    player_name: Optional[str]
    action_type: str        # "2pt", "3pt", "freethrow", "turnover", "foul", "rebound", "substitution", etc.
    sub_type: Optional[str] # "missed", "made", "offensive", "defensive", "away from play", etc.
    description: str
    shot_distance: Optional[float]
    shot_result: Optional[str]  # "Made" | "Missed"
    is_field_goal: bool
    score_home: Optional[int]
    score_away: Optional[int]
    x_legacy: Optional[float]
    y_legacy: Optional[float]


@dataclass
class Possession:
    """
    A single offensive possession.
    offense_team_id: the team that has the ball.
    events: ordered list of PbpEvents that occurred during this possession.
    lineup_offense: list of 5 player_ids for the offensive team.
    lineup_defense: list of 5 player_ids for the defensive team.
    start_score_offense / defense: score at possession start.
    points_scored: actual outcome (0, 1, 2, or 3).
    end_reason: how the possession ended.
    game_state_*: contextual fields used as model features.
    ev_deltas: filled later by model — {player_id: delta} per event.
    """
    game_id: str
    possession_number: int
    period: int
    start_clock_seconds: float
    end_clock_seconds: float
    game_seconds_start: float

    offense_team_id: int
    defense_team_id: int

    lineup_offense: list[int] = field(default_factory=list)   # 5 player_ids
    lineup_defense: list[int] = field(default_factory=list)   # 5 player_ids

    events: list[PbpEvent] = field(default_factory=list)
    lineup_offense: list[int] = field(default_factory=list)
    lineup_defense: list[int] = field(default_factory=list)

    # Outcome
    points_scored: int = 0
    end_reason: str = ""   # "made_fg", "missed_fg", "turnover", "foul", "end_period"

    # Game state features (populated during stitching)
    score_margin_offense: int = 0   # offense score minus defense score at possession start
    shot_clock_at_shot: Optional[float] = None
    num_ft_attempts: int = 0

    # Filled by model layer later
    ev_deltas: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Clock parsing
# ---------------------------------------------------------------------------

def parse_clock(clock_str: str) -> float:
    """
    Convert NBA clock string "PT08M23.00S" → seconds remaining in period.
    Also handles plain "MM:SS" format as fallback.
    """
    if not clock_str:
        return 0.0
    try:
        if clock_str.startswith("PT"):
            # ISO 8601 duration: PT08M23.00S
            clock_str = clock_str[2:]  # strip PT
            minutes = 0.0
            seconds = 0.0
            if "M" in clock_str:
                parts = clock_str.split("M")
                minutes = float(parts[0])
                seconds = float(parts[1].rstrip("S")) if parts[1].rstrip("S") else 0.0
            else:
                seconds = float(clock_str.rstrip("S"))
            return minutes * 60 + seconds
        elif ":" in clock_str:
            parts = clock_str.split(":")
            return float(parts[0]) * 60 + float(parts[1])
    except Exception:
        pass
    return 0.0


def clock_to_game_seconds(period: int, clock_seconds: float) -> float:
    """
    Convert period + clock_seconds_remaining → total seconds elapsed in game.
    Regulation periods are 12 min (720s). OT periods are 5 min (300s).
    """
    if period <= 4:
        elapsed_in_period = 720 - clock_seconds
        return (period - 1) * 720 + elapsed_in_period
    else:
        # Overtime
        elapsed_in_period = 300 - clock_seconds
        return 4 * 720 + (period - 5) * 300 + elapsed_in_period


# ---------------------------------------------------------------------------
# Action type normalization
# ---------------------------------------------------------------------------

# Maps PlayByPlayV3 actionType values → our normalized categories
POSSESSION_ENDING_ACTIONS = {
    "2pt",
    "3pt",
    "turnover",
    "jumpball",  # treat as turnover-like
}

FREE_THROW_ACTIONS = {"freethrow"}

# Actions that don't end a possession
NON_POSSESSION_ACTIONS = {
    "substitution",
    "timeout",
    "period",
    "game",
    "replay",
    "stoppage",
    "challenge",
    "violation",  # most violations end possession but handled below
}

def is_possession_ending(event: PbpEvent) -> bool:
    """Determine if this event ends the current offensive possession."""
    at = event.action_type.lower()

    if at in ("2pt", "3pt"):
        # Made FG ends possession; missed FG only ends if not offensive rebound
        # (offensive rebound continuation handled in stitcher)
        return True

    if at == "turnover":
        return True

    if at == "freethrow":
        # Only the last FT in a sequence ends possession
        # We detect this via sub_type containing "1 of 1", "2 of 2", "3 of 3", or "technical"
        st = (event.sub_type or "").lower()
        if any(x in st for x in ["1 of 1", "2 of 2", "3 of 3", "free throw technical"]):
            return True
        # Last FT in a sequence (e.g. "2 of 2")
        import re
        m = re.search(r"(\d+) of (\d+)", st)
        if m and m.group(1) == m.group(2):
            return True
        return False

    if at == "violation":
        st = (event.sub_type or "").lower()
        # Offensive violations end the possession
        if any(x in st for x in ["offensive", "kicked ball", "lane"]):
            return True
        return False

    if at == "jumpball":
        return True

    return False


def is_offensive_rebound(event: PbpEvent, offense_team_id: int) -> bool:
    return (
        event.action_type.lower() == "rebound"
        and event.team_id == offense_team_id
        and (event.sub_type or "").lower() == "offensive"
    )


# ---------------------------------------------------------------------------
# Rotation → lineup resolver
# ---------------------------------------------------------------------------

def build_rotation_index(rotation_df):
    """
    Given a GameRotation dataframe (home or away), build a list of
    (player_id, in_seconds, out_seconds) intervals.
    IN_TIME_REAL and OUT_TIME_REAL are in tenths of seconds elapsed.
    """
    intervals = []
    for _, row in rotation_df.iterrows():
        intervals.append({
            "player_id": int(row["PERSON_ID"]),
            "in_seconds": row["IN_TIME_REAL"] / 10.0,
            "out_seconds": row["OUT_TIME_REAL"] / 10.0,
        })
    return intervals


def get_lineup_at(intervals: list[dict], game_seconds: float) -> list[int]:
    """
    Return list of player_ids on the floor at game_seconds elapsed.
    Handles edge cases where rotation data is slightly misaligned.
    """
    on_floor = [
        i["player_id"] for i in intervals
        if i["in_seconds"] <= game_seconds < i["out_seconds"]
    ]
    return on_floor


# ---------------------------------------------------------------------------
# Raw event parsing
# ---------------------------------------------------------------------------

def parse_events(pbp_df) -> list[PbpEvent]:
    """Convert raw PlayByPlayV3 dataframe rows into typed PbpEvent objects."""
    events = []
    for _, row in pbp_df.iterrows():
        period = int(row["period"])
        clock_str = row.get("clock", "") or ""
        clock_seconds = parse_clock(clock_str)
        game_seconds = clock_to_game_seconds(period, clock_seconds)

        # Score parsing — may be None early in game
        def _score(val):
            try:
                return int(val) if val is not None and str(val).strip() != "" else None
            except (ValueError, TypeError):
                return None

        events.append(PbpEvent(
            action_number=int(row["actionNumber"]),
            action_id=int(row["actionId"]) if row.get("actionId") else 0,
            period=period,
            clock_str=clock_str,
            clock_seconds=clock_seconds,
            game_seconds=game_seconds,
            team_id=int(row["teamId"]) if row.get("teamId") else None,
            player_id=int(row["personId"]) if row.get("personId") else None,
            player_name=row.get("playerName") or row.get("playerNameI"),
            action_type=(row.get("actionType") or "").strip().lower(),
            sub_type=(row.get("subType") or "").strip().lower(),
            description=row.get("description") or "",
            shot_distance=float(row["shotDistance"]) if row.get("shotDistance") else None,
            shot_result=row.get("shotResult"),
            is_field_goal=bool(row.get("isFieldGoal")),
            score_home=_score(row.get("scoreHome")),
            score_away=_score(row.get("scoreAway")),
            x_legacy=float(row["xLegacy"]) if row.get("xLegacy") else None,
            y_legacy=float(row["yLegacy"]) if row.get("yLegacy") else None,
        ))
    return events


# ---------------------------------------------------------------------------
# Possession stitcher
# ---------------------------------------------------------------------------

def stitch_possessions(
    game_id: str,
    events: list[PbpEvent],
    home_intervals: list[dict],
    away_intervals: list[dict],
    home_team_id: int,
    away_team_id: int,
) -> list[Possession]:
    """
    Walk through events in order and group them into possessions.

    Key rules:
    - Possession starts on: tip-off, change of possession (made FG, turnover,
      defensive rebound, last FT made/missed), start of period.
    - Offensive rebound CONTINUES the same possession (does not start new one).
    - Free throw sequences count as one possession (the shooting team's).
    - Jump balls: treat won jump ball as start of new possession for winning team.
    - We track current offensive team by watching who gains possession.
    """
    possessions = []
    possession_number = 0

    # Determine starting team from tip-off (first jump ball)
    # Default to home team if we can't determine
    current_offense_id = home_team_id
    current_defense_id = away_team_id

    current_events: list[PbpEvent] = []
    current_start_clock: float = 720.0
    current_start_game_seconds: float = 0.0
    current_period: int = 1
    current_score_home: int = 0
    current_score_away: int = 0

    def flush_possession(end_event: PbpEvent, reason: str, points: int):
        nonlocal possession_number, current_events, current_start_clock, current_start_game_seconds

        if not current_events and not end_event:
            return

        all_events = current_events[:]
        if end_event and (not all_events or all_events[-1].action_number != end_event.action_number):
            all_events.append(end_event)

        if not all_events:
            return

        gs = current_start_game_seconds
        lineup_off = get_lineup_at(
            home_intervals if current_offense_id == home_team_id else away_intervals, gs
        )
        lineup_def = get_lineup_at(
            away_intervals if current_offense_id == home_team_id else home_intervals, gs
        )

        # Score margin from offense perspective
        if current_offense_id == home_team_id:
            margin = current_score_home - current_score_away
        else:
            margin = current_score_away - current_score_home

        p = Possession(
            game_id=game_id,
            possession_number=possession_number,
            period=current_period,
            start_clock_seconds=current_start_clock,
            end_clock_seconds=end_event.clock_seconds if end_event else current_start_clock,
            game_seconds_start=gs,
            offense_team_id=current_offense_id,
            defense_team_id=current_defense_id,
            lineup_offense=lineup_off,
            lineup_defense=lineup_def,
            events=all_events,
            points_scored=points,
            end_reason=reason,
            score_margin_offense=margin,
        )
        possessions.append(p)
        possession_number += 1
        current_events = []

    def start_new_possession(offense_id: int, defense_id: int, event: PbpEvent):
        nonlocal current_offense_id, current_defense_id
        nonlocal current_start_clock, current_start_game_seconds, current_period
        current_offense_id = offense_id
        current_defense_id = defense_id
        current_start_clock = event.clock_seconds
        current_start_game_seconds = event.game_seconds
        current_period = event.period

    # Track FT sequences: (num_made, num_attempted, shooting_team_id)
    in_ft_sequence = False
    ft_made = 0
    ft_team_id = None

    for i, event in enumerate(events):
        at = event.action_type
        st = event.sub_type or ""

        # Update running score from event
        if event.score_home is not None:
            current_score_home = event.score_home
        if event.score_away is not None:
            current_score_away = event.score_away

        # --- Period boundary ---
        if at in ("period", "game"):
            if current_events:
                flush_possession(event, "end_period", 0)
            # Reset for new period
            if "start" in st or "begin" in st or at == "period":
                current_period = event.period
                current_start_clock = 720.0 if event.period <= 4 else 300.0
                current_start_game_seconds = event.game_seconds
            continue

        # --- Jump ball ---
        if at == "jumpball":
            if "won" in st and event.team_id:
                if current_events:
                    flush_possession(event, "jumpball", 0)
                winner_id = event.team_id
                loser_id = away_team_id if winner_id == home_team_id else home_team_id
                start_new_possession(winner_id, loser_id, event)
            continue

        # --- Substitution / timeout / stoppage — add to current possession context but don't end it ---
        if at in ("substitution", "timeout", "stoppage", "replay", "challenge"):
            current_events.append(event)
            continue

        # --- Free throws ---
        if at == "freethrow":
            import re
            if not in_ft_sequence:
                # Starting a new FT sequence — flush current possession first
                if current_events:
                    flush_possession(None, "fouled", 0)
                in_ft_sequence = True
                ft_made = 0
                ft_team_id = event.team_id
                # FT shooting team is on offense for this possession
                if ft_team_id:
                    def_id = away_team_id if ft_team_id == home_team_id else home_team_id
                    start_new_possession(ft_team_id, def_id, event)

            current_events.append(event)
            ft_made += (1 if (event.shot_result or "").lower() == "made" else 0)

            # Check if last FT in sequence
            m = re.search(r"(\d+) of (\d+)", st)
            if m and m.group(1) == m.group(2):
                flush_possession(event, "freethrow", ft_made)
                in_ft_sequence = False
                ft_made = 0
                # Defense gets the ball after FTs
                new_off = away_team_id if ft_team_id == home_team_id else home_team_id
                new_def = ft_team_id
                start_new_possession(new_off, new_def, event)
            continue

        # --- Field goal attempts ---
        if at in ("2pt", "3pt"):
            current_events.append(event)
            shot_value = 2 if at == "2pt" else 3
            made = (event.shot_result or "").lower() == "made"

            if made:
                flush_possession(event, "made_fg", shot_value)
                # Defense gets ball
                new_off = away_team_id if current_offense_id == home_team_id else home_team_id
                new_def = current_offense_id
                start_new_possession(new_off, new_def, event)
            # If missed, wait to see if offensive or defensive rebound
            continue

        # --- Rebounds ---
        if at == "rebound":
            current_events.append(event)
            if (st == "offensive") and event.team_id == current_offense_id:
                # Offensive rebound — possession continues, don't flush
                pass
            else:
                # Defensive rebound or team rebound — ends possession
                flush_possession(event, "missed_fg", 0)
                # Rebounding team gets the ball
                new_off = event.team_id if event.team_id else (
                    away_team_id if current_offense_id == home_team_id else home_team_id
                )
                new_def = away_team_id if new_off == home_team_id else home_team_id
                start_new_possession(new_off, new_def, event)
            continue

        # --- Turnovers ---
        if at == "turnover":
            current_events.append(event)
            flush_possession(event, "turnover", 0)
            # Defense gets ball
            new_off = current_defense_id
            new_def = current_offense_id
            start_new_possession(new_off, new_def, event)
            continue

        # --- Fouls (non-shooting) — just add context, FTs handled separately ---
        if at == "foul":
            current_events.append(event)
            continue

        # --- Violations ---
        if at == "violation":
            current_events.append(event)
            if any(x in st for x in ["offensive", "kicked", "lane"]):
                flush_possession(event, "violation", 0)
                new_off = current_defense_id
                new_def = current_offense_id
                start_new_possession(new_off, new_def, event)
            continue

        # --- Everything else ---
        current_events.append(event)

    # Flush any remaining events
    if current_events:
        flush_possession(None, "end_game", 0)

    return possessions


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def build_possessions(game_id: str, delay: float = 0.6, session: requests.Session = None) -> list[Possession]:
    """
    Fetch raw data for a game and return a list of structured Possession objects.

    Args:
        game_id: NBA game ID string e.g. "0022300001"
        delay: seconds to sleep between API calls (be polite to stats.nba.com)
    """
    headers = dict(session.headers) if session else NBA_HEADERS

    log.info(f"Fetching play-by-play for game {game_id}...")
    pbp_df = playbyplayv3.PlayByPlayV3(
        game_id=game_id,
        headers=headers,
        timeout=60,
    ).get_data_frames()[0]
    time.sleep(delay)

    log.info(f"Fetching rotation data for game {game_id}...")
    rot = gamerotation.GameRotation(
        game_id=game_id,
        headers=headers,
        timeout=60,
    )
    home_rot_df = rot.home_team.get_data_frame()
    away_rot_df = rot.away_team.get_data_frame()
    time.sleep(delay)

    # Derive team IDs from rotation data
    home_team_id = int(home_rot_df["TEAM_ID"].iloc[0])
    away_team_id = int(away_rot_df["TEAM_ID"].iloc[0])

    home_intervals = build_rotation_index(home_rot_df)
    away_intervals = build_rotation_index(away_rot_df)

    log.info(f"Parsing {len(pbp_df)} events...")
    events = parse_events(pbp_df)

    log.info(f"Stitching possessions...")
    possessions = stitch_possessions(
        game_id=game_id,
        events=events,
        home_intervals=home_intervals,
        away_intervals=away_intervals,
        home_team_id=home_team_id,
        away_team_id=away_team_id,
    )

    log.info(f"Done — {len(possessions)} possessions built.")
    return possessions


# ---------------------------------------------------------------------------
# Serialization helper (for saving training data)
# ---------------------------------------------------------------------------

def possession_to_dict(p: Possession) -> dict:
    """Convert a Possession to a JSON-serializable dict for storage."""
    return {
        "game_id": p.game_id,
        "possession_number": p.possession_number,
        "period": p.period,
        "start_clock_seconds": p.start_clock_seconds,
        "end_clock_seconds": p.end_clock_seconds,
        "game_seconds_start": p.game_seconds_start,
        "offense_team_id": p.offense_team_id,
        "defense_team_id": p.defense_team_id,
        "lineup_offense": p.lineup_offense,
        "lineup_defense": p.lineup_defense,
        "points_scored": p.points_scored,
        "end_reason": p.end_reason,
        "score_margin_offense": p.score_margin_offense,
        "events": [
            {
                "action_number": e.action_number,
                "action_type": e.action_type,
                "sub_type": e.sub_type,
                "player_id": e.player_id,
                "player_name": e.player_name,
                "team_id": e.team_id,
                "clock_seconds": e.clock_seconds,
                "game_seconds": e.game_seconds,
                "shot_distance": e.shot_distance,
                "shot_result": e.shot_result,
                "is_field_goal": e.is_field_goal,
                "description": e.description,
                "x_legacy": e.x_legacy,
                "y_legacy": e.y_legacy,
            }
            for e in p.events
        ],
    }