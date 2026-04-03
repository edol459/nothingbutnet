"""
ydkball — Leverage-Filtered Lineup Stats via Play-by-Play
================================================================
python backend/fetch_lineups_pbp.py [--season 2025-26] [--team BOS]

Uses play-by-play + rotation data to compute lineup stats with garbage time
removed, then updates team_lineups.min_lev / ortg_lev / drtg_lev / net_lev.

Garbage time definition (Cleaning the Glass — 4th quarter only, irreversible):
  · ≥25-pt margin with 9:00–12:00 remaining in Q4
  · ≥20-pt margin with 6:00–9:00  remaining in Q4
  · ≥10-pt margin with 0:00–6:00  remaining in Q4
Once triggered within Q4 it does not revert even if the lead shrinks.

Lineup tracking uses GameRotation (explicit stint timestamps) rather than
substitution events — more reliable and avoids PBP sub-parsing quirks.

Per-lineup ORtg/DRtg computed from PBP event counts:
  possessions ≈ FGA − OREB + TOV + 0.44 × FTA  (Dean Oliver formula)

Runtime: ~3–5 min per team (≈80 games × 2 API calls each).
"""
import os, sys, re, time, math, argparse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
import psycopg2, psycopg2.extras

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("DATABASE_URL not found."); sys.exit(1)

TEAM_IDS = {
    "ATL":1610612737,"BOS":1610612738,"BKN":1610612751,"CHA":1610612766,
    "CHI":1610612741,"CLE":1610612739,"DAL":1610612742,"DEN":1610612743,
    "DET":1610612765,"GSW":1610612744,"HOU":1610612745,"IND":1610612754,
    "LAC":1610612746,"LAL":1610612747,"MEM":1610612763,"MIA":1610612748,
    "MIL":1610612749,"MIN":1610612750,"NOP":1610612740,"NYK":1610612752,
    "OKC":1610612760,"ORL":1610612753,"PHI":1610612755,"PHX":1610612756,
    "POR":1610612757,"SAC":1610612758,"SAS":1610612759,"TOR":1610612761,
    "UTA":1610612762,"WAS":1610612764,
}

# ── Time helpers ───────────────────────────────────────────────────────────────

def parse_clock(clock_str):
    """'PT12M00.00S' → seconds remaining in period (float)."""
    m = re.match(r'PT(\d+)M([\d.]+)S', str(clock_str))
    if m:
        return int(m.group(1)) * 60 + float(m.group(2))
    return 0.0


def event_to_game_seconds(period, secs_left_in_period):
    """
    Convert PBP event time to elapsed seconds from tip-off.
    GameRotation uses the same coordinate system (tenths of seconds / 10).
    """
    if period <= 4:
        return (period - 1) * 720.0 + (720.0 - secs_left_in_period)
    else:
        ot_num = period - 4          # 1 for OT1, 2 for OT2, …
        return 2880.0 + (ot_num - 1) * 300.0 + (300.0 - secs_left_in_period)


def is_garbage_time(period, secs_left, margin):
    """
    Cleaning the Glass garbage time. Q4 only; irreversible once triggered.
    margin — absolute point differential (always positive).
    """
    if period != 4:
        return False
    if secs_left > 9 * 60:    # 9:00–12:00 remaining
        return margin >= 25
    if secs_left > 6 * 60:    # 6:00–9:00 remaining
        return margin >= 20
    return margin >= 10        # 0:00–6:00 remaining


def safe_round(v, digits=1):
    try:
        f = float(v)
        return None if (math.isnan(f) or math.isinf(f)) else round(f, digits)
    except Exception:
        return None


# ── Lineup lookup from GameRotation ───────────────────────────────────────────

def _fetch_with_retry(fn, retries=3, base_sleep=5.0):
    """
    Call fn() up to `retries` times.
    - Timeout/connection errors: retry with backoff.
    - Empty-response JSON errors: skip immediately (retrying won't help).
    """
    import json as _json
    for attempt in range(retries):
        try:
            return fn()
        except (_json.JSONDecodeError, ValueError) as e:
            # Empty or malformed response — NBA API has no data for this game.
            raise
        except Exception as e:
            if attempt == retries - 1:
                raise
            wait = base_sleep * (2 ** attempt)   # 5s, 10s, 20s
            print(f"      retry {attempt+1}/{retries-1} after {wait:.0f}s ({e})", flush=True)
            time.sleep(wait)


def build_rotation(game_id, team_id):
    """
    Returns list of (player_id_str, in_seconds, out_seconds) for the given team.
    GameRotation IN_TIME_REAL / OUT_TIME_REAL are in tenths of a second from tip.
    """
    from nba_api.stats.endpoints import GameRotation
    gr_dfs = _fetch_with_retry(
        lambda: GameRotation(game_id=game_id, timeout=60).get_data_frames()
    )
    stints = []
    for df in gr_dfs:
        if df.empty:
            continue
        team_rows = df[df["TEAM_ID"] == team_id]
        if team_rows.empty:
            continue
        for _, row in team_rows.iterrows():
            in_s  = float(row["IN_TIME_REAL"])  / 10.0
            out_s = float(row["OUT_TIME_REAL"]) / 10.0
            stints.append((str(int(row["PERSON_ID"])), in_s, out_s))
    return stints


def get_starters_v3(game_id, team_id_int):
    """
    Returns frozenset of 5 starter player-ID strings using BoxScoreTraditionalV3.
    Non-empty `position` field identifies starters.
    """
    from nba_api.stats.endpoints import BoxScoreTraditionalV3
    bs_df = _fetch_with_retry(
        lambda: BoxScoreTraditionalV3(game_id=game_id, timeout=60).get_data_frames()[0]
    )
    starters = frozenset(
        str(int(row["personId"]))
        for _, row in bs_df.iterrows()
        if int(row.get("teamId", 0) or 0) == team_id_int
        and str(row.get("position", "") or "").strip()
    )
    return starters if len(starters) == 5 else None


def build_rotation_from_pbp(pbp_df, starters, team_id_int):
    """
    Fallback when GameRotation is unavailable.
    Reconstructs player stints from PlayByPlayV3 substitution events.

    Each sub event has two rows with the same description ("SUB: X FOR Y"):
      - The row where playerName appears after "FOR" → player going OUT
      - The other row → player coming IN
    Returns list of (player_id_str, in_seconds, out_seconds).
    """
    current_lineup  = set(starters)
    current_in_time = {pid: 0.0 for pid in starters}
    all_stints      = []

    for _, ev in pbp_df.iterrows():
        if str(ev.get("actionType", "") or "") != "Substitution":
            continue
        if int(ev.get("teamId", 0) or 0) != team_id_int:
            continue

        pid         = str(int(ev.get("personId", 0) or 0))
        player_name = str(ev.get("playerName", "") or "")
        desc        = str(ev.get("description", "") or "")
        secs_left   = parse_clock(ev.get("clock", "PT0M0.00S"))
        gs          = event_to_game_seconds(int(ev.get("period", 1) or 1), secs_left)

        if " FOR " not in desc:
            continue

        after_for = desc.split(" FOR ", 1)[-1].strip()
        going_out = player_name.strip() in after_for

        if going_out:
            if pid in current_lineup:
                all_stints.append((pid, current_in_time.get(pid, 0.0), gs))
                current_lineup.discard(pid)
                current_in_time.pop(pid, None)
        else:
            if pid not in current_lineup:
                current_lineup.add(pid)
                current_in_time[pid] = gs

    # Close remaining stints at the last event's timestamp
    if not pbp_df.empty:
        last = pbp_df.iloc[-1]
        game_end = event_to_game_seconds(
            int(last.get("period", 4) or 4),
            parse_clock(last.get("clock", "PT0M0.00S")),
        )
    else:
        game_end = 2880.0

    for pid in current_lineup:
        all_stints.append((pid, current_in_time.get(pid, 0.0), game_end))

    return all_stints


def lineup_at(stints, game_seconds):
    """Return frozenset of 5 player-ID strings on court at game_seconds, or None."""
    on_court = frozenset(
        pid for pid, in_s, out_s in stints
        if in_s <= game_seconds < out_s
    )
    return on_court if len(on_court) == 5 else None


# ── Single-game PBP processing ─────────────────────────────────────────────────

def process_game(game_id, team_id, is_home):
    """
    Returns dict mapping frozenset(player_id_strings) → stats dict:
        secs, pts_for, pts_against,
        fga_for, oreb_for, tov_for, fta_for,
        fga_against, oreb_against, tov_against, fta_against
    Only non-garbage-time stints are counted.
    """
    from nba_api.stats.endpoints import PlayByPlayV3

    team_id_int = int(team_id)

    # ── Play-by-play (always needed) ──────────────────────────────
    try:
        pbp_df = _fetch_with_retry(
            lambda: PlayByPlayV3(game_id=game_id, timeout=60).get_data_frames()[0]
        )
        time.sleep(0.5)
    except Exception as e:
        print(f"      pbp err (skipping): {e}", flush=True)
        time.sleep(2.0)
        return {}

    if pbp_df.empty:
        return {}

    # ── GameRotation for lineup tracking (PBP fallback if unavailable) ──
    stints = None
    try:
        stints = build_rotation(game_id, team_id_int)
        time.sleep(0.5)
    except Exception:
        time.sleep(0.5)

    if not stints:
        # GameRotation unavailable — reconstruct from BoxScore + PBP subs
        try:
            starters = get_starters_v3(game_id, team_id_int)
            time.sleep(0.5)
        except Exception:
            starters = None
            time.sleep(0.5)

        if starters:
            stints = build_rotation_from_pbp(pbp_df, starters, team_id_int)
            print(f"      (pbp fallback: {len(stints)} stints)", end=" ", flush=True)

    if not stints:
        return {}

    # ── Event loop ─────────────────────────────────────────────────
    lineup_stats = defaultdict(lambda: {
        'secs': 0.0,
        'pts_for': 0, 'pts_against': 0,
        'fga_for': 0, 'oreb_for': 0, 'tov_for': 0, 'fta_for': 0,
        'fga_against': 0, 'oreb_against': 0, 'tov_against': 0, 'fta_against': 0,
    })

    score_home    = 0
    score_away    = 0
    garbage       = False
    current_period = 0
    prev_gs       = 0.0          # game_seconds at previous event
    prev_lineup   = None         # lineup key at previous event

    for _, ev in pbp_df.iterrows():
        try:
            period = int(ev["period"])
        except Exception:
            continue

        secs_left = parse_clock(ev.get("clock", "PT0M0.00S"))
        game_seconds = event_to_game_seconds(period, secs_left)

        # ── Period boundary: reset garbage flag ───────────────────
        if period != current_period:
            current_period = period
            garbage = False

        # ── Update running score ──────────────────────────────────
        try:
            sh = ev.get("scoreHome")
            if sh not in (None, "") and str(sh) not in ("nan", "None"):
                score_home = int(float(sh))
        except Exception:
            pass
        try:
            sa = ev.get("scoreAway")
            if sa not in (None, "") and str(sa) not in ("nan", "None"):
                score_away = int(float(sa))
        except Exception:
            pass

        our_margin = (score_home - score_away) if is_home else (score_away - score_home)

        # ── Garbage time check (irreversible within Q4) ───────────
        if not garbage and is_garbage_time(period, secs_left, abs(our_margin)):
            garbage = True

        # ── Lineup at this moment ─────────────────────────────────
        current_lineup = lineup_at(stints, game_seconds)

        # ── Accumulate time for previous interval [prev_gs → gs] ──
        if prev_lineup is not None and not garbage:
            elapsed = game_seconds - prev_gs
            if elapsed > 0:
                lineup_stats[prev_lineup]['secs'] += elapsed

        prev_gs     = game_seconds
        prev_lineup = current_lineup

        # ── Accumulate event stats ────────────────────────────────
        if garbage or current_lineup is None:
            continue

        action   = str(ev.get("actionType",  "") or "")
        desc     = str(ev.get("description", "") or "").upper()
        team_ev  = int(ev.get("teamId", 0)   or 0)
        is_fg    = int(ev.get("isFieldGoal", 0) or 0)
        our_ev   = (team_ev == team_id_int)

        s = lineup_stats[current_lineup]

        if is_fg:
            # Field goal attempt — action is "Made Shot" or "Missed Shot"
            made   = (action == "Made Shot")
            pts    = 3 if "3PT" in desc else 2
            if our_ev:
                s['fga_for'] += 1
                if made: s['pts_for'] += pts
            else:
                s['fga_against'] += 1
                if made: s['pts_against'] += pts

        elif action == "Free Throw":
            made = ("MISS" not in desc)
            if our_ev:
                s['fta_for'] += 1
                if made: s['pts_for'] += 1
            else:
                s['fta_against'] += 1
                if made: s['pts_against'] += 1

        elif action == "Rebound":
            # subType "Offensive" = offensive rebound
            sub = str(ev.get("subType", "") or "")
            if sub == "Offensive":
                if our_ev:
                    s['oreb_for'] += 1
                else:
                    s['oreb_against'] += 1

        elif action == "Turnover":
            if our_ev:
                s['tov_for'] += 1
            else:
                s['tov_against'] += 1

    return dict(lineup_stats)


# ── Per-team orchestration ─────────────────────────────────────────────────────

def fetch_team_lev(cur, team_abbr, season):
    from nba_api.stats.endpoints import TeamGameLog

    team_id = TEAM_IDS[team_abbr]

    # ── Load existing lineup records ──────────────────────────────
    cur.execute("""
        SELECT id, player_ids FROM team_lineups
        WHERE team_abbr = %s AND season = %s
    """, (team_abbr, season))
    db_rows = cur.fetchall()
    if not db_rows:
        print(f"  No lineups in DB for {team_abbr} {season}. Run fetch_lineups.py first.")
        return

    lineup_lookup = {
        frozenset(str(p) for p in row["player_ids"]): row["id"]
        for row in db_rows
    }
    print(f"  {len(lineup_lookup)} lineups loaded from DB", flush=True)

    # ── Game list ─────────────────────────────────────────────────
    print(f"  Fetching game list...", end=" ", flush=True)
    try:
        tgl_df = _fetch_with_retry(lambda: TeamGameLog(
            team_id=team_id,
            season=season,
            season_type_all_star="Regular Season",
            timeout=60,
        ).get_data_frames()[0])
        time.sleep(0.8)
    except Exception as e:
        print(f"error: {e}")
        time.sleep(2.0)
        return

    if tgl_df.empty:
        print("no games found")
        return

    games = []
    for _, row in tgl_df.iterrows():
        gid = None
        for col in ("Game_ID", "GAME_ID"):
            if col in row.index:
                gid = str(row[col])
                break
        matchup = str(row.get("MATCHUP", row.get("Matchup", "")))
        if gid:
            games.append({"game_id": gid, "is_home": "vs." in matchup})

    print(f"{len(games)} games", flush=True)

    # ── Accumulate PBP stats across all games (3 threads) ─────────
    all_stats = defaultdict(lambda: {
        'secs': 0.0,
        'pts_for': 0, 'pts_against': 0,
        'fga_for': 0, 'oreb_for': 0, 'tov_for': 0, 'fta_for': 0,
        'fga_against': 0, 'oreb_against': 0, 'tov_against': 0, 'fta_against': 0,
    })

    completed = 0
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = {
            pool.submit(process_game, g["game_id"], team_id, g["is_home"]): g["game_id"]
            for g in games
        }
        for fut in as_completed(futures):
            gid = futures[fut]
            completed += 1
            try:
                game_stats = fut.result()
                matched = 0
                for lineup_key, stats in game_stats.items():
                    if lineup_key in lineup_lookup and stats['secs'] > 0:
                        for k, v in stats.items():
                            all_stats[lineup_key][k] += v
                        matched += 1
                print(f"    [{completed:2d}/{len(games)}] {gid}... {matched} lineups", flush=True)
            except Exception as e:
                print(f"    [{completed:2d}/{len(games)}] {gid}... error: {e}", flush=True)

    # ── Compute ratings and update DB ─────────────────────────────
    updated = 0
    for lineup_key, s in all_stats.items():
        db_id = lineup_lookup.get(lineup_key)
        if db_id is None:
            continue

        poss_for     = s['fga_for']     - s['oreb_for']     + s['tov_for']     + 0.44 * s['fta_for']
        poss_against = s['fga_against'] - s['oreb_against'] + s['tov_against'] + 0.44 * s['fta_against']

        min_lev  = safe_round(s['secs'] / 60)
        ortg_lev = safe_round(s['pts_for']     / poss_for     * 100) if poss_for     > 0 else None
        drtg_lev = safe_round(s['pts_against'] / poss_against * 100) if poss_against > 0 else None
        net_lev  = safe_round(ortg_lev - drtg_lev) if (ortg_lev is not None and drtg_lev is not None) else None

        cur.execute("""
            UPDATE team_lineups
            SET min_lev=%s, ortg_lev=%s, drtg_lev=%s, net_lev=%s, updated_at=NOW()
            WHERE id=%s
        """, (min_lev, ortg_lev, drtg_lev, net_lev, db_id))
        updated += 1

    print(f"  {updated} lineup records updated with leverage stats.")


# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", default="2025-26")
    parser.add_argument("--team",   default=None, help="Single team abbr, e.g. BOS")
    args = parser.parse_args()

    teams = [args.team.upper()] if args.team else list(TEAM_IDS.keys())
    unknown = [t for t in teams if t not in TEAM_IDS]
    if unknown:
        print(f"Unknown teams: {unknown}"); sys.exit(1)

    conn = psycopg2.connect(DATABASE_URL)
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    print(f"Processing {len(teams)} team(s) for {args.season}...\n")
    for i, abbr in enumerate(teams, 1):
        print(f"[{i}/{len(teams)}] {abbr}")
        fetch_team_lev(cur, abbr, args.season)
        conn.commit()
        print()
        if i < len(teams):
            time.sleep(1.0)

    cur.close()
    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
