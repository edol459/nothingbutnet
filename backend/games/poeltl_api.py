"""
poeltl_api.py — "guess the performance" daily game (server-facing engine).
=========================================================================

A daily Wordle-style puzzle: we show ONE player's single-game box score with the identity
hidden; you guess the player. Each WRONG guess peels back a context clue (reg/playoffs →
season → team → opponent → result → date → initial). 8 guesses.

Data: `player_gamelogs` (all-time, 1996-97→now). The daily answer + clues live server-side
(in `poeltl_daily`) — only the box-score line ships to the client, so the answer can't be
sniffed from the payload. Imported by server.py; not run directly.
"""

import os
import sys
import random
import hashlib

import psycopg2.extras

sys.path.insert(0, os.path.dirname(__file__))
import question_engine as qe  # reuse get_conn()  # noqa: E402

# ── config ────────────────────────────────────────────────────────────────────
MAX_GUESSES = 8

# A performance is "daily-worthy" (memorable + guessable) if it clears one of these bars.
# (gamelogs carry pts/reb/ast/fg3m + shooting — no stl/blk — so milestones are scoring/
# rebounding/assists/threes + big playoff nights.)
_POOL_WHERE = """
    g.pts >= 40
 OR (g.pts >= 10 AND g.reb >= 10 AND g.ast >= 10)
 OR g.reb >= 20
 OR g.ast >= 15
 OR g.fg3m >= 8
 OR (g.season_type = 'Playoffs' AND g.pts >= 35)
"""

_POOL_CACHE = None   # ordered list of (player_id, game_id, season_type) — stable for seeding


# ── performance pool ──────────────────────────────────────────────────────────
def _pool(conn):
    """All daily-worthy performances by answer-eligible players, in a STABLE order so a
    date seed maps to the same performance for everyone. Cached in-process."""
    global _POOL_CACHE
    if _POOL_CACHE is None:
        with conn.cursor() as cur:
            cur.execute(f"""
                WITH elig AS (   -- "recognizable" players: a real career OR any accolade
                    SELECT player_id FROM player_seasons
                    GROUP BY player_id
                    HAVING SUM(gp) >= 200
                        OR BOOL_OR(awards IS NOT NULL AND array_length(awards, 1) > 0))
                SELECT g.player_id, g.game_id, g.season_type
                FROM   player_gamelogs g JOIN elig e ON e.player_id = g.player_id
                WHERE  {_POOL_WHERE}
                ORDER BY g.season, g.game_id, g.player_id
            """)
            _POOL_CACHE = [(r["player_id"], r["game_id"], r["season_type"]) for r in cur.fetchall()]
    return _POOL_CACHE


def _load_perf(conn, player_id, game_id, season_type):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT g.player_id, p.player_name, g.season, g.season_type, g.game_date, g.matchup, g.wl,
                   g.min, g.pts, g.reb, g.ast, g.fg3m, g.fgm, g.fga, g.ftm, g.fta, g.ts_pct
            FROM   player_gamelogs g JOIN players p ON p.player_id = g.player_id
            WHERE  g.player_id = %s AND g.game_id = %s AND g.season_type = %s
        """, (player_id, game_id, season_type))
        return cur.fetchone()


# ── puzzle construction ───────────────────────────────────────────────────────
def _parse_matchup(matchup):
    """'LAL vs. PHX' → (team, opp, home=True);  'LAL @ PHX' → (team, opp, home=False)."""
    m = matchup or ""
    if " vs. " in m:
        t, o = m.split(" vs. ", 1); return t.strip(), o.strip(), True
    if " @ " in m:
        t, o = m.split(" @ ", 1);   return t.strip(), o.strip(), False
    return m.strip(), "", True


def _last_initial(name):
    parts = (name or "").split()
    return (parts[-1][0] if parts else "?").upper()


def _f(v):
    """gamelog stats come back as floats; show as ints (a box score has no decimals)."""
    return None if v is None else int(round(float(v)))


def _build(perf):
    """The full daily object: the box-score line (shown), the ordered clues (revealed on
    misses), and the answer (server-side only)."""
    team, opp, _home = _parse_matchup(perf["matchup"])
    box = {
        "pts": _f(perf["pts"]), "reb": _f(perf["reb"]), "ast": _f(perf["ast"]),
        "fg3m": _f(perf["fg3m"]), "fgm": _f(perf["fgm"]), "fga": _f(perf["fga"]),
        "ftm": _f(perf["ftm"]), "fta": _f(perf["fta"]), "min": _f(perf["min"]),
        "ts_pct": (round(float(perf["ts_pct"]) * 100, 1) if perf["ts_pct"] is not None else None),
    }
    clues = [
        {"label": "Type",     "value": "Playoffs" if perf["season_type"] == "Playoffs" else "Regular season"},
        {"label": "Season",   "value": perf["season"]},
        {"label": "Team",     "value": team},
        {"label": "Opponent", "value": opp},
        {"label": "Result",   "value": "Win" if perf["wl"] == "W" else "Loss"},
        {"label": "Date",     "value": str(perf["game_date"])},
        {"label": "Initial",  "value": _last_initial(perf["player_name"]) + "."},
    ]
    return {
        "box": box,
        "clues": clues,
        "max_guesses": MAX_GUESSES,
        "answer": {"player_id": perf["player_id"], "name": perf["player_name"]},
    }


def build_daily(conn, date_str, seed=None):
    """Today's shared performance, deterministically seeded by date (everyone gets the same)."""
    pool = _pool(conn)
    if not pool:
        return None
    if seed is None:
        seed = int(hashlib.sha256(("poeltl" + date_str).encode()).hexdigest(), 16)
    key = pool[seed % len(pool)]
    perf = _load_perf(conn, *key)
    return _build(perf) if perf else None


def random_performance(conn):
    """A random daily-worthy performance — for Pro unlimited practice."""
    pool = _pool(conn)
    if not pool:
        return None
    perf = _load_perf(conn, *random.choice(pool))
    return _build(perf) if perf else None


# ── client view + guess scoring ───────────────────────────────────────────────
def puzzle_view(daily):
    """What ships to the client up front: the box score + guess budget. NO clues, NO answer."""
    return {"box": daily["box"], "max_guesses": daily["max_guesses"], "clue_count": len(daily["clues"])}


def score_guesses(daily, guesses):
    """Score an ordered list of guessed player_ids against the stored answer. Reveals one clue
    per WRONG guess; returns the answer only once the round is done (solved or out of guesses)."""
    answer_id = daily["answer"]["player_id"]
    guesses = [g for g in (guesses or []) if isinstance(g, int)][:daily["max_guesses"]]
    results = [g == answer_id for g in guesses]
    solved = any(results)
    wrong = 0
    for r in results:
        if r:
            break
        wrong += 1
    revealed = daily["clues"][:min(wrong, len(daily["clues"]))]
    done = solved or len(guesses) >= daily["max_guesses"]
    return {
        "results": results,
        "solved": solved,
        "guesses_used": len(guesses),
        "revealed": revealed,
        "done": done,
        "answer": daily["answer"] if done else None,
    }


# ── persistence (daily store) ─────────────────────────────────────────────────
def ensure_daily(conn, date_str, force=False, fresh=False):
    """Return the stored daily object for `date_str`, generating + storing it if absent.
    Mirrors survival_api.ensure_daily."""
    with conn.cursor() as cur:
        if force:
            cur.execute("DELETE FROM poeltl_daily WHERE date = %s", (date_str,))
        else:
            cur.execute("SELECT payload FROM poeltl_daily WHERE date = %s", (date_str,))
            row = cur.fetchone()
            if row:
                return row["payload"]

    seed = random.randrange(2 ** 32) if fresh else None
    daily = build_daily(conn, date_str, seed=seed)
    if daily is None:
        return None

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO poeltl_daily(date, payload) VALUES(%s, %s) ON CONFLICT(date) DO NOTHING",
            (date_str, psycopg2.extras.Json(daily)),
        )
    conn.commit()
    return daily
