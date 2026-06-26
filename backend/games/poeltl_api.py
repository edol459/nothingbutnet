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

import bisect

# Two-stage selection so the daily isn't the same prolific stars on repeat: pick a PLAYER with
# a capped weight, then pick uniformly among ALL their qualifying performances (so every game
# stays reachable). A player's selection weight = min(their #games, _WEIGHT_CAP) — i.e. a star
# is at most _WEIGHT_CAP× a one-off player, instead of ~hundreds×.
_WEIGHT_CAP = 6

_POOL_CACHE = None   # {"players": [(pid, [keys...]), ...], "cum": [cumulative weights], "total": W}


# ── performance pool ──────────────────────────────────────────────────────────
def _pool(conn):
    """All daily-worthy performances by answer-eligible players, grouped by player with a
    capped per-player selection weight. Stable order so a date seed is reproducible. Cached."""
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
                ORDER BY g.player_id, g.game_id
            """)
            groups = {}
            for r in cur.fetchall():
                groups.setdefault(r["player_id"], []).append(
                    (r["player_id"], r["game_id"], r["season_type"]))
        players = sorted(groups.items())          # [(pid, [keys])] in stable pid order
        cum, total = [], 0
        for _pid, keys in players:
            total += min(len(keys), _WEIGHT_CAP)
            cum.append(total)
        _POOL_CACHE = {"players": players, "cum": cum, "total": total}
    return _POOL_CACHE


def _pick(pool, seed):
    """Deterministic two-stage pick: weighted player (capped), then uniform among their games."""
    players, cum, total = pool["players"], pool["cum"], pool["total"]
    if not total:
        return None
    pi = bisect.bisect_right(cum, seed % total)   # which player (by cumulative weight)
    _pid, keys = players[pi]
    return keys[(seed // total) % len(keys)]       # which of their games (uniform)


def _load_perf(conn, player_id, game_id, season_type):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT g.player_id, p.player_name, g.season, g.season_type, g.game_date, g.matchup, g.wl,
                   g.min, g.pts, g.reb, g.ast, g.fg3m, g.fgm, g.fga, g.ftm, g.fta, g.ts_pct,
                   p.position, p.position_group, p.height_inches, p.draft_year, p.draft_number, p.college
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


def _name_mask(name, reveal):
    """Hangman skeleton: each word's first letter (initial) is always shown; the next `reveal`
    alphabetic letters (left→right across the name) are shown; the rest become '_'. Spaces and
    punctuation (apostrophes, hyphens, periods) are always shown.
      reveal=0  → "S________ O'____"   reveal=2 → "Sha______ O'____"  (Shaquille O'Neal)"""
    budget = max(0, reveal)
    out = []
    for w in name.split(" "):
        chars = []
        for i, ch in enumerate(w):
            if i == 0 or not ch.isalpha():
                chars.append(ch)                       # initial / punctuation — always shown
            elif budget > 0:
                chars.append(ch); budget -= 1          # revealed letter
            else:
                chars.append("_")
        out.append("".join(chars))
    return " ".join(out)


def _f(v):
    """gamelog stats come back as floats; show as ints (a box score has no decimals)."""
    return None if v is None else int(round(float(v)))


_POS_MAP = {"G": "Guard", "F": "Forward", "C": "Center"}

def _pos_label(perf):
    p = perf.get("position") or perf.get("position_group")
    if not p:
        return None
    return "-".join(_POS_MAP.get(t.strip(), t.strip()) for t in p.split("-") if t.strip())

def _height_label(perf):
    h = perf.get("height_inches")
    if not h:
        return None
    h = int(round(float(h)))
    return f"{h // 12}'{h % 12}\""

def _draft_label(perf):
    """'2003 · #1 pick' / '2003'. None when unknown — we can't tell undrafted from missing,
    so we skip rather than risk asserting a false 'undrafted'."""
    y = perf.get("draft_year")
    if not y:
        return None
    n = perf.get("draft_number")
    return f"{int(y)} · #{int(n)} pick" if n else f"{int(y)}"

def _college_label(perf):
    # "No college" is itself a strong clue (prep-to-pro or international), so always show it.
    c = (perf.get("college") or "").strip()
    return c if c and c.lower() != "none" else "No college"

def _date_label(d):
    """'2002-04-15' → 'Apr 15, 2002'."""
    import datetime as _dt
    try:
        x = _dt.datetime.strptime(str(d)[:10], "%Y-%m-%d")
        return x.strftime("%b ") + str(x.day) + x.strftime(", %Y")
    except Exception:
        return str(d)


def _build(perf):
    """The full daily object: the box-score line + opponent (shown on the card), the ordered
    clues (revealed on misses), and the answer (server-side only)."""
    team, opp, home = _parse_matchup(perf["matchup"])
    box = {
        "pts": _f(perf["pts"]), "reb": _f(perf["reb"]), "ast": _f(perf["ast"]),
        "fg3m": _f(perf["fg3m"]), "fgm": _f(perf["fgm"]), "fga": _f(perf["fga"]),
        "ftm": _f(perf["ftm"]), "fta": _f(perf["fta"]), "min": _f(perf["min"]),
        "ts_pct": (round(float(perf["ts_pct"]) * 100, 1) if perf["ts_pct"] is not None else None),
    }
    season_label = perf["season"] + (" · Playoffs" if perf["season_type"] == "Playoffs"
                                     else " · Regular season")
    name = perf["player_name"]
    # Clue ladder, revealed one per guess used (a miss OR a "reveal a clue"). Season leads so you
    # get an era gauge first; then bio → team → name skeleton. Skipped when genuinely unknown.
    ladder = [
        ("Position", _pos_label(perf)),
        ("Season",   season_label),
        ("Height",   _height_label(perf)),
        ("Draft",    _draft_label(perf)),
        ("College",  _college_label(perf)),
        ("Team",     team),
        ("Name",     _name_mask(name, 0)),   # first + last initials, rest as underscores
    ]
    clues = [{"label": k, "value": v} for k, v in ladder if v]
    return {
        "box": box,
        "opponent": opp,                       # part of the end-of-game reveal (not shown during play)
        "home": home,                          # True = player's team hosted (vs.), False = away (@)
        "game_date": _date_label(perf["game_date"]),
        "clues": clues,
        "max_guesses": MAX_GUESSES,
        "answer": {"player_id": perf["player_id"], "name": name},
    }


def build_daily(conn, date_str, seed=None):
    """Today's shared performance, deterministically seeded by date (everyone gets the same)."""
    pool = _pool(conn)
    if not pool["players"]:
        return None
    if seed is None:
        seed = int(hashlib.sha256(("poeltl" + date_str).encode()).hexdigest(), 16)
    key = _pick(pool, seed)
    perf = _load_perf(conn, *key)
    return _build(perf) if perf else None


def unlimited_round(conn):
    """A random round for Pro unlimited practice. Unlike the daily, this ships the answer + the
    full clue ladder so the client can play and reveal locally — it's solo practice, not a
    shared/scored daily, so that's fine."""
    daily = random_performance(conn)
    if daily is None:
        return None
    return {
        "box": daily["box"],
        "opponent": daily.get("opponent"),
        "home": daily.get("home"),
        "game_date": daily.get("game_date"),
        "max_guesses": daily["max_guesses"],
        "clue_plan": [c["label"] for c in daily["clues"]],
        "clues": daily["clues"],
        "answer": daily["answer"],
    }


def random_performance(conn):
    """A random daily-worthy performance (same capped-per-player weighting as the daily, so
    unlimited practice is varied too). Used by unlimited_round."""
    pool = _pool(conn)
    if not pool["players"]:
        return None
    weights = [min(len(keys), _WEIGHT_CAP) for _pid, keys in pool["players"]]
    _pid, keys = random.choices(pool["players"], weights=weights)[0]
    perf = _load_perf(conn, *random.choice(keys))
    return _build(perf) if perf else None


# ── client view + guess scoring ───────────────────────────────────────────────
def puzzle_view(daily):
    """What ships to the client up front: the box score + opponent (shown on the card, abbr
    only) + guess budget + the clue ladder's labels. NO clue values, NO game date, NO answer."""
    return {
        "box": daily["box"],
        "opponent": daily.get("opponent"),
        "home": daily.get("home"),
        "max_guesses": daily["max_guesses"],
        "clue_plan": [c["label"] for c in daily["clues"]],
    }


def end_reveal(daily):
    """Everything shown once the round is over: all clues, the answer, opponent + game date."""
    return {
        "clues": daily["clues"],
        "answer": daily["answer"],
        "opponent": daily.get("opponent"),
        "home": daily.get("home"),
        "game_date": daily.get("game_date"),
    }


def score_guesses(daily, guesses):
    """Score an ordered list of guesses against the stored answer. Each guess USED (a wrong
    player guess, OR a 0 sentinel = 'reveal a clue') reveals the next clue. Once the round is
    done (solved or out of guesses) EVERYTHING is revealed: all clues, opponent, date, answer."""
    answer_id = daily["answer"]["player_id"]
    guesses = [g for g in (guesses or []) if isinstance(g, int)][:daily["max_guesses"]]
    results = [g == answer_id for g in guesses]
    solved = any(results)
    wrong = 0
    for r in results:
        if r:
            break
        wrong += 1
    done = solved or len(guesses) >= daily["max_guesses"]
    if done:
        revealed = list(daily["clues"])                 # reveal everything when the round ends
    else:
        ctx = daily["clues"][:-1]                        # context clues (all but the name skeleton)
        revealed = list(ctx[:wrong])
        if len(guesses) >= daily["max_guesses"] - 1:     # on the FINAL guess, surface the name too
            revealed.append(daily["clues"][-1])
    out = {
        "results": results,
        "solved": solved,
        "guesses_used": len(guesses),
        "revealed": revealed,
        "done": done,
        "answer": daily["answer"] if done else None,
    }
    if done:
        out["opponent"]  = daily.get("opponent")
        out["home"]      = daily.get("home")
        out["game_date"] = daily.get("game_date")
    return out


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
