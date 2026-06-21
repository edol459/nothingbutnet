"""
survival_api.py — server-facing wrapper around the trivia engine.
=================================================================

Turns the console Survival prototype (`survival.py`) into something `server.py` can
call: it generates **serialized** runs (a list of question dicts, each with its valid
answer player_ids) that the web / iOS clients play locally.

Answer validation is client-side: the autocomplete picker yields a real player_id, and
the client checks it against the question's `answer_ids`. (The picker *is* the resolver,
so the tolerant fuzzy matcher isn't needed on the client.)

Imported by server.py — not run directly.
"""

import os
import sys
import random
import hashlib

import psycopg2.extras

# so `import question_engine` resolves whether server.py runs from backend/ or elsewhere
sys.path.insert(0, os.path.dirname(__file__))
import question_engine as qe  # noqa: E402

# ── config ────────────────────────────────────────────────────────────────────
LIVES         = 3       # cushion — you can miss up to 2 and still finish the daily
DAILY_LENGTH  = 10      # the daily is a fixed 10-question gauntlet — clear it = "you know ball"
RECENT_TEXTS  = 400     # cross-day dedup: how many recent question texts to avoid

# Every question is a this-or-that ("Who did more X — A or B?"). No difficulty labels.


# ── serialization ─────────────────────────────────────────────────────────────
def serialize(q, idx):
    """A question as a plain JSON-able dict. `answer_ids` is the valid-answer set the
    client checks picks against; `answers` is the reveal text shown on a miss."""
    return {
        "i":           idx,
        "text":        q.text,
        "difficulty":  q.difficulty,
        "season":      q.season,
        "season_type": q.season_type,
        "operator":    q.operator,
        "stat":        q.stat.key,
        "team":        q.team,
        "n":           q.n,
        "answer_ids":  [a.player_id for a in q.answers],
        "answers":     [a.name for a in q.answers],
        # this-or-that: the two players to choose between (headshot buttons in the UI)
        "options":     [{"id": o.player_id, "name": o.name} for o in q.options] if q.options else None,
    }


# ── run generation ────────────────────────────────────────────────────────────
def _gen_run(conn, seasons, length, seed=None, asked=None):
    """Build a serialized run of `length` this-or-that questions, avoiding repeated
    question text (within the run and across `asked`) and a repeated answer player."""
    if seed is not None:
        random.seed(seed)
    asked = set(asked or [])
    run_answers = set()
    out = []
    guard = 0
    while len(out) < length and guard < length * 10:
        guard += 1
        q = qe.generate_thisorthat(conn, seasons, exclude=run_answers)
        if q is None or q.text in asked:
            continue
        asked.add(q.text)
        run_answers.add(q.answers[0].player_id)
        out.append(serialize(q, len(out) + 1))
    return out


def build_daily(conn, date_str, recent_texts=None, seed=None):
    """Today's shared daily run. By default deterministically seeded by date (so a
    regenerate reproduces it); pass an explicit `seed` (e.g. random) to get a different
    run. Avoids question text used on recent days."""
    seasons = qe.list_seasons(conn)
    if seed is None:
        seed = int(hashlib.sha256(("survival" + date_str).encode()).hexdigest(), 16) % (2 ** 32)
    return _gen_run(conn, seasons, DAILY_LENGTH, seed=seed, asked=recent_texts)


def next_unlimited(conn, pos, exclude=None):
    """One on-demand this-or-that question for an Unlimited run. `exclude` = answer
    player_ids already used this run, so the same player isn't the answer twice.
    (Unlimited is open-ended — 3 lives, go as far as you can.)"""
    seasons = qe.list_seasons(conn)
    q = qe.generate_thisorthat(conn, seasons, exclude=set(exclude or ()))
    return serialize(q, pos) if q else None


# ── persistence (daily store + cross-day dedup) ───────────────────────────────
def ensure_daily(conn, date_str, force=False, fresh=False):
    """Return the stored daily run for `date_str`, generating + storing it if absent.

    `force` regenerates even if a row exists (drops the old daily row first; keeps the
    used-text history so the new run avoids prior questions). `fresh` uses a random seed
    so a forced regen actually yields *different* questions — handy for testing.

    Generation is slow over a remote DB (many leaderboard round-trips) so this is meant
    to be called ahead of time by a cron (see generate_daily.py); the endpoint reads the
    cached row. The inline-generate fallback keeps it correct if the cron ever misses
    (fast when the DB is co-located in production)."""
    with conn.cursor() as cur:
        if force:
            cur.execute("DELETE FROM survival_daily WHERE date = %s", (date_str,))
        else:
            cur.execute("SELECT payload FROM survival_daily WHERE date = %s", (date_str,))
            row = cur.fetchone()
            if row:
                return row["payload"]
        cur.execute("SELECT text FROM survival_used ORDER BY used_on DESC LIMIT %s", (RECENT_TEXTS,))
        recent = [r["text"] for r in cur.fetchall()]

    seed = random.randrange(2 ** 32) if fresh else None
    run = build_daily(conn, date_str, recent, seed=seed)

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO survival_daily(date, payload) VALUES(%s, %s) ON CONFLICT(date) DO NOTHING",
            (date_str, psycopg2.extras.Json(run)),
        )
        for q in run:
            cur.execute(
                "INSERT INTO survival_used(text, used_on) VALUES(%s, %s) ON CONFLICT(text) DO NOTHING",
                (q["text"], date_str),
            )
    conn.commit()
    return run


# ── autocomplete player bank ──────────────────────────────────────────────────
_PLAYERS_CACHE = None


def player_list(conn):
    """[{id, name}] over all players, for the client's autocomplete dropdown. Cached —
    the roster is effectively static within a process."""
    global _PLAYERS_CACHE
    if _PLAYERS_CACHE is None:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT player_id, player_name FROM players "
                "WHERE player_name IS NOT NULL ORDER BY player_name"
            )
            _PLAYERS_CACHE = [{"id": r["player_id"], "name": r["player_name"]} for r in cur.fetchall()]
    return _PLAYERS_CACHE
