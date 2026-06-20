"""
ydkball — Survival trivia (console prototype)
=============================================

The base trivia game: answer single-answer questions that get HARDER the deeper you
go. 3 lives. How far can you survive?

  * DAILY SURVIVAL: a shared, seeded run — same question sequence for everyone today,
    so scores are comparable ("I got 14, you?"). One attempt.
  * ENDLESS: random runs. A few free/day; unlimited for Pro.

Onboarding safety (so nobody bounces on question 2):
  * 3 lives, not sudden death.
  * The first few questions are GUARANTEED gimmes (easy + common stat + recent),
    independent of the heuristic difficulty model.
  * Difficulty ramps gradually after that.

    python backend/games/survival.py                 # today's daily survival
    python backend/games/survival.py --endless        # a free random run
    python backend/games/survival.py --endless --pro   # unlimited
    python backend/games/survival.py --show 15         # preview today's run (no play)
    python backend/games/survival.py --reset
"""

import os
import json
import random
import hashlib
import argparse
import datetime

import question_engine as qe

# ── config ───────────────────────────────────────────────────────────────────
LIVES         = 3
SAFE_OPENING  = 3      # first N questions are guaranteed gimmes
RECENT_WINDOW = 60     # don't repeat a question seen within the last N (across runs)
STORE = os.path.join(os.path.dirname(__file__), ".survival_store.json")
# Freemium: free users get the one daily survival run; Pro gets unlimited endless runs.


def target_difficulty(i):
    """Difficulty ramp by question number (1-based) — a gentle on-ramp before the wall."""
    if i <= 3:  return "easy"
    if i <= 9:  return "medium"
    return "hard"


# ── persistence ──────────────────────────────────────────────────────────────
def load_store():
    if os.path.exists(STORE):
        with open(STORE) as f:
            return json.load(f)
    return {"daily": {}, "streak": {"count": 0, "last": None}, "best": 0, "recent_q": []}


def save_store(s):
    with open(STORE, "w") as f:
        json.dump(s, f, indent=2)


# ── question selection (single-answer only, difficulty-targeted) ─────────────
def gen_single(conn, seasons, target, safe, asked, run_stats, run_answers, run_teams):
    """A single-answer question of the target difficulty — built directly (no guess-and-
    check), avoiding repeated questions, stats, answer players, and teams in a run."""
    return qe.generate_targeted(conn, seasons, target, safe=safe, asked=asked,
                                avoid_stats=run_stats, avoid_answers=run_answers,
                                avoid_teams=run_teams)


# ── one question — a single guess ────────────────────────────────────────────
def ask(q, bank):
    """Return True (correct), False (wrong → lose a life), or None (quit).
    Correct = naming any player in the answer set ('any' questions have several)."""
    answer_ids = {a.player_id for a in q.answers}
    print(f"\n  {q.text}")
    try:
        guess = input("  > ").strip()
    except EOFError:
        return None
    if guess.lower() in ("quit", "q"):
        return None
    res = qe.resolve_player(guess, bank) if guess else None
    if res and res[0] in answer_ids:
        print(f"    ✓  {res[1]}")
        return True
    if len(q.answers) == 1:
        print(f"    ✗  it was {q.answers[0].name}")
    else:
        names = ", ".join(a.name for a in q.answers[:3])
        print(f"    ✗  could've said: {names}…")
    return False


# ── a run ────────────────────────────────────────────────────────────────────
def play_run(conn, seasons, bank, seed=None, recent_q=None):
    """recent_q: rolling list of recently-shown question texts (across runs) to avoid
    repeats. Mutated in place; the caller persists it."""
    if seed is not None:
        random.seed(seed)
    recent_q = recent_q if recent_q is not None else []
    lives, score, i = LIVES, 0, 0
    asked, run_stats, run_answers, run_teams = set(recent_q), set(), set(), set()  # seed w/ cross-run
    print(f"\n  ❤️ {LIVES} lives · one guess each · how far can you get?\n" + "─" * 60)
    while lives > 0:
        i += 1
        q = gen_single(conn, seasons, target_difficulty(i), i <= SAFE_OPENING,
                       asked, run_stats, run_answers, run_teams)
        if q is None:
            break
        asked.add(q.text); run_stats.add((q.operator, q.stat.key))
        if q.n == 1:
            run_answers.add(q.answers[0].player_id)
        if q.team:
            run_teams.add(q.team)
        recent_q.append(q.text)
        del recent_q[:-RECENT_WINDOW]               # keep only the last N across runs
        print(f"\n  Q{i}  ·  score {score}  ·  {'❤' * lives}")
        result = ask(q, bank)
        if result is None:
            break
        if result:
            score += 1
        else:
            lives -= 1
            if lives:
                print(f"    💔 {lives} {'life' if lives == 1 else 'lives'} left")
    print("\n" + "═" * 60)
    print(f"  💀  Game over — you survived {score}")
    print("═" * 60)
    return score


# ── modes ────────────────────────────────────────────────────────────────────
def daily_survival(conn, seasons, date_str):
    store = load_store()
    bank = qe.load_player_bank(conn)
    print("═" * 60)
    print(f"  🏀  ydkball DAILY SURVIVAL — {date_str}")
    if date_str in store["daily"]:
        print(f"  (you already played today: {store['daily'][date_str]} — this is just for fun)")
    print("═" * 60)

    seed = int(hashlib.sha256(("survival" + date_str).encode()).hexdigest(), 16) % (2**32)
    score = play_run(conn, seasons, bank, seed=seed)

    best_today = max(score, store["daily"].get(date_str, 0))
    store["daily"][date_str] = best_today
    store["best"] = max(store["best"], score)
    # streak: consecutive days played
    st = store["streak"]
    if st["last"] != date_str:
        gap = (datetime.date.fromisoformat(date_str) - datetime.date.fromisoformat(st["last"])).days if st["last"] else 1
        st["count"] = st["count"] + 1 if gap == 1 else 1
        st["last"] = date_str
    save_store(store)
    print(f"\n  today's best: {best_today}  ·  all-time best: {store['best']}  ·  🔥 streak {st['count']}")
    print(f"\n  ydkball survival {date_str}: I survived {score} 🏀\n")


def endless(conn, seasons, pro):
    if not pro:
        print("\n  🔒 Endless runs are a Pro feature.")
        print("     Free includes today's daily survival — go Pro for unlimited runs!\n")
        return
    store = load_store()
    store.setdefault("recent_q", [])
    bank = qe.load_player_bank(conn)
    print("═" * 60)
    print("  🏀  ENDLESS SURVIVAL — ∞ Pro")
    print("═" * 60)
    score = play_run(conn, seasons, bank, seed=None, recent_q=store["recent_q"])
    store["best"] = max(store["best"], score)
    save_store(store)
    print(f"\n  all-time best: {store['best']}\n")


def show(conn, seasons, date_str, n):
    seed = int(hashlib.sha256(("survival" + date_str).encode()).hexdigest(), 16) % (2**32)
    random.seed(seed)
    print(f"\nDaily survival — {date_str} (first {n} questions, with answers):\n")
    asked, run_stats, run_answers, run_teams = set(), set(), set(), set()
    for i in range(1, n + 1):
        q = gen_single(conn, seasons, target_difficulty(i), i <= SAFE_OPENING,
                       asked, run_stats, run_answers, run_teams)
        if not q:
            break
        asked.add(q.text); run_stats.add((q.operator, q.stat.key))
        if q.n == 1:
            run_answers.add(q.answers[0].player_id)
        if q.team:
            run_teams.add(q.team)
        print(f"  Q{i:<2} [{q.difficulty:<6}] {q.text}")
        print(f"        → {q.answers[0].name}")


# ── CLI ──────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=datetime.date.today().isoformat())
    ap.add_argument("--endless", action="store_true")
    ap.add_argument("--pro", action="store_true")
    ap.add_argument("--show", type=int, default=0, metavar="N")
    ap.add_argument("--reset", action="store_true")
    args = ap.parse_args()

    if args.reset:
        if os.path.exists(STORE):
            os.remove(STORE)
        print("survival store reset.")
        return

    conn = qe.get_conn()
    seasons = qe.list_seasons(conn)
    if args.show:
        show(conn, seasons, args.date, args.show)
    elif args.endless:
        endless(conn, seasons, args.pro)
    else:
        daily_survival(conn, seasons, args.date)
    conn.close()


if __name__ == "__main__":
    main()
