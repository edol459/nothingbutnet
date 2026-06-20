"""
ydkball — Daily + Practice (console prototype)
==============================================

DAILY (shared, the habit/social anchor)
  * 5 questions, the SAME for everyone on a given date (deterministic by date).
  * Soft easy→hard ramp — difficulty only shapes the order & keeps a day from being
    all-brutal; we never label a question "easy", so a mislabel can't break trust.
  * No repeats across days (signature tracking). Score, streak, shareable result.

PRACTICE (the grind, freemium)
  * Endless random questions. 10/day free; unlimited for Pro.

    python backend/games/daily.py                 # play today's daily
    python backend/games/daily.py --date 2026-06-25
    python backend/games/daily.py --practice       # practice (10 free/day)
    python backend/games/daily.py --practice --pro  # unlimited practice
    python backend/games/daily.py --show           # print today's daily + answers
    python backend/games/daily.py --reset          # wipe schedule/streak/practice
"""

import os
import json
import random
import hashlib
import argparse
import datetime

import question_engine as qe

# ── config (tune freely) ─────────────────────────────────────────────────────
DAILY_RAMP    = ["easy", "easy", "medium", "medium", "hard"]   # 5/day, gentle ramp
PRACTICE_FREE = 10                                             # free practice Qs/day
STORE = os.path.join(os.path.dirname(__file__), ".daily_store.json")
TILES = {"full": "🟩", "partial": "🟨", "none": "⬜"}


# ── persistence ──────────────────────────────────────────────────────────────
def load_store():
    if os.path.exists(STORE):
        with open(STORE) as f:
            return json.load(f)
    return {"dates": {}, "used": [], "streak": {"count": 0, "last": None}, "practice": {}}


def save_store(s):
    with open(STORE, "w") as f:
        json.dump(s, f, indent=2)


# ── building the daily set ───────────────────────────────────────────────────
def _serialize(q):
    scale = qe.detect_pct_scale([a.value for a in q.answers])
    answers = [
        {"id": a.player_id, "name": a.name,
         "val": a.display or f"{qe.fmt_val(q.stat, a.value, scale)} {q.stat.short}"}
        for a in sorted(q.answers, key=lambda x: -x.value)
    ]
    return {"text": q.text, "difficulty": q.difficulty, "answers": answers}


_CHAINS = {"easy":   ["easy", "medium", "hard"],     # difficulty fallback per ramp slot
           "medium": ["medium", "easy", "hard"],
           "hard":   ["hard", "medium", "easy"]}


def _compose(buckets):
    """Fill the ramp from the candidate buckets, requiring a DIFFERENT stat for each
    of the 5 questions (no two PPG questions in one day), with difficulty fallback."""
    pools = {d: list(buckets[d]) for d in buckets}
    chosen, used_stats = [], set()
    for want in DAILY_RAMP:
        picked = None
        for d in _CHAINS[want]:
            for i, q in enumerate(pools[d]):
                if q.stat.key not in used_stats:
                    picked = pools[d].pop(i)
                    break
            if picked:
                break
        if picked:
            chosen.append(picked)
            used_stats.add(picked.stat.key)
    return chosen


def build_day(conn, seasons, date_str, store):
    """The 5 frozen questions for `date_str` (generate + persist if new)."""
    if date_str in store["dates"]:
        return store["dates"][date_str]

    seed = int(hashlib.sha256(date_str.encode()).hexdigest(), 16) % (2**32)
    random.seed(seed)                       # qe shares the stdlib RNG → deterministic
    used = set(store["used"])
    buckets, seen, chosen = {"easy": [], "medium": [], "hard": []}, set(), []
    for _ in range(12000):
        q = qe.generate_question(conn, seasons)
        if q and q.text not in used and q.text not in seen:
            seen.add(q.text)
            buckets[q.difficulty].append(q)
        chosen = _compose(buckets)
        if len(chosen) == len(DAILY_RAMP):
            break

    day = [_serialize(q) for q in chosen]
    store["dates"][date_str] = day
    store["used"].extend(q["text"] for q in day)
    save_store(store)
    return day


# ── streak ───────────────────────────────────────────────────────────────────
def update_streak(store, date_str):
    st = store["streak"]
    if st["last"] == date_str:
        return st["count"]
    if st["last"]:
        gap = (datetime.date.fromisoformat(date_str) - datetime.date.fromisoformat(st["last"])).days
        st["count"] = st["count"] + 1 if gap == 1 else 1
    else:
        st["count"] = 1
    st["last"] = date_str
    save_store(store)
    return st["count"]


# ── playing one question ─────────────────────────────────────────────────────
def play_question(q, bank, header):
    answer_ids = {a["id"] for a in q["answers"]}
    by_id = {a["id"]: a for a in q["answers"]}
    n = len(q["answers"])
    print(f"\n  {header}  {q['text']}")
    print(f"  ({'name the player' if n == 1 else f'name all {n}'} · 'pass' to skip)\n")

    found = set()
    while len(found) < n:
        prog = "" if n == 1 else f"[{len(found)}/{n}] "
        try:
            guess = input(f"  {prog}> ").strip()
        except EOFError:
            break
        if guess.lower() in ("pass", "skip", "quit", "q", ""):
            break
        res = qe.resolve_player(guess, bank)
        if res is None:
            print("    ?  no player found — try a fuller name")
        elif res[0] not in answer_ids:
            print(f"    ✗  {res[1]} — not in the set")
        elif res[0] in found:
            print(f"    •  already got {res[1]}")
        else:
            found.add(res[0])
            print(f"    ✓  {by_id[res[0]]['name']}  ({by_id[res[0]]['val']})")

    missed = [a for a in q["answers"] if a["id"] not in found]
    if missed:
        print("    missed: " + ", ".join(f"{a['name']} ({a['val']})" for a in missed))
    return len(found), n


def _tile(got, tot):
    return TILES["full"] if got == tot else TILES["partial"] if got else TILES["none"]


# ── the daily ────────────────────────────────────────────────────────────────
def play_day(conn, seasons, date_str):
    store = load_store()
    day = build_day(conn, seasons, date_str, store)
    bank = qe.load_player_bank(conn)

    print("═" * 60)
    print(f"  🏀  ydkball daily — {date_str}   ({len(day)} questions)")
    print("═" * 60)

    tiles, got_tot, ans_tot, solved = [], 0, 0, 0
    for i, q in enumerate(day, 1):
        got, tot = play_question(q, bank, f"Q{i}/{len(day)}")
        tiles.append(_tile(got, tot))
        got_tot += got; ans_tot += tot
        solved += (got == tot)

    streak = update_streak(store, date_str)
    print("\n" + "═" * 60)
    print(f"  {''.join(tiles)}   {solved}/{len(day)} solved · {got_tot}/{ans_tot} names · 🔥 {streak}")
    print("═" * 60)
    print(f"\n  ydkball {date_str}\n  {''.join(tiles)}  {solved}/{len(day)}\n")


# ── practice ─────────────────────────────────────────────────────────────────
def play_practice(conn, seasons, date_str, pro):
    store = load_store()
    played = store["practice"].get(date_str, 0)
    bank = qe.load_player_bank(conn)
    print("═" * 60)
    print(f"  🏀  ydkball practice — {'∞ Pro' if pro else f'{PRACTICE_FREE - played} free left today'}")
    print("═" * 60)

    got_tot = ans_tot = n = 0
    while pro or played < PRACTICE_FREE:
        q = qe.generate_question(conn, seasons)
        if not q:
            continue
        got, tot = play_question(_serialize(q), bank, f"#{played + 1}")
        played += 1; n += 1; got_tot += got; ans_tot += tot
        store["practice"][date_str] = played
        save_store(store)
        try:
            if input("\n  [enter] next · [q] quit > ").strip().lower() in ("q", "quit"):
                break
        except EOFError:
            break

    if not pro and played >= PRACTICE_FREE:
        print("\n  🔒 That's your 10 free for today — go Pro for unlimited practice!")
    print(f"\n  session: {got_tot}/{ans_tot} names over {n} questions\n")


# ── CLI ──────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=datetime.date.today().isoformat())
    ap.add_argument("--practice", action="store_true", help="endless practice mode")
    ap.add_argument("--pro", action="store_true", help="unlimited practice (simulate Pro)")
    ap.add_argument("--show", action="store_true", help="print the daily set + answers")
    ap.add_argument("--reset", action="store_true", help="wipe schedule/streak/practice")
    args = ap.parse_args()

    if args.reset:
        if os.path.exists(STORE):
            os.remove(STORE)
        print("store reset.")
        return

    conn = qe.get_conn()
    seasons = qe.list_seasons(conn)

    if args.show:
        day = build_day(conn, seasons, args.date, load_store())
        print(f"\nydkball daily — {args.date}\n")
        for i, q in enumerate(day, 1):
            print(f"Q{i}. {q['text']}")
            for a in q["answers"]:
                print(f"     • {a['name']}  ({a['val']})")
            print()
    elif args.practice:
        play_practice(conn, seasons, args.date, args.pro)
    else:
        play_day(conn, seasons, args.date)
    conn.close()


if __name__ == "__main__":
    main()
