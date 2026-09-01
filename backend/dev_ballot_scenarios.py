"""
ydkball — Dev helper: drive a ballot through its states
=============================================================
python backend/dev_ballot_scenarios.py you@example.com --state partial
python backend/dev_ballot_scenarios.py you@example.com --state graded --league nba
python backend/dev_ballot_scenarios.py you@example.com --reset

Almost everything a ballot does is driven by dates: the window opens when a
schedule is published, picks freeze at tipoff, results land in April, and the
WNBA runs on the opposite half of the calendar. That makes the interesting
states unreachable by waiting — you'd need eight months and a time machine to
see a graded ballot once.

Everything that decides those states is a column, though, so they can all be
staged: `locked_at` fakes tipoff, rows in `award_results` fake the league
announcing, and `xp_events` can be cleared to replay the payout moment. This
drives them in one command so the app can be checked against each.

States:
  empty     ballot exists, no picks               — banner: "Make your picks"
  partial   half the slots filled                 — banner: "4 of 8 filled"
  complete  every slot filled, still open         — banner: hidden
  locked    complete + tipoff passed              — banner: hidden, sheet frozen
  graded    locked + results, XP unclaimed        — banner: "collect", +XP on open
  claimed   graded and the XP already banked      — banner: hidden

--reset removes the ballot, its picks, the fake results and the ballot XP, and
restores the user's XP total. Run it when you're done: these are fabricated
winners and they would otherwise grade real ballots.

Safety: refuses to run without an explicit email, and only ever touches that
user's ballot for the league given. It will not delete a ballot it can't account
for unless you pass --force.

DEV ONLY. Writes to whatever DATABASE_URL points at, which is production.
"""
import argparse
import os
import sys

from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("DATABASE_URL not found."); sys.exit(1)

STATES = ["empty", "partial", "complete", "locked", "graded", "claimed"]

# Stamped on every ballot this script creates, so it can clear its own leftovers
# freely while still refusing to touch one a human filled in.
MARKER = "[dev-scenario]"

# Fabricated picks, one per slot code. Real ids so headshots and crests resolve.
PICKS = {
    "nba": {
        "CHAMPION": (None,    "New York Knicks",       "NYK"),
        "MVP":      (1641705, "Victor Wembanyama",     "SAS"),
        "ROTY":     (1643407, "AJ Dybantsa",           "WAS"),
        "DPOY":     (1631096, "Amen Thompson",         "HOU"),
        "6MOY":     (203903,  "Jordan Clarkson",       "NYK"),
        "MIP":      (1629673, "Jordan Poole",          "NOP"),
        "CPOY":     (201939,  "Stephen Curry",         "GSW"),
        "COY":      (1628188, "Mitch Johnson",         "SAS"),
    },
    "wnba": {
        "CHAMPION": (None, "Las Vegas Aces", "LV"),
        "MVP":      (None, "A'ja Wilson",    "LV"),
        "ROTY":     (None, "Paige Bueckers", "DAL"),
        "DPOY":     (None, "Alanna Smith",   "MIN"),
        "6WOY":     (None, "Naz Hillmon",    "ATL"),
        "MIP":      (None, "Rickea Jackson", "LA"),
    },
}

# Which slots the fake results agree with, so a graded ballot isn't all-correct
# or all-wrong — both look plausible and neither exercises the mixed layout.
CORRECT_SLOTS = {"CHAMPION", "MVP", "DPOY", "MIP"}


def connect():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


def find_user(cur, email):
    cur.execute("SELECT id, display_name, xp FROM users WHERE email = %s", (email,))
    row = cur.fetchone()
    if not row:
        print(f"No user with email {email}."); sys.exit(1)
    return row


def season_for(cur, league):
    """The season a ballot would predict — same rule the app uses."""
    cur.execute("""
        SELECT season, MIN(game_date) AS first_date, MIN(game_time_utc) AS first_tip
        FROM scheduled_games
        WHERE league = %s AND season_type = 'Regular Season'
        GROUP BY season HAVING MAX(game_date) >= CURRENT_DATE
        ORDER BY season ASC LIMIT 1
    """, (league,))
    row = cur.fetchone()
    if not row:
        print(f"No upcoming {league} season in scheduled_games."); sys.exit(1)
    return row["season"], (row["first_tip"] or row["first_date"])


def wipe(conn, user, league, force=False):
    """Remove the ballot, its picks, the fabricated results and the XP it paid."""
    with conn.cursor() as cur:
        cur.execute("""SELECT id, season, description FROM game_lists
                       WHERE user_id = %s AND list_type = 'awards' AND league = %s""",
                    (user["id"], league))
        rows = cur.fetchall()
        for r in rows:
            mine = (r.get("description") or "").startswith(MARKER)
            if not mine and not force:
                cur.execute("SELECT COUNT(*) AS n FROM award_ballot_items WHERE list_id = %s",
                            (r["id"],))
                n = int(cur.fetchone()["n"])
                if n:
                    print(f"  ballot {r['id']} has {n} pick(s) and wasn't made by this "
                          "script — refusing to delete.")
                    print("  Re-run with --force if you're sure it's disposable.")
                    sys.exit(1)
            cur.execute("""DELETE FROM xp_events
                           WHERE user_id = %s AND event_type IN ('ballot_lock','ballot_correct')
                             AND (reference_id = %s OR reference_id LIKE %s)""",
                        (user["id"], str(r["id"]), f"{r['id']}:%"))
            cur.execute("DELETE FROM award_results WHERE league = %s AND season = %s",
                        (league, r["season"]))
            cur.execute("DELETE FROM game_lists WHERE id = %s", (r["id"],))
            print(f"  removed ballot {r['id']} ({league} {r['season']})")
        # users.xp is a running total, so it has to be recomputed rather than
        # decremented — a partial run may have paid some slots and not others.
        cur.execute("""UPDATE users SET xp = COALESCE(
                           (SELECT SUM(xp_amount) FROM xp_events WHERE user_id = %s), 0)
                       WHERE id = %s RETURNING xp""", (user["id"], user["id"]))
        print(f"  xp recomputed to {cur.fetchone()['xp']}")
    conn.commit()


def build(conn, user, league, state):
    season, tip = season_for(conn.cursor(), league)
    picks = PICKS[league]
    codes = list(picks.keys())

    with conn.cursor() as cur:
        # locked/graded/claimed need tipoff in the past; the others keep it real.
        locked_at = "NOW() - INTERVAL '1 day'" if state in ("locked", "graded", "claimed") else "%s"
        params = [user["id"], f"{league.upper()} Awards {season}",
                  f"{MARKER} {state}", league, season]
        if locked_at == "%s":
            params.append(tip)
        cur.execute(f"""
            INSERT INTO game_lists (user_id, title, description, is_ranked, list_type,
                                    allow_copy, league, season, locked_at, is_public)
            VALUES (%s, %s, %s, FALSE, 'awards', FALSE, %s, %s, {locked_at}, TRUE)
            RETURNING id
        """, params)
        list_id = cur.fetchone()["id"]

        fill = {
            "empty":    [],
            "partial":  codes[:len(codes) // 2],
        }.get(state, codes)

        for code in fill:
            pid, name, team = picks[code]
            cur.execute("""INSERT INTO award_ballot_items
                           (list_id, award_code, person_id, player_name, team)
                           VALUES (%s, %s, %s, %s, %s)""", (list_id, code, pid, name, team))

        if state in ("graded", "claimed"):
            for code in codes:
                pid, name, team = picks[code]
                if code in CORRECT_SLOTS:
                    win_pid, win_name, win_team = pid, name, team
                else:
                    # A different winner, so the slot grades as a miss.
                    win_pid, win_name, win_team = None, f"Someone Else ({code})", "BOS"
                cur.execute("""INSERT INTO award_results
                               (league, season, award_code, person_id, player_name, team)
                               VALUES (%s,%s,%s,%s,%s,%s)
                               ON CONFLICT (league, season, award_code) DO UPDATE
                                 SET person_id = EXCLUDED.person_id,
                                     player_name = EXCLUDED.player_name,
                                     team = EXCLUDED.team""",
                            (league, season, code, win_pid, win_name, win_team))

        if state == "claimed":
            # Pretend the payout already happened, so the banner should stay away.
            total = 50
            cur.execute("""INSERT INTO xp_events (user_id, event_type, reference_id, xp_amount)
                           VALUES (%s, 'ballot_lock', %s, 50)""", (user["id"], str(list_id)))
            for code in codes:
                if code in CORRECT_SLOTS:
                    cur.execute("""INSERT INTO xp_events (user_id, event_type, reference_id, xp_amount)
                                   VALUES (%s, 'ballot_correct', %s, 100)""",
                                (user["id"], f"{list_id}:{code}"))
                    total += 100
            cur.execute("UPDATE users SET xp = xp + %s WHERE id = %s", (total, user["id"]))
    conn.commit()
    return list_id, season


EXPECT = {
    "empty":    'Home banner: "Make your picks…". Sheet: 8 empty slots, all tappable.',
    "partial":  'Home banner: "N of M filled · locks …". Sheet: some filled, some empty.',
    "complete": "Home banner: GONE. Sheet reachable from the profile Ballots row.",
    "locked":   "Home banner: GONE. Sheet: read-only, status says LOCKED. Pickers refuse.",
    "graded":   'Home banner: "You called N of M — collect". Opening it grants XP once '
                "(watch the total), then the banner disappears.",
    "claimed":  "Home banner: GONE (XP already banked). Sheet shows ticks and crosses.",
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("email")
    ap.add_argument("--state", choices=STATES, default="partial")
    ap.add_argument("--league", choices=["nba", "wnba"], default="nba")
    ap.add_argument("--reset", action="store_true", help="tear down and exit")
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    conn = connect()
    with conn.cursor() as cur:
        user = find_user(cur, args.email)
    print(f"user {user['id']} ({user['display_name']}), xp {user['xp']}")

    print(f"\nClearing any existing {args.league} ballot…")
    wipe(conn, user, args.league, args.force)
    if args.reset:
        print("\nReset complete."); conn.close(); return

    list_id, season = build(conn, user, args.league, args.state)
    with conn.cursor() as cur:
        cur.execute("SELECT xp FROM users WHERE id = %s", (user["id"],))
        xp = cur.fetchone()["xp"]

    print(f"\n{args.league.upper()} {season} ballot {list_id} → state '{args.state}' (xp now {xp})")
    print(f"\nExpect: {EXPECT[args.state]}")
    print("\nWhen finished:  python backend/dev_ballot_scenarios.py "
          f"{args.email} --reset --league {args.league}")
    conn.close()


if __name__ == "__main__":
    main()
