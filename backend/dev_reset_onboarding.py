"""
ydkball — Dev helper: replay onboarding
=============================================================
python backend/dev_reset_onboarding.py you@example.com
python backend/dev_reset_onboarding.py you@example.com --full

Onboarding shows exactly once per account (`users.onboarded_at`), which is
correct behaviour and makes it impossible to test twice. This clears the flag so
the flow runs again on the next app launch.

Two modes, because the flow deliberately behaves differently for the two
audiences it has:

  default  clears only `onboarded_at`. The account keeps its team, follows and
           watchlist, so you get the RETURNING-USER path — every screen shows
           with the existing choices pre-selected. This is what all 310 existing
           accounts will see.

  --full   also clears allegiance, player follows, watchlist subscriptions and
           pinned favourites, for the TRUE FIRST-RUN path a brand-new signup
           gets. Destructive to that account's data — it is a dev tool, so it
           refuses to run without an explicit email.

Allegiance history rows are removed too under --full; otherwise the next pick
would look like a team switch and show a broken-streak warning.

DEV ONLY. Writes to whatever DATABASE_URL points at, which is production.
"""
import os, sys, argparse
from dotenv import load_dotenv
import psycopg2

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("DATABASE_URL not found."); sys.exit(1)

parser = argparse.ArgumentParser()
parser.add_argument("email", help="account to reset (no default — this is destructive)")
parser.add_argument("--full", action="store_true",
                    help="also clear team, follows, watchlist and pins (true first-run)")
args = parser.parse_args()


def run():
    conn = psycopg2.connect(DATABASE_URL)
    cur  = conn.cursor()
    cur.execute("SELECT id, display_name FROM users WHERE email = %s", (args.email,))
    row = cur.fetchone()
    if not row:
        print(f"No user with email {args.email}"); sys.exit(1)
    uid, name = row

    cur.execute("UPDATE users SET onboarded_at = NULL WHERE id = %s", (uid,))
    cleared = ["onboarded_at"]

    if args.full:
        for table, label in (
            ("team_allegiance",  "allegiance (incl. history)"),
            ("player_follows",   "player follows"),
            ("watchlist_teams",  "watchlist teams"),
            ("watchlist_games",  "watchlist games"),
            ("favorite_players", "pinned favourites"),
        ):
            cur.execute(f"DELETE FROM {table} WHERE user_id = %s", (uid,))
            cleared.append(f"{label} ({cur.rowcount})")
        # favorite_team is the denormalised cache of allegiance — leaving it set
        # would pre-select a team the allegiance table no longer knows about.
        cur.execute("UPDATE users SET favorite_team = NULL WHERE id = %s", (uid,))
        cleared.append("favorite_team")

    conn.commit()
    cur.close(); conn.close()

    mode = "FULL first-run" if args.full else "returning-user"
    print(f"✅ {name} (id {uid}) reset — {mode} path")
    for c in cleared:
        print(f"   · cleared {c}")
    print("\nRelaunch the app (or pull-to-refresh auth) and onboarding will show again.")


if __name__ == "__main__":
    run()
