"""
ydkball — watchlist notifications
=============================================================
python backend/ingest/send_notifications.py [--dry-run] [--kind digest|final]

Two jobs, one entry point, run on a Railway cron every 15 minutes.

  digest  Once a day, at the time each user chose, listing their watchlist games
          for that day. Skipped entirely on days they have none — "0 games today"
          is not worth a notification.

  final   When a watched game ends. This is the one tied to logging, so it is the
          only type on by default.

Everything is server-side on purpose. An earlier design scheduled these as local
notifications on the device for to-the-minute delivery, but that traded a 15-minute
window for a 64-notification cap, rescheduling on every watchlist edit, and content
that froze at scheduling time. Computing at send time is correct by construction.

WHAT MAKES THIS SAFE TO RE-RUN

`notification_sends` has PRIMARY KEY (user_id, kind, reference), and every send is
recorded BEFORE the push goes out. A crash, an overlapping cron, or a manual re-run
cannot double-notify: the insert simply conflicts and that user is skipped. Recording
first means the worst case is a missed notification, never a duplicate — the right
way round for something that buzzes a phone.

TIMING

Digest fires on the first run at or after the user's local time, so it is never
early and at most one cron interval late. `notify_digest_at` is a local wall-clock
time and `users.timezone` an IANA name, so DST is handled by Postgres rather than by
arithmetic that would be an hour wrong for half the year.
"""
import os, sys, argparse
from datetime import date
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import push  # noqa: E402

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

DEFAULT_TZ = "America/New_York"

# Everyone whose local clock has reached their chosen time today, who has at least one
# watchlist game today, and who has not already been sent today's digest.
DIGEST_DUE = """
WITH me AS (
    SELECT u.id AS user_id,
           COALESCE(u.timezone, %(default_tz)s) AS tz,
           u.notify_digest_at AS at
      FROM users u
     WHERE u.notify_digest
       AND EXISTS (SELECT 1 FROM device_tokens d
                    WHERE d.user_id = u.id AND d.invalid_at IS NULL)
),
local AS (
    SELECT me.*,
           (NOW() AT TIME ZONE tz)::date AS local_date,
           (NOW() AT TIME ZONE tz)::time AS local_time
      FROM me
)
SELECT user_id, local_date, tz
  FROM local
 WHERE local_time >= at
   AND NOT EXISTS (SELECT 1 FROM notification_sends ns
                    WHERE ns.user_id = local.user_id AND ns.kind = 'digest'
                      AND ns.reference = local.local_date::text)
"""

# Games that finished recently and are on someone's watchlist. `games` only ever holds
# completed games, so presence there IS the "it ended" signal.
FINAL_DUE = """
SELECT g.game_id, g.league, g.home_team_abbr, g.away_team_abbr,
       g.home_score, g.away_score, u.id AS user_id
  FROM games g
  JOIN users u ON u.notify_final
  JOIN device_tokens d ON d.user_id = u.id AND d.invalid_at IS NULL
 WHERE g.game_date >= CURRENT_DATE - 1
   AND (
     EXISTS (SELECT 1 FROM watchlist_teams wt
              WHERE wt.user_id = u.id AND wt.league = g.league
                AND (wt.team_abbr = g.home_team_abbr OR wt.team_abbr = g.away_team_abbr))
     OR EXISTS (SELECT 1 FROM watchlist_games wg
                 WHERE wg.user_id = u.id AND wg.game_id = g.game_id AND wg.action = 'add')
   )
   AND NOT EXISTS (SELECT 1 FROM watchlist_games wg
                    WHERE wg.user_id = u.id AND wg.game_id = g.game_id AND wg.action = 'remove')
   AND NOT EXISTS (SELECT 1 FROM notification_sends ns
                    WHERE ns.user_id = u.id AND ns.kind = 'final' AND ns.reference = g.game_id)
 GROUP BY g.game_id, g.league, g.home_team_abbr, g.away_team_abbr,
          g.home_score, g.away_score, u.id
"""

DIGEST_GAMES = """
SELECT sg.away_team_abbr, sg.home_team_abbr
  FROM scheduled_games sg
 WHERE sg.game_date = %(day)s
   AND (
     EXISTS (SELECT 1 FROM watchlist_teams wt
              WHERE wt.user_id = %(uid)s AND wt.league = sg.league
                AND (wt.team_abbr = sg.home_team_abbr OR wt.team_abbr = sg.away_team_abbr))
     OR EXISTS (SELECT 1 FROM watchlist_games wg
                 WHERE wg.user_id = %(uid)s AND wg.game_id = sg.game_id AND wg.action = 'add')
   )
   AND NOT EXISTS (SELECT 1 FROM watchlist_games wg
                    WHERE wg.user_id = %(uid)s AND wg.game_id = sg.game_id AND wg.action = 'remove')
 ORDER BY sg.game_time_utc NULLS LAST
"""


def _tokens_for(cur, user_id):
    cur.execute("""SELECT token, environment FROM device_tokens
                    WHERE user_id = %s AND invalid_at IS NULL""", (user_id,))
    return cur.fetchall()


def _deliver(cur, user_id, kind, reference, title, body, data, dry):
    """Claim the send, then push. Claiming first is what makes a re-run safe."""
    if dry:
        print(f"  [dry] user {user_id} {kind}/{reference}: {title} — {body}")
        return 0
    cur.execute("""INSERT INTO notification_sends (user_id, kind, reference)
                   VALUES (%s, %s, %s) ON CONFLICT DO NOTHING""",
                (user_id, kind, reference))
    if cur.rowcount == 0:
        return 0            # someone else already sent it
    sent = 0
    by_env = {}
    for row in _tokens_for(cur, user_id):
        by_env.setdefault(row["environment"], []).append(row["token"])
    for env, tokens in by_env.items():
        for res in push.send(tokens, title, body, data=data, environment=env,
                             collapse_id=f"{kind}:{reference}"):
            if res["dead"]:
                cur.execute("UPDATE device_tokens SET invalid_at = NOW() WHERE token = %s",
                            (res["token"],))
            elif res["status"] == 200:
                sent += 1
    return sent


def run_digest(conn, dry=False):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(DIGEST_DUE, {"default_tz": DEFAULT_TZ})
    due = cur.fetchall()
    print(f"digest: {len(due)} user(s) due")
    total = 0
    for row in due:
        cur.execute(DIGEST_GAMES, {"uid": row["user_id"], "day": row["local_date"]})
        games = cur.fetchall()
        if not games:
            # Nothing on today. Claim the day anyway so we don't re-check every 15
            # minutes until midnight, but send nothing.
            if not dry:
                cur.execute("""INSERT INTO notification_sends (user_id, kind, reference)
                               VALUES (%s, 'digest', %s) ON CONFLICT DO NOTHING""",
                            (row["user_id"], str(row["local_date"])))
            continue
        matchups = " · ".join(f"{g['away_team_abbr']} @ {g['home_team_abbr']}" for g in games[:3])
        if len(games) > 3:
            matchups += f" +{len(games) - 3}"
        n = len(games)
        total += _deliver(cur, row["user_id"], "digest", str(row["local_date"]),
                          f"{n} game{'' if n == 1 else 's'} on your watchlist today",
                          matchups, {"kind": "digest", "date": str(row["local_date"])}, dry)
        conn.commit()
    print(f"digest: {total} push(es) delivered")
    cur.close()


def run_final(conn, dry=False):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(FINAL_DUE)
    due = cur.fetchall()
    print(f"final: {len(due)} (user, game) pair(s) due")
    total = 0
    for g in due:
        away, home = g["away_team_abbr"], g["home_team_abbr"]
        a, h = g["away_score"], g["home_score"]
        # Winner first — it reads like a score line rather than a database row.
        if a is not None and h is not None and h > a:
            line = f"{home} {h}, {away} {a}"
        else:
            line = f"{away} {a}, {home} {h}"
        total += _deliver(cur, g["user_id"], "final", g["game_id"],
                          f"Final · {line}",
                          "Log it and pick your Player of the Game.",
                          {"kind": "final", "game_id": g["game_id"], "league": g["league"]}, dry)
        conn.commit()
    print(f"final: {total} push(es) delivered")
    cur.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="report without sending or recording")
    ap.add_argument("--kind", choices=["digest", "final"], help="run only one job")
    args = ap.parse_args()

    if not push.is_configured() and not args.dry_run:
        print("APNs is not configured — set APNS_* and try again."); sys.exit(1)

    conn = psycopg2.connect(DATABASE_URL)
    try:
        if args.kind in (None, "final"):  run_final(conn, args.dry_run)
        if args.kind in (None, "digest"): run_digest(conn, args.dry_run)
        if args.dry_run:
            conn.rollback(); print("\ndry run — nothing sent, nothing recorded")
        else:
            conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
