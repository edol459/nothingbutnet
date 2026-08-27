#!/usr/bin/env python3
"""What testers actually do with the game log.

    python backend/ingest/log_funnel.py            # last 14 days
    python backend/ingest/log_funnel.py --days 3

Why this exists: the grading control was rebuilt six times on taste alone, because the only
number anyone had was "8 users have ever graded a performance" — and that predated all of
it. These events answer the questions that were being guessed at.

Deliberately fed by SERVER-side events. The build in testers' hands can't be asked to report
anything without another App Store release, but the server already sees every open, draft
and publish: `live_stats` on /api/players/today is requested by the log sheet and nothing
else, so that call IS someone opening a log.
"""
from __future__ import annotations

import argparse
import os
import sys

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
load_dotenv()


def bar(n: int, of: int, width: int = 26) -> str:
    if of <= 0:
        return ""
    return "█" * round(width * n / of) + "░" * (width - round(width * n / of))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=14)
    args = ap.parse_args()
    d = args.days
    since = f"created_at >= NOW() - INTERVAL '{d} days'"

    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    def scalar(sql, params=()):
        cur.execute(sql, params)
        row = cur.fetchone()
        return (list(row.values())[0] if row else 0) or 0

    print(f"\n─── game log · last {d} days ───\n")

    # Counted in PEOPLE, not events: one enthusiast opening forty logs would otherwise
    # look like forty interested users, which is exactly the kind of number that gets a
    # feature declared a success.
    opened = scalar(f"SELECT COUNT(DISTINCT user_id) FROM analytics_events "
                    f"WHERE event_type='log_opened' AND {since}")
    drafted = scalar(f"SELECT COUNT(DISTINCT user_id) FROM analytics_events "
                     f"WHERE event_type='draft_started' AND {since}")
    published = scalar(f"SELECT COUNT(DISTINCT user_id) FROM analytics_events "
                       f"WHERE event_type='log_published' AND {since}")

    for label, n in [("opened a log", opened), ("started a draft", drafted),
                     ("published a log", published)]:
        pct = f"{100 * n / opened:.0f}%" if opened else "—"
        print(f"  {label:<17} {n:>4} users  {bar(n, opened)}  {pct}")

    if not opened:
        print("\n  Nothing recorded yet. Either no one has opened a log since the")
        print("  instrumented backend deployed, or it hasn't deployed.\n")
        cur.close(); conn.close()
        return 0

    print(f"\n─── do people actually grade? ───\n")
    cur.execute(f"""
        SELECT COUNT(*) logs,
               COUNT(*) FILTER (WHERE (metadata->>'grades')::int > 0)        with_grades,
               COALESCE(SUM((metadata->>'grades')::int), 0)                  total_grades,
               COALESCE(ROUND(AVG(NULLIF((metadata->>'grades')::int, 0)), 1), 0) avg_graded,
               COUNT(*) FILTER (WHERE (metadata->>'has_rating')::bool)       with_rating,
               COUNT(*) FILTER (WHERE (metadata->>'note_length')::int > 0)   with_note,
               COUNT(*) FILTER (WHERE (metadata->>'attended')::bool)         attended,
               COUNT(*) FILTER (WHERE (metadata->>'is_update')::bool)        updates
        FROM analytics_events WHERE event_type='log_published' AND {since}
    """)
    r = cur.fetchone()
    logs = r["logs"] or 0
    if logs:
        def pc(n): return f"{100 * n / logs:>3.0f}%"
        print(f"  {logs} logs published\n")
        print(f"    graded a player    {r['with_grades']:>3}  {pc(r['with_grades'])}  {bar(r['with_grades'], logs)}")
        print(f"    rated the game     {r['with_rating']:>3}  {pc(r['with_rating'])}  {bar(r['with_rating'], logs)}")
        print(f"    wrote a note       {r['with_note']:>3}  {pc(r['with_note'])}  {bar(r['with_note'], logs)}")
        print(f"    was at the game    {r['attended']:>3}  {pc(r['attended'])}")
        print(f"    edit, not a first  {r['updates']:>3}  {pc(r['updates'])}")
        print(f"\n    {r['total_grades']} grades total · {r['avg_graded']} per log that graded anyone")

    # The number the unfinished-logs surface was arguing about, and the reason the submit
    # gate exists at all.
    print(f"\n─── drafts that never became logs ───\n")
    started = scalar(f"""SELECT COUNT(*) FROM (
                           SELECT DISTINCT user_id, metadata->>'game_id' g
                           FROM analytics_events WHERE event_type='draft_started' AND {since}) t""")
    abandoned = scalar(f"""
        SELECT COUNT(*) FROM (
            SELECT DISTINCT user_id, metadata->>'game_id' g
            FROM analytics_events WHERE event_type='draft_started' AND {since}
            EXCEPT
            SELECT DISTINCT user_id, metadata->>'game_id'
            FROM analytics_events WHERE event_type='log_published' AND {since}
        ) t
    """)
    print(f"  {abandoned} of {started} abandoned"
          + (f"  ({100 * abandoned / started:.0f}%)" if started else ""))

    # Decides whether bullet lists and paragraph blocks were ever worth building.
    print(f"\n─── note length ───\n")
    cur.execute(f"""
        SELECT (metadata->>'note_length')::int n FROM analytics_events
        WHERE event_type='log_published' AND (metadata->>'note_length')::int > 0 AND {since}
        ORDER BY n
    """)
    lens = [x["n"] for x in cur.fetchall()]
    if lens:
        print(f"  {len(lens)} notes · shortest {lens[0]} · median {lens[len(lens)//2]} · longest {lens[-1]}")
        print(f"  over 200 chars: {sum(1 for x in lens if x > 200)}")
        print(f"  (lists and blocks are only worth building if that last number grows)")
    else:
        print("  none yet")

    # Which games get logged tells you whether this is a live-game behaviour or a
    # next-morning one — that distinction decides whether push is worth building.
    print(f"\n─── live or after the fact? ───\n")
    cur.execute(f"""
        SELECT metadata->>'status' status, COUNT(*) n
        FROM analytics_events WHERE event_type='log_opened' AND {since}
        GROUP BY 1 ORDER BY 2 DESC
    """)
    rows = cur.fetchall()
    total = sum(x["n"] for x in rows) or 1
    for x in rows:
        print(f"  {(x['status'] or 'unknown'):<10} {x['n']:>4} opens  {bar(x['n'], total)}"
              f"  {100 * x['n'] / total:.0f}%")

    print()
    cur.close(); conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
