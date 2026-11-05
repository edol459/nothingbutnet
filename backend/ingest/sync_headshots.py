"""
sync_headshots.py — record which players actually have NBA headshot art.
=======================================================================

The CDN answers **200 for every player id**, serving a shared silhouette when it
has no photo — the same trap LockerVision sets for jerseys (see
docs/jersey-ingest.md). HTTP status tells you nothing; only the image bytes do.

So this probes each player once, hashes the result, and writes
`players.has_headshot`. Surfaces that render a face (Higher or Lower's panels,
browse, profiles) can then skip the players who would show an empty grey card.

The placeholder is identified by **sharing** rather than by a hardcoded hash: a
real headshot is unique to its player, so any image returned for several players
is by definition a stand-in. That keeps working if the CDN swaps its silhouette.

    python backend/ingest/sync_headshots.py              # players with gamelogs
    python backend/ingest/sync_headshots.py --all        # every row in players
    python backend/ingest/sync_headshots.py --recheck    # re-probe, don't skip known
"""

import os
import sys
import hashlib
import argparse
import collections
import concurrent.futures as cf

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv

load_dotenv()

CDN = "https://cdn.nba.com/headshots/nba/latest/260x190/{}.png"
# A hash returned for at least this many players is a stand-in, not a portrait.
SHARED_MIN = 3
WORKERS = 16


def _probe(session, pid):
    try:
        r = session.get(CDN.format(pid), timeout=15)
        if r.status_code != 200 or not r.content:
            return pid, None
        return pid, hashlib.sha256(r.content).hexdigest()
    except requests.RequestException:
        return pid, None          # transient: left unknown, retried next run


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--all", action="store_true",
                    help="probe every row in players, not just those with gamelogs")
    ap.add_argument("--recheck", action="store_true",
                    help="re-probe players already recorded")
    args = ap.parse_args()

    conn = psycopg2.connect(os.getenv("DATABASE_URL"),
                            cursor_factory=psycopg2.extras.RealDictCursor)
    with conn.cursor() as cur:
        cur.execute("ALTER TABLE players ADD COLUMN IF NOT EXISTS has_headshot BOOLEAN")
    conn.commit()

    where = [] if args.all else [
        "p.player_id IN (SELECT DISTINCT player_id FROM player_gamelogs)"]
    if not args.recheck:
        where.append("p.has_headshot IS NULL")
    clause = ("WHERE " + " AND ".join(where)) if where else ""
    with conn.cursor() as cur:
        cur.execute(f"SELECT p.player_id FROM players p {clause}")
        ids = [r["player_id"] for r in cur.fetchall()]

    if not ids:
        print("nothing to probe (use --recheck to re-run)")
        return

    print(f"probing {len(ids)} players…")
    session = requests.Session()
    results = {}
    with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for i, (pid, h) in enumerate(ex.map(lambda p: _probe(session, p), ids), 1):
            results[pid] = h
            if i % 500 == 0:
                print(f"  {i}/{len(ids)}")

    counts = collections.Counter(h for h in results.values() if h)
    placeholders = {h for h, n in counts.items() if n >= SHARED_MIN}
    print(f"  {len(placeholders)} placeholder image(s) identified "
          f"covering {sum(counts[h] for h in placeholders)} players")

    rows = [(pid, h not in placeholders) for pid, h in results.items() if h]
    unknown = sum(1 for h in results.values() if h is None)
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            "UPDATE players SET has_headshot = v.ok FROM (VALUES %s) "
            "AS v(player_id, ok) WHERE players.player_id = v.player_id",
            rows, template="(%s, %s::boolean)")
    conn.commit()

    have = sum(1 for _, ok in rows if ok)
    print(f"recorded {len(rows)}: {have} with art, {len(rows) - have} placeholder"
          + (f", {unknown} unreachable (left unknown, retried next run)" if unknown else ""))
    conn.close()


if __name__ == "__main__":
    main()
