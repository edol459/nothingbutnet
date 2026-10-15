"""
ydkball — pipeline doctor (read-only)
=============================================================
python backend/ingest/pipeline_doctor.py

Run this ON THE WINDOWS PC when the health report says the local pipeline
did not run but the PC says it did.

That disagreement has only a few possible causes, and this tells them apart:
the PC is pointed at a different database, it cannot reach the database at
all, or the scheduled job is not actually launching. The decisive question is
whether the rows this prints match the ones the health report sees — if the
PC's view of pipeline_runs differs from production's, it is writing somewhere
else.

Writes nothing. Safe to run any time.
"""
import os, sys, socket, traceback
from datetime import datetime

print("=" * 64)
print(f"ydkball pipeline doctor — {datetime.now():%Y-%m-%d %H:%M:%S} local")
print(f"host: {socket.gethostname()}")
print("=" * 64)

# ── 1. Which .env, and which database does it name? ────────────────
try:
    from dotenv import load_dotenv, find_dotenv
    found = find_dotenv(usecwd=True)
    print(f"\n[1] .env file: {found or 'NOT FOUND'}")
    load_dotenv(found or None)
except Exception as e:
    print(f"\n[1] dotenv failed: {e}")

url = os.getenv("DATABASE_URL")
if not url:
    print("    DATABASE_URL: NOT SET  ← nothing can work without this")
    sys.exit(1)

# Show enough to compare against Railway without printing the password.
try:
    from urllib.parse import urlparse
    u = urlparse(url)
    print(f"    host: {u.hostname}")
    print(f"    port: {u.port}")
    print(f"    db:   {(u.path or '').lstrip('/')}")
    print(f"    user: {u.username}")
except Exception as e:
    print(f"    could not parse DATABASE_URL: {e}")

# ── 2. Can it actually connect? ────────────────────────────────────
print("\n[2] connecting…")
try:
    import psycopg2
    conn = psycopg2.connect(url, connect_timeout=15)
    cur = conn.cursor()
    cur.execute("SELECT current_database(), inet_server_addr()::text, NOW() AT TIME ZONE 'UTC'")
    db, addr, now = cur.fetchone()
    print(f"    OK — database={db}  server={addr}  utc_now={now}")
except Exception as e:
    print(f"    FAILED: {e}")
    print("\n    → The PC cannot reach the database. That alone explains a")
    print("      missing run: the pipeline dies before it can record anything.")
    sys.exit(1)

# ── 3. Does the PC see the same pipeline_runs as production? ───────
print("\n[3] last 5 local_daily runs AS THIS PC SEES THEM:")
try:
    cur.execute("""
        SELECT id, status, host, started_at, duration_s
          FROM pipeline_runs WHERE pipeline='local_daily'
         ORDER BY started_at DESC LIMIT 5
    """)
    rows = cur.fetchall()
    if not rows:
        print("    (none at all — almost certainly a different database)")
    for rid, st, host, started, dur in rows:
        print(f"    #{rid:<5} {st:<9} {host or '-':<17} {str(started)[:19]} "
              f"{f'{dur:.0f}s' if dur else '-'}")
    print("\n    → Compare the top row to the health report. If this PC sees a")
    print("      newer run than production does, it is writing to another database.")
except Exception as e:
    print(f"    query failed: {e}")

# ── 4. The calls main() makes BEFORE it registers the run. ─────────
# A failure here kills the script before start_run(), which is why a broken
# run can leave no row at all — not even a failed one.
print("\n[4] season resolution (runs before the pipeline registers itself):")
try:
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import season_util
    print(f"    current_season():      {season_util.current_season()}")
    print(f"    season_type_from_date(): {season_util.season_type_from_date()}")
    print(f"    roster_season():       {season_util.roster_season()}")
    print("    OK — main() would get past this point")
except Exception:
    print("    FAILED — this is what kills the run before it is recorded:")
    traceback.print_exc()

try:
    cur.close(); conn.rollback(); conn.close()
except Exception:
    pass
print("\n" + "=" * 64)
