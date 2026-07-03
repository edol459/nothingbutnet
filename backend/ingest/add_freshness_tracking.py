"""
add_freshness_tracking.py — make the wholesale-recompute tables measurable.
==========================================================================

Most tables the daily pipelines write (player_seasons, player_matchups,
team_seasons, team_rosters, wowy_lineups, player_pctiles) are fully
recomputed each run via UPDATE / INSERT ... ON CONFLICT. They have no
timestamp column, so from the data alone you cannot tell "refreshed today"
from "stale but same row count" — the health check can only catch row-count
drops, not a silently-not-refreshing pipeline.

This migration adds an `updated_at TIMESTAMPTZ` column to each of those tables
and a `BEFORE INSERT OR UPDATE` trigger that stamps it to NOW() on every write.
No pipeline/script changes needed — the upserts they already do will stamp the
column automatically. health_check.check_freshness() then reads MAX(updated_at)
per table and knows whether the pipeline actually landed data.

Idempotent and safe to re-run:
  - ADD COLUMN IF NOT EXISTS       → instant, metadata-only (nullable, no default,
                                      so no table rewrite; existing rows stay NULL
                                      until the next pipeline write stamps them).
  - CREATE OR REPLACE FUNCTION     → updates the shared trigger fn in place.
  - DROP TRIGGER IF EXISTS + CREATE → re-attaches cleanly.

    python backend/ingest/add_freshness_tracking.py          # apply
    python backend/ingest/add_freshness_tracking.py --check   # report only
"""

import os
import sys
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# Tables that are wholesale-recomputed daily and lack any date/timestamp column.
TABLES = [
    "player_seasons",
    "player_matchups",
    "team_seasons",
    "team_rosters",
    "wowy_lineups",
    "player_pctiles",
]

_FUNC = """
CREATE OR REPLACE FUNCTION set_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
"""


def _db_url():
    return os.getenv("DATABASE_URL") or os.getenv("DATABASE_PUBLIC_URL")


def apply(conn):
    cur = conn.cursor()
    cur.execute(_FUNC)
    for t in TABLES:
        # to_regclass returns NULL if the table doesn't exist — skip cleanly.
        cur.execute("SELECT to_regclass(%s)", (f"public.{t}",))
        if cur.fetchone()[0] is None:
            print(f"  ⏭  {t} — table not found, skipping")
            continue
        cur.execute(f"ALTER TABLE {t} ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ")
        cur.execute(f"DROP TRIGGER IF EXISTS trg_{t}_updated_at ON {t}")
        cur.execute(
            f"CREATE TRIGGER trg_{t}_updated_at "
            f"BEFORE INSERT OR UPDATE ON {t} "
            f"FOR EACH ROW EXECUTE FUNCTION set_updated_at()")
        print(f"  ✅ {t} — updated_at column + auto-stamp trigger installed")
    conn.commit()
    cur.close()


def check(conn):
    cur = conn.cursor()
    print("Current freshness-tracking state:")
    for t in TABLES:
        cur.execute("""SELECT 1 FROM information_schema.columns
                       WHERE table_name=%s AND column_name='updated_at'""", (t,))
        has_col = cur.fetchone() is not None
        cur.execute("""SELECT 1 FROM pg_trigger
                       WHERE tgname=%s AND NOT tgisinternal""", (f"trg_{t}_updated_at",))
        has_trg = cur.fetchone() is not None
        newest = None
        if has_col:
            cur.execute(f"SELECT MAX(updated_at) FROM {t}")
            newest = cur.fetchone()[0]
        flag = "✅" if (has_col and has_trg) else "❌"
        print(f"  {flag} {t:16s} col={has_col!s:5} trigger={has_trg!s:5} "
              f"newest={newest}")
    cur.close()


def main():
    url = _db_url()
    if not url:
        print("ERROR: neither DATABASE_URL nor DATABASE_PUBLIC_URL is set",
              file=sys.stderr)
        sys.exit(2)
    conn = psycopg2.connect(url)
    if "--check" in sys.argv:
        check(conn)
    else:
        print("Installing updated_at freshness tracking…")
        apply(conn)
        print()
        check(conn)
    conn.close()


if __name__ == "__main__":
    main()
