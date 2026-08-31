"""
ydkball — Shared season resolution
==================================
One source of truth for "what season are we in?" across the ingest pipelines.

Both leagues resolve from the `games` table — the newest season that has a
Final regular-season game. That table is kept current by server.py's background
scoreboard poller, which runs independently of these pipelines, so the season
advances on its own the night real games first tip off.

Two rules worth keeping:

1. Never resolve the season from a table the pipelines themselves write (e.g.
   player_seasons). The season would then be read from data that only exists
   once that season has already been fetched, so it can never advance — the
   pipeline stays pinned to the previous season forever.

2. Date math is a fallback, not the primary source. The date rule flips weeks
   before opening night (Oct 1 for the NBA, May 1 for the WNBA) while the stats
   APIs still have nothing to serve, so leading with it would produce weeks of
   empty fetches every year.

Manual backfills should keep passing --season explicitly; these helpers only
supply the default.
"""

import os
from datetime import date

import psycopg2


# ── Date-based fallbacks ──────────────────────────────────────────────

def season_from_date(today=None) -> str:
    """NBA season label for a date, e.g. '2025-26'. Season starts in October."""
    today = today or date.today()
    y, m = today.year, today.month
    if m >= 10:
        return f"{y}-{str(y + 1)[2:]}"
    return f"{y - 1}-{str(y)[2:]}"


def season_type_from_date(today=None) -> str:
    """'Playoffs' from ~Apr 20 through June, else 'Regular Season'."""
    today = today or date.today()
    m, d = today.month, today.day
    if (m == 4 and d >= 20) or m in (5, 6):
        return "Playoffs"
    return "Regular Season"


def wnba_season_from_date(today=None) -> str:
    """WNBA season year, e.g. '2026'. Season runs May–October."""
    today = today or date.today()
    return str(today.year) if today.month >= 5 else str(today.year - 1)


# ── Games-table resolution ────────────────────────────────────────────

def _max_played_season(league: str, pattern: str):
    """Newest season with a Final regular-season game, or None if unavailable.

    `pattern` is a LIKE mask matching that league's season label shape, so a bad
    `league` value on a row can never leak a WNBA-style label ('2026') into an
    NBA answer ('2026-27') or vice versa — they sort against each other badly.
    """
    url = os.getenv("DATABASE_URL")
    if not url:
        return None
    conn = None
    try:
        conn = psycopg2.connect(url)
        cur = conn.cursor()
        cur.execute("""
            SELECT MAX(season) FROM games
            WHERE league = %s
              AND season_type = 'Regular Season'
              AND status = 'Final'
              AND season LIKE %s
        """, (league, pattern))
        row = cur.fetchone()
        cur.close()
        return row[0] if row and row[0] else None
    except Exception as e:
        print(f"⚠️  Could not detect {league.upper()} season from games: {e}")
        return None
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def current_season() -> str:
    """Active NBA season, e.g. '2025-26'."""
    return _max_played_season("nba", "____-__") or season_from_date()


def wnba_current_season() -> str:
    """Active WNBA season, e.g. '2026'."""
    return _max_played_season("wnba", "____") or wnba_season_from_date()


def roster_season(league: str = "nba") -> str:
    """The season to ask for when fetching *membership* — rosters, coaches, team
    assignments — as opposed to stats.

    Rule 2 above says date math is a fallback because the stats APIs have nothing
    to serve until games are played. Rosters are the exception: the league
    publishes next season's team assignments months before tipoff, so pinning
    them to current_season() means refetching last season's roster every day all
    summer and every offseason trade staying invisible.

    Resolved from `scheduled_games`, which is the same source the awards ballot
    uses to decide which season it's predicting — the schedule is published well
    before opening night, which is exactly the window this matters in. Falls back
    to the played-games season when no schedule exists yet, so this can never
    return something the API has nothing for.
    """
    pattern = "____-__" if league == "nba" else "____"
    try:
        conn = psycopg2.connect(os.getenv("DATABASE_URL"))
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT season FROM scheduled_games
                    WHERE league = %s AND season_type = 'Regular Season'
                      AND season LIKE %s
                    GROUP BY season
                    HAVING MAX(game_date) >= CURRENT_DATE
                    ORDER BY season ASC
                    LIMIT 1
                """, (league, pattern))
                row = cur.fetchone()
                if row and row[0]:
                    return row[0]
        finally:
            conn.close()
    except Exception:
        pass
    return current_season() if league == "nba" else wnba_current_season()
