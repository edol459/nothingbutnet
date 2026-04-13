"""
ydkball — Player PVA (Possession Value Added) Computation
=========================================================
Aggregates possession-level expected-value credits into per-player,
per-season stats and writes them to player_pva_season.

Must be run AFTER:
  1. collect_to_db.py  — populates possessions / events / lineups tables
  2. train_ev_model.py — backfills possessions.expected_points

Usage:
    python backend/ingest/compute_pva.py
    python backend/ingest/compute_pva.py --seasons 2024-25
    python backend/ingest/compute_pva.py --seasons 2024-25 --season-type Playoffs
"""

import argparse
import logging
import os

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def get_conn():
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(url, cursor_factory=psycopg2.extras.RealDictCursor)


# ─────────────────────────────────────────────────────────────────────────────
# Attribution logic (SQL-based for performance)
#
# Offensive credit:
#   - We identify the "primary actor" for each possession as the player
#     who executed the final meaningful event (shot, turnover, free throw).
#   - ASSISTED made FGs: 50% to shooter, 50% to assister.
#     The assister comes directly from possession_events.assist_player_id,
#     which pbpstats populates from event.player2_id (no name parsing needed).
#     If assist_player_id is NULL the shooter keeps 100%.
#   - All other endings (unassisted FG, missed FG, turnover, FT): 100% to
#     the primary actor.
#   - end_reason = end_period / end_game / jumpball → excluded entirely.
#
# Defensive credit:
#   - All 5 defenders split the negated PVA equally:
#     each gets -(points_scored - expected_points) / 5
# ─────────────────────────────────────────────────────────────────────────────

ATTRIBUTION_SQL = """
WITH

-- ── Step 1: Identify the primary offensive actor per possession ──────────────
-- Walk backward through events; grab the last shot, turnover, or FT,
-- plus the assist_player_id on that event (set by pbpstats via event.player2_id).
last_actor AS (
    SELECT DISTINCT ON (pe.possession_id)
        pe.possession_id,
        pe.player_id          AS primary_player_id,
        pe.action_type        AS actor_action_type,
        pe.assist_player_id
    FROM possession_events pe
    JOIN possessions p ON p.id = pe.possession_id
    WHERE p.season      = %s
      AND p.expected_points IS NOT NULL
      AND pe.action_type IN ('2pt', '3pt', 'turnover', 'freethrow')
      AND pe.player_id IS NOT NULL
      AND NOT (ABS(p.score_margin_offense) > 15 AND p.game_seconds_start > 3 * 720)
    ORDER BY pe.possession_id, pe.event_index DESC
),

-- ── Step 2: Pass through assist_player_id directly ───────────────────────────
-- pbpstats gives us event.player2_id for the assister, so no name matching needed.
-- Only include when it's a made FG (assist only meaningful on makes).
assist_resolved AS (
    SELECT
        la.possession_id,
        la.assist_player_id
    FROM last_actor la
    WHERE la.assist_player_id IS NOT NULL
      AND la.actor_action_type IN ('2pt', '3pt')
),

-- ── Step 3: Compute raw PVA per possession ────────────────────────────────────
poss_pva AS (
    SELECT
        p.id               AS possession_id,
        p.season,
        p.points_scored,
        p.expected_points,
        p.points_scored - p.expected_points  AS pva,
        p.end_reason,
        la.primary_player_id,
        la.actor_action_type,
        ar.assist_player_id,
        -- is_assisted: true when we successfully resolved an assister
        (ar.assist_player_id IS NOT NULL)    AS is_assisted
    FROM possessions p
    LEFT JOIN last_actor la ON la.possession_id = p.id
    LEFT JOIN assist_resolved ar ON ar.possession_id = p.id
    WHERE p.season         = %s
      AND p.expected_points IS NOT NULL
      AND NOT (ABS(p.score_margin_offense) > 15 AND p.game_seconds_start > 3 * 720)
),

-- ── Step 4a: Shooter credits ──────────────────────────────────────────────────
-- Unassisted: 100%% of PVA.  Assisted made FG: 50%% of PVA.
off_shooter_credits AS (
    SELECT
        pp.primary_player_id AS player_id,
        pp.season,
        -- shooter share: 50%% if assisted made FG, 100%% otherwise
        CASE WHEN pp.is_assisted AND pp.end_reason = 'made_fg'
             THEN pp.pva * 0.5
             ELSE pp.pva
        END                                         AS credit,
        CASE WHEN pp.end_reason = 'made_fg'
             THEN CASE WHEN pp.is_assisted THEN pp.pva * 0.5 ELSE pp.pva END
             ELSE 0 END                             AS pva_make,
        CASE WHEN pp.end_reason = 'missed_fg'       THEN pp.pva ELSE 0 END AS pva_miss,
        CASE WHEN pp.end_reason = 'turnover'        THEN pp.pva ELSE 0 END AS pva_tov,
        pp.points_scored,
        pp.expected_points
    FROM poss_pva pp
    WHERE pp.primary_player_id IS NOT NULL
),

-- ── Step 4b: Assister credits (50%% of PVA on assisted made FGs only) ─────────
off_assister_credits AS (
    SELECT
        pp.assist_player_id  AS player_id,
        pp.season,
        pp.pva * 0.5         AS credit,
        pp.pva * 0.5         AS pva_make,  -- assister credit always comes from a make
        0::real              AS pva_miss,
        0::real              AS pva_tov,
        pp.points_scored,
        pp.expected_points
    FROM poss_pva pp
    WHERE pp.is_assisted
      AND pp.end_reason  = 'made_fg'
      AND pp.assist_player_id IS NOT NULL
),

-- ── Step 5: Union all offensive credits ──────────────────────────────────────
off_credits AS (
    SELECT * FROM off_shooter_credits
    UNION ALL
    SELECT * FROM off_assister_credits
),

-- ── Step 6: Defensive credits (1/5 of negated PVA per defender) ─────────────
def_credits AS (
    SELECT
        pl.player_id,
        pp.season,
        -pp.pva / 5.0                             AS credit,
        pp.points_scored,
        pp.expected_points,
        1 AS possession_count
    FROM poss_pva pp
    JOIN possession_lineups pl ON pl.possession_id = pp.possession_id
                               AND pl.side = 'defense'
),

-- ── Step 7: Aggregate offensive ─────────────────────────────────────────────
off_agg AS (
    SELECT
        player_id,
        season,
        COUNT(*)                AS off_possessions,
        SUM(credit)             AS off_pva,
        SUM(pva_make)           AS pva_from_makes,
        SUM(pva_miss)           AS pva_from_misses,
        SUM(pva_tov)            AS pva_from_turnovers,
        AVG(expected_points)    AS avg_expected_pts,
        AVG(points_scored::real) AS avg_actual_pts
    FROM off_credits
    GROUP BY player_id, season
),

-- ── Step 8: Aggregate defensive ─────────────────────────────────────────────
def_agg AS (
    SELECT
        player_id,
        season,
        COUNT(*)    AS def_possessions,
        SUM(credit) AS def_pva
    FROM def_credits
    GROUP BY player_id, season
),

-- ── Step 9: Join and compute per-100 rates ───────────────────────────────────
combined AS (
    SELECT
        COALESCE(o.player_id, d.player_id)    AS player_id,
        COALESCE(o.season,    d.season)        AS season,
        COALESCE(o.off_possessions,  0)        AS off_possessions,
        COALESCE(o.off_pva,          0)        AS off_pva,
        COALESCE(d.def_possessions,  0)        AS def_possessions,
        COALESCE(d.def_pva,          0)        AS def_pva,
        COALESCE(o.off_possessions, 0) + COALESCE(d.def_possessions, 0) AS total_possessions,
        COALESCE(o.off_pva, 0) + COALESCE(d.def_pva, 0)                  AS total_pva,
        COALESCE(o.pva_from_makes,      0) AS pva_from_makes,
        COALESCE(o.pva_from_misses,     0) AS pva_from_misses,
        COALESCE(o.pva_from_turnovers,  0) AS pva_from_turnovers,
        COALESCE(o.avg_expected_pts,    0) AS avg_expected_pts,
        COALESCE(o.avg_actual_pts,      0) AS avg_actual_pts
    FROM off_agg o
    FULL OUTER JOIN def_agg d USING (player_id, season)
)

SELECT
    c.*,
    -- per-100 normalisations (guard against zero-division)
    CASE WHEN c.off_possessions > 0
         THEN c.off_pva   / c.off_possessions   * 100 ELSE 0 END AS off_pva_per_100,
    CASE WHEN c.def_possessions > 0
         THEN c.def_pva   / c.def_possessions   * 100 ELSE 0 END AS def_pva_per_100,
    CASE WHEN c.total_possessions > 0
         THEN c.total_pva / c.total_possessions * 100 ELSE 0 END AS total_pva_per_100
FROM combined c
WHERE c.total_possessions > 0
ORDER BY total_pva DESC
"""


def resolve_player_names(conn, player_ids: list[int]) -> dict[int, str]:
    """Look up player names from the players table."""
    if not player_ids:
        return {}
    cur = conn.cursor()
    cur.execute(
        "SELECT player_id, player_name FROM players WHERE player_id = ANY(%s)",
        (player_ids,)
    )
    rows = cur.fetchall()
    cur.close()
    return {r["player_id"]: r["player_name"] for r in rows}


def compute_and_write(season: str, season_type: str):
    log.info(f"Computing PVA for {season} {season_type}...")

    conn = get_conn()
    cur  = conn.cursor()

    # Check we have possession data
    cur.execute(
        "SELECT COUNT(*) AS n FROM possessions WHERE season = %s AND expected_points IS NOT NULL",
        (season,)
    )
    n = cur.fetchone()["n"]
    if n == 0:
        log.error(
            f"No possessions with expected_points found for {season}. "
            "Run collect_to_db.py then train_ev_model.py first."
        )
        cur.close(); conn.close()
        return

    log.info(f"  Found {n:,} possessions with expected_points")

    # Run attribution SQL
    cur.execute(ATTRIBUTION_SQL, (season, season))  # season used twice: last_actor + poss_pva
    rows = cur.fetchall()
    log.info(f"  Attribution produced {len(rows)} player rows")

    if not rows:
        log.warning("No rows produced. Check that possession_lineups is populated.")
        cur.close(); conn.close()
        return

    # Resolve player names
    player_ids  = [r["player_id"] for r in rows]
    name_map    = resolve_player_names(conn, player_ids)

    # Upsert into player_pva_season
    upsert_cur = conn.cursor()
    inserted = 0
    for r in rows:
        pid  = r["player_id"]
        name = name_map.get(pid, f"Player {pid}")

        upsert_cur.execute("""
            INSERT INTO player_pva_season (
                player_id, player_name, season, season_type,
                off_possessions, off_pva,
                def_possessions, def_pva,
                total_possessions, total_pva,
                off_pva_per_100, def_pva_per_100, total_pva_per_100,
                pva_from_makes, pva_from_misses, pva_from_turnovers,
                avg_expected_pts, avg_actual_pts,
                computed_at
            ) VALUES (
                %(player_id)s, %(player_name)s, %(season)s, %(season_type)s,
                %(off_possessions)s, %(off_pva)s,
                %(def_possessions)s, %(def_pva)s,
                %(total_possessions)s, %(total_pva)s,
                %(off_pva_per_100)s, %(def_pva_per_100)s, %(total_pva_per_100)s,
                %(pva_from_makes)s, %(pva_from_misses)s, %(pva_from_turnovers)s,
                %(avg_expected_pts)s, %(avg_actual_pts)s,
                NOW()
            )
            ON CONFLICT (player_id, season, season_type) DO UPDATE SET
                off_possessions   = EXCLUDED.off_possessions,
                off_pva           = EXCLUDED.off_pva,
                def_possessions   = EXCLUDED.def_possessions,
                def_pva           = EXCLUDED.def_pva,
                total_possessions = EXCLUDED.total_possessions,
                total_pva         = EXCLUDED.total_pva,
                off_pva_per_100   = EXCLUDED.off_pva_per_100,
                def_pva_per_100   = EXCLUDED.def_pva_per_100,
                total_pva_per_100 = EXCLUDED.total_pva_per_100,
                pva_from_makes    = EXCLUDED.pva_from_makes,
                pva_from_misses   = EXCLUDED.pva_from_misses,
                pva_from_turnovers = EXCLUDED.pva_from_turnovers,
                avg_expected_pts  = EXCLUDED.avg_expected_pts,
                avg_actual_pts    = EXCLUDED.avg_actual_pts,
                computed_at       = NOW()
        """, {
            "player_id":         pid,
            "player_name":       name,
            "season":            season,
            "season_type":       season_type,
            "off_possessions":   int(r["off_possessions"]),
            "off_pva":           float(r["off_pva"]),
            "def_possessions":   int(r["def_possessions"]),
            "def_pva":           float(r["def_pva"]),
            "total_possessions": int(r["total_possessions"]),
            "total_pva":         float(r["total_pva"]),
            "off_pva_per_100":   float(r["off_pva_per_100"]),
            "def_pva_per_100":   float(r["def_pva_per_100"]),
            "total_pva_per_100": float(r["total_pva_per_100"]),
            "pva_from_makes":    float(r["pva_from_makes"]),
            "pva_from_misses":   float(r["pva_from_misses"]),
            "pva_from_turnovers":float(r["pva_from_turnovers"]),
            "avg_expected_pts":  float(r["avg_expected_pts"]),
            "avg_actual_pts":    float(r["avg_actual_pts"]),
        })
        inserted += 1

    conn.commit()
    cur.close(); upsert_cur.close(); conn.close()
    log.info(f"  Wrote {inserted} player rows to player_pva_season")

    # Quick diagnostics
    top = sorted(rows, key=lambda x: x["total_pva_per_100"], reverse=True)[:5]
    log.info("  Top 5 by total PVA/100:")
    for r in top:
        name = name_map.get(r["player_id"], str(r["player_id"]))
        log.info(
            f"    {name:<25} off={r['off_pva_per_100']:+.2f}  "
            f"def={r['def_pva_per_100']:+.2f}  "
            f"total={r['total_pva_per_100']:+.2f}  "
            f"({r['total_possessions']:,} poss)"
        )


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Compute player PVA from possession data")
    parser.add_argument(
        "--seasons", nargs="+", default=["2024-25"],
        help="Seasons to process"
    )
    parser.add_argument(
        "--season-type", default="Regular Season",
        choices=["Regular Season", "Playoffs", "All Star"],
    )
    args = parser.parse_args()

    for season in args.seasons:
        compute_and_write(season, args.season_type)


if __name__ == "__main__":
    main()
