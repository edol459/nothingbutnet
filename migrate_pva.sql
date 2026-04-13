-- ydkball — PVA (Possession Value Added) tables
-- Run with: psql $DATABASE_URL -f migrate_pva.sql

-- ============================================================
-- player_pva_season
-- One row per player per season per season_type.
-- Aggregated from possession-level PVA credits.
-- ============================================================
CREATE TABLE IF NOT EXISTS player_pva_season (
    id                  BIGSERIAL PRIMARY KEY,

    player_id           BIGINT       NOT NULL,
    player_name         TEXT         NOT NULL,
    season              VARCHAR(8)   NOT NULL,   -- e.g. "2024-25"
    season_type         VARCHAR(20)  NOT NULL DEFAULT 'Regular Season',

    -- Offensive possessions (player was primary actor — shooter/turnover player/FT shooter)
    off_possessions     INTEGER      NOT NULL DEFAULT 0,
    off_pva             REAL         NOT NULL DEFAULT 0,  -- sum of (actual_pts - expected_pts)

    -- Defensive possessions (player was on the floor when opponent had ball)
    def_possessions     INTEGER      NOT NULL DEFAULT 0,
    def_pva             REAL         NOT NULL DEFAULT 0,  -- sum of -(actual_pts - expected_pts) / 5

    -- Totals
    total_possessions   INTEGER      NOT NULL DEFAULT 0,
    total_pva           REAL         NOT NULL DEFAULT 0,

    -- Per-100-possession rates (the comparable metric)
    off_pva_per_100     REAL         NOT NULL DEFAULT 0,
    def_pva_per_100     REAL         NOT NULL DEFAULT 0,
    total_pva_per_100   REAL         NOT NULL DEFAULT 0,

    -- Outcome breakdown for off_pva (sums to off_pva)
    pva_from_makes      REAL         NOT NULL DEFAULT 0,  -- from made FGs + FTs
    pva_from_misses     REAL         NOT NULL DEFAULT 0,  -- from missed FGs
    pva_from_turnovers  REAL         NOT NULL DEFAULT 0,  -- from turnovers

    -- Calibration / diagnostics
    avg_expected_pts    REAL         NOT NULL DEFAULT 0,  -- mean expected_points on their possessions
    avg_actual_pts      REAL         NOT NULL DEFAULT 0,  -- mean actual points on their possessions

    computed_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    UNIQUE (player_id, season, season_type)
);

CREATE INDEX IF NOT EXISTS idx_pva_season       ON player_pva_season (season, season_type);
CREATE INDEX IF NOT EXISTS idx_pva_player       ON player_pva_season (player_id);
CREATE INDEX IF NOT EXISTS idx_pva_total        ON player_pva_season (total_pva_per_100 DESC);
