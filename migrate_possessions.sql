-- ydkball — Possession Tables Migration
-- Run with: psql $DATABASE_URL -f migrate_possessions.sql

-- ============================================================
-- 1. possessions
--    One row per possession. Core game state + outcome.
-- ============================================================
CREATE TABLE IF NOT EXISTS possessions (
    id                      BIGSERIAL PRIMARY KEY,

    -- Identity
    game_id                 VARCHAR(12)     NOT NULL,
    possession_number       INTEGER         NOT NULL,  -- 0-indexed within game
    season                  VARCHAR(8)      NOT NULL,  -- e.g. "2024-25"

    -- Teams
    offense_team_id         BIGINT          NOT NULL,
    defense_team_id         BIGINT          NOT NULL,

    -- Game state at possession start
    period                  SMALLINT        NOT NULL,
    start_clock_seconds     REAL            NOT NULL,  -- seconds remaining in period
    end_clock_seconds       REAL            NOT NULL,
    game_seconds_start      REAL            NOT NULL,  -- elapsed seconds in game
    score_margin_offense    INTEGER         NOT NULL,  -- offense pts minus defense pts

    -- Outcome
    points_scored           SMALLINT        NOT NULL DEFAULT 0,
    end_reason              VARCHAR(20)     NOT NULL DEFAULT '',
    -- end_reason values: made_fg | missed_fg | turnover | freethrow | violation | end_period | end_game | jumpball | fouled

    -- Model output (filled after training — null until then)
    expected_points         REAL            NULL,      -- model's predicted EV at possession start

    created_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    UNIQUE (game_id, possession_number)
);

CREATE INDEX IF NOT EXISTS idx_possessions_game       ON possessions (game_id);
CREATE INDEX IF NOT EXISTS idx_possessions_season     ON possessions (season);
CREATE INDEX IF NOT EXISTS idx_possessions_offense    ON possessions (offense_team_id);
CREATE INDEX IF NOT EXISTS idx_possessions_end_reason ON possessions (end_reason);


-- ============================================================
-- 2. possession_events
--    One row per play-by-play event within a possession.
--    This is the sequence the LSTM reads.
-- ============================================================
CREATE TABLE IF NOT EXISTS possession_events (
    id                  BIGSERIAL PRIMARY KEY,
    possession_id       BIGINT          NOT NULL REFERENCES possessions(id) ON DELETE CASCADE,

    -- Ordering
    event_index         SMALLINT        NOT NULL,  -- 0-indexed position within possession
    action_number       INTEGER         NOT NULL,  -- original NBA API action number

    -- Event identity
    action_type         VARCHAR(30)     NOT NULL,  -- normalized: "2pt", "3pt", "turnover", etc.
    sub_type            VARCHAR(60)     NOT NULL DEFAULT '',
    description         TEXT            NOT NULL DEFAULT '',

    -- Actor
    player_id           BIGINT          NULL,
    team_id             BIGINT          NULL,

    -- Timing
    clock_seconds       REAL            NOT NULL,
    game_seconds        REAL            NOT NULL,

    -- Shot context (null for non-shot events)
    shot_distance       REAL            NULL,
    shot_result         VARCHAR(10)     NULL,       -- "Made" | "Missed"
    is_field_goal       BOOLEAN         NOT NULL DEFAULT FALSE,
    x_legacy            REAL            NULL,       -- shot chart coordinates
    y_legacy            REAL            NULL,

    -- Model output (filled after training)
    ev_before           REAL            NULL,       -- expected points before this event
    ev_after            REAL            NULL,       -- expected points after this event
    ev_delta            REAL            NULL        -- ev_after - ev_before (player's credit)
);

CREATE INDEX IF NOT EXISTS idx_pevents_possession ON possession_events (possession_id);
CREATE INDEX IF NOT EXISTS idx_pevents_player     ON possession_events (player_id);
CREATE INDEX IF NOT EXISTS idx_pevents_action     ON possession_events (action_type);


-- ============================================================
-- 3. possession_lineups
--    10 rows per possession (5 offense, 5 defense).
--    Enables "all possessions for player X" queries with index.
-- ============================================================
CREATE TABLE IF NOT EXISTS possession_lineups (
    id              BIGSERIAL PRIMARY KEY,
    possession_id   BIGINT      NOT NULL REFERENCES possessions(id) ON DELETE CASCADE,
    player_id       BIGINT      NOT NULL,
    side            VARCHAR(10) NOT NULL,  -- "offense" | "defense"
    team_id         BIGINT      NOT NULL,

    -- Model output: this player's total credit for the possession
    credit          REAL        NULL
);

CREATE INDEX IF NOT EXISTS idx_plineups_possession ON possession_lineups (possession_id);
CREATE INDEX IF NOT EXISTS idx_plineups_player     ON possession_lineups (player_id);
CREATE INDEX IF NOT EXISTS idx_plineups_player_side ON possession_lineups (player_id, side);


-- ============================================================
-- 4. collection_progress
--    Tracks which game_ids have been fetched so the collector
--    can resume safely after interruptions.
-- ============================================================
CREATE TABLE IF NOT EXISTS collection_progress (
    game_id     VARCHAR(12)     PRIMARY KEY,
    season      VARCHAR(8)      NOT NULL,
    status      VARCHAR(10)     NOT NULL DEFAULT 'done',  -- "done" | "failed"
    error_msg   TEXT            NULL,
    collected_at TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_progress_season ON collection_progress (season, status);