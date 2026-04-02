"""
NothingButNet — Schema Additions
==================================
python backend/schema_additions.py

Adds new tables for accounts + game reviews WITHOUT touching existing tables.
Safe to run multiple times (all CREATE TABLE IF NOT EXISTS).
"""
import os, sys
from dotenv import load_dotenv
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("❌ DATABASE_URL not found."); sys.exit(1)

NEW_TABLES = """

-- ── Users ────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS users (
    id              SERIAL PRIMARY KEY,
    google_id       TEXT    UNIQUE NOT NULL,
    email           TEXT    UNIQUE NOT NULL,
    display_name    TEXT    NOT NULL,
    avatar_url      TEXT,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_users_google_id ON users(google_id);

-- ── Games ─────────────────────────────────────────────────────────────────────
-- Stores completed NBA games. Populated by fetch_games.py.
-- game_id matches the NBA API's 10-digit game ID (e.g. "0022500001").
CREATE TABLE IF NOT EXISTS games (
    game_id         TEXT    PRIMARY KEY,         -- NBA game ID (10 chars)
    season          TEXT    NOT NULL,            -- e.g. "2025-26"
    season_type     TEXT    NOT NULL,            -- "Regular Season" | "Playoffs"
    game_date       DATE    NOT NULL,
    home_team_abbr  TEXT    NOT NULL,
    away_team_abbr  TEXT    NOT NULL,
    home_score      INTEGER,
    away_score      INTEGER,
    status          TEXT    DEFAULT 'Final',     -- 'Final' | 'Upcoming' etc.

    -- Bayesian rating components (updated on every new review)
    review_count    INTEGER DEFAULT 0,
    rating_sum      REAL    DEFAULT 0,           -- sum of all ratings (1–10 scale)
    bayesian_rating REAL    DEFAULT 0,           -- computed weighted average

    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_games_season      ON games(season, season_type);
CREATE INDEX IF NOT EXISTS idx_games_date        ON games(game_date DESC);
CREATE INDEX IF NOT EXISTS idx_games_bayesian    ON games(bayesian_rating DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_games_home_team   ON games(home_team_abbr);
CREATE INDEX IF NOT EXISTS idx_games_away_team   ON games(away_team_abbr);

-- ── Game Reviews ──────────────────────────────────────────────────────────────
-- rating stored as INTEGER 1–10 (half-stars: 1=½★, 2=1★, 3=1½★ ... 10=5★)
CREATE TABLE IF NOT EXISTS game_reviews (
    id              SERIAL PRIMARY KEY,
    user_id         INTEGER REFERENCES users(id) ON DELETE CASCADE,
    game_id         TEXT    REFERENCES games(game_id) ON DELETE CASCADE,
    rating          INTEGER NOT NULL CHECK (rating BETWEEN 1 AND 10),
    review_text     TEXT,                        -- optional written review
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW(),
    UNIQUE(user_id, game_id)                     -- one review per user per game
);

CREATE INDEX IF NOT EXISTS idx_reviews_game_id   ON game_reviews(game_id);
CREATE INDEX IF NOT EXISTS idx_reviews_user_id   ON game_reviews(user_id);
CREATE INDEX IF NOT EXISTS idx_reviews_rating    ON game_reviews(rating DESC);
CREATE INDEX IF NOT EXISTS idx_reviews_created   ON game_reviews(created_at DESC);

-- ── Trigger: keep games.review_count / rating_sum / bayesian_rating in sync ──
-- Bayesian average formula (same as Letterboxd):
--   C  = global mean rating across all games
--   m  = minimum reviews threshold (we use 5)
--   n  = this game's review count
--   Σ  = this game's rating sum
--   bayesian = (m * C + Σ) / (m + n)
--
-- We store review_count and rating_sum on the games row and recompute
-- bayesian_rating in a trigger after every insert/update/delete on game_reviews.

CREATE OR REPLACE FUNCTION update_game_rating()
RETURNS TRIGGER AS $$
DECLARE
    v_game_id   TEXT;
    v_count     INTEGER;
    v_sum       REAL;
    v_global_mean REAL;
    m           REAL := 5;  -- minimum votes weight
BEGIN
    -- Determine which game to update
    IF TG_OP = 'DELETE' THEN
        v_game_id := OLD.game_id;
    ELSE
        v_game_id := NEW.game_id;
    END IF;

    -- Recompute count + sum for this game
    SELECT COUNT(*), COALESCE(SUM(rating), 0)
    INTO v_count, v_sum
    FROM game_reviews
    WHERE game_id = v_game_id;

    -- Global mean across all reviews
    SELECT COALESCE(AVG(rating), 5)
    INTO v_global_mean
    FROM game_reviews;

    -- Bayesian weighted average
    UPDATE games SET
        review_count    = v_count,
        rating_sum      = v_sum,
        bayesian_rating = CASE
            WHEN v_count = 0 THEN 0
            ELSE (m * v_global_mean + v_sum) / (m + v_count)
        END,
        updated_at = NOW()
    WHERE game_id = v_game_id;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_update_game_rating ON game_reviews;
CREATE TRIGGER trg_update_game_rating
AFTER INSERT OR UPDATE OR DELETE ON game_reviews
FOR EACH ROW EXECUTE FUNCTION update_game_rating();
"""


def run():
    print("⚠️  Connecting to database...")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        print("🏗️  Creating new tables (users, games, game_reviews)...")
        cur.execute(NEW_TABLES)
        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name IN ('users', 'games', 'game_reviews')
            ORDER BY table_name
        """)
        tables = [r[0] for r in cur.fetchall()]
        print(f"\n✅ Tables ready:")
        for t in tables:
            cur.execute("""
                SELECT COUNT(*) FROM information_schema.columns
                WHERE table_schema='public' AND table_name=%s
            """, (t,))
            print(f"   {t:<20} {cur.fetchone()[0]} columns")
        cur.close(); conn.close()
        print("\n✅ Done. Existing tables untouched.")
    except Exception as e:
        print(f"❌ Error: {e}"); sys.exit(1)


if __name__ == "__main__":
    run()