"""
ydkball — API Server
============================
python backend/server.py

Endpoints:
  GET  /api/seasons                    — available seasons in DB
  GET  /api/players?season=&q=&pos=    — player list for stats table
  GET  /api/stats?season=&player_id=   — single player full stat row
  GET  /api/stat-keys?season=          — all stats available for Builder
  POST /api/builder                    — run Builder composite
       body: { season, selected:[stat_keys], min_minutes }
"""

import os
import certifi
# macOS 26 beta breaks Python SSL initialization — use certifi's static bundle to bypass it
os.environ.setdefault("SSL_CERT_FILE", certifi.where())
os.environ.setdefault("REQUESTS_CA_BUNDLE", certifi.where())
import json
import math
from datetime import date
from flask import Flask, jsonify, request, Response
from flask_cors import CORS
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras
import threading

load_dotenv()

# Serve frontend/index.html at / so `python backend/server.py` is the only command needed
FRONTEND_DIR = os.path.join(os.path.dirname(__file__), '..', 'frontend')
app = Flask(__name__, static_folder=FRONTEND_DIR, static_url_path='')
CORS(app)

@app.route('/')
def index():
    return app.send_static_file('index.html')

@app.route('/.well-known/apple-app-site-association')
def aasa():
    import json
    data = {
        "applinks": {
            "details": [
                {
                    "appIDs": ["RHB7DB5Q97.net.ydkball.ydkball"],
                    "components": [
                        { "/": "/profile/*" }
                    ]
                }
            ]
        }
    }
    return app.response_class(
        response=json.dumps(data),
        status=200,
        mimetype='application/json'
    )

DATABASE_URL  = os.getenv("DATABASE_URL")


def get_current_season() -> str:
    """Returns the active NBA season string, e.g. '2025-26'.
    October–December → the season that just started.
    January–September → the season that started last October.
    """
    today = date.today()
    y, m = today.year, today.month
    if m >= 10:
        return f"{y}-{str(y + 1)[2:]}"
    return f"{y - 1}-{str(y)[2:]}"


def get_current_season_type() -> str:
    """Returns 'Playoffs' during late April–June, else 'Regular Season'."""
    today = date.today()
    m, d = today.month, today.day
    if (m == 4 and d >= 20) or m in (5, 6):
        return "Playoffs"
    return "Regular Season"


DEFAULT_SEASON      = os.getenv("NBA_SEASON",      get_current_season())
DEFAULT_SEASON_TYPE = os.getenv("NBA_SEASON_TYPE", get_current_season_type())


class _PersistentConn:
    """Wraps a psycopg2 connection so close() resets state instead of closing it.
    Kept in thread-local storage so each gunicorn thread reuses one long-lived
    connection rather than opening a new TCP connection on every request."""
    __slots__ = ("_raw",)

    def __init__(self, raw):
        self._raw = raw

    def __getattr__(self, name):
        return getattr(self._raw, name)

    def close(self):
        try:
            if not self._raw.closed:
                self._raw.rollback()
        except Exception:
            pass

_tl = threading.local()

import time as _time            # noqa: E402 — needed here; also imported later
_CONN_MAX_AGE = 900.0          # recycle a thread-local connection after this long.
                               # Kept high so connections stay warm between the
                               # bursty app-open request fans — a fresh handshake
                               # (esp. over a public DB proxy) costs 1-2s, and
                               # get_conn already detects/replaces dead conns via
                               # the rollback health-check + TCP keepalives.

def _new_raw_conn():
    return psycopg2.connect(
        DATABASE_URL,
        cursor_factory=psycopg2.extras.RealDictCursor,
        connect_timeout=10,
        # TCP keepalives so a dead peer (Railway network blip / PG failover) is
        # detected in ~80s instead of hanging on a half-open socket until the
        # process is redeployed.
        keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=5,
        # Cap any single statement so a stuck query can't pin a thread forever.
        options="-c statement_timeout=30000",
    )

def get_conn():
    w    = getattr(_tl, "conn", None)
    born = getattr(_tl, "conn_born", 0.0)
    if w is not None:
        try:
            if not w._raw.closed and (_time.time() - born) < _CONN_MAX_AGE:
                w._raw.rollback()
                return w
        except Exception:
            pass
        # aged out, closed, or unhealthy — drop it and open a fresh one so a bad
        # connection heals on its own instead of surviving until the next deploy.
        try:
            w._raw.close()
        except Exception:
            pass
    raw = _new_raw_conn()
    w = _PersistentConn(raw)
    _tl.conn = w
    _tl.conn_born = _time.time()
    return w

def _resolve_1900_game_time(status_text: str, game_date: str) -> str:
    """Convert a CDN 1900-era placeholder gameTimeUTC to the real UTC time.
    Uses gameStatusText (e.g. '8:00 pm ET') + the ET game date (e.g. '2026-05-21').
    Returns a proper UTC string like '2026-05-22T00:00:00Z', or '' on failure."""
    import re
    m = re.match(r'(\d+):(\d+)\s*(am|pm)', status_text or "", re.IGNORECASE)
    if not m:
        return ""
    h, mins = int(m.group(1)), int(m.group(2))
    if m.group(3).lower() == "pm" and h != 12:
        h += 12
    elif m.group(3).lower() == "am" and h == 12:
        h = 0
    try:
        try:
            from zoneinfo import ZoneInfo
        except ImportError:
            from backports.zoneinfo import ZoneInfo
        from datetime import datetime, timezone
        et = ZoneInfo("America/New_York")
        naive = datetime.strptime(game_date, "%Y-%m-%d").replace(hour=h, minute=mins)
        return naive.replace(tzinfo=et).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return ""


def _fmt_game_time(val) -> str:
    """Return gameTimeUTC as a proper ISO 8601 UTC string (e.g. '2026-04-25T01:30:00Z').
    PostgreSQL returns naive datetimes via psycopg2 as Python datetime objects; str() gives
    '2026-04-25 01:30:00' which JS parses as local time instead of UTC."""
    if not val:
        return ""
    from datetime import datetime, timezone
    if isinstance(val, datetime):
        # If naive, assume it was stored as UTC
        if val.tzinfo is None:
            val = val.replace(tzinfo=timezone.utc)
        return val.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    s = str(val).strip()
    # Already has Z or offset — normalize to Z form
    if s.endswith("Z"):
        return s.replace(" ", "T")
    if "+" in s[10:] or (s[10:].count("-") > 0):
        # Has offset, parse and re-emit as Z
        try:
            dt = datetime.fromisoformat(s)
            return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception:
            pass
    # Naive string — assume UTC
    return s.replace(" ", "T") + "Z"



from auth import auth_bp, init_oauth, login_required, current_user
# auth.py's own get_conn opens a fresh TCP+TLS connection per call — every
# authenticated request paid that handshake (2-8s under concurrent load).
# Swap it for the pooled thread-local get_conn defined above; _PersistentConn
# makes auth.py's conn.close() calls a no-op rollback instead of a real close.
import auth as _auth_mod
_auth_mod.get_conn = get_conn
from datetime import timedelta

# Survival trivia engine (backend/games/) — explicit path so `import survival_api`
# works regardless of how gunicorn resolves the `games` package.
import sys as _sys
_sys.path.insert(0, os.path.join(os.path.dirname(__file__), "games"))
import survival_api  # noqa: E402
import poeltl_api    # noqa: E402  — "guess the performance" daily game

app.secret_key = os.getenv("SECRET_KEY")
app.permanent_session_lifetime = timedelta(days=60)
init_oauth(app)
app.register_blueprint(auth_bp)

_PERF_TABLE = """
    CREATE TABLE IF NOT EXISTS performance_reviews (
        id          SERIAL PRIMARY KEY,
        game_id     TEXT    NOT NULL,
        person_id   INTEGER NOT NULL,
        user_id     INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        rating      INTEGER NOT NULL CHECK (rating >= 1 AND rating <= 10),
        player_name TEXT,
        review_text TEXT,
        created_at  TIMESTAMPTZ DEFAULT NOW(),
        updated_at  TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(user_id, game_id, person_id)
    )
"""
_PERF_MIGRATE = "ALTER TABLE performance_reviews ADD COLUMN IF NOT EXISTS player_name TEXT"

def _ensure_tables():
    try:
        conn = get_conn()
        cur  = conn.cursor()
        # DB-level lock so concurrent gunicorn workers queue here instead of deadlocking.
        # pg_advisory_xact_lock is transaction-scoped and auto-released on conn.commit().
        cur.execute("SELECT pg_advisory_xact_lock(191823)")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS review_likes (
                user_id    INTEGER REFERENCES users(id)        ON DELETE CASCADE,
                review_id  INTEGER REFERENCES game_reviews(id) ON DELETE CASCADE,
                created_at TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (user_id, review_id)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS favorite_games (
                user_id    INTEGER  REFERENCES users(id) ON DELETE CASCADE,
                game_id    TEXT     NOT NULL,
                position   SMALLINT NOT NULL CHECK (position BETWEEN 1 AND 4),
                created_at TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (user_id, game_id),
                UNIQUE (user_id, position)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS favorite_players (
                user_id     INTEGER  REFERENCES users(id) ON DELETE CASCADE,
                person_id   INTEGER  NOT NULL,
                player_name TEXT     NOT NULL,
                team        TEXT,
                league      TEXT     NOT NULL DEFAULT 'nba',
                position    SMALLINT NOT NULL CHECK (position BETWEEN 1 AND 4),
                created_at  TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (user_id, person_id),
                UNIQUE (user_id, position)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS review_replies (
                id          SERIAL  PRIMARY KEY,
                review_id   INTEGER REFERENCES game_reviews(id) ON DELETE CASCADE,
                user_id     INTEGER REFERENCES users(id)        ON DELETE CASCADE,
                reply_text  TEXT    NOT NULL,
                created_at  TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_review_replies_review_id
            ON review_replies(review_id)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_game_reviews_created_at
            ON game_reviews(created_at DESC)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_game_reviews_user_id
            ON game_reviews(user_id)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_game_reviews_game_id
            ON game_reviews(game_id)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_review_likes_review_id
            ON review_likes(review_id)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_review_likes_user_review
            ON review_likes(user_id, review_id)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_friendships_sender
            ON friendships(sender_id, status)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_friendships_receiver
            ON friendships(receiver_id, status)
        """)
        # Fix playoff games that were incorrectly stored as 'Regular Season'
        # due to the _season_type_from_game_id bug (was using game_id[2:4] instead of game_id[2])
        cur.execute("""
            UPDATE games
            SET season_type = 'Playoffs'
            WHERE LEFT(game_id, 3) = '004'
              AND season_type != 'Playoffs'
        """)
        cur.execute("""
            ALTER TABLE users ADD COLUMN IF NOT EXISTS night_mode BOOLEAN DEFAULT FALSE
        """)
        cur.execute("""
            ALTER TABLE users ADD COLUMN IF NOT EXISTS is_pro BOOLEAN DEFAULT FALSE
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS content_reports (
                id          SERIAL PRIMARY KEY,
                reporter_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
                review_id   INTEGER REFERENCES game_reviews(id) ON DELETE CASCADE,
                created_at  TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS user_blocks (
                blocker_id  INTEGER REFERENCES users(id) ON DELETE CASCADE,
                blocked_id  INTEGER REFERENCES users(id) ON DELETE CASCADE,
                created_at  TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (blocker_id, blocked_id)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS game_lists (
                id          SERIAL PRIMARY KEY,
                user_id     INTEGER REFERENCES users(id) ON DELETE CASCADE,
                title       TEXT    NOT NULL,
                description TEXT,
                is_public   BOOLEAN DEFAULT TRUE,
                created_at  TIMESTAMP DEFAULT NOW(),
                updated_at  TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS game_list_items (
                list_id   INTEGER REFERENCES game_lists(id) ON DELETE CASCADE,
                game_id   TEXT    NOT NULL,
                added_at  TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (list_id, game_id)
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_game_lists_user_id ON game_lists(user_id)
        """)
        cur.execute("""
            ALTER TABLE game_lists ADD COLUMN IF NOT EXISTS is_ranked BOOLEAN DEFAULT FALSE
        """)
        cur.execute("""
            ALTER TABLE game_list_items ADD COLUMN IF NOT EXISTS sort_order INTEGER
        """)
        cur.execute("""
            ALTER TABLE game_lists ADD COLUMN IF NOT EXISTS list_type TEXT DEFAULT 'games'
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS player_list_items (
                id          SERIAL PRIMARY KEY,
                list_id     INTEGER REFERENCES game_lists(id) ON DELETE CASCADE,
                player_id   INTEGER,
                player_name TEXT NOT NULL,
                team        TEXT,
                season      TEXT,
                sort_order  INTEGER,
                added_at    TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_player_list_items_list_id ON player_list_items(list_id)
        """)
        # Creator-attached stat tags (snapshot display strings, e.g. ["32.7 PPG"])
        cur.execute("ALTER TABLE player_list_items ADD COLUMN IF NOT EXISTS stats JSONB DEFAULT '[]'::jsonb")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS jersey_list_items (
                id         SERIAL PRIMARY KEY,
                list_id    INTEGER REFERENCES game_lists(id) ON DELETE CASCADE,
                jersey_id  INTEGER REFERENCES jerseys(id),
                label      TEXT NOT NULL,
                image_url  TEXT NOT NULL,
                sort_order INTEGER,
                added_at   TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_jersey_list_items_list_id ON jersey_list_items(list_id)
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS team_list_items (
                id          SERIAL PRIMARY KEY,
                list_id     INTEGER REFERENCES game_lists(id) ON DELETE CASCADE,
                team_abbr   TEXT NOT NULL,
                team_name   TEXT NOT NULL,
                season      TEXT,
                wins        INTEGER,
                losses      INTEGER,
                sort_order  INTEGER,
                added_at    TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_team_list_items_list_id ON team_list_items(list_id)
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS performance_list_items (
                id          SERIAL PRIMARY KEY,
                list_id     INTEGER REFERENCES game_lists(id) ON DELETE CASCADE,
                game_id     TEXT    NOT NULL,
                person_id   INTEGER NOT NULL,
                player_name TEXT    NOT NULL,
                league      TEXT    NOT NULL DEFAULT 'nba',
                sort_order  INTEGER,
                added_at    TIMESTAMP DEFAULT NOW(),
                UNIQUE (list_id, game_id, person_id)
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_perf_list_items_list_id ON performance_list_items(list_id)
        """)
        cur.execute("""
            ALTER TABLE games ADD COLUMN IF NOT EXISTS league TEXT NOT NULL DEFAULT 'nba'
        """)
        cur.execute("""
            ALTER TABLE team_seasons ADD COLUMN IF NOT EXISTS league TEXT NOT NULL DEFAULT 'nba'
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_team_seasons_league ON team_seasons(league)
        """)
        cur.execute("""
            ALTER TABLE team_list_items ADD COLUMN IF NOT EXISTS league TEXT NOT NULL DEFAULT 'nba'
        """)
        cur.execute("""
            ALTER TABLE player_list_items ADD COLUMN IF NOT EXISTS league TEXT NOT NULL DEFAULT 'nba'
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS wnba_player_game_stats (
                player_id   INTEGER NOT NULL,
                player_name TEXT,
                team        TEXT,
                game_id     TEXT    NOT NULL,
                season      TEXT,
                pts         INTEGER DEFAULT 0,
                reb         INTEGER DEFAULT 0,
                ast         INTEGER DEFAULT 0,
                tov         INTEGER DEFAULT 0,
                fgm         INTEGER DEFAULT 0,
                fga         INTEGER DEFAULT 0,
                fg3m        INTEGER DEFAULT 0,
                fg3a        INTEGER DEFAULT 0,
                PRIMARY KEY (player_id, game_id)
            )
        """)
        # Add columns introduced after initial table creation (safe on existing DBs)
        for col, defn in [
            ("tov",  "INTEGER DEFAULT 0"),
            ("fgm",  "INTEGER DEFAULT 0"),
            ("fga",  "INTEGER DEFAULT 0"),
            ("fg3m", "INTEGER DEFAULT 0"),
            ("fg3a", "INTEGER DEFAULT 0"),
        ]:
            try:
                cur.execute(f"ALTER TABLE wnba_player_game_stats ADD COLUMN IF NOT EXISTS {col} {defn}")
            except Exception:
                pass
        cur.execute("""
            CREATE TABLE IF NOT EXISTS wnba_player_seasons (
                player_id   INTEGER NOT NULL,
                player_name TEXT    NOT NULL,
                season      TEXT    NOT NULL,
                season_type TEXT    NOT NULL DEFAULT 'Regular Season',
                team        TEXT,
                gp          INTEGER,
                min         REAL,
                pts         REAL,
                reb         REAL,
                ast         REAL,
                stl         REAL,
                blk         REAL,
                tov         REAL,
                fgm         REAL,
                fga         REAL,
                fg_pct      REAL,
                fg3m        REAL,
                fg3a        REAL,
                fg3_pct     REAL,
                ftm         REAL,
                fta         REAL,
                ft_pct      REAL,
                oreb        REAL,
                dreb        REAL,
                eff         REAL,
                updated_at  TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (player_id, season, season_type)
            )
        """)
        # Backfill review_count / rating_sum from game_reviews (in case they drifted out of sync)
        cur.execute("""
            UPDATE games g
            SET review_count = agg.cnt,
                rating_sum   = agg.rsum
            FROM (
                SELECT game_id, COUNT(*) AS cnt, COALESCE(SUM(rating), 0) AS rsum
                FROM game_reviews
                GROUP BY game_id
            ) agg
            WHERE g.game_id = agg.game_id
              AND (g.review_count != agg.cnt OR g.rating_sum != agg.rsum)
        """)
        # Ball Knowledge XP system
        cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS xp INTEGER NOT NULL DEFAULT 0")
        cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS equipped_ring   INTEGER DEFAULT NULL")
        cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS equipped_title INTEGER DEFAULT NULL")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS xp_events (
                id           SERIAL  PRIMARY KEY,
                user_id      INTEGER REFERENCES users(id) ON DELETE CASCADE,
                event_type   TEXT    NOT NULL,
                reference_id TEXT,
                xp_amount    INTEGER NOT NULL,
                created_at   TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_xp_events_user
            ON xp_events(user_id)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_xp_events_user_type_ref
            ON xp_events(user_id, event_type, reference_id)
        """)
        # Game predictions
        cur.execute("""
            CREATE TABLE IF NOT EXISTS game_odds (
                game_id        TEXT PRIMARY KEY,
                league         TEXT NOT NULL,
                home_team      TEXT NOT NULL,
                away_team      TEXT NOT NULL,
                home_odds      INTEGER,
                away_odds      INTEGER,
                game_starts_at TIMESTAMPTZ,
                fetched_at     TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS game_predictions (
                id               SERIAL PRIMARY KEY,
                user_id          INTEGER REFERENCES users(id) ON DELETE CASCADE,
                game_id          TEXT NOT NULL,
                league           TEXT NOT NULL DEFAULT 'nba',
                predicted_winner TEXT NOT NULL,
                home_team        TEXT NOT NULL,
                away_team        TEXT NOT NULL,
                home_odds        INTEGER,
                away_odds        INTEGER,
                game_starts_at   TIMESTAMPTZ,
                resolved_at      TIMESTAMPTZ,
                actual_winner    TEXT,
                is_correct       BOOLEAN,
                xp_change        INTEGER,
                created_at       TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(user_id, game_id)
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_predictions_game ON game_predictions(game_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_predictions_user ON game_predictions(user_id)")
        # ── Survival trivia ──────────────────────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS survival_daily (
                date       DATE PRIMARY KEY,
                payload    JSONB NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS survival_results (
                id         SERIAL PRIMARY KEY,
                user_id    INTEGER REFERENCES users(id) ON DELETE CASCADE,
                mode       TEXT NOT NULL DEFAULT 'daily',
                date       DATE NOT NULL,
                score      INTEGER NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(user_id, mode, date)
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_survival_results_user ON survival_results(user_id, mode)")

        # ── Poeltl ("guess the performance") ─────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS poeltl_daily (
                date       DATE PRIMARY KEY,
                payload    JSONB NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS poeltl_results (
                id         SERIAL PRIMARY KEY,
                user_id    INTEGER REFERENCES users(id) ON DELETE CASCADE,
                mode       TEXT NOT NULL DEFAULT 'daily',
                date       DATE NOT NULL,
                solved     BOOLEAN NOT NULL,
                guesses    INTEGER NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(user_id, mode, date)
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_poeltl_results_user ON poeltl_results(user_id, mode)")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS game_watches (
                user_id    INTEGER REFERENCES users(id) ON DELETE CASCADE,
                game_id    TEXT    NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (user_id, game_id)
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_game_watches_user ON game_watches(user_id)")
        # performance_reviews DDL (definitions live further down the module;
        # resolved at call time since _ensure_tables runs on first request).
        # This must ONLY run here: ALTER TABLE takes an ACCESS EXCLUSIVE lock
        # even when the column already exists, so running it per-request made
        # concurrent /api/feed calls queue on each other until statement_timeout.
        cur.execute(_PERF_TABLE)
        cur.execute(_PERF_MIGRATE)
        conn.commit()
        cur.close(); conn.close()
    except Exception as e:
        print(f"[startup] _ensure_tables warning: {e}")

# Schema init runs at worker boot (module import), NOT on the first user
# request — with Railway's healthcheck gating the traffic switch, users never
# see this delay. A /tmp stamp (keyed by deployment) makes worker recycles
# skip it entirely, so DDL runs once per deployment. The pg_advisory_xact_lock
# inside _ensure_tables() serializes the two workers racing at initial boot.
_SCHEMA_STAMP = "/tmp/_ydkball_schema_ready_" + os.environ.get("RAILWAY_DEPLOYMENT_ID", "dev")
if not os.path.exists(_SCHEMA_STAMP):
    _ensure_tables()
    try:
        open(_SCHEMA_STAMP, "w").close()
    except Exception:
        pass

# ── slow-request instrumentation ─────────────────────────────────
# Logs any request the app spent >1s handling. Compare against the time the
# client saw: if the client waited 30s but nothing is logged here, the time
# went to queueing for a free gunicorn thread (thread starvation), not to
# handler code. If a path IS logged here, that handler is the bottleneck.
from flask import g as _g

@app.before_request
def _slowlog_start():
    _g._t0 = _time.perf_counter()

@app.after_request
def _slowlog_end(resp):
    try:
        t0 = getattr(_g, "_t0", None)
        if t0 is not None:
            ms = (_time.perf_counter() - t0) * 1000
            if ms > 1000:
                print(f"[slow] {ms:.0f}ms {request.method} {request.full_path}", flush=True)
    except Exception:
        pass
    return resp

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Ball Knowledge — XP / rank system
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

BALL_KNOWLEDGE_RANKS = [
    {"level": 1,  "title": "Ball Newbie",   "xp_required": 0},
    {"level": 2,  "title": "Casual",        "xp_required": 250},
    {"level": 3,  "title": "Ball Novice",   "xp_required": 650},
    {"level": 4,  "title": "Box Score Guy", "xp_required": 1250},
    {"level": 5,  "title": "Game Watcher",  "xp_required": 2200},
    {"level": 6,  "title": "Hooper",        "xp_required": 3700},
    {"level": 7,  "title": "Stat Head",     "xp_required": 6000},
    {"level": 8,  "title": "Basketphile",   "xp_required": 9500},
    {"level": 9,  "title": "Hall of Famer", "xp_required": 14500},
    {"level": 10, "title": "Ball Knower",   "xp_required": 21000},
]


def _xp_to_level(xp: int) -> int:
    level = 1
    for rank in BALL_KNOWLEDGE_RANKS:
        if xp >= rank["xp_required"]:
            level = rank["level"]
        else:
            break
    return level


def get_rank_info(xp: int) -> dict:
    current = BALL_KNOWLEDGE_RANKS[0]
    next_rank = None
    for i, rank in enumerate(BALL_KNOWLEDGE_RANKS):
        if xp >= rank["xp_required"]:
            current = rank
            next_rank = BALL_KNOWLEDGE_RANKS[i + 1] if i + 1 < len(BALL_KNOWLEDGE_RANKS) else None
        else:
            break
    xp_in_rank  = xp - current["xp_required"]
    xp_for_rank = (next_rank["xp_required"] - current["xp_required"]) if next_rank else None
    return {
        "level":           current["level"],
        "title":           current["title"],
        "xp":              xp,
        "xp_in_rank":      xp_in_rank,
        "xp_for_rank":     xp_for_rank,
        "next_rank_title": next_rank["title"] if next_rank else None,
        "next_rank_xp":    next_rank["xp_required"] if next_rank else None,
        "ranks":           BALL_KNOWLEDGE_RANKS,
    }


def _grant_xp(cur, user_id: int, event_type: str, reference_id: str, amount: int) -> int:
    """Insert an xp_event and increment users.xp. Returns the new total xp, or -1 if already granted."""
    cur.execute(
        "SELECT 1 FROM xp_events WHERE user_id = %s AND event_type = %s AND reference_id = %s",
        (user_id, event_type, reference_id)
    )
    if cur.fetchone():
        return -1
    cur.execute(
        "INSERT INTO xp_events (user_id, event_type, reference_id, xp_amount) VALUES (%s, %s, %s, %s)",
        (user_id, event_type, reference_id, amount)
    )
    cur.execute(
        "UPDATE users SET xp = xp + %s WHERE id = %s RETURNING xp",
        (amount, user_id)
    )
    return cur.fetchone()["xp"]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Game predictions — odds, submission, resolution
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# ESPN uses different abbreviations for 6 NBA teams — map them to ours
_ESPN_TO_OURS = {
    "GS":   "GSW",
    "NO":   "NOP",
    "NY":   "NYK",
    "SA":   "SAS",
    "UTAH": "UTA",
    "WSH":  "WAS",
}

# WNBA abbreviation corrections from ESPN
_ESPN_TO_OURS_WNBA = {
    "WSH": "WSH",  # same in WNBA
}

_PREDICTION_XP_CORRECT = 50
_PREDICTION_XP_WRONG   = -50
_ODDS_CACHE_SECONDS    = 4 * 3600  # refresh at most once per 4 hours


def _fetch_and_cache_odds(game_id: str, league: str, home_abbr: str, away_abbr: str) -> dict | None:
    """Fetch odds from ESPN scoreboard, cache in game_odds, return the row dict."""
    import requests as _req

    sport = "wnba" if league == "wnba" else "nba"
    mapping = _ESPN_TO_OURS_WNBA if league == "wnba" else _ESPN_TO_OURS

    try:
        resp = _req.get(
            f"https://site.api.espn.com/apis/site/v2/sports/basketball/{sport}/scoreboard",
            timeout=8,
        )
        resp.raise_for_status()
        events = resp.json().get("events", [])
    except Exception as e:
        print(f"[odds] ESPN fetch error: {e}", flush=True)
        return None

    for event in events:
        comp = (event.get("competitions") or [{}])[0]
        competitors = comp.get("competitors", [])
        if len(competitors) < 2:
            continue

        # ESPN lists home first (homeAway="home"), away second
        espn_home = next((c for c in competitors if c.get("homeAway") == "home"), competitors[0])
        espn_away = next((c for c in competitors if c.get("homeAway") == "away"), competitors[1])

        h = mapping.get(espn_home["team"]["abbreviation"], espn_home["team"]["abbreviation"])
        a = mapping.get(espn_away["team"]["abbreviation"], espn_away["team"]["abbreviation"])

        if h != home_abbr or a != away_abbr:
            continue

        # Found the matching game — extract moneyline odds
        odds_list = comp.get("odds", [])
        if not odds_list:
            return None

        ml = odds_list[0].get("moneyline", {})
        try:
            home_odds = int(ml["home"]["close"]["odds"])
            away_odds = int(ml["away"]["close"]["odds"])
        except (KeyError, TypeError, ValueError):
            return None

        starts_at = event.get("date")  # ISO string e.g. "2026-06-14T00:30Z"

        try:
            conn = get_conn()
            cur  = conn.cursor()
            cur.execute("""
                INSERT INTO game_odds
                    (game_id, league, home_team, away_team, home_odds, away_odds, game_starts_at, fetched_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (game_id) DO UPDATE SET
                    home_odds      = EXCLUDED.home_odds,
                    away_odds      = EXCLUDED.away_odds,
                    game_starts_at = EXCLUDED.game_starts_at,
                    fetched_at     = NOW()
                RETURNING *
            """, (game_id, league, home_abbr, away_abbr, home_odds, away_odds, starts_at))
            row = dict(cur.fetchone())
            conn.commit()
            cur.close(); conn.close()
            return row
        except Exception as e:
            print(f"[odds] db error: {e}", flush=True)
            return None

    return None  # game not found in ESPN scoreboard


def _resolve_game_predictions(game_id: str, home_abbr: str, away_abbr: str,
                               home_score: int, away_score: int) -> None:
    """Resolve all pending predictions for a finished game. Grants +50 XP for correct
    picks and deducts 50 XP (floored at 0) for wrong ones. Safe to call multiple times."""
    if home_score == away_score:
        return  # no winner in overtime tie — skip (shouldn't happen in NBA/WNBA)
    winner = home_abbr if home_score > away_score else away_abbr
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT gp.id, gp.user_id, gp.predicted_winner
            FROM game_predictions gp
            WHERE gp.game_id = %s AND gp.resolved_at IS NULL
        """, (game_id,))
        rows = cur.fetchall()
        if not rows:
            cur.close(); conn.close()
            return
        for row in rows:
            uid      = row["user_id"]
            correct  = (row["predicted_winner"] == winner)
            if correct:
                xp_delta = _PREDICTION_XP_CORRECT
                cur.execute("UPDATE users SET xp = xp + %s WHERE id = %s", (xp_delta, uid))
            else:
                # Deduct up to 50 XP but never go below 0
                cur.execute("""
                    UPDATE users
                    SET xp = GREATEST(0, xp + %s)
                    WHERE id = %s
                    RETURNING xp
                """, (_PREDICTION_XP_WRONG, uid))
                new_xp = cur.fetchone()["xp"]
                # Record the actual amount deducted (might be less than 50 if near floor)
                cur.execute("SELECT xp FROM users WHERE id = %s", (uid,))
                xp_delta = _PREDICTION_XP_WRONG  # logged as -50 regardless of floor

            event_type = "prediction_correct" if correct else "prediction_wrong"
            cur.execute("""
                INSERT INTO xp_events (user_id, event_type, reference_id, xp_amount)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (uid, event_type, game_id, xp_delta))

            cur.execute("""
                UPDATE game_predictions
                SET resolved_at   = NOW(),
                    actual_winner = %s,
                    is_correct    = %s,
                    xp_change     = %s
                WHERE id = %s
            """, (winner, correct, xp_delta, row["id"]))

        conn.commit()
        cur.close(); conn.close()
        print(f"[predictions] resolved {len(rows)} predictions for {game_id}, winner={winner}", flush=True)
    except Exception as e:
        print(f"[predictions] resolution error for {game_id}: {e}", flush=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GET /api/games/<game_id>/odds
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/games/<game_id>/odds")
def get_game_odds(game_id):
    home = (request.args.get("home") or "").upper()
    away = (request.args.get("away") or "").upper()
    league = "wnba" if str(game_id).startswith("10") else "nba"
    try:
        from datetime import datetime as _dt, timezone as _tz, timedelta as _td
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("SELECT * FROM game_odds WHERE game_id = %s", (game_id,))
        cached = cur.fetchone()
        cur.close(); conn.close()

        # Use cache if fresh enough
        if cached:
            age = (_dt.now(_tz.utc) - cached["fetched_at"].replace(tzinfo=_tz.utc)).total_seconds()
            if age < _ODDS_CACHE_SECONDS:
                return jsonify({
                    "game_id": game_id, "home_team": cached["home_team"],
                    "away_team": cached["away_team"], "home_odds": cached["home_odds"],
                    "away_odds": cached["away_odds"],
                    "game_starts_at": cached["game_starts_at"].isoformat() if cached["game_starts_at"] else None,
                })

        if not home or not away:
            return jsonify({"error": "home and away query params required"}), 400

        row = _fetch_and_cache_odds(game_id, league, home, away)
        if not row:
            return jsonify({"error": "odds not available"}), 404

        return jsonify({
            "game_id": game_id, "home_team": row["home_team"],
            "away_team": row["away_team"], "home_odds": row["home_odds"],
            "away_odds": row["away_odds"],
            "game_starts_at": row["game_starts_at"].isoformat() if row["game_starts_at"] else None,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# POST /api/games/<game_id>/predict   — submit / update prediction
# GET  /api/games/<game_id>/predict   — fetch own prediction
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/games/<game_id>/predict", methods=["POST"])
@login_required
def submit_prediction(game_id):
    from datetime import datetime as _dt, timezone as _tz, timedelta as _td
    user = current_user()
    body = request.get_json() or {}
    predicted_winner = (body.get("predicted_winner") or "").upper()
    if not predicted_winner:
        return jsonify({"error": "predicted_winner required"}), 400

    try:
        conn = get_conn()
        cur  = conn.cursor()

        # Game must not be final
        cur.execute("SELECT status FROM games WHERE game_id = %s", (game_id,))
        game_row = cur.fetchone()
        if game_row and game_row["status"] == "Final":
            cur.close(); conn.close()
            return jsonify({"error": "Game is already final"}), 409

        # Odds must exist (they hold the start time and validate the game)
        cur.execute("SELECT * FROM game_odds WHERE game_id = %s", (game_id,))
        odds_row = cur.fetchone()
        if not odds_row:
            cur.close(); conn.close()
            return jsonify({"error": "No odds available for this game"}), 404

        # Must predict before tipoff
        starts_at = odds_row["game_starts_at"]
        if starts_at:
            now = _dt.now(_tz.utc)
            starts_utc = starts_at if starts_at.tzinfo else starts_at.replace(tzinfo=_tz.utc)
            if now >= starts_utc:
                cur.close(); conn.close()
                return jsonify({"error": "Predictions close at tipoff"}), 409
            if (starts_utc - now) > _td(hours=48):
                cur.close(); conn.close()
                return jsonify({"error": "Predictions open within 48 hours of tipoff"}), 409

        league = "wnba" if str(game_id).startswith("10") else "nba"
        cur.execute("""
            INSERT INTO game_predictions
                (user_id, game_id, league, predicted_winner, home_team, away_team,
                 home_odds, away_odds, game_starts_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (user_id, game_id) DO UPDATE SET
                predicted_winner = EXCLUDED.predicted_winner,
                created_at       = NOW()
            WHERE game_predictions.resolved_at IS NULL
            RETURNING *
        """, (user["id"], game_id, league, predicted_winner,
              odds_row["home_team"], odds_row["away_team"],
              odds_row["home_odds"], odds_row["away_odds"],
              odds_row["game_starts_at"]))
        pred = cur.fetchone()
        conn.commit()
        cur.close(); conn.close()
        if not pred:
            return jsonify({"error": "Cannot change a resolved prediction"}), 409
        return jsonify({"prediction": dict(pred)}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/games/<game_id>/predict", methods=["GET"])
@login_required
def get_my_prediction(game_id):
    user = current_user()
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT * FROM game_predictions WHERE user_id = %s AND game_id = %s
        """, (user["id"], game_id))
        row = cur.fetchone()
        cur.close(); conn.close()
        if not row:
            return jsonify({"prediction": None})
        return jsonify({"prediction": dict(row)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def _fix_wnba_league_column():
    """Fix games whose game_id starts with '10' but were inserted with wrong
    league, season, or raw CDN tricodes (PDX→POR, LVA→LV, etc.) by the
    live-boxscore path before those fields were handled correctly."""
    from datetime import date as _d
    today = _d.today()
    wnba_season = str(today.year) if today.month >= 5 else str(today.year - 1)
    # Raw CDN tricodes that need remapping to app abbreviations
    _abbr_fixes = {"LVA": "LV", "LAS": "LA", "NYL": "NY", "GSV": "GS",
                   "WAS": "WSH", "PDX": "POR"}
    try:
        conn = get_conn()
        cur  = conn.cursor()
        # Fix league and season for any game_id starting with '10'
        cur.execute(
            """UPDATE games SET league = 'wnba', season = %s
               WHERE game_id LIKE %s
                 AND (league IS NULL OR league != 'wnba' OR season != %s)""",
            (wnba_season, "10%", wnba_season)
        )
        updated = cur.rowcount
        # Fix raw CDN tricodes stored as home/away team abbreviations
        for raw, mapped in _abbr_fixes.items():
            cur.execute(
                "UPDATE games SET home_team_abbr = %s WHERE home_team_abbr = %s AND game_id LIKE %s",
                (mapped, raw, "10%")
            )
            cur.execute(
                "UPDATE games SET away_team_abbr = %s WHERE away_team_abbr = %s AND game_id LIKE %s",
                (mapped, raw, "10%")
            )
        conn.commit()
        cur.close(); conn.close()
        if updated:
            print(f"[startup] fixed league/season for {updated} WNBA game(s)", flush=True)
    except Exception as e:
        print(f"[startup] _fix_wnba_league_column warning: {e}", flush=True)

_fix_wnba_league_column()

# ── /api/seasons ─────────────────────────────────────────────

@app.route("/api/seasons")
def get_seasons():
    try:
        source = request.args.get("source", "stats")  # "stats" | "games"
        league = request.args.get("league", "nba").lower().strip()
        conn = get_conn()
        cur  = conn.cursor()
        if source == "games":
            cur.execute("""
                SELECT DISTINCT season, season_type
                FROM games
                WHERE league = %s
                ORDER BY season DESC, season_type
            """, (league,))
        else:
            table = "wnba_player_seasons" if league == "wnba" else "player_seasons"
            cur.execute(f"""
                SELECT DISTINCT season, season_type
                FROM {table}
                ORDER BY season DESC, season_type
            """)
        rows = [dict(r) for r in cur.fetchall()]
        cur.close(); conn.close()
        return jsonify({"seasons": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── /api/current-season ───────────────────────────────────────

@app.route("/api/current-season")
def current_season():
    """Returns the active season and season type derived from today's date."""
    return jsonify({
        "season":      get_current_season(),
        "season_type": get_current_season_type(),
    })


# ── /api/players ─────────────────────────────────────────────

@app.route("/api/players")
def get_players():
    season      = request.args.get("season",      DEFAULT_SEASON)
    season_type = request.args.get("season_type", DEFAULT_SEASON_TYPE)
    q           = request.args.get("q",           "").strip()
    pos         = request.args.get("pos",         "ALL")
    sort_col    = request.args.get("sort",        "pts")
    sort_dir    = request.args.get("dir",         "desc").lower()
    min_min     = int(request.args.get("min_min", 0))
    limit       = min(int(request.args.get("limit", 500)), 500)

    # Whitelist sortable columns
    SORTABLE = {
        "player_name", "pts", "ast", "reb", "stl", "blk", "tov",
        "fg_pct", "fg3_pct", "ft_pct", "ts_pct", "efg_pct", "usg_pct",
        "off_rating", "def_rating", "net_rating", "min_per_game",
        "oreb_pct", "dreb_pct", "reb_pct", "ast_pct", "ast_to", "plus_minus", "gp", "pie",
        "drives", "drive_pts", "drive_ast", "drive_fga", "drive_fg_pct",
        "passes_made", "ast_pts_created", "potential_ast", "secondary_ast",
        "touches", "time_of_poss", "pull_up_efg_pct", "cs_efg_pct",
        "pull_up_fga", "cs_fga", "dist_miles", "avg_speed",
        "contested_shots", "deflections", "charges_drawn", "screen_assists",
        "loose_balls", "box_outs", "bad_pass_tov", "lost_ball_tov",
        "pct_uast_fgm", "pct_pts_paint", "pct_pts_3pt", "pct_pts_ft", "pts_paint",
        "def_rim_fga", "def_rim_fg_pct", "oreb", "dreb", "fga", "fta",
        "fgm", "fg3m", "ftm", "pf", "pfd",
        "post_touches", "paint_touches", "elbow_touches",
        "iso_ppp", "iso_fga", "iso_efg_pct", "iso_tov_pct",
        "pnr_bh_ppp", "pnr_bh_fga", "pnr_roll_ppp", "pnr_roll_poss",
        "post_ppp", "post_poss", "spotup_ppp", "spotup_efg_pct",
        "transition_ppp", "transition_fga",
        "def_iso_ppp", "def_pnr_bh_ppp", "def_post_ppp",
        "def_spotup_ppp", "def_pnr_roll_ppp",
        "drive_fgm", "pull_up_fgm", "cs_fgm",
        "clutch_net_rating", "clutch_ts_pct", "clutch_fgm", "def_ws",
        "gravity_score", "gravity_onball_perimeter", "gravity_offball_perimeter",
        "gravity_onball_interior", "gravity_offball_interior",
        "leverage_full", "leverage_offense", "leverage_defense",
        "leverage_shooting", "leverage_creation", "leverage_turnovers",
        "leverage_rebounds", "leverage_onball_def",
        "sq_avg_shot_quality", "sq_fg_pct_above_expected",
        "sq_avg_defender_distance", "sq_avg_defender_pressure",
        "sq_avg_shooter_speed", "sq_avg_made_quality", "sq_avg_missed_quality",
        "darko_dpm", "darko_odpm", "darko_ddpm", "darko_box",
        "lebron", "o_lebron", "d_lebron", "war",
        "net_pts100", "o_net_pts100", "d_net_pts100",
        "min", "post_touch_fga", "pull_up_fg3a", "pull_up_fg3_pct",
        "cs_fg3a", "cs_fg3_pct", "contested_2pt", "contested_3pt",
        "screen_ast_pts", "def_rim_fgm",
        "cd_fga_vt", "cd_fga_tg", "cd_fga_op", "cd_fga_wo",
    }
    if sort_col not in SORTABLE:
        sort_col = "pts"
    dir_sql = "ASC" if sort_dir == "asc" else "DESC"

    filters = ["ps.season = %s", "ps.season_type = %s"]
    params  = [season, season_type]

    if q:
        filters.append("p.player_name ILIKE %s")
        params.append(f"%{q}%")
    if pos and pos != "ALL":
        filters.append("p.position_group = %s")
        params.append(pos)
    if min_min > 0:
        filters.append("ps.min >= %s")
        params.append(min_min)

    where = " AND ".join(filters)
    params.append(limit)

    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute(f"""
            SELECT
                p.player_id, p.player_name, p.position, p.position_group,
                ps.team_abbr, ps.gp, ps.min, ps.min_per_game,
                ps.pts, ps.ast, ps.reb, ps.oreb, ps.dreb,
                ps.stl, ps.blk, ps.tov, ps.pf, ps.pfd,
                ps.fgm, ps.fga, ps.fg_pct,
                ps.fg3m, ps.fg3a, ps.fg3_pct,
                ps.ftm, ps.fta, ps.ft_pct, ps.plus_minus,
                ps.off_rating, ps.def_rating, ps.net_rating,
                ps.ts_pct, ps.efg_pct, ps.usg_pct,
                ps.ast_pct, ps.oreb_pct, ps.dreb_pct, ps.reb_pct,
                ps.ast_to, ps.pie,
                ps.pct_uast_fgm, ps.pct_pts_paint, ps.pct_pts_3pt, ps.pct_pts_ft, ps.pts_paint,
                ps.bad_pass_tov, ps.lost_ball_tov,
                ps.def_ws,
                ps.drives, ps.drive_fga, ps.drive_fgm, ps.drive_fg_pct,
                ps.drive_pts, ps.drive_ast, ps.drive_tov, ps.drive_passes, ps.drive_pf,
                ps.passes_made, ps.passes_received, ps.ast_pts_created,
                ps.potential_ast, ps.secondary_ast, ps.ft_ast,
                ps.touches, ps.time_of_poss, ps.avg_sec_per_touch, ps.avg_drib_per_touch,
                ps.elbow_touches, ps.post_touches, ps.paint_touches,
                ps.pull_up_fga, ps.pull_up_fgm, ps.pull_up_fg_pct,
                ps.pull_up_fg3a, ps.pull_up_fg3_pct, ps.pull_up_efg_pct,
                ps.cs_fga, ps.cs_fgm, ps.cs_fg_pct, ps.cs_fg3a, ps.cs_fg3_pct, ps.cs_efg_pct,
                ps.post_touch_fga, ps.post_touch_fg_pct, ps.post_touch_pts,
                ps.post_touch_ast, ps.post_touch_tov,
                ps.dist_miles, ps.dist_miles_off, ps.dist_miles_def,
                ps.avg_speed, ps.avg_speed_off, ps.avg_speed_def,
                ps.def_rim_fga, ps.def_rim_fgm, ps.def_rim_fg_pct,
                ps.contested_shots, ps.contested_2pt, ps.contested_3pt,
                ps.deflections, ps.charges_drawn, ps.screen_assists, ps.screen_ast_pts,
                ps.loose_balls, ps.box_outs, ps.off_box_outs, ps.def_box_outs,
                ps.cd_fga_vt, ps.cd_fgm_vt, ps.cd_fg3a_vt, ps.cd_fg3m_vt,
                ps.cd_fga_tg, ps.cd_fgm_tg, ps.cd_fg3a_tg, ps.cd_fg3m_tg,
                ps.cd_fga_op, ps.cd_fgm_op, ps.cd_fg3a_op, ps.cd_fg3m_op,
                ps.cd_fga_wo, ps.cd_fgm_wo, ps.cd_fg3a_wo, ps.cd_fg3m_wo,
                ps.iso_ppp, ps.iso_fga, ps.iso_efg_pct, ps.iso_tov_pct,
                ps.pnr_bh_ppp, ps.pnr_bh_fga,
                ps.pnr_roll_ppp, ps.pnr_roll_poss, ps.post_ppp, ps.post_poss,
                ps.spotup_ppp, ps.spotup_efg_pct,
                ps.transition_ppp, ps.transition_fga,
                ps.def_iso_ppp, ps.def_pnr_bh_ppp, ps.def_post_ppp,
                ps.def_spotup_ppp, ps.def_pnr_roll_ppp,
                ps.clutch_net_rating, ps.clutch_ts_pct, ps.clutch_usg_pct, ps.clutch_min, ps.clutch_fgm,
                ps.gravity_score, ps.gravity_onball_perimeter, ps.gravity_offball_perimeter,
                ps.gravity_onball_interior, ps.gravity_offball_interior,
                ps.leverage_full, ps.leverage_offense, ps.leverage_defense,
                ps.leverage_shooting, ps.leverage_creation, ps.leverage_turnovers,
                ps.leverage_rebounds, ps.leverage_onball_def,
                ps.sq_avg_shot_quality, ps.sq_fg_pct_above_expected,
                ps.sq_avg_defender_distance, ps.sq_avg_defender_pressure,
                ps.sq_avg_shooter_speed, ps.sq_avg_made_quality, ps.sq_avg_missed_quality,
                ps.darko_dpm, ps.darko_odpm, ps.darko_ddpm, ps.darko_box,
                ps.lebron, ps.o_lebron, ps.d_lebron, ps.war,
                ps.net_pts100, ps.o_net_pts100, ps.d_net_pts100
            FROM player_seasons ps
            JOIN players p ON ps.player_id = p.player_id
            WHERE {where}
            ORDER BY ps.{sort_col} {dir_sql} NULLS LAST
            LIMIT %s
        """, params)
        rows = [dict(r) for r in cur.fetchall()]
        cur.close(); conn.close()
        return jsonify({"players": rows, "season": season, "count": len(rows)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── /api/stats ────────────────────────────────────────────────

@app.route("/api/stats")
def get_stats():
    """Full stat row for a single player."""
    player_id   = request.args.get("player_id")
    season      = request.args.get("season",      DEFAULT_SEASON)
    season_type = request.args.get("season_type", DEFAULT_SEASON_TYPE)

    if not player_id:
        return jsonify({"error": "player_id required"}), 400

    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT ps.*, p.player_name, p.position, p.position_group
            FROM player_seasons ps
            JOIN players p ON ps.player_id = p.player_id
            WHERE ps.player_id = %s AND ps.season = %s AND ps.season_type = %s
        """, (player_id, season, season_type))
        row = cur.fetchone()
        cur.close(); conn.close()
        if not row:
            return jsonify({"error": "Not found"}), 404
        return jsonify({"stats": dict(row)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── /api/stat-keys ────────────────────────────────────────────

@app.route("/api/stat-keys")
def get_stat_keys():
    """Return stat keys available in player_pctiles for the Builder."""
    season      = request.args.get("season",      DEFAULT_SEASON)
    season_type = request.args.get("season_type", DEFAULT_SEASON_TYPE)
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT stat_key FROM player_pctiles
            WHERE season = %s AND season_type = %s
            ORDER BY stat_key
        """, (season, season_type))
        keys = [r["stat_key"] for r in cur.fetchall()]
        cur.close(); conn.close()
        return jsonify({"stat_keys": keys, "season": season})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── /api/builder ──────────────────────────────────────────────

@app.route("/api/builder", methods=["POST"])
def run_builder():
    """
    Rank players by average percentile across selected stats.

    Body:
      {
        "season": "2024-25",
        "season_type": "Regular Season",
        "selected": ["pts", "ast", "ts_pct"],   // stat keys
        "min_minutes": 500,
        "pos": "ALL"    // optional position filter
      }

    Response:
      {
        "results": [
          {
            "rank": 1,
            "player_id": 123,
            "player_name": "...",
            "position_group": "G",
            "team_abbr": "GSW",
            "score": 87.3,      // average percentile (0–100)
            "covered": 3,       // stats with percentile data
            "total": 3,
            "breakdown": [
              { "stat": "pts", "pctile": 91.2 },
              ...
            ]
          }
        ]
      }
    """
    body        = request.get_json() or {}
    season      = body.get("season",      DEFAULT_SEASON)
    season_type = body.get("season_type", DEFAULT_SEASON_TYPE)
    selected    = body.get("selected",    [])
    min_minutes = int(body.get("min_minutes", 500))
    pos_filter  = body.get("pos", "ALL")
    mode        = body.get("mode", "flat")  # 'flat' or 'impact'

    if not selected:
        return jsonify({"error": "No stats selected"}), 400
    if len(selected) > 150:
        return jsonify({"error": "Max 150 stats at a time"}), 400

    # Load win-correlation weights for impact mode
    impact_weights = {}
    if mode == 'impact':
        season_key = season.replace('-', '_')
        corr_path  = os.path.join(
            os.path.dirname(__file__), 'ingest', 'data',
            f'win_correlations_{season_key}.json'
        )
        if os.path.exists(corr_path):
            with open(corr_path) as f:
                corr_data = json.load(f)
            impact_weights = corr_data.get('weights', {})

    try:
        conn = get_conn()
        cur  = conn.cursor()

        # Load percentile maps for selected stats
        cur.execute("""
            SELECT stat_key, pctile_map
            FROM player_pctiles
            WHERE season = %s AND season_type = %s
              AND stat_key = ANY(%s)
        """, (season, season_type, selected))
        pctile_rows = cur.fetchall()

        if not pctile_rows:
            cur.close(); conn.close()
            return jsonify({"error": "No percentile data found. Run compute_pctiles.py first."}), 404

        # Build stat→{player_id: pctile} lookup
        pct_maps = {r["stat_key"]: r["pctile_map"] for r in pctile_rows}

        # Fetch qualifying players with all raw stat columns
        pos_clause = "AND p.position_group = %s" if pos_filter != "ALL" else ""
        pos_params = [pos_filter] if pos_filter != "ALL" else []

        cur.execute(f"""
            SELECT ps.*, p.player_name, p.position_group
            FROM player_seasons ps
            JOIN players p ON ps.player_id = p.player_id
            WHERE ps.season = %s AND ps.season_type = %s
              AND ps.min >= %s
              {pos_clause}
        """, [season, season_type, min_minutes] + pos_params)
        players = cur.fetchall()

        cur.close(); conn.close()

        # Stats stored as season totals in player_seasons — divide by GP for per-game value
        TOTAL_KEYS = {
            'drives', 'drive_fga', 'drive_fgm', 'drive_pts', 'drive_passes', 'drive_pf', 'drive_tov',
            'bad_pass_tov', 'lost_ball_tov', 'passes_made', 'passes_received', 'ast_pts_created',
            'potential_ast', 'touches', 'paint_touches', 'elbow_touches',
            'pull_up_fga', 'pull_up_fgm', 'pull_up_fg3a', 'cs_fga', 'cs_fgm', 'cs_fg3a',
            'contested_shots', 'contested_2pt', 'contested_3pt', 'deflections',
            'def_rim_fga', 'def_rim_fgm', 'screen_ast_pts',
            'cd_fga_vt', 'cd_fga_tg', 'cd_fga_op', 'cd_fga_wo',
            'cd_fgm_vt', 'cd_fgm_tg', 'cd_fgm_op', 'cd_fgm_wo',
            'iso_fga', 'pnr_bh_fga', 'transition_fga', 'pts_paint',
        }

        def get_raw_value(row, stat):
            """Return the display value for a stat from a player_seasons row."""
            if stat == 'pot_ast_per_bad_pass_tov':
                pa  = row.get('potential_ast')
                bpt = row.get('bad_pass_tov')
                if pa is not None and bpt and float(bpt) > 0:
                    return round(float(pa) / float(bpt), 2)
                return None
            val = row.get(stat)
            if val is None:
                return None
            val = float(val)
            if stat in TOTAL_KEYS:
                gp = row.get('gp')
                if gp and float(gp) > 0:
                    val = val / float(gp)
                else:
                    return None
            return round(val, 2)

        # Score each player
        results = []
        for p in players:
            pid = str(p["player_id"])
            breakdown = []
            total_wgt = 0.0
            total_wpct = 0.0
            covered   = 0

            for stat in selected:
                pmap = pct_maps.get(stat, {})
                pct  = pmap.get(pid) or pmap.get(int(pid))
                if pct is not None:
                    w = impact_weights.get(stat, 1.0) if mode == 'impact' else 1.0
                    raw_val = get_raw_value(p, stat)
                    breakdown.append({"stat": stat, "pctile": round(float(pct), 1), "weight": round(w, 4), "value": raw_val})
                    total_wpct += float(pct) * w
                    total_wgt  += w
                    covered    += 1

            if covered == 0:
                continue
            # Require at least 80% stat coverage to avoid severely skewed scores
            # (e.g. playtypes missing for low-usage players, PBP stats for some)
            if covered < len(selected) * 0.8:
                continue

            score = round(total_wpct / total_wgt, 2)
            results.append({
                "player_id":      int(p["player_id"]),
                "player_name":    p["player_name"],
                "position_group": p["position_group"],
                "team_abbr":      p["team_abbr"],
                "min":            p["min"],
                "score":          score,
                "covered":        covered,
                "total":          len(selected),
                "breakdown":      sorted(breakdown, key=lambda x: -x["pctile"]),
            })

        results.sort(key=lambda r: -r["score"])
        for i, r in enumerate(results):
            r["rank"] = i + 1

        return jsonify({
            "results": results,
            "season":  season,
            "n":       len(results),
            "mode":    mode,
            "stats_found": list(pct_maps.keys()),
            "stats_missing": [s for s in selected if s not in pct_maps],
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── /api/builder/pctiles ──────────────────────────────────────

@app.route("/api/builder/pctiles", methods=["GET"])
def builder_pctiles():
    """
    Return per-player percentile data for client-side matching in the Builder.

    Query params:
      season, season_type, selected (comma-sep stat keys, max 10), min_minutes

    Response:
      {
        "players": [
          { "player_id": 123, "player_name": "...", "position_group": "G",
            "team_abbr": "GSW", "min": 1200, "pctiles": {"pts": 91.2, ...} }
        ],
        "impact_weights": { "pts": 0.62, ... },
        "season": "2024-25",
        "n": 320
      }
    """
    season      = request.args.get("season",      DEFAULT_SEASON)
    season_type = request.args.get("season_type", DEFAULT_SEASON_TYPE)
    raw_sel     = request.args.get("selected",    "")
    selected    = [s.strip() for s in raw_sel.split(",") if s.strip()]
    min_minutes = int(request.args.get("min_minutes", 500))
    raw_pos     = request.args.get("positions", "")
    positions   = [p.strip() for p in raw_pos.split(",") if p.strip()]

    if not selected:
        return jsonify({"error": "No stats selected"}), 400

    # Load win-correlation weights for impact mode
    impact_weights = {}
    season_key = season.replace('-', '_')
    corr_path  = os.path.join(
        os.path.dirname(__file__), 'ingest', 'data',
        f'win_correlations_{season_key}.json'
    )
    if os.path.exists(corr_path):
        with open(corr_path) as f:
            corr_data = json.load(f)
        raw_w = corr_data.get('weights', corr_data.get('correlations', {}))
        for stat in selected:
            if stat in raw_w:
                impact_weights[stat] = round(abs(float(raw_w[stat])), 4)

    try:
        conn = get_conn()
        cur  = conn.cursor()

        cur.execute("""
            SELECT stat_key, pctile_map
            FROM player_pctiles
            WHERE season = %s AND season_type = %s AND stat_key = ANY(%s)
        """, (season, season_type, selected))
        pct_maps = {r["stat_key"]: r["pctile_map"] for r in cur.fetchall()}

        # Use substring matching so compound groups (GF, FC) are included
        if positions:
            pos_conditions = " OR ".join(["p.position_group ILIKE %s"] * len(positions))
            pos_clause = f"AND ({pos_conditions})"
            pos_param  = [f"%{p}%" for p in positions]
        else:
            pos_clause = ""
            pos_param  = []
        cur.execute(f"""
            SELECT ps.*, p.player_name, p.position_group
            FROM player_seasons ps
            JOIN players p ON ps.player_id = p.player_id
            WHERE ps.season = %s AND ps.season_type = %s AND ps.min >= %s
            {pos_clause}
        """, [season, season_type, min_minutes] + pos_param)
        players = cur.fetchall()
        cur.close(); conn.close()

        # Stats stored as season totals — divide by GP for per-game display value
        TOTAL_KEYS = {
            'drives', 'drive_fga', 'drive_fgm', 'drive_pts', 'drive_passes', 'drive_pf', 'drive_tov',
            'bad_pass_tov', 'lost_ball_tov', 'passes_made', 'passes_received', 'ast_pts_created',
            'potential_ast', 'touches', 'paint_touches', 'elbow_touches',
            'pull_up_fga', 'pull_up_fgm', 'pull_up_fg3a', 'cs_fga', 'cs_fgm', 'cs_fg3a',
            'contested_shots', 'contested_2pt', 'contested_3pt', 'deflections',
            'def_rim_fga', 'def_rim_fgm', 'screen_ast_pts',
            'cd_fga_vt', 'cd_fga_tg', 'cd_fga_op', 'cd_fga_wo',
            'cd_fgm_vt', 'cd_fgm_tg', 'cd_fgm_op', 'cd_fgm_wo',
            'iso_fga', 'pnr_bh_fga', 'transition_fga', 'pts_paint',
        }

        def get_raw_value(row, stat):
            if stat == 'pot_ast_per_bad_pass_tov':
                pa  = row.get('potential_ast')
                bpt = row.get('bad_pass_tov')
                if pa is not None and bpt and float(bpt) > 0:
                    return round(float(pa) / float(bpt), 2)
                return None
            val = row.get(stat)
            if val is None:
                return None
            val = float(val)
            if stat in TOTAL_KEYS:
                gp = row.get('gp')
                if gp and float(gp) > 0:
                    val = val / float(gp)
            return round(val, 3)

        result = []
        for p in players:
            pid = str(p["player_id"])
            pctiles = {}
            values  = {}
            covered = 0
            for stat in selected:
                pmap = pct_maps.get(stat, {})
                pct  = pmap.get(pid) or pmap.get(int(pid))
                if pct is not None:
                    pctiles[stat] = round(float(pct), 1)
                    covered += 1
                raw = get_raw_value(p, stat)
                if raw is not None:
                    values[stat] = raw
            if covered < len(selected) * 0.8:
                continue
            result.append({
                "player_id":      int(p["player_id"]),
                "player_name":    p["player_name"],
                "position_group": p["position_group"],
                "team_abbr":      p["team_abbr"],
                "min":            float(p["min"]),
                "pctiles":        pctiles,
                "values":         values,
            })

        return jsonify({
            "players":        result,
            "impact_weights": impact_weights,
            "season":         season,
            "n":              len(result),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


"""
ADD THESE ROUTES TO backend/server.py
Paste them before the `

if __name__ == "__main__":` block.
"""

import requests as _requests
import time as _time
import threading as _threading
from datetime import datetime as _dt
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── Past-date cache ───────────────────────────────────────────────
# Scores never change once Final — cache forever after first ScoreboardV3 fetch.
# Cleared on server restart, which forces a fresh ScoreboardV3 call.
_past_sb_cache: dict = {}   # date -> {"payload": dict, "ts": float}

# ── Future-date cache (schedule can change — TTL 60 min) ──────────
_future_sb_cache: dict = {}  # date -> {"payload": dict, "ts": float}

# ── ESPN injury report cache (TTL 30 min), keyed per league ──────
_injury_cache: dict = {"nba": {"data": {}, "ts": 0.0}, "wnba": {"data": {}, "ts": 0.0}}

# ── Full season schedule from CDN (cached 2 h — used for future dates) ──
_schedule_cache: dict = {"data": None, "ts": 0.0}


def _fetch_nba_schedule() -> dict | None:
    """Fetch the NBA season schedule from the CDN (not rate-limited on cloud IPs).
    Cached in memory for 2 hours.  Returns the raw JSON dict or None on failure."""
    if _schedule_cache["data"] and _time.time() - _schedule_cache["ts"] < 7200:
        return _schedule_cache["data"]
    try:
        url  = "https://cdn.nba.com/static/json/staticData/scheduleLeagueV2_1.json"
        resp = _cdn_get(url, headers=_CDN_HEADERS, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        _schedule_cache["data"] = data
        _schedule_cache["ts"]   = _time.time()
        return data
    except Exception:
        return None

# ── Today's scoreboard — kept fresh by the background poller ─────────────────
_today_sb_cache: dict = {}   # {"payload": dict, "ts": float, "date": str}

# ── Background scoreboard poller ──────────────────────────────────────────────
# Polls the NBA CDN every 30 s when games are live, 5 min when idle.
# Writes directly into _today_sb_cache so the endpoint never blocks on a live
# CDN call.  Falls back to the season schedule when CDN is unreachable, so the
# correct games still show (without scores) instead of "No games today."

_sb_poller_stop   = _threading.Event()
_sb_poller_thread = None
_POLL_LIVE_S      = 30   # games in progress
_POLL_SOON_S      = 20   # upcoming games today (pregame window)
_POLL_IDLE_S      = 300  # no games today


def _parse_cdn_scoreboard(cdn_data: dict, game_today: str) -> dict | None:
    """Parse CDN scoreboard JSON into payload format.
    Returns None if the CDN gameDate doesn't match game_today (CDN still on prior date)."""
    cdn_games = cdn_data.get("scoreboard", {}).get("games", [])
    cdn_date  = cdn_data.get("scoreboard", {}).get("gameDate", "")
    if cdn_date != game_today:
        return None
    games = []
    for g in cdn_games:
        # Skip if-necessary playoff games that won't be played (status TBD = not confirmed)
        if g.get("ifNecessary") and str(g.get("gameStatusText", "")).strip().upper() == "TBD":
            continue
        away = g.get("awayTeam", {}); home = g.get("homeTeam", {})
        raw_utc = g.get("gameTimeUTC", "")
        if raw_utc.startswith("1900-"):
            raw_utc = _resolve_1900_game_time(g.get("gameStatusText", ""), game_today) or raw_utc
        games.append({
            "gameId":         g.get("gameId", ""),
            "gameStatus":     g.get("gameStatus", 1),
            "gameStatusText": g.get("gameStatusText", ""),
            "period":         g.get("period", 0),
            "gameClock":      g.get("gameClock", ""),
            "gameTimeUTC":    raw_utc,
            "away": {"abbr": away.get("teamTricode", ""), "score": int(away.get("score", 0) or 0),
                     "wins": away.get("wins"), "losses": away.get("losses")},
            "home": {"abbr": home.get("teamTricode", ""), "score": int(home.get("score", 0) or 0),
                     "wins": home.get("wins"), "losses": home.get("losses")},
        })
        if int(g.get("gameStatus", 1) or 1) == 3 and g.get("gameId"):
            _upsert_game_from_boxscore(g["gameId"], g)
    _enrich_games_with_records(games)
    return {"games": games, "date": cdn_date}


def _crosscheck_tipoff(games: list, nba: bool = True) -> None:
    """For each upcoming game whose scheduled tipoff has passed, fetch its live
    boxscore to get the real status.  The per-game boxscore CDN updates in
    real-time while the scoreboard CDN can lag 10+ minutes on game starts.
    Modifies game dicts in-place."""
    import datetime as _dt_mod
    now_utc = _dt_mod.datetime.now(_dt_mod.timezone.utc).replace(tzinfo=None)
    for g in games:
        status = g.get("gameStatus")
        if status == 2:
            pass  # already live — always fetch boxscore for real-time clock/period
        elif status == 1:
            # upcoming — only fetch if tipoff time has passed
            tipoff_str = g.get("gameTimeUTC", "")
            if not tipoff_str:
                continue
            try:
                tipoff = _dt.strptime(tipoff_str[:19].replace("T", " "), "%Y-%m-%d %H:%M:%S")
            except Exception:
                continue
            if now_utc < tipoff:
                continue  # hasn't tipped yet
        else:
            continue  # final or unknown — skip
        gid = g.get("gameId", "")
        if not gid:
            continue
        try:
            cdn_base = "https://cdn.nba.com" if nba else "https://cdn.wnba.com"
            url = f"{cdn_base}/static/json/liveData/boxscore/boxscore_{gid}.json"
            hdrs = _CDN_HEADERS if nba else _WNBA_CDN_HEADERS
            r = _cdn_get(url, headers=hdrs, timeout=5)
            r.raise_for_status()
            box = r.json().get("game", {})
            real_status = int(box.get("gameStatus", 1) or 1)
            if real_status in (2, 3):
                g["gameStatus"]     = real_status
                g["gameStatusText"] = box.get("gameStatusText", g["gameStatusText"])
                g["period"]         = box.get("period", g.get("period", 0))
                g["gameClock"]      = box.get("gameClock", g.get("gameClock", ""))
                away_box = box.get("awayTeam", {}); home_box = box.get("homeTeam", {})
                g["away"]["score"]  = int(away_box.get("score", 0) or 0)
                g["home"]["score"]  = int(home_box.get("score", 0) or 0)
        except Exception:
            pass


def _sb_poller_tick() -> tuple[bool, bool, bool]:
    """One poll iteration. Returns (has_live_game, cdn_succeeded, has_upcoming_game)."""
    game_today = _compute_game_today()

    # ── Primary: NBA live CDN ─────────────────────────────────────────────────
    try:
        r = _requests.get(
            "https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json",
            headers=_CDN_HEADERS, timeout=8,
        )
        r.raise_for_status()
        payload = _parse_cdn_scoreboard(r.json(), game_today)
        if payload:
            _crosscheck_tipoff(payload["games"], nba=True)
            _today_sb_cache.update({"payload": payload, "ts": _time.time(), "date": game_today})
            has_live     = any(g["gameStatus"] == 2 for g in payload["games"])
            has_upcoming = any(g["gameStatus"] == 1 for g in payload["games"])
            return has_live, True, has_upcoming
        # CDN returned a past date — fall through to schedule
    except Exception:
        pass

    # ── Fallback: season schedule (games shown without scores) ───────────────
    # Only seeds the cache if we don't already have live/final data for today,
    # so we never overwrite real scores with schedule stubs.
    try:
        sched = _fetch_nba_schedule()
        if sched:
            dt_obj    = _dt.strptime(game_today, "%Y-%m-%d")
            sched_key = f"{dt_obj.month:02d}/{dt_obj.day:02d}/{dt_obj.year} 00:00:00"
            target    = next(
                (gd for gd in sched.get("leagueSchedule", {}).get("gameDates", [])
                 if gd.get("gameDate") == sched_key),
                None,
            )
            if target:
                games = []
                for g in target.get("games", []):
                    # Skip if-necessary playoff games that won't be played (status TBD = not confirmed)
                    if g.get("ifNecessary") and str(g.get("gameStatusText", "")).strip().upper() == "TBD":
                        continue
                    away = g.get("awayTeam", {}); home = g.get("homeTeam", {})
                    raw_utc = g.get("gameTimeUTC", "")
                    if raw_utc.startswith("1900-"):
                        raw_utc = _resolve_1900_game_time(g.get("gameStatusText", ""), game_today) or raw_utc
                    games.append({
                        "gameId": g.get("gameId", ""), "gameStatus": 1,
                        "gameStatusText": g.get("gameStatusText", ""),
                        "period": 0, "gameClock": "",
                        "gameTimeUTC": raw_utc,
                        "away": {"abbr": away.get("teamTricode", ""), "score": 0,
                                 "wins": None, "losses": None},
                        "home": {"abbr": home.get("teamTricode", ""), "score": 0,
                                 "wins": None, "losses": None},
                    })
                if games:
                    _enrich_games_with_records(games)
                    cached = _today_sb_cache.get("payload", {})
                    has_real_data = (
                        _today_sb_cache.get("date") == game_today
                        and any(g["gameStatus"] in (2, 3) for g in cached.get("games", []))
                    )
                    if not has_real_data:
                        _today_sb_cache.update({
                            "payload": {"games": games, "date": game_today},
                            "ts": _time.time(), "date": game_today,
                        })
                    has_upcoming = bool(games)
                    return False, False, has_upcoming
    except Exception:
        pass

    return False, False, False


def _sb_poller_loop():
    import logging
    log = logging.getLogger("sb_poller")
    log.info("[POLLER] Scoreboard background poller started")
    while not _sb_poller_stop.is_set():
        try:
            has_live, cdn_ok, has_upcoming = _sb_poller_tick()
        except Exception:
            has_live, cdn_ok, has_upcoming = False, False, False
        # 30 s when live, 60 s when games are upcoming today, 5 min when truly idle
        if has_live:
            interval = _POLL_LIVE_S
        elif has_upcoming:
            interval = _POLL_SOON_S
        elif cdn_ok:
            interval = _POLL_IDLE_S
        else:
            interval = 60  # CDN failing — retry sooner
        _sb_poller_stop.wait(interval)
    log.info("[POLLER] Scoreboard background poller stopped")


def start_sb_poller():
    global _sb_poller_thread
    if _sb_poller_thread and _sb_poller_thread.is_alive():
        return
    _sb_poller_stop.clear()
    _sb_poller_thread = _threading.Thread(
        target=_sb_poller_loop, daemon=True, name="ScoreboardPoller",
    )
    _sb_poller_thread.start()


def _compute_game_today():
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        from backports.zoneinfo import ZoneInfo
    from datetime import datetime, timedelta
    now_et = datetime.now(ZoneInfo('America/New_York'))
    if now_et.hour < 6:
        return (now_et - timedelta(days=1)).strftime('%Y-%m-%d')
    return now_et.strftime('%Y-%m-%d')

# Headers for NBA CDN (live boxscore proxy)
_CDN_HEADERS = {
    "User-Agent":       "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Referer":          "https://www.nba.com/",
    "Origin":           "https://www.nba.com",
    "Accept":           "application/json, text/plain, */*",
    "Accept-Language":  "en-US,en;q=0.9",
    "sec-ch-ua":        '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest":   "empty",
    "sec-fetch-mode":   "cors",
    "sec-fetch-site":   "same-site",
}
_WNBA_CDN_HEADERS = {
    **_CDN_HEADERS,
    "Referer":        "https://www.wnba.com/",
    "Origin":         "https://www.wnba.com",
    "sec-fetch-site": "cross-site",
}

# The NBA/WNBA CDNs (Akamai) sit behind Akamai Bot Manager, which fingerprints the
# client's TLS handshake (JA3) and serves a challenge HTML page to plain HTTP clients
# like `requests`/`urllib`.  curl_cffi replicates Chrome's exact TLS+HTTP2 fingerprint,
# so Akamai treats it as a real browser and returns the JSON.  Use this for every
# cdn.nba.com / cdn.wnba.com request.
from curl_cffi import requests as _cffi_requests

def _cdn_get(url, headers=None, timeout=10, impersonate="chrome"):
    """GET a CDN URL with a Chrome TLS fingerprint to pass Akamai Bot Manager.

    Returns a curl_cffi Response, which is API-compatible with the requests.Response
    used elsewhere (.json(), .text, .status_code, .headers, .raise_for_status())."""
    return _cffi_requests.get(url, headers=headers, timeout=timeout, impersonate=impersonate)


def _fetch_boxscores_parallel(game_ids, timeout=8):
    """Fetch CDN boxscores for multiple game IDs in parallel.
    Returns a dict mapping game_id -> box dict (or None on failure)."""
    def _fetch_one(gid):
        try:
            url  = f"https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{gid}.json"
            resp = _cdn_get(url, headers=_CDN_HEADERS, timeout=timeout)
            if resp.status_code == 200:
                return gid, resp.json().get("game", {})
        except Exception:
            pass
        return gid, None

    results = {}
    with ThreadPoolExecutor(max_workers=min(len(game_ids), 12)) as pool:
        futures = {pool.submit(_fetch_one, gid): gid for gid in game_ids}
        for fut in as_completed(futures):
            gid, box = fut.result()
            results[gid] = box
    return results


def _enrich_games_with_records(games):
    """
    Enrich scoreboard game dicts in-place with W-L / series records and review stats.
    Always sets avg_stars and review_count on every game, even if DB queries fail.
    """
    if not games:
        return

    season = os.getenv("NBA_SEASON", "2025-26")
    all_abbrs = set()
    playoff_pairs = set()
    for g in games:
        away_abbr = g.get("away", {}).get("abbr", "")
        home_abbr = g.get("home", {}).get("abbr", "")
        if away_abbr: all_abbrs.add(away_abbr)
        if home_abbr: all_abbrs.add(home_abbr)
        game_id = str(g.get("gameId", ""))
        is_po = game_id.startswith("004")
        g["is_playoffs"] = is_po
        if is_po and away_abbr and home_abbr:
            playoff_pairs.add(tuple(sorted([away_abbr, home_abbr])))

    reg_records    = {}
    series_records = {}
    review_stats   = {}

    try:
        conn = get_conn()
        cur  = conn.cursor()

        # ── Regular season W-L ──
        if all_abbrs:
            abbr_list = list(all_abbrs)
            cur.execute("""
                SELECT team_abbr,
                       SUM(CASE WHEN won THEN 1 ELSE 0 END)::int AS wins,
                       SUM(CASE WHEN NOT won THEN 1 ELSE 0 END)::int AS losses
                FROM (
                    SELECT home_team_abbr AS team_abbr, home_score > away_score AS won
                    FROM games
                    WHERE season = %s AND season_type = 'Regular Season'
                      AND status = 'Final' AND home_team_abbr = ANY(%s)
                    UNION ALL
                    SELECT away_team_abbr AS team_abbr, away_score > home_score AS won
                    FROM games
                    WHERE season = %s AND season_type = 'Regular Season'
                      AND status = 'Final' AND away_team_abbr = ANY(%s)
                ) sub
                GROUP BY team_abbr
            """, (season, abbr_list, season, abbr_list))
            for r in cur.fetchall():
                reg_records[r["team_abbr"]] = (int(r["wins"]), int(r["losses"]))

        # ── Playoff series records ──
        for pair in playoff_pairs:
            t1, t2 = pair
            cur.execute("""
                SELECT home_team_abbr, away_team_abbr, home_score, away_score
                FROM games
                WHERE season = %s AND season_type = 'Playoffs' AND status = 'Final'
                  AND ((home_team_abbr = %s AND away_team_abbr = %s)
                    OR (home_team_abbr = %s AND away_team_abbr = %s))
            """, (season, t1, t2, t2, t1))
            wins = {t1: 0, t2: 0}
            for sg in cur.fetchall():
                h, a = sg["home_team_abbr"], sg["away_team_abbr"]
                if int(sg["home_score"] or 0) > int(sg["away_score"] or 0):
                    wins[h] = wins.get(h, 0) + 1
                else:
                    wins[a] = wins.get(a, 0) + 1
            series_records[pair] = wins

        # ── Review stats ──
        game_ids = [str(g.get("gameId", "")) for g in games if g.get("gameId")]
        if game_ids:
            cur.execute("""
                SELECT game_id,
                       COUNT(*)                          AS review_count,
                       ROUND((AVG(rating) / 2.0)::numeric, 2)::float AS avg_stars
                FROM game_reviews
                WHERE game_id = ANY(%s)
                GROUP BY game_id
            """, (game_ids,))
            for r in cur.fetchall():
                review_stats[r["game_id"]] = {
                    "avg_stars":    r["avg_stars"],
                    "review_count": int(r["review_count"] or 0),
                }

        cur.close()
        conn.close()
    except Exception as e:
        print(f"[enrich] DB error: {e}", flush=True)

    # ── Apply to games (always runs even if DB failed) ──
    for g in games:
        away      = g.get("away", {})
        home      = g.get("home", {})
        away_abbr = away.get("abbr", "")
        home_abbr = home.get("abbr", "")
        game_id   = str(g.get("gameId", ""))

        if game_id.startswith("004") and away_abbr and home_abbr:
            pair = tuple(sorted([away_abbr, home_abbr]))
            sr   = series_records.get(pair, {})
            away["series_wins"] = sr.get(away_abbr, 0)
            home["series_wins"] = sr.get(home_abbr, 0)
        else:
            if away.get("wins") is None and away_abbr in reg_records:
                away["wins"], away["losses"] = reg_records[away_abbr]
            if home.get("wins") is None and home_abbr in reg_records:
                home["wins"], home["losses"] = reg_records[home_abbr]

        rs = review_stats.get(game_id, {})
        g["avg_stars"]    = rs.get("avg_stars")
        g["review_count"] = rs.get("review_count", 0)

    # ── Drop ghost playoff games (series already decided, game never played) ──
    # Keep a game if: not a playoff game, OR already final, OR series still alive.
    # series_wins defaults to 0 if DB failed, so we fail-safe toward showing games.
    games[:] = [
        g for g in games
        if not str(g.get("gameId", "")).startswith("004")
        or int(g.get("gameStatus", 1) or 1) == 3
        or max(
            (g.get("away", {}).get("series_wins", 0) or 0),
            (g.get("home", {}).get("series_wins", 0) or 0),
        ) < 4
    ]


# ── /api/scoreboard?date=YYYY-MM-DD ──────────────────────────────
@app.route("/api/scoreboard")
def get_scoreboard():
    """
    No ?date  → today (with 6 AM ET cutoff).
                Primary: NBA live CDN (fast, works on cloud IPs).
                Fallback: ScoreboardV3 (works locally, may be blocked on production).
    ?date=YYYY-MM-DD → DB first for past dates, then ScoreboardV3.
    Results are cached: past dates forever (after first ScoreboardV3 fetch),
    today for 30 s, future for 60 min.
    """
    date = request.args.get("date", "").strip()
    _game_today = _compute_game_today()

    if not date:
        date = _game_today

    is_past   = date < _game_today
    is_today  = date == _game_today

    # Past dates — cache forever once fetched from ScoreboardV3
    if is_past and date in _past_sb_cache:
        return jsonify(_past_sb_cache[date]["payload"])

    # Past dates — DB-first path (games table is populated by fetch_games.py and
    # _upsert_game_from_boxscore).  ScoreboardV3 is rate-limited on cloud IPs, so
    # the DB is the only reliable source for historical dates on production.
    if is_past:
        try:
            conn = get_conn()
            cur  = conn.cursor()
            cur.execute("""
                SELECT game_id, home_team_abbr, away_team_abbr,
                       home_score, away_score, status
                FROM games
                WHERE game_date = %s AND (league IS NULL OR league = 'nba')
                  AND (COALESCE(home_score, 0) > 0 OR COALESCE(away_score, 0) > 0)
                ORDER BY game_id
            """, (date,))
            db_rows = cur.fetchall()
            cur.close(); conn.close()

            if db_rows:
                games = []
                for g in db_rows:
                    games.append({
                        "gameId":         g["game_id"],
                        "gameStatus":     3,
                        "gameStatusText": "Final",
                        "period":         0,
                        "gameClock":      "",
                        "gameTimeUTC":    "",
                        "away": {"abbr": g["away_team_abbr"], "score": int(g["away_score"] or 0),
                                 "wins": None, "losses": None},
                        "home": {"abbr": g["home_team_abbr"], "score": int(g["home_score"] or 0),
                                 "wins": None, "losses": None},
                    })
                _enrich_games_with_records(games)
                payload = {"games": games, "date": date}
                _past_sb_cache[date] = {"payload": payload, "ts": _time.time()}
                return jsonify(payload)
            # DB has nothing for this date — fall through to ScoreboardV3
        except Exception:
            pass  # DB error — fall through to ScoreboardV3

    # Today — serve from poller-maintained cache (refreshed every 30 s when live,
    # 5 min when idle). Falls back to on-demand CDN fetch if cache is cold or
    # the poller has been silent for more than 5 minutes.
    if is_today and _today_sb_cache.get("date") == _game_today:
        if _time.time() - _today_sb_cache.get("ts", 0) < 300:
            return jsonify(_today_sb_cache["payload"])

    # Today — try the NBA live CDN first (not rate-limited on cloud IPs)
    if is_today:
        try:
            cdn_url  = "https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json"
            cdn_resp = _cdn_get(cdn_url, headers=_CDN_HEADERS, timeout=8)
            cdn_resp.raise_for_status()
            cdn_data  = cdn_resp.json()
            cdn_games = cdn_data.get("scoreboard", {}).get("games", [])
            cdn_date  = cdn_data.get("scoreboard", {}).get("gameDate", "")
            # Accept CDN data even if the date lags by one day — the CDN sometimes
            # reports yesterday's date for a few hours after midnight ET.
            from datetime import datetime as _datetime_cls
            try:
                cdn_dt   = _datetime_cls.strptime(cdn_date, "%Y-%m-%d")
                today_dt = _datetime_cls.strptime(_game_today, "%Y-%m-%d")
                date_ok  = abs((today_dt - cdn_dt).days) <= 1
            except Exception:
                date_ok = cdn_date == _game_today
            # Authoritative even with zero games: in the offseason the CDN reports
            # today's date with an empty games list, and that's the correct answer.
            # Requiring cdn_games here made every offseason request fall through to
            # ScoreboardV3, which times out (~15 s) on cloud IPs.
            if date_ok:
                away_k = "awayTeam"; home_k = "homeTeam"
                games = []
                for g in cdn_games:
                    # Skip if-necessary playoff games that won't be played
                    if g.get("ifNecessary") and str(g.get("gameStatusText", "")).strip().upper() == "TBD":
                        continue
                    away = g.get(away_k, {}); home = g.get(home_k, {})
                    raw_utc = g.get("gameTimeUTC", "")
                    if raw_utc.startswith("1900-"):
                        raw_utc = _resolve_1900_game_time(g.get("gameStatusText", ""), _game_today) or raw_utc
                    games.append({
                        "gameId":         g.get("gameId", ""),
                        "gameStatus":     g.get("gameStatus", 1),
                        "gameStatusText": g.get("gameStatusText", ""),
                        "period":         g.get("period", 0),
                        "gameClock":      g.get("gameClock", ""),
                        "gameTimeUTC":    raw_utc,
                        "away": {"abbr": away.get("teamTricode",""), "score": int(away.get("score",0) or 0),
                                 "wins": away.get("wins"), "losses": away.get("losses")},
                        "home": {"abbr": home.get("teamTricode",""), "score": int(home.get("score",0) or 0),
                                 "wins": home.get("wins"), "losses": home.get("losses")},
                    })
                    # Persist Final games to DB — skip ghost games (0-0 score means never played)
                    game_status = int(g.get("gameStatus", 1) or 1)
                    away_sc = int(away.get("score", 0) or 0)
                    home_sc = int(home.get("score", 0) or 0)
                    if game_status == 3 and g.get("gameId") and (away_sc > 0 or home_sc > 0):
                        _upsert_game_from_boxscore(g["gameId"], g)
                _enrich_games_with_records(games)
                payload = {"games": games, "date": _game_today}
                _today_sb_cache.update({"payload": payload, "ts": _time.time(), "date": _game_today})
                return jsonify(payload)
        except Exception:
            pass  # CDN failed — fall through to cached schedule then ScoreboardV3

    # Today — fast fallback using season schedule IF already cached in memory.
    # Avoids the 8.4 MB download; only serves if no live/final data is cached.
    if is_today and _schedule_cache.get("data"):
        try:
            sched = _schedule_cache["data"]
            dt_obj    = _dt.strptime(_game_today, "%Y-%m-%d")
            sched_key = f"{dt_obj.month:02d}/{dt_obj.day:02d}/{dt_obj.year} 00:00:00"
            target    = next(
                (gd for gd in sched.get("leagueSchedule", {}).get("gameDates", [])
                 if gd.get("gameDate") == sched_key),
                None,
            )
            if target:
                games = []
                for g in target.get("games", []):
                    if g.get("ifNecessary") and str(g.get("gameStatusText", "")).strip().upper() == "TBD":
                        continue
                    away = g.get("awayTeam", {}); home = g.get("homeTeam", {})
                    raw_utc = g.get("gameTimeUTC", "")
                    if raw_utc.startswith("1900-"):
                        raw_utc = _resolve_1900_game_time(g.get("gameStatusText", ""), _game_today) or raw_utc
                    games.append({
                        "gameId": g.get("gameId", ""), "gameStatus": 1,
                        "gameStatusText": g.get("gameStatusText", ""),
                        "period": 0, "gameClock": "",
                        "gameTimeUTC": raw_utc,
                        "away": {"abbr": away.get("teamTricode", ""), "score": 0,
                                 "wins": None, "losses": None},
                        "home": {"abbr": home.get("teamTricode", ""), "score": 0,
                                 "wins": None, "losses": None},
                    })
                if games:
                    _enrich_games_with_records(games)
                    return jsonify({"games": games, "date": _game_today})
        except Exception:
            pass  # Fall through to ScoreboardV3

    # Future dates — cache for 60 min
    if not is_past and not is_today and date in _future_sb_cache:
        entry = _future_sb_cache[date]
        if _time.time() - entry["ts"] < 3600:
            return jsonify(entry["payload"])

    # Future dates — CDN season schedule (not rate-limited on cloud IPs)
    if not is_past and not is_today:
        try:
            sched = _fetch_nba_schedule()
            if sched:
                dt = _dt.strptime(date, "%Y-%m-%d")
                sched_key  = f"{dt.month:02d}/{dt.day:02d}/{dt.year} 00:00:00"
                game_dates = sched.get("leagueSchedule", {}).get("gameDates", [])
                target     = next((gd for gd in game_dates if gd.get("gameDate") == sched_key), None)
                if target is not None:
                    games = []
                    for g in target.get("games", []):
                        if g.get("ifNecessary") and str(g.get("gameStatusText", "")).strip().upper() == "TBD":
                            continue
                        away = g.get("awayTeam", {})
                        home = g.get("homeTeam", {})
                        raw_utc = g.get("gameTimeUTC", "")
                        if raw_utc.startswith("1900-"):
                            raw_utc = _resolve_1900_game_time(g.get("gameStatusText", ""), date) or raw_utc
                        games.append({
                            "gameId":         g.get("gameId", ""),
                            "gameStatus":     1,
                            "gameStatusText": g.get("gameStatusText", ""),
                            "period":         0,
                            "gameClock":      "",
                            "gameTimeUTC":    raw_utc,
                            "away": {"abbr": away.get("teamTricode", ""), "score": 0,
                                     "wins": None, "losses": None},
                            "home": {"abbr": home.get("teamTricode", ""), "score": 0,
                                     "wins": None, "losses": None},
                        })
                    _enrich_games_with_records(games)
                    payload = {"games": games, "date": date}
                    _future_sb_cache[date] = {"payload": payload, "ts": _time.time()}
                    return jsonify(payload)
        except Exception:
            pass  # Fall through to ScoreboardV3

    try:
        from nba_api.stats.endpoints import scoreboardv3

        dt    = _dt.strptime(date, "%Y-%m-%d")
        board = scoreboardv3.ScoreboardV3(
            game_date=dt.strftime("%Y-%m-%d"),
            league_id="00",
            timeout=15,
        )
        gh_df = board.game_header.get_data_frame()

        if gh_df.empty:
            payload = {"games": [], "date": date}
            if is_past:
                _past_sb_cache[date] = {"payload": payload, "ts": _time.time()}
            elif is_today:
                _today_sb_cache.update({"payload": payload, "ts": _time.time(), "date": _game_today})
            else:
                _future_sb_cache[date] = {"payload": payload, "ts": _time.time()}
            return jsonify(payload)

        rows = [(str(row.get("gameId", "") or row.get("GAME_ID", "")), row)
                for _, row in gh_df.iterrows()
                if row.get("gameId") or row.get("GAME_ID")]

        gids = [gid for gid, _ in rows]
        boxscores = _fetch_boxscores_parallel(gids) if gids else {}

        games = []
        for gid, row in rows:
            box = boxscores.get(gid)

            away_abbr = home_abbr = ""
            away_score = home_score = 0
            away_wins = away_losses = home_wins = home_losses = None

            if box:
                away        = box.get("awayTeam", {})
                home        = box.get("homeTeam", {})
                away_abbr   = away.get("teamTricode", "")
                home_abbr   = home.get("teamTricode", "")
                away_score  = int(away.get("score", 0) or 0)
                home_score  = int(home.get("score", 0) or 0)
                away_wins   = away.get("wins")
                away_losses = away.get("losses")
                home_wins   = home.get("wins")
                home_losses = home.get("losses")
                if is_past and (away_score > 0 or home_score > 0):
                    _upsert_game_from_boxscore(gid, box)
            else:
                code = str(row.get("gameCode", "") or row.get("GAMECODE", "") or "")
                if "/" in code:
                    teams = code.split("/")[1]
                    away_abbr = teams[:3] if len(teams) >= 6 else ""
                    home_abbr = teams[3:6] if len(teams) >= 6 else ""

            # Skip past games with 0-0 scores — these are ghost playoff games that never happened
            if is_past and away_score == 0 and home_score == 0:
                continue

            if is_past:
                game_status_id   = 3
                game_status_text = "Final"
            else:
                raw_status = row.get("gameStatus", row.get("GAME_STATUS_ID", 1))
                game_status_id   = int(raw_status or 1)
                game_status_text = str(row.get("gameStatusText", row.get("GAME_STATUS_TEXT", "")) or "")

            games.append({
                "gameId":         gid,
                "gameStatus":     game_status_id,
                "gameStatusText": game_status_text,
                "period":         box.get("period", 0) if box else 0,
                "gameClock":      box.get("gameClock", "") if box else "",
                "gameTimeUTC":    _fmt_game_time(row.get("gameTimeUTC", row.get("GAME_TIME_UTC", ""))),
                "away": {"abbr": away_abbr, "score": away_score,
                         "wins": away_wins, "losses": away_losses},
                "home": {"abbr": home_abbr, "score": home_score,
                         "wins": home_wins, "losses": home_losses},
            })

        _enrich_games_with_records(games)
        payload = {"games": games, "date": date}
        if is_past:
            _past_sb_cache[date] = {"payload": payload, "ts": _time.time()}
        elif is_today:
            _today_sb_cache.update({"payload": payload, "ts": _time.time(), "date": _game_today})
        else:
            _future_sb_cache[date] = {"payload": payload, "ts": _time.time()}
        return jsonify(payload)

    except Exception as e:
        # ScoreboardV3 failed (rate-limited on cloud) — last resort: CDN season schedule
        if not is_past:
            try:
                sched = _fetch_nba_schedule()
                if sched:
                    dt_obj   = _dt.strptime(date, "%Y-%m-%d")
                    sched_key = f"{dt_obj.month:02d}/{dt_obj.day:02d}/{dt_obj.year} 00:00:00"
                    game_dates = sched.get("leagueSchedule", {}).get("gameDates", [])
                    target = next((gd for gd in game_dates if gd.get("gameDate") == sched_key), None)
                    if target is not None:
                        games = []
                        for g in target.get("games", []):
                            away = g.get("awayTeam", {})
                            home = g.get("homeTeam", {})
                            games.append({
                                "gameId":         g.get("gameId", ""),
                                "gameStatus":     1,
                                "gameStatusText": g.get("gameStatusText", ""),
                                "period":         0,
                                "gameClock":      "",
                                "gameTimeUTC":    g.get("gameTimeUTC", ""),
                                "away": {"abbr": away.get("teamTricode", ""), "score": 0,
                                         "wins": None, "losses": None},
                                "home": {"abbr": home.get("teamTricode", ""), "score": 0,
                                         "wins": None, "losses": None},
                            })
                        _enrich_games_with_records(games)
                        if games:
                            return jsonify({"games": games, "date": date})
            except Exception:
                pass
        # Cache the empty result so we don't re-pay the ScoreboardV3 timeout on every
        # request (today caches for 5 min via the is_today fast path above).
        empty = {"games": [], "date": date}
        if is_today:
            _today_sb_cache.update({"payload": empty, "ts": _time.time(), "date": _game_today})
        elif not is_past:
            _future_sb_cache[date] = {"payload": empty, "ts": _time.time()}
        return jsonify({**empty, "error": str(e)}), 200


# ── /api/news ─────────────────────────────────────────────────────
_news_cache: dict = {}       # {"payload": list, "ts": float}
_wnba_news_cache: dict = {}  # {"payload": list, "ts": float}

_NEWS_SOURCES = [
    ("https://news.google.com/rss/search?q=NBA+basketball&hl=en-US&gl=US&ceid=US:en", None),
]
_WNBA_NEWS_SOURCES = [
    ("https://news.google.com/rss/search?q=WNBA+basketball&hl=en-US&gl=US&ceid=US:en", None),
]

def _parse_rss(content, default_source):
    import xml.etree.ElementTree as ET
    root = ET.fromstring(content)
    items = []
    for item in root.iter("item"):
        title    = (item.findtext("title") or "").strip()
        link     = (item.findtext("link") or "").strip()
        pub_date = (item.findtext("pubDate") or "").strip()
        source   = (item.findtext("source") or default_source or "NBA").strip()
        # Google News titles end with " - Source Name"; strip it when we have the source
        if source and title.endswith(f" - {source}"):
            title = title[: -len(f" - {source}")].strip()
        if title:
            items.append({"title": title, "link": link, "pubDate": pub_date, "source": source})
        if len(items) >= 10:
            break
    return items

def _fetch_news(sources: list, cache: dict) -> dict:
    """Fetch RSS news from sources, populate cache, return jsonifiable response dict."""
    if cache.get("payload") and _time.time() - cache.get("ts", 0) < 300:
        return {"status": "ok", "items": cache["payload"]}
    for url, default_source in sources:
        try:
            resp = _requests.get(
                url,
                headers={"User-Agent": "Mozilla/5.0 (compatible; NothingButNet/1.0)"},
                timeout=10,
            )
            if resp.status_code != 200 or not resp.content:
                continue
            items = _parse_rss(resp.content, default_source)
            if items:
                cache["payload"] = items
                cache["ts"] = _time.time()
                return {"status": "ok", "items": items}
        except Exception as ex:
            print(f"[news] error: {ex}", flush=True)
    if cache.get("payload"):
        return {"status": "ok", "items": cache["payload"]}
    return {"status": "error", "message": "all news sources unavailable"}

@app.route("/api/news")
def get_news():
    league = request.args.get("league", "nba").lower()
    if league == "wnba":
        result = _fetch_news(_WNBA_NEWS_SOURCES, _wnba_news_cache)
    else:
        result = _fetch_news(_NEWS_SOURCES, _news_cache)
    status = 200 if result.get("status") == "ok" else 200
    return jsonify(result), status


# ── Injury helpers ───────────────────────────────────────────────
def _norm_name(name: str) -> str:
    n = name.lower().strip()
    for suffix in (" jr.", " sr.", " ii", " iii", " iv", " v"):
        n = n.replace(suffix, "")
    return n.strip()


def _fetch_injury_report(league: str = "nba") -> dict:
    """Fetch player injury statuses from ESPN for a league. Returns
    {norm_name: {"status": lower, "statusDisplay": original, "reason": short
    comment}}. Cached 30 minutes per league; returns stale data on error."""
    league = "wnba" if league == "wnba" else "nba"
    cache = _injury_cache[league]
    now = _time.time()
    if now - cache["ts"] < 1800:
        return cache["data"]
    try:
        url  = f"https://site.api.espn.com/apis/site/v2/sports/basketball/{league}/injuries"
        resp = _requests.get(url, timeout=8)
        resp.raise_for_status()
        injured = {}
        for team_entry in resp.json().get("injuries", []):
            for inj in team_entry.get("injuries", []):
                name       = inj.get("athlete", {}).get("displayName", "").strip()
                status_raw = (inj.get("status") or "").strip()
                if name:
                    injured[_norm_name(name)] = {
                        "status": status_raw.lower(),
                        "statusDisplay": status_raw,
                        "reason": inj.get("shortComment", "") or "",
                    }
        cache["data"] = injured
        cache["ts"]   = now
        return injured
    except Exception:
        return cache["data"]


def _is_out(player_name: str, injury_report: dict) -> bool:
    """True if player is Out / Doubtful / Injured Reserve / Suspension."""
    if not injury_report or not player_name:
        return False
    status = (injury_report.get(_norm_name(player_name)) or {}).get("status", "")
    return status in ("out", "doubtful", "injured reserve", "out for season",
                      "suspension", "not with team", "inactive")


def _box_star(team_data: dict):
    """Top P+R+A player ID from a CDN boxscore team dict (must have played ≥1 min)."""
    best_id, best_total = None, -1
    for p in team_data.get("players", []):
        s = p.get("statistics", {})
        min_str = s.get("minutes", "PT0M0.00S") or "PT0M0.00S"
        try:
            mins = float(min_str.replace("PT", "").replace("S", "").split("M")[0])
        except Exception:
            mins = 0
        if mins < 1:
            continue
        total = (int(s.get("points", 0) or 0)
                 + int(s.get("reboundsTotal", 0) or 0)
                 + int(s.get("assists", 0) or 0))
        if total > best_total:
            best_total = total
            best_id    = p.get("personId")
    return best_id


# ── /api/game-posters ────────────────────────────────────────────
_game_posters_cache: dict = {}  # gameId -> {"away": int|None, "home": int|None}

@app.route("/api/game-posters", methods=["POST"])
def get_game_posters():
    """
    Returns best-fit player headshot IDs for each team per game.

    Final games   → actual P+R+A leader from CDN boxscore.
    Upcoming/live → highest season P+R+A among non-injured players.

    Body:    {"games": [{"gameId":"...","away":"LAL","home":"BOS","status":3}, ...]}
    Returns: {"posters": {"<gameId>": {"away": <playerId>, "home": <playerId>}}}
    """
    body  = request.get_json(force=True, silent=True) or {}
    games = body.get("games", [])
    if not games:
        return jsonify({"posters": {}})

    posters: dict = {}

    # ── Final games: CDN boxscore actual leaders (cached forever — result never changes) ──
    final_games    = [g for g in games if int(g.get("status", 1) or 1) == 3]
    nonfinal_games = [g for g in games if int(g.get("status", 1) or 1) != 3]

    if final_games:
        uncached = [g for g in final_games if g.get("gameId") not in _game_posters_cache]
        if uncached:
            boxscores = _fetch_boxscores_parallel([g["gameId"] for g in uncached])
            for g in uncached:
                gid = g.get("gameId", "")
                box = boxscores.get(gid)
                if box:
                    _game_posters_cache[gid] = {
                        "away": _box_star(box.get("awayTeam", {})),
                        "home": _box_star(box.get("homeTeam", {})),
                    }
        for g in final_games:
            gid = g.get("gameId", "")
            if gid in _game_posters_cache:
                posters[gid] = _game_posters_cache[gid]
            else:
                nonfinal_games.append(g)  # CDN miss → fall back to season stats

    # ── Upcoming / live: season stats + ESPN injury filter ────────
    if nonfinal_games:
        now    = _dt.utcnow()
        season = (f"{now.year}-{str(now.year + 1)[2:]}"
                  if now.month >= 10
                  else f"{now.year - 1}-{str(now.year)[2:]}")

        teams_needed = {(g.get("away") or "").upper() for g in nonfinal_games} | \
                       {(g.get("home") or "").upper() for g in nonfinal_games}
        teams_needed.discard("")

        team_candidates: dict[str, list] = {}
        try:
            conn = get_conn()
            cur  = conn.cursor()
            for abbr in teams_needed:
                cur.execute("""
                    SELECT ps.player_id, p.player_name,
                           COALESCE(ps.pts,0)+COALESCE(ps.ast,0)+COALESCE(ps.reb,0) AS total
                    FROM player_seasons ps
                    JOIN players p ON p.player_id = ps.player_id
                    WHERE ps.team_abbr = %s
                      AND ps.season = %s
                      AND ps.season_type = 'Regular Season'
                      AND COALESCE(ps.gp,0) >= 5
                    ORDER BY total DESC
                    LIMIT 5
                """, (abbr, season))
                team_candidates[abbr] = cur.fetchall()
            cur.close(); conn.close()
        except Exception as e:
            return jsonify({"error": str(e), "posters": posters}), 500

        injury_report = _fetch_injury_report()

        def best_season_id(abbr):
            for row in team_candidates.get(abbr, []):
                if not _is_out(row["player_name"], injury_report):
                    return row["player_id"]
            rows = team_candidates.get(abbr, [])
            return rows[0]["player_id"] if rows else None

        for g in nonfinal_games:
            gid = g.get("gameId", "")
            if not gid:
                continue
            posters[gid] = {
                "away": best_season_id((g.get("away") or "").upper()),
                "home": best_season_id((g.get("home") or "").upper()),
            }

    return jsonify({"posters": posters})


# ── /api/top-performers?date=YYYY-MM-DD ──────────────────────────
@app.route("/api/top-performers")
def get_top_performers():
    """
    Returns top 5 players by PTS+REB+AST for a given date.
    Uses the same CDN boxscores as the scoreboard — no extra API calls
    if the scoreboard was already fetched (browser hits this separately).
    No ?date = today via live CDN scoreboard.
    ?date = historical via ScoreboardV2 game IDs + CDN boxscores.
    """
    date = request.args.get("date", "").strip()
    _game_today = _compute_game_today()

    # Resolve actual date string for labeling
    if not date or date == _game_today:
        # No date, or explicit today — NBA live CDN (fast, works on cloud IPs).
        # Authoritative even with zero games: the offseason correctly returns
        # an empty slate. Do NOT fall back to ScoreboardV3 here — stats.nba.com
        # is BLOCKED on Railway's datacenter IP and hangs the full timeout on
        # every single request. See docs/cdn-akamai-bot-manager.md.
        try:
            url  = "https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json"
            resp = _cdn_get(url, headers=_CDN_HEADERS, timeout=8)
            resp.raise_for_status()
            sb_data    = resp.json()
            raw_games  = sb_data.get("scoreboard", {}).get("games", [])
            actual_date = sb_data.get("scoreboard", {}).get("gameDate", "") or _game_today
        except Exception as e:
            return jsonify({"error": str(e), "players": [], "date": date or _game_today}), 200
    elif date < _game_today:
        actual_date = date
        raw_games = []
        # Past date: the DB is authoritative (the daily pipeline ingests every
        # final game). If it has none, there were none — do NOT fall back to
        # ScoreboardV3, which hits stats.nba.com and is BLOCKED on Railway's
        # datacenter IP, hanging the full 30s timeout and stalling the scores
        # page. See docs/cdn-akamai-bot-manager.md.
        try:
            conn = get_conn(); cur = conn.cursor()
            cur.execute("SELECT game_id FROM games WHERE game_date = %s AND status = 'Final'", (date,))
            raw_games = [{"gameId": r["game_id"]} for r in cur.fetchall()]
            cur.close(); conn.close()
        except Exception:
            pass
    else:
        actual_date = date
        raw_games = []
        # Future date explicitly requested — best-effort ScoreboardV3, but with
        # a short timeout so a blocked cloud IP fails fast instead of pinning
        # a worker thread for 30s.
        try:
            from nba_api.stats.endpoints import scoreboardv3
            dt = _dt.strptime(date, "%Y-%m-%d")
            board = scoreboardv3.ScoreboardV3(
                game_date=dt.strftime("%Y-%m-%d"),
                league_id="00",
                timeout=8,
            )
            gh_df = board.game_header.get_data_frame()
            raw_games = [{"gameId": str(r.get("gameId", "") or r.get("GAME_ID", ""))}
                         for _, r in gh_df.iterrows()
                         if r.get("gameId") or r.get("GAME_ID")]
        except Exception as e:
            return jsonify({"error": str(e), "players": [], "date": date}), 200

    def get_gid(g):
        return g.get("gameId") or g.get("GAME_ID") or ""

    game_ids = [get_gid(g) for g in raw_games if get_gid(g)]
    if not game_ids:
        return jsonify({"players": [], "date": actual_date})

    # Fetch all boxscores in parallel, then collect player lines
    boxscores = _fetch_boxscores_parallel(game_ids)
    all_players = []
    game_stars  = {}  # gameId → {away: playerId, home: playerId} — top scorer per team

    def _top_scorer_id(team_data):
        best_id, best_pts = None, -1
        for p in team_data.get("players", []):
            s = p.get("statistics", {})
            min_str = s.get("minutes", "PT0M0.00S") or "PT0M0.00S"
            try:
                mins = float(min_str.replace("PT","").replace("S","").split("M")[0])
            except Exception:
                mins = 0
            if mins < 1:
                continue
            pts = int(s.get("points", 0) or 0)
            if pts > best_pts:
                best_pts = pts
                best_id  = p.get("personId")
        return best_id

    for gid, box in boxscores.items():
        if not box:
            continue
        away = box.get("awayTeam", {})
        home = box.get("homeTeam", {})
        away_abbr = away.get("teamTricode", "")
        home_abbr = home.get("teamTricode", "")
        matchup   = f"{away_abbr} @ {home_abbr}"

        game_status = box.get("gameStatus", 1)
        is_live     = game_status == 2

        game_stars[gid] = {
            "away": _top_scorer_id(away),
            "home": _top_scorer_id(home),
        }

        for team, abbr in [(away, away_abbr), (home, home_abbr)]:
            for p in team.get("players", []):
                s       = p.get("statistics", {})
                min_str = s.get("minutes", "PT0M0.00S")
                try:
                    mins = float(min_str.replace("PT","").replace("S","").split("M")[0]) if min_str else 0
                except Exception:
                    mins = 0
                if mins < 1:
                    continue

                pts = int(s.get("points", 0) or 0)
                reb = int(s.get("reboundsTotal", 0) or 0)
                ast = int(s.get("assists", 0) or 0)
                all_players.append({
                    "player_id": p.get("personId"),
                    "name":      p.get("name", ""),
                    "team":      abbr,
                    "matchup":   matchup,
                    "game_id":   gid,
                    "is_live":   is_live,
                    "pts":       pts,
                    "reb":       reb,
                    "ast":       ast,
                    "total":     pts + reb + ast,
                })

    # Sort by total desc, take top 5
    all_players.sort(key=lambda x: x["total"], reverse=True)
    top5 = all_players[:5]

    # Attach the crowd's avg performance rating (0–5) to each, if any exist.
    try:
        pconn = get_conn(); pcur = pconn.cursor()
        for pl in top5:
            pcur.execute("""
                SELECT AVG(rating::float) AS a, COUNT(*) AS n
                FROM performance_reviews
                WHERE game_id = %s AND person_id = %s
            """, (str(pl["game_id"]), pl["player_id"]))
            r = pcur.fetchone()
            n = int(r["n"]) if r and r["n"] else 0
            pl["fan_stars"] = round(r["a"] / 2, 1) if n > 0 else None
        pcur.close()
    except Exception:
        for pl in top5:
            pl.setdefault("fan_stars", None)

    return jsonify({"players": top5, "date": actual_date, "game_stars": game_stars})


# ── /api/preview/records/<away>/<home> ───────────────────────────
@app.route("/api/preview/records/<away>/<home>")
def preview_records(away, home):
    """Returns regular-season W/L records for both teams from the games table."""
    away   = away.upper()
    home   = home.upper()
    league = request.args.get("league", "nba").lower()
    season = _get_wnba_season() if league == "wnba" else get_current_season()
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT
                team_abbr,
                COUNT(*) FILTER (WHERE won) AS wins,
                COUNT(*) FILTER (WHERE NOT won) AS losses
            FROM (
                SELECT
                    home_team_abbr AS team_abbr,
                    home_score > away_score AS won
                FROM games
                WHERE season_type = 'Regular Season'
                  AND status = 'Final'
                  AND season = %s
                  AND league = %s
                  AND home_team_abbr = ANY(%s)
                UNION ALL
                SELECT
                    away_team_abbr AS team_abbr,
                    away_score > home_score AS won
                FROM games
                WHERE season_type = 'Regular Season'
                  AND status = 'Final'
                  AND season = %s
                  AND league = %s
                  AND away_team_abbr = ANY(%s)
            ) t
            GROUP BY team_abbr
        """, (season, league, [away, home], season, league, [away, home]))
        rows = {r["team_abbr"]: r for r in cur.fetchall()}
        conn.close()

        def rec(abbr):
            r = rows.get(abbr)
            return {"wins": r["wins"], "losses": r["losses"]} if r else {"wins": None, "losses": None}

        return jsonify({"away": rec(away), "home": rec(home)})
    except Exception as e:
        return jsonify({"away": {"wins": None, "losses": None},
                        "home": {"wins": None, "losses": None},
                        "error": str(e)}), 200


# ── /api/preview/team-stats/<abbr> ───────────────────────────────
@app.route("/api/preview/team-stats/<abbr>")
def preview_team_stats(abbr):
    abbr = abbr.upper()
    try:
        conn = get_conn()   # ← was get_db()
        cur  = conn.cursor()
        cur.execute("""
            SELECT
              SUM(pts * gp)  AS tot_pts,
              SUM(reb * gp)  AS tot_reb,
              SUM(ast * gp)  AS tot_ast,
              SUM(tov * gp)  AS tot_tov,
              SUM(fgm * gp)  AS tot_fgm,
              SUM(fga * gp)  AS tot_fga,
              SUM(fg3m * gp) AS tot_fg3m,
              SUM(fg3a * gp) AS tot_fg3a,
              MAX(gp)        AS max_gp
            FROM player_seasons ps
            JOIN players p ON ps.player_id = p.player_id
            WHERE ps.team_abbr = %s          -- ← was p.team_abbreviation
              AND ps.season = %s
              AND ps.season_type = %s
              AND ps.gp >= 5
        """, (abbr, get_current_season(), "Regular Season"))
        row = cur.fetchone()
        cur.close()
        conn.close()   # ← also close the connection

        if not row or not row["max_gp"]:
            return jsonify({"error": "no data", "abbr": abbr})

        max_gp = float(row["max_gp"])
        def safe_div(a, b): return round(a / b, 4) if b else None

        return jsonify({
            "abbr":    abbr,
            "ppg":     round(row["tot_pts"]  / max_gp, 1) if row["tot_pts"]  else None,
            "rpg":     round(row["tot_reb"]  / max_gp, 1) if row["tot_reb"]  else None,
            "apg":     round(row["tot_ast"]  / max_gp, 1) if row["tot_ast"]  else None,
            "topg":    round(row["tot_tov"]  / max_gp, 1) if row["tot_tov"]  else None,
            "fg_pct":  safe_div(row["tot_fgm"], row["tot_fga"]),
            "fg3_pct": safe_div(row["tot_fg3m"], row["tot_fg3a"]),
        })
    except Exception as e:
        return jsonify({"error": str(e), "abbr": abbr}), 200

# ── /api/preview/h2h/<away>/<home> ───────────────────────────────
@app.route("/api/preview/h2h/<away>/<home>")
def preview_h2h(away, home):
    """
    Returns last 5 head-to-head games between two teams from the local DB.
    """
    away   = away.upper()
    home   = home.upper()
    league = request.args.get("league", "nba").lower()

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT game_id, game_date, home_team_abbr, away_team_abbr,
                   home_score, away_score
            FROM games
            WHERE status = 'Final'
              AND league = %s
              AND (
                    (home_team_abbr = %s AND away_team_abbr = %s)
                 OR (home_team_abbr = %s AND away_team_abbr = %s)
              )
            ORDER BY game_date DESC
            LIMIT 5
        """, (league, home, away, away, home))
        rows = cur.fetchall()
        conn.close()

        games_out = []
        for row in rows:
            game_date = row["game_date"].strftime("%Y-%m-%d") if hasattr(row["game_date"], "strftime") else str(row["game_date"])
            games_out.append({
                "game_id":   row["game_id"],
                "date":      game_date,
                "away_abbr": row["away_team_abbr"],
                "home_abbr": row["home_team_abbr"],
                "away_pts":  row["away_score"],
                "home_pts":  row["home_score"],
            })

        return jsonify({"games": games_out, "away": away, "home": home})

    except Exception as e:
        return jsonify({"games": [], "error": str(e)}), 200


# ── /api/preview/injuries/<away>/<home> ──────────────────────────
@app.route("/api/preview/injuries/<away>/<home>")
def preview_injuries(away, home):
    """
    Injury report for both teams ahead of tipoff — every current-roster
    player (gp > 0) with an active ESPN injury listing (Out, Doubtful,
    Questionable, Day-To-Day, etc.), with the status + short reason.
    """
    away   = away.upper()
    home   = home.upper()
    league = request.args.get("league", "nba").lower()
    season = _get_wnba_season() if league == "wnba" else get_current_season()

    conn = get_conn(); cur = conn.cursor()
    try:
        injury = _fetch_injury_report(league)
        out = {"away": [], "home": []}
        for side, abbr in (("away", away), ("home", home)):
            for p in _roster_with_avg_minutes(cur, abbr, league, season):
                info = injury.get(_norm_name(p["playerName"]))
                if not info or not info.get("statusDisplay"):
                    continue
                out[side].append({
                    "playerId": p["playerId"],
                    "playerName": p["playerName"],
                    "status": info["statusDisplay"],
                    "reason": info.get("reason", ""),
                })
        return jsonify(out)
    except Exception as e:
        return jsonify({"away": [], "home": [], "error": str(e)}), 200
    finally:
        cur.close(); conn.close()


# ── Serve preview.html ────────────────────────────────────────────
@app.route("/preview")
@app.route("/preview.html")
def preview_page():
    return app.send_static_file("preview.html")


# Boxscore data never changes once a game is Final — cache indefinitely so
# repeated internal callers (e.g. /api/players/today) don't re-hit the CDN.
_final_boxscore_cache: dict = {}  # game_id -> dict


def _fetch_live_boxscore_data(game_id: str) -> dict | None:
    """Core boxscore fetch (NBA or WNBA): CDN first, nba_api fallback for
    historical NBA games. Returns the normalized game dict, or None if
    unavailable. Auto-upserts completed games. Cached indefinitely once Final."""
    if game_id in _final_boxscore_cache:
        return _final_boxscore_cache[game_id]

    is_wnba = str(game_id).startswith("10")
    # Try CDN first (works for current season)
    try:
        if is_wnba:
            url = f"https://cdn.wnba.com/static/json/liveData/boxscore/boxscore_{game_id}.json"
        else:
            url = f"https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json"
        hdrs = _WNBA_CDN_HEADERS if is_wnba else _CDN_HEADERS
        resp = _cdn_get(url, headers=hdrs, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        game = data.get("game", data)
        if game.get("gameStatus") == 3:
            _upsert_game_from_boxscore(game_id, game, league="wnba" if is_wnba else "nba")
            _final_boxscore_cache[game_id] = game
        return game
    except Exception:
        pass

    # CDN failed — fall back to nba_api for historical NBA games only
    if is_wnba:
        return None

    try:
        from nba_api.stats.endpoints import boxscoretraditionalv3
        box = boxscoretraditionalv3.BoxScoreTraditionalV3(game_id=game_id, timeout=30)
        raw = box.get_dict()
        # Normalise to the same shape the frontend expects
        bd = raw.get("boxScoreTraditional", {})
        home_team = bd.get("homeTeam", {})
        away_team = bd.get("awayTeam", {})

        def norm_player(p):
            s = p.get("statistics", {})
            return {
                "personId": p.get("personId"),
                "name": f"{p.get('firstName','')} {p.get('familyName','')}".strip(),
                "nameI": p.get("nameI", ""),
                "jerseyNum": p.get("jerseyNum", ""),
                "position": p.get("position", ""),
                "starter": p.get("starter", "0"),
                "played": p.get("played", "1"),
                "statistics": s,
            }

        def norm_team(t):
            players_raw = t.get("players", [])
            score = t.get("score", 0) or 0
            if not score:
                score = sum(int(p.get("statistics", {}).get("points", 0) or 0) for p in players_raw)
            return {
                "teamId": t.get("teamId"),
                "teamCity": t.get("teamCity", ""),
                "teamName": t.get("teamName", ""),
                "teamTricode": t.get("teamTricode", ""),
                "score": score,
                "players": [norm_player(p) for p in players_raw],
            }

        game_meta = bd.get("game", {})
        result = {
            "gameId": game_id,
            "gameStatus": 3,
            "gameStatusText": "Final",
            "homeTeam": norm_team(home_team),
            "awayTeam": norm_team(away_team),
            "gameTimeUTC": game_meta.get("gameTimeUTC", ""),
            "period": game_meta.get("period", 4),
            "gameClock": "",
        }
        _final_boxscore_cache[game_id] = result
        return result
    except Exception:
        return None


# ── /api/live/boxscore/<game_id> ──────────────────────────────────
@app.route("/api/live/boxscore/<game_id>")
def get_live_boxscore(game_id):
    """Proxy CDN live boxscore (NBA or WNBA) + auto-upsert completed games.
    Falls back to nba_api BoxScoreTraditionalV3 for historical NBA games."""
    game = _fetch_live_boxscore_data(game_id)
    if game is None:
        return jsonify({"error": "boxscore unavailable"}), 404
    return jsonify(game)


def _roster_with_avg_minutes(cur, abbr: str, league: str, season: str) -> list:
    """Current-season roster (gp > 0) + season avg minutes/points for one team.
    WNBA abbr aliasing (team_seasons vs wnba_player_seasons drift) handled here."""
    if league == "wnba":
        alt = _WNBA_GAMES_TO_STANDINGS.get(abbr)
        variants = [abbr] + ([alt] if alt else [])
        cur.execute("""
            SELECT player_id, player_name, min AS avg_min, pts AS avg_pts
            FROM wnba_player_seasons
            WHERE team = ANY(%s) AND season = %s AND season_type = 'Regular Season'
              AND COALESCE(gp, 0) > 0
        """, (variants, season))
    else:
        cur.execute("""
            SELECT ps.player_id, p.player_name, ps.min_per_game AS avg_min, ps.pts AS avg_pts
            FROM player_seasons ps
            JOIN players p ON p.player_id = ps.player_id
            WHERE ps.team_abbr = %s AND ps.season = %s AND ps.season_type = 'Regular Season'
              AND COALESCE(ps.gp, 0) > 0
        """, (abbr, season))
    return [{"playerId": r["player_id"], "playerName": r["player_name"],
             "avgMinutes": r["avg_min"], "avgPts": r["avg_pts"]} for r in cur.fetchall()]


@app.route("/api/players/today", methods=["POST"])
def players_today():
    """
    "Today's Players" rail: every active player across the day's games,
    sorted followed-first then by tier (Final > Live > Scheduled). Final uses
    real minutes/points (one-time boxscore fetch, cached forever); Live and
    Scheduled use season avg minutes/points (no live polling needed — cheap
    and stable while a game is in progress). Scheduled players confirmed OUT
    (ESPN injury report) are excluded entirely; Live/Final are unaffected
    since they're built from actual boxscore/roster participants.

    Body: {"date": "YYYY-MM-DD", "games": [{"gameId", "status": "scheduled"|
    "live"|"final", "gameTimeUTC", "homeAbbr", "awayAbbr"}, ...]}
    The caller (already polling the scoreboard) supplies the games list so
    this endpoint never has to re-derive game status itself.
    """
    body     = request.get_json(silent=True) or {}
    date     = (body.get("date") or "").strip()
    games_in = body.get("games") or []
    if not games_in:
        return jsonify({"players": [], "date": date})

    user    = current_user()
    user_id = user["id"] if user else None

    conn = get_conn(); cur = conn.cursor()
    try:
        followed_ids = set()
        if user_id:
            cur.execute("SELECT person_id FROM player_follows WHERE user_id = %s", (user_id,))
            followed_ids = {r["person_id"] for r in cur.fetchall()}

        final_game_ids = [g["gameId"] for g in games_in if g.get("status") == "final" and g.get("gameId")]
        my_ratings = {}
        if user_id and final_game_ids:
            cur.execute("""
                SELECT game_id, person_id, rating FROM performance_reviews
                WHERE user_id = %s AND game_id = ANY(%s)
            """, (user_id, final_game_ids))
            for r in cur.fetchall():
                my_ratings[(r["game_id"], r["person_id"])] = r["rating"]

        nba_season, wnba_season = get_current_season(), _get_wnba_season()
        injury_by_league: dict = {}
        rows = []

        for g in games_in:
            game_id = g.get("gameId", "")
            status  = g.get("status", "scheduled")
            if not game_id or status not in ("scheduled", "live", "final"):
                continue
            league    = "wnba" if str(game_id).startswith("10") else "nba"
            home_abbr = (g.get("homeAbbr") or "").upper()
            away_abbr = (g.get("awayAbbr") or "").upper()
            tipoff    = g.get("gameTimeUTC", "")

            if status == "final":
                box = _fetch_live_boxscore_data(game_id)
                if not box:
                    continue
                for side in ("homeTeam", "awayTeam"):
                    team = box.get(side, {})
                    abbr = team.get("teamTricode", "")
                    if league == "wnba":
                        abbr = _wnba_cdn_abbr(abbr)
                    for p in team.get("players", []):
                        pid = p.get("personId")
                        if not pid:
                            continue
                        min_str = (p.get("statistics", {}) or {}).get("minutes", "PT0M0.00S") or "PT0M0.00S"
                        try:
                            mins = float(min_str.replace("PT", "").replace("S", "").split("M")[0])
                        except Exception:
                            mins = 0.0
                        if mins <= 0:
                            continue  # DNP — nothing to rate
                        pts = (p.get("statistics", {}) or {}).get("points", 0) or 0
                        rows.append({
                            "playerId": pid, "playerName": p.get("name", ""),
                            "teamAbbr": abbr, "league": league,
                            "gameId": game_id, "gameStatus": "final", "gameTimeUTC": tipoff,
                            "avgMinutes": None, "finalMinutes": round(mins, 1),
                            "avgPts": None, "finalPts": int(pts),
                            "isFollowed": pid in followed_ids,
                            "myRating": my_ratings.get((game_id, pid)),
                        })
            else:
                if league not in injury_by_league:
                    injury_by_league[league] = _fetch_injury_report(league)
                injury = injury_by_league[league]
                season = wnba_season if league == "wnba" else nba_season
                for abbr in (home_abbr, away_abbr):
                    if not abbr:
                        continue
                    for p in _roster_with_avg_minutes(cur, abbr, league, season):
                        if status == "scheduled" and _is_out(p["playerName"], injury):
                            continue
                        rows.append({
                            "playerId": p["playerId"], "playerName": p["playerName"],
                            "teamAbbr": abbr, "league": league,
                            "gameId": game_id, "gameStatus": status, "gameTimeUTC": tipoff,
                            "avgMinutes": p["avgMinutes"], "finalMinutes": None,
                            "avgPts": p["avgPts"], "finalPts": None,
                            "isFollowed": p["playerId"] in followed_ids,
                            "myRating": None,
                        })

        tier_rank = {"final": 0, "live": 1, "scheduled": 2}

        def _sort_key(r):
            followed = 0 if r["isFollowed"] else 1
            tier = tier_rank.get(r["gameStatus"], 2)
            if tier == 0:
                sub, sub2 = -(r["finalMinutes"] or 0.0), 0.0
            elif tier == 1:
                sub, sub2 = -(r["avgMinutes"] or 0.0), 0.0
            else:
                # Avg minutes first — a tipoff-time-primary sort surfaced
                # low-usage players just because their game starts earlier.
                sub, sub2 = -(r["avgMinutes"] or 0.0), (r["gameTimeUTC"] or "9999")
            return (followed, tier, sub, sub2)

        rows.sort(key=_sort_key)
        return jsonify({"players": rows, "date": date})
    finally:
        cur.close(); conn.close()


def _season_type_from_game_id(game_id: str) -> str:
    """
    Derive season type from the NBA game ID.
    Format: 00TYYYYYY where T is a single digit at position [2]:
      1 = Pre-Season, 2 = Regular Season, 4 = Playoffs, 5 = Play-In
    e.g. 0022400001 → Regular Season, 0042400001 → Playoffs
    """
    prefix = game_id[2] if len(game_id) >= 3 else ""
    return {
        "1": "Pre Season",
        "2": "Regular Season",
        "4": "Playoffs",
        "5": "PlayIn",
    }.get(prefix, os.getenv("NBA_SEASON_TYPE", "Regular Season"))


def _season_from_game_id(game_id: str) -> str:
    """
    Extract season string from NBA game ID.
    Format: 00TYYXXXX where YY at positions [3:5] is the 2-digit season start year.
    e.g. 0022400001 → '24' → 2024 → '2024-25'
    """
    try:
        yr = int(game_id[3:5])
        year = 2000 + yr
        return f"{year}-{str(year + 1)[-2:]}"
    except Exception:
        return os.getenv("NBA_SEASON", "2025-26")


def _upsert_game_from_boxscore(game_id: str, game: dict, league: str = "nba"):
    """
    Upsert a completed game into the games table from CDN boxscore data.
    Silently swallows errors so it never breaks the main response.
    """
    try:
        away = game.get("awayTeam", {})
        home = game.get("homeTeam", {})
        # Never save ghost games (0-0 means the game was never played)
        if int(away.get("score", 0) or 0) == 0 and int(home.get("score", 0) or 0) == 0:
            return
        _raw_away = away.get("teamTricode", "")
        _raw_home = home.get("teamTricode", "")
        # Map raw WNBA CDN tricodes (e.g. PDX→POR, LVA→LV) to app abbreviations
        _wnba_map = {"LVA": "LV", "LAS": "LA", "NYL": "NY", "GSV": "GS",
                     "WAS": "WSH", "PDX": "POR"}
        if league == "wnba":
            away_abbr = _wnba_map.get(_raw_away, _raw_away)
            home_abbr = _wnba_map.get(_raw_home, _raw_home)
        else:
            away_abbr = _raw_away
            home_abbr = _raw_home
        away_score = int(away.get("score", 0) or 0)
        home_score = int(home.get("score", 0) or 0)

        # Derive season type from game ID prefix (002=Regular, 004=Playoffs, 005=PlayIn)
        season_type = _season_type_from_game_id(game_id)

        # Parse game date from gameTimeUTC
 
        # Parse game date from gameTimeUTC, converted to ET.
        # NBA game dates are defined in ET — a 10 PM PT tip-off is still "that day"
        # in ET, but its UTC timestamp flips to the next calendar day, so we must
        # localise to ET before extracting the date.
        game_time_utc = game.get("gameTimeUTC", "")
        if game_time_utc:
            from datetime import datetime as _dt2
            try:
                from zoneinfo import ZoneInfo as _ZI
            except ImportError:
                from backports.zoneinfo import ZoneInfo as _ZI
            utc_dt    = _dt2.fromisoformat(game_time_utc.replace("Z", "+00:00"))
            game_date = utc_dt.astimezone(_ZI("America/New_York")).date()
        else:
            from datetime import date as _date2
            game_date = _date2.today()

        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            INSERT INTO games (
                game_id, season, season_type, game_date,
                home_team_abbr, away_team_abbr,
                home_score, away_score, status, league
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'Final', %s)
            ON CONFLICT (game_id) DO UPDATE SET
                home_score     = EXCLUDED.home_score,
                away_score     = EXCLUDED.away_score,
                season         = EXCLUDED.season,
                season_type    = EXCLUDED.season_type,
                league         = EXCLUDED.league,
                status         = 'Final',
                updated_at     = NOW()
            WHERE games.status != 'Final'
               OR games.home_score IS NULL
               OR games.season_type != EXCLUDED.season_type
               OR games.league != EXCLUDED.league
               OR games.season != EXCLUDED.season
        """, (
            game_id,
            _get_wnba_season() if league == "wnba" else _season_from_game_id(game_id),
            season_type,
            game_date,
            home_abbr, away_abbr,
            home_score, away_score,
            league,
        ))
        conn.commit()
        cur.close()
        conn.close()
        # Resolve any pending predictions now that the game is final
        threading.Thread(
            target=_resolve_game_predictions,
            args=(game_id, home_abbr, away_abbr, home_score, away_score),
            daemon=True,
        ).start()
    except Exception:
        pass  # Never break the main response

# ── /api/live/pbp/<game_id> ───────────────────────────────────────
@app.route("/api/live/pbp/<game_id>")
def get_live_pbp(game_id):
    """Proxy CDN live play-by-play (NBA or WNBA).
    Falls back to nba_api PlayByPlayV3 for historical NBA games."""
    is_wnba = str(game_id).startswith("10")
    # Try CDN first
    try:
        if is_wnba:
            url = f"https://cdn.wnba.com/static/json/liveData/playbyplay/playbyplay_{game_id}.json"
        else:
            url = f"https://cdn.nba.com/static/json/liveData/playbyplay/playbyplay_{game_id}.json"
        hdrs = _WNBA_CDN_HEADERS if is_wnba else _CDN_HEADERS
        resp = _cdn_get(url, headers=hdrs, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        return jsonify(data.get("game", data))
    except Exception:
        pass

    # Fall back to nba_api for historical NBA games only
    if not is_wnba:
        try:
            from nba_api.stats.endpoints import playbyplayv3
            pbp = playbyplayv3.PlayByPlayV3(game_id=game_id, timeout=30)
            raw = pbp.get_dict()
            actions = raw.get("game", {}).get("actions", [])
            return jsonify({"gameId": game_id, "actions": actions})
        except Exception as e:
            return jsonify({"error": str(e)}), 404

    return jsonify({"error": "play-by-play unavailable"}), 404


# ── Serve game.html ───────────────────────────────────────────────
@app.route("/game")
def game_page():
    return app.send_static_file("game.html")

# ── Serve team.html ───────────────────────────────────────────────
@app.route("/team")
def team_page():
    return app.send_static_file("team.html")

# ── Serve builder.html ────────────────────────────────────────────
@app.route("/builder.html")
@app.route("/builder")
def builder_page():
    return app.send_static_file("builder.html")

@app.route("/stats")
@app.route("/stats-hub")
@app.route("/stats-hub.html")
def stats_hub():
    return app.send_static_file("stats-hub.html")

@app.route("/leaderboard")
@app.route("/stats.html")
def stats_page():
    return app.send_static_file("stats.html")

_PBPSTATS_TEAM_IDS = {
    "ATL":1610612737,"BOS":1610612738,"BKN":1610612751,"CHA":1610612766,
    "CHI":1610612741,"CLE":1610612739,"DAL":1610612742,"DEN":1610612743,
    "DET":1610612765,"GSW":1610612744,"HOU":1610612745,"IND":1610612754,
    "LAC":1610612746,"LAL":1610612747,"MEM":1610612763,"MIA":1610612748,
    "MIL":1610612749,"MIN":1610612750,"NOP":1610612740,"NYK":1610612752,
    "OKC":1610612760,"ORL":1610612753,"PHI":1610612755,"PHX":1610612756,
    "POR":1610612757,"SAC":1610612758,"SAS":1610612759,"TOR":1610612761,
    "UTA":1610612762,"WAS":1610612764,
}

# Cache pbpstats lineup responses: key=(team_abbr, season, leverage) → (fetched_at, lineups)
# Past seasons never change so they're kept indefinitely; current season TTL is 1 hour.
import time as _time
_pbp_cache: dict = {}
_PBP_CURRENT_TTL = 3600  # 1 hour for live season

_ALL_LEV = {"Low", "Medium", "High", "VeryHigh"}

def _fetch_pbp_lineups(team_abbr, season, leverage):
    """Return parsed lineup list from pbpstats, using in-memory cache.

    leverage: comma-separated string of leverage types to include,
              e.g. "Medium,High,VeryHigh". Pass all four (or empty) for no filter.
    """
    # Normalise to a frozenset for a stable cache key
    lev_set = frozenset(v.strip() for v in leverage.split(",") if v.strip()) if leverage else _ALL_LEV
    cache_key = (team_abbr, season, lev_set)
    current_season = get_current_season()
    now = _time.monotonic()

    if cache_key in _pbp_cache:
        fetched_at, cached = _pbp_cache[cache_key]
        if season != current_season or (now - fetched_at) < _PBP_CURRENT_TTL:
            return cached

    team_id = _PBPSTATS_TEAM_IDS[team_abbr]
    params = {
        "TeamId":     team_id,
        "Season":     season,
        "SeasonType": "Regular Season",
        "Type":       "Team",
    }

    # Build URL manually so commas in Leverage are NOT percent-encoded —
    # pbpstats expects literal commas and rejects %2C.
    import urllib.parse as _urlparse
    base_url = "https://api.pbpstats.com/get-wowy-stats/nba?" + _urlparse.urlencode(params)
    if lev_set and lev_set != _ALL_LEV:
        base_url += "&Leverage=" + ",".join(lev_set)

    print(f"[pbpstats] GET {base_url}")
    try:
        resp = _requests.get(base_url, timeout=25)
        resp.raise_for_status()
    except _requests.exceptions.Timeout:
        print(f"[pbpstats] TIMEOUT after 50s")
        raise
    except Exception as _e:
        print(f"[pbpstats] ERROR {type(_e).__name__}: {_e}")
        raise

    lineups = []
    for row in resp.json().get("multi_row_table_data", []):
        if not row or not row.get("EntityId") or not row.get("Minutes"):
            continue
        pids     = [p for p in row["EntityId"].split("-") if p.strip()]
        names    = [n.strip() for n in row.get("Name", "").split(",")]
        off_poss = row.get("OffPoss") or 0
        def_poss = row.get("DefPoss") or 0
        points   = row.get("Points") or 0
        opp_pts  = row.get("OpponentPoints") or 0
        ortg = round(points  / off_poss * 100, 1) if off_poss else None
        drtg = round(opp_pts / def_poss * 100, 1) if def_poss else None
        net  = round(ortg - drtg, 1) if ortg is not None and drtg is not None else None
        lineups.append({"pids": pids, "_ids": pids, "_names": names,
                        "min": round(row["Minutes"]),
                        "ortg": ortg, "drtg": drtg, "net": net})

    _pbp_cache[cache_key] = (now, lineups)
    return lineups

@app.route("/api/wowy/roster")
def get_wowy_roster():
    """Fast endpoint: returns only roster from DB, no pbpstats call."""
    team_abbr = request.args.get("team", "").upper()
    season    = request.args.get("season", get_current_season())

    if not team_abbr:
        return jsonify({"error": "team param required"}), 400

    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT player_id, player_name, number, position
            FROM team_rosters
            WHERE team_abbr = %s AND season = %s
            ORDER BY player_name
        """, (team_abbr, season))
        rows = cur.fetchall()
        cur.close()
        conn.close()

        return jsonify({
            "roster": [
                {"player_id": r["player_id"], "player_name": r["player_name"],
                 "number": r["number"] or "", "position": r["position"] or ""}
                for r in rows
            ]
        })
    except Exception as exc:
        import traceback
        return jsonify({"error": str(exc), "trace": traceback.format_exc()}), 500

@app.route("/api/wowy/lineups")
def get_wowy_lineups():
    """Returns leverage-filtered lineup data from wowy_lineups table (pre-fetched locally)."""
    team_abbr = request.args.get("team", "").upper()
    season    = request.args.get("season", get_current_season())

    if not team_abbr:
        return jsonify({"error": "team param required"}), 400

    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT group_id, player_ids, player_names, min, ortg, drtg, net
            FROM wowy_lineups
            WHERE team_abbr = %s AND season = %s
            ORDER BY "min" DESC
        """, (team_abbr, season))
        rows = cur.fetchall()
        cur.close()
        conn.close()

        lineups = [
            {
                "pids":   list(r["player_ids"]),
                "_ids":   list(r["player_ids"]),
                "_names": list(r["player_names"]),
                "min":    float(r["min"]) if r["min"] is not None else None,
                "ortg":   float(r["ortg"]) if r["ortg"] is not None else None,
                "drtg":   float(r["drtg"]) if r["drtg"] is not None else None,
                "net":    float(r["net"])  if r["net"]  is not None else None,
            }
            for r in rows
        ]
        return jsonify({"team": team_abbr, "season": season, "lineups": lineups})
    except Exception as exc:
        import traceback
        return jsonify({"error": str(exc), "trace": traceback.format_exc()}), 500

@app.route("/api/wowy")
def get_wowy():
    team_abbr = request.args.get("team", "").upper()
    season    = request.args.get("season", get_current_season())
    leverage  = request.args.get("leverage", "Low,Medium,High,VeryHigh")

    if not team_abbr:
        return jsonify({"error": "team param required"}), 400

    if team_abbr not in _PBPSTATS_TEAM_IDS:
        return jsonify({"error": f"Unknown team: {team_abbr}"}), 400

    try:
        # ── Roster from DB ────────────────────────────────────────
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT player_id, player_name, number, position
            FROM team_rosters
            WHERE team_abbr = %s AND season = %s
            ORDER BY player_name
        """, (team_abbr, season))
        roster_rows = cur.fetchall()
        cur.close()
        conn.close()

        roster = [
            {"player_id": r["player_id"], "player_name": r["player_name"],
             "number": r["number"] or "", "position": r["position"] or ""}
            for r in roster_rows
        ]

        # ── Lineups from pbpstats (cached) ────────────────────────
        lineups = _fetch_pbp_lineups(team_abbr, season, leverage)

        if not roster and not lineups:
            return jsonify({"error": f"No data found for {team_abbr} {season}."}), 404

        return jsonify({
            "team":     team_abbr,
            "season":   season,
            "leverage": leverage,
            "roster":   roster,
            "lineups":  lineups,
        })

    except _requests.exceptions.Timeout:
        return jsonify({"error": "pbpstats API timed out. Try again."}), 504
    except Exception as exc:
        import traceback
        return jsonify({"error": str(exc), "trace": traceback.format_exc()}), 500

# ── Serve wowy.html ──────────────────────────────────────────
@app.route("/wowy")
@app.route("/wowy.html")
def wowy_page():
    return app.send_static_file("wowy.html")



"""
ydkball — Reviews API Routes (v2)
=========================================
Replaces the original reviews_routes.py paste-in in server.py.

Changes from v1:
- Profanity/slur filter on review submit
- Admin endpoints (delete any review, list all reviews)
- Admin check reads ADMIN_GOOGLE_IDS from .env
- /api/games/<id>/reviews supports offset for load-more
- GET /api/reviews/recent supports offset for load-more
"""

import re as _re
import os as _os

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ADMIN — read from env
# ADMIN_GOOGLE_IDS=id1,id2,id3  (comma-separated Google sub IDs)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _get_admin_ids():
    raw = _os.getenv("ADMIN_GOOGLE_IDS", "")
    return {s.strip() for s in raw.split(",") if s.strip()}

def _is_admin(user: dict) -> bool:
    if not user:
        return False
    return user.get("google_id") in _get_admin_ids()

def _admin_required(f):
    from functools import wraps
    @wraps(f)
    def decorated(*args, **kwargs):
        user = current_user()
        if not user:
            return jsonify({"error": "Authentication required"}), 401
        if not _is_admin(user):
            return jsonify({"error": "Admin access required"}), 403
        return f(*args, **kwargs)
    return decorated


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PROFANITY FILTER
# Loose filter — blocks slurs and hate speech, not general profanity.
# Add terms as lowercase; checked as whole words and substrings of
# compound words (e.g. "xxxword" in "xxxwordhere").
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Core list — racial/ethnic/sexual slurs and hate speech terms.
# Stored as a tuple so it's not trivially enumerable in source.
_BLOCKED = (
    "nigger","nigga","chink","spic","wetback","kike","faggot","fag",
    "dyke","tranny","retard","cunt","gook","towelhead","sandnigger",
    "raghead","beaner","zipperhead","cracker","honky","cripple",
    "spastic","mongoloid","trannies","shemale","ladyboy","fags",
    "kikes","niggers","chinks","spics","wetbacks","faggots","dykes",
    "retards","cunts","gooks",
)

_BLOCKED_PATTERN = _re.compile(
    r'(' + '|'.join(_re.escape(w) for w in _BLOCKED) + r')',
    _re.IGNORECASE
)

def _contains_slur(text: str) -> bool:
    """Return True if text contains a blocked term."""
    if not text:
        return False
    # Normalise: collapse repeated chars (e.g. "niiiigger" → "nigger")
    normalised = _re.sub(r'(.)\1{2,}', r'\1\1', text.lower())
    # Strip common leet substitutions
    normalised = normalised.replace('3', 'e').replace('0', 'o').replace('1', 'i').replace('@', 'a')
    return bool(_BLOCKED_PATTERN.search(normalised))


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# HELPERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _format_review(r: dict) -> dict:
    return {
        "id":             r["id"],
        "game_id":        r["game_id"],
        "user_id":        r["user_id"],
        "display_name":   r.get("display_name", ""),
        "avatar_url":     r.get("avatar_url", ""),
        "favorite_team":  r.get("favorite_team") or "",
        "is_pro":         bool(r.get("is_pro", False)),
        "rating":         r["rating"],
        "stars":          r["rating"] / 2,
        "review_text":    r.get("review_text"),
        "created_at":     str(r.get("created_at", "")),
        "updated_at":     str(r.get("updated_at", "")),
        "like_count":     int(r.get("like_count", 0)),
        "liked_by_me":    bool(r.get("liked_by_me", False)),
        "reply_count":    int(r.get("reply_count", 0)),
        "tags":           r.get("tags") or [],
        "attended":       bool(r.get("attended", False)),
        "ball_knowledge_level": _xp_to_level(int(r.get("xp") or 0)),
        "equipped_ring":        r.get("equipped_ring"),   # null=use rank, 0=no ring,  1-10=specific
        "equipped_title":       r.get("equipped_title"),  # null=use rank, 0=no title, 1-10=specific
    }


def _format_game(g: dict) -> dict:
    avg_stars = None
    if g.get("review_count", 0) > 0:
        avg_stars = round(g["rating_sum"] / g["review_count"] / 2, 2)
    return {
        "game_id":        g["game_id"],
        "season":         g["season"],
        "season_type":    g["season_type"],
        "game_date":      str(g["game_date"]),
        "home_team_abbr": g["home_team_abbr"],
        "away_team_abbr": g["away_team_abbr"],
        "home_score":     g["home_score"],
        "away_score":     g["away_score"],
        "status":         g["status"],
        "league":         g.get("league") or ("wnba" if str(g["game_id"]).startswith("10") else "nba"),
        "review_count":   g.get("review_count", 0),
        "avg_stars":      avg_stars,
        "bayesian_rating": g.get("bayesian_rating"),
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GET /api/games
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/games")
def get_games():
    league      = request.args.get("league",      "nba").lower().strip()
    default_season = _get_wnba_season() if league == "wnba" else get_current_season()
    season      = request.args.get("season",      default_season)
    season_type = request.args.get("season_type", "").strip()
    team        = request.args.get("team",        "").upper().strip()
    sort        = request.args.get("sort",        "date")
    direction   = "ASC" if request.args.get("dir", "desc").lower() == "asc" else "DESC"
    limit       = min(int(request.args.get("limit", 50)), 100)
    offset      = int(request.args.get("offset", 0))
    reviewed_by = request.args.get("reviewed_by")

    SORT_MAP = {
        "date":    "g.game_date",
        "rating":  "(g.rating_sum::float / NULLIF(g.review_count, 0))",
        "reviews": "g.review_count",
    }
    order_col = SORT_MAP.get(sort, "g.game_date")
    secondary_sort = ", g.review_count DESC" if sort == "rating" else ""

    all_seasons = season.lower() in ("all", "")
    filters = ["g.status = 'Final'", "g.league = %s"]
    params  = [league]
    if not all_seasons:
        filters.insert(0, "g.season = %s")
        params.insert(0, season)
    if season_type:
        filters.append("g.season_type = %s")
        params.append(season_type)

    if team:
        filters.append("(g.home_team_abbr = %s OR g.away_team_abbr = %s)")
        params += [team, team]

    if reviewed_by:
        filters.append("""
            EXISTS (
                SELECT 1 FROM game_reviews gr
                WHERE gr.game_id = g.game_id AND gr.user_id = %s
            )
        """)
        params.append(int(reviewed_by))

    where = " AND ".join(filters)

    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute(f"""
            SELECT g.*
            FROM games g
            WHERE {where}
            ORDER BY {order_col} {direction} NULLS LAST{secondary_sort}
            LIMIT %s OFFSET %s
        """, params + [limit, offset])
        games = [_format_game(dict(r)) for r in cur.fetchall()]

        cur.execute(f"SELECT COUNT(*) FROM games g WHERE {where}", params)
        total = cur.fetchone()["count"]

        cur.close(); conn.close()
        return jsonify({"games": games, "total": total, "limit": limit, "offset": offset})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# Best-effort date extraction for free-text game search (no dateutil dep in
# this repo — just try a handful of common formats against a regex match).
_GAME_SEARCH_DATE_PATTERNS = [
    (r'\d{4}-\d{1,2}-\d{1,2}',  ["%Y-%m-%d"]),
    (r'\d{1,2}/\d{1,2}/\d{4}',  ["%m/%d/%Y"]),
    (r'\d{1,2}/\d{1,2}/\d{2}\b', ["%m/%d/%y"]),
    (r'\d{1,2}/\d{1,2}\b',      ["%m/%d"]),
    (r'(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.?\s+\d{1,2}(?:,\s*\d{4}|\s+\d{4})?',
     ["%B %d, %Y", "%B %d %Y", "%b %d, %Y", "%b %d %Y", "%B %d", "%b %d"]),
]


def _extract_game_search_date(q: str):
    """Pull a specific calendar date out of a free-text query. Returns (date_or_None, remaining_text)."""
    for pattern, fmts in _GAME_SEARCH_DATE_PATTERNS:
        m = _re.search(pattern, q, _re.IGNORECASE)
        if not m:
            continue
        for fmt in fmts:
            try:
                parsed = _dt.strptime(m.group(0), fmt)
            except ValueError:
                continue
            if parsed.year == 1900:   # strptime's placeholder when %Y/%y is absent
                parsed = parsed.replace(year=date.today().year)
            remainder = (q[:m.start()] + " " + q[m.end():]).strip()
            return parsed.date(), remainder
    return None, q


def _extract_game_search_year(q: str):
    """Pull a bare 4-digit year out of a free-text query (e.g. 'pacers 2025').
    Only called when no full date matched, so this doesn't double-consume one."""
    m = _re.search(r'\b(19\d{2}|20\d{2})\b', q)
    if not m:
        return None, q
    remainder = (q[:m.start()] + " " + q[m.end():]).strip()
    return int(m.group(1)), remainder


# season_type keywords, checked longest/most-specific pattern first so
# "play-in"/"playin" doesn't get swallowed by the "playoffs?" pattern.
_SEASON_TYPE_KEYWORDS = [
    (r'\bplay-?in\b',   "PlayIn"),
    (r'\bplayoffs?\b',  "Playoffs"),
]


def _extract_game_search_season_type(q: str):
    """Pull a 'playoffs' / 'play-in' keyword out of a free-text query."""
    for pattern, season_type in _SEASON_TYPE_KEYWORDS:
        m = _re.search(pattern, q, _re.IGNORECASE)
        if m:
            remainder = (q[:m.start()] + " " + q[m.end():]).strip()
            return season_type, remainder
    return None, q


# Fan nicknames that aren't literal substrings of the official team_name
# (e.g. "Cavaliers" doesn't contain "cavs"), unlike "Warriors"/"blazers"/
# "wolves" etc. which already ILIKE-match their team_name directly.
_TEAM_NICKNAME_ALIASES = {
    "cavs":    "cavaliers",
    "mavs":    "mavericks",
    "sixers":  "76ers",
    "niners":  "76ers",
    "dubs":    "warriors",
}


@app.route("/api/games/search")
def search_games():
    """Free-text game lookup for the Explore search bar. Matches:
    - one or more teams by name/abbr, splitting on whitespace so a query like
      "pacers pistons" resolves to two teams and is treated as a matchup
      (both teams involved) rather than either team's games
    - a specific date ("1/15/2025", "Dec 25 2025", ...) and/or a bare year
      ("pacers 2025") as a season/year filter
    - a "playoffs" / "play-in" keyword to filter to that season_type
    Only Final games (this is for finding a game to read/rate, not tonight's
    schedule)."""
    q = request.args.get("q", "").strip()
    if len(q) < 2:
        return jsonify({"games": []})

    limit  = min(int(request.args.get("limit", 10)), 50)
    offset = int(request.args.get("offset", 0))

    search_date, remainder = _extract_game_search_date(q)
    search_year = None
    if not search_date:
        search_year, remainder = _extract_game_search_year(remainder)
    search_season_type, remainder = _extract_game_search_season_type(remainder)

    tokens = [t for t in remainder.split() if len(t) >= 2]

    conn = get_conn(); cur = conn.cursor()
    try:
        nba_abbrs, wnba_abbrs = set(), set()
        for tok in tokens:
            canonical = _TEAM_NICKNAME_ALIASES.get(tok.lower(), tok)
            pattern = f"%{canonical}%"
            cur.execute("""
                SELECT DISTINCT team_abbr, league FROM team_seasons
                WHERE team_name ILIKE %s OR team_abbr ILIKE %s
            """, (pattern, pattern))
            for r in cur.fetchall():
                if r["league"] == "wnba":
                    wnba_abbrs.add(_WNBA_STANDINGS_TO_GAMES.get(r["team_abbr"], r["team_abbr"]))
                else:
                    nba_abbrs.add(r["team_abbr"])

        if not nba_abbrs and not wnba_abbrs and not search_date and not search_year and not search_season_type:
            cur.close(); conn.close()
            return jsonify({"games": []})

        filters = ["g.status = 'Final'"]
        params = []
        # NBA/WNBA share some abbreviations (e.g. IND = Pacers/Fever), so a
        # matched abbr must be tied to the league it was matched in. When two+
        # teams matched in a league, require both home AND away from that set
        # (a matchup query) rather than OR (either team's full schedule).
        team_clauses = []
        for abbrs, league in ((sorted(nba_abbrs), "nba"), (sorted(wnba_abbrs), "wnba")):
            if not abbrs:
                continue
            if len(abbrs) == 1:
                team_clauses.append("(g.league = %s AND (g.home_team_abbr = ANY(%s) OR g.away_team_abbr = ANY(%s)))")
            else:
                team_clauses.append("(g.league = %s AND g.home_team_abbr = ANY(%s) AND g.away_team_abbr = ANY(%s))")
            params += [league, abbrs, abbrs]
        if team_clauses:
            filters.append(f"({' OR '.join(team_clauses)})")

        if search_date:
            filters.append("g.game_date = %s")
            params.append(search_date)
        elif search_year:
            # NBA seasons are labeled by their start year ("2024-25"; WNBA is
            # just "2024"), but an NBA season's playoffs are played the
            # following spring — so "2024" should match both games dated in
            # calendar 2024 AND any game in the season starting in 2024
            # (e.g. May 2025 playoff games from season "2024-25").
            filters.append("(EXTRACT(YEAR FROM g.game_date) = %s OR g.season LIKE %s)")
            params += [search_year, f"{search_year}%"]

        if search_season_type:
            filters.append("g.season_type = %s")
            params.append(search_season_type)

        cur.execute(f"""
            SELECT g.* FROM games g
            WHERE {' AND '.join(filters)}
            ORDER BY g.game_date DESC
            LIMIT %s OFFSET %s
        """, params + [limit, offset])
        games = [_format_game(dict(r)) for r in cur.fetchall()]
        cur.close(); conn.close()
        return jsonify({"games": games})
    except Exception as e:
        cur.close(); conn.close()
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GET /api/games/<game_id>
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/games/<game_id>")
def get_game(game_id):
    user    = current_user()
    user_id = user["id"] if user else None
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("SELECT * FROM games WHERE game_id = %s", (game_id,))
        row = cur.fetchone()
        if not row:
            cur.close(); conn.close()
            return jsonify({"error": "Game not found"}), 404
        game = _format_game(dict(row))
        if user_id:
            cur.execute(
                "SELECT 1 FROM game_watches WHERE user_id = %s AND game_id = %s",
                (user_id, game_id)
            )
            game["is_watched"] = cur.fetchone() is not None
        else:
            game["is_watched"] = False
        cur.close(); conn.close()
        return jsonify({"game": game})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GET /api/games/<game_id>/reviews
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/games/<game_id>/reviews")
def get_game_reviews(game_id):
    limit  = min(int(request.args.get("limit", 20)), 100)
    offset = int(request.args.get("offset", 0))
    sort   = request.args.get("sort", "date")  # "date" | "likes"
    order_sql = "like_count DESC, gr.created_at DESC" if sort == "likes" else "gr.created_at DESC"
    user    = current_user()
    user_id = user["id"] if user else None
    try:
        conn = get_conn()
        cur  = conn.cursor()
        if user_id:
            cur.execute(f"""
                SELECT gr.*, u.display_name, u.avatar_url, u.favorite_team, u.is_pro, u.xp, u.equipped_ring, u.equipped_title,
                       COUNT(rl.review_id)                                   AS like_count,
                       BOOL_OR(rl_me.user_id IS NOT NULL)                    AS liked_by_me,
                       (SELECT COUNT(*) FROM review_replies rr WHERE rr.review_id = gr.id) AS reply_count
                FROM game_reviews gr
                JOIN users u ON gr.user_id = u.id
                LEFT JOIN review_likes rl    ON rl.review_id    = gr.id
                LEFT JOIN review_likes rl_me ON rl_me.review_id = gr.id
                                            AND rl_me.user_id   = %s
                WHERE gr.game_id = %s
                GROUP BY gr.id, u.display_name, u.avatar_url, u.favorite_team, u.is_pro, u.xp, u.equipped_ring, u.equipped_title
                ORDER BY {order_sql}
                LIMIT %s OFFSET %s
            """, (user_id, game_id, limit, offset))
        else:
            cur.execute(f"""
                SELECT gr.*, u.display_name, u.avatar_url, u.favorite_team, u.is_pro, u.xp, u.equipped_ring, u.equipped_title,
                       COUNT(rl.review_id) AS like_count,
                       FALSE               AS liked_by_me,
                       (SELECT COUNT(*) FROM review_replies rr WHERE rr.review_id = gr.id) AS reply_count
                FROM game_reviews gr
                JOIN users u ON gr.user_id = u.id
                LEFT JOIN review_likes rl ON rl.review_id = gr.id
                WHERE gr.game_id = %s
                GROUP BY gr.id, u.display_name, u.avatar_url, u.favorite_team, u.is_pro, u.xp, u.equipped_ring, u.equipped_title
                ORDER BY {order_sql}
                LIMIT %s OFFSET %s
            """, (game_id, limit, offset))
        reviews = [_format_review(dict(r)) for r in cur.fetchall()]
        cur.execute("SELECT COUNT(*) FROM game_reviews WHERE game_id = %s", (game_id,))
        total = cur.fetchone()["count"]
        cur.close(); conn.close()
        return jsonify({"reviews": reviews, "total": total, "has_more": offset + len(reviews) < total})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# POST /api/games/<game_id>/reviews  — submit/update
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/games/<game_id>/reviews", methods=["POST"])
@login_required
def submit_review(game_id):
    user = current_user()
    body = request.get_json() or {}

    rating = body.get("rating")
    if rating is None or not isinstance(rating, int) or not (1 <= rating <= 10):
        return jsonify({"error": "rating must be an integer 1–10"}), 400

    review_text = (body.get("review_text") or "").strip() or None

    # ── Character limit for free users ───────────────────────────
    _FREE_REVIEW_LIMIT = 500
    if review_text and not user.get("is_pro") and len(review_text) > _FREE_REVIEW_LIMIT:
        return jsonify({"error": f"Review exceeds {_FREE_REVIEW_LIMIT} characters. Upgrade to Pro for unlimited length."}), 400

    # ── Profanity filter ──────────────────────────────────────────
    if review_text and _contains_slur(review_text):
        return jsonify({"error": "Your review contains language that isn't allowed. Please edit and resubmit."}), 400

    # ── Sanitize tags ─────────────────────────────────────────────
    import json as _json
    raw_tags = body.get("tags") or []
    if not isinstance(raw_tags, list):
        raw_tags = []
    clean_tags = []
    for t in raw_tags[:5]:
        if isinstance(t, dict):
            clean_tags.append({
                "player_id":    str(t.get("player_id", ""))[:20],
                "player_name":  str(t.get("player_name", ""))[:60],
                "team_abbr":    str(t.get("team_abbr", ""))[:5],
                "stat_label":   str(t.get("stat_label", ""))[:10],
                "stat_display": str(t.get("stat_display", ""))[:20],
            })

    attended = bool(body.get("attended", False))

    try:
        conn = get_conn()
        cur  = conn.cursor()

        # One-time idempotent migrations
        cur.execute("ALTER TABLE game_reviews ADD COLUMN IF NOT EXISTS tags JSONB DEFAULT '[]'")
        cur.execute("ALTER TABLE game_reviews ADD COLUMN IF NOT EXISTS attended BOOLEAN DEFAULT FALSE")

        cur.execute("SELECT game_id FROM games WHERE game_id = %s", (game_id,))
        if not cur.fetchone():
            cur.close(); conn.close()
            return jsonify({"error": "Game not found"}), 404

        cur.execute("""
            INSERT INTO game_reviews (user_id, game_id, rating, review_text, tags, attended)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (user_id, game_id) DO UPDATE SET
                rating      = EXCLUDED.rating,
                review_text = EXCLUDED.review_text,
                tags        = EXCLUDED.tags,
                attended    = EXCLUDED.attended,
                updated_at  = NOW()
            RETURNING *
        """, (user["id"], game_id, rating, review_text, _json.dumps(clean_tags), attended))

        review = dict(cur.fetchone())
        cur.execute("SELECT avatar_url, favorite_team FROM users WHERE id = %s", (user["id"],))
        user_row = cur.fetchone()
        review["display_name"]  = user["display_name"]
        review["avatar_url"]    = (user_row["avatar_url"] if user_row else None) or ""
        review["favorite_team"] = (user_row["favorite_team"] if user_row else None) or ""

        cur.execute("""
            UPDATE games
            SET review_count = (SELECT COUNT(*) FROM game_reviews WHERE game_id = %s),
                rating_sum   = (SELECT COALESCE(SUM(rating), 0) FROM game_reviews WHERE game_id = %s)
            WHERE game_id = %s
        """, (game_id, game_id, game_id))

        # Rating a game implies you watched it — keep the diary invariant true.
        cur.execute("""
            INSERT INTO game_watches (user_id, game_id)
            VALUES (%s, %s) ON CONFLICT DO NOTHING
        """, (user["id"], game_id))

        # Invalidate scoreboard caches so home page reflects the new review
        cur.execute("SELECT game_date FROM games WHERE game_id = %s", (game_id,))
        date_row = cur.fetchone()
        if date_row:
            date_str = str(date_row["game_date"])
            _past_sb_cache.pop(date_str, None)
            if _today_sb_cache.get("date") == date_str:
                _today_sb_cache.clear()

        conn.commit()
        cur.close(); conn.close()
        return jsonify({"review": _format_review(review)}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DELETE /api/games/<game_id>/reviews  — user deletes own review
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/games/<game_id>/reviews", methods=["DELETE"])
@login_required
def delete_review(game_id):
    user = current_user()
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            DELETE FROM game_reviews
            WHERE user_id = %s AND game_id = %s
            RETURNING id
        """, (user["id"], game_id))
        deleted = cur.fetchone()
        if deleted:
            cur.execute("""
                UPDATE games
                SET review_count = (SELECT COUNT(*) FROM game_reviews WHERE game_id = %s),
                    rating_sum   = (SELECT COALESCE(SUM(rating), 0) FROM game_reviews WHERE game_id = %s)
                WHERE game_id = %s
            """, (game_id, game_id, game_id))
            cur.execute("SELECT game_date FROM games WHERE game_id = %s", (game_id,))
            date_row = cur.fetchone()
            if date_row:
                date_str = str(date_row["game_date"])
                _past_sb_cache.pop(date_str, None)
                if _today_sb_cache.get("date") == date_str:
                    _today_sb_cache.clear()
        conn.commit()
        cur.close(); conn.close()
        if not deleted:
            return jsonify({"error": "Review not found"}), 404
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# POST/DELETE /api/games/<game_id>/watch  — mark / unmark watched
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/games/<game_id>/watch", methods=["POST"])
@login_required
def watch_game(game_id):
    user = current_user()
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            INSERT INTO game_watches (user_id, game_id)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING
        """, (user["id"], game_id))
        conn.commit()
        cur.close(); conn.close()
        return jsonify({"is_watched": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/games/<game_id>/watch", methods=["DELETE"])
@login_required
def unwatch_game(game_id):
    user = current_user()
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            DELETE FROM game_watches WHERE user_id = %s AND game_id = %s
        """, (user["id"], game_id))
        conn.commit()
        cur.close(); conn.close()
        return jsonify({"is_watched": False})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Performance reviews
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _format_perf_review(r: dict) -> dict:
    return {
        "id":           r["id"],
        "game_id":      r["game_id"],
        "person_id":    r["person_id"],
        "user_id":      r["user_id"],
        "player_name":  r.get("player_name") or "",
        "display_name": r.get("display_name", ""),
        "avatar_url":   r.get("avatar_url") or "",
        "rating":       r["rating"],
        "stars":        round(r["rating"] / 2, 1),
        "review_text":  r.get("review_text"),
        "created_at":   str(r.get("created_at", "")),
    }


# GET /api/performances/<game_id>/<person_id>/reviews
@app.route("/api/performances/<game_id>/<int:person_id>/reviews")
def get_performance_reviews(game_id, person_id):
    limit  = min(int(request.args.get("limit", 20)), 100)
    offset = int(request.args.get("offset", 0))
    user    = current_user()
    user_id = user["id"] if user else None
    try:
        conn = get_conn()
        cur  = conn.cursor()

        cur.execute("""
            SELECT COUNT(*) AS review_count,
                   COALESCE(AVG(rating::float), 0) AS avg_rating
            FROM performance_reviews
            WHERE game_id = %s AND person_id = %s
        """, (game_id, person_id))
        agg = dict(cur.fetchone())
        count = int(agg["review_count"])
        avg_stars = round(agg["avg_rating"] / 2, 2) if count > 0 else None

        my_rating, my_stars, my_text = None, None, None
        if user_id:
            cur.execute("""
                SELECT rating, review_text FROM performance_reviews
                WHERE game_id = %s AND person_id = %s AND user_id = %s
            """, (game_id, person_id, user_id))
            row = cur.fetchone()
            if row:
                my_rating = row["rating"]
                my_stars  = round(row["rating"] / 2, 1)
                my_text   = row["review_text"]

        cur.execute("""
            SELECT pr.*, u.display_name, u.avatar_url
            FROM performance_reviews pr
            JOIN users u ON pr.user_id = u.id
            WHERE pr.game_id = %s AND pr.person_id = %s
            ORDER BY pr.created_at DESC
            LIMIT %s OFFSET %s
        """, (game_id, person_id, limit, offset))
        reviews = [_format_perf_review(dict(r)) for r in cur.fetchall()]

        cur.close(); conn.close()
        return jsonify({
            "avg_stars":    avg_stars,
            "review_count": count,
            "my_rating":    my_rating,
            "my_stars":     my_stars,
            "my_text":      my_text,
            "reviews":      reviews,
            "has_more":     offset + len(reviews) < count,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# GET /api/performances/<game_id>/summary
# Batch per-player rating summary for one game (fan avg + count + my rating),
# so the box score can show every player's rating in one request.
@app.route("/api/performances/<game_id>/summary")
def get_performance_summary(game_id):
    user    = current_user()
    user_id = user["id"] if user else None
    try:
        conn = get_conn()
        cur  = conn.cursor()

        cur.execute("""
            SELECT person_id,
                   COUNT(*)            AS review_count,
                   AVG(rating::float)  AS avg_rating
            FROM performance_reviews
            WHERE game_id = %s
            GROUP BY person_id
        """, (game_id,))
        players = {}
        for r in cur.fetchall():
            pid = r["person_id"]
            cnt = int(r["review_count"])
            players[str(pid)] = {
                "person_id":    pid,
                "avg_stars":    round(r["avg_rating"] / 2, 2) if cnt > 0 else None,
                "review_count": cnt,
                "my_rating":    None,
                "my_stars":     None,
            }

        if user_id:
            cur.execute("""
                SELECT person_id, rating FROM performance_reviews
                WHERE game_id = %s AND user_id = %s
            """, (game_id, user_id))
            for r in cur.fetchall():
                pid   = r["person_id"]
                entry = players.setdefault(str(pid), {
                    "person_id": pid, "avg_stars": None, "review_count": 0,
                    "my_rating": None, "my_stars": None,
                })
                entry["my_rating"] = r["rating"]
                entry["my_stars"]  = round(r["rating"] / 2, 1)

        cur.close(); conn.close()
        return jsonify({"players": players})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# POST /api/performances/<game_id>/<person_id>/reviews
@app.route("/api/performances/<game_id>/<int:person_id>/reviews", methods=["POST"])
@login_required
def submit_performance_review(game_id, person_id):
    user = current_user()
    body = request.get_json() or {}
    rating = body.get("rating")
    if rating is None or not isinstance(rating, int) or not (1 <= rating <= 10):
        return jsonify({"error": "rating must be an integer 1–10"}), 400
    review_text = (body.get("review_text") or "").strip() or None
    if review_text and _contains_slur(review_text):
        return jsonify({"error": "Your review contains language that isn't allowed."}), 400
    player_name = (body.get("player_name") or "").strip()[:100] or None
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            INSERT INTO performance_reviews (user_id, game_id, person_id, rating, player_name, review_text)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (user_id, game_id, person_id) DO UPDATE SET
                rating      = EXCLUDED.rating,
                player_name = EXCLUDED.player_name,
                review_text = EXCLUDED.review_text,
                updated_at  = NOW()
            RETURNING *
        """, (user["id"], game_id, person_id, rating, player_name, review_text))
        row = dict(cur.fetchone())
        row["display_name"] = user["display_name"]
        cur.execute("SELECT avatar_url FROM users WHERE id = %s", (user["id"],))
        u = cur.fetchone()
        row["avatar_url"] = (u["avatar_url"] if u else None) or ""
        # Rating a player's game implies you watched that game.
        cur.execute("""
            INSERT INTO game_watches (user_id, game_id)
            VALUES (%s, %s) ON CONFLICT DO NOTHING
        """, (user["id"], game_id))
        conn.commit(); cur.close(); conn.close()
        return jsonify({"review": _format_perf_review(row)}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# DELETE /api/performances/<game_id>/<person_id>/reviews
@app.route("/api/performances/<game_id>/<int:person_id>/reviews", methods=["DELETE"])
@login_required
def delete_performance_review(game_id, person_id):
    user = current_user()
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            DELETE FROM performance_reviews
            WHERE user_id = %s AND game_id = %s AND person_id = %s
            RETURNING id
        """, (user["id"], game_id, person_id))
        deleted = cur.fetchone()
        conn.commit(); cur.close(); conn.close()
        if not deleted:
            return jsonify({"error": "Review not found"}), 404
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# POST /api/reviews/<review_id>/like  — toggle like on a review
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/reviews/<int:review_id>/like", methods=["POST"])
@login_required
def toggle_review_like(review_id):
    user = current_user()
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS review_likes (
                user_id    INTEGER REFERENCES users(id)        ON DELETE CASCADE,
                review_id  INTEGER REFERENCES game_reviews(id) ON DELETE CASCADE,
                created_at TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (user_id, review_id)
            )
        """)
        cur.execute(
            "SELECT 1 FROM review_likes WHERE user_id = %s AND review_id = %s",
            (user["id"], review_id)
        )
        if cur.fetchone():
            cur.execute(
                "DELETE FROM review_likes WHERE user_id = %s AND review_id = %s",
                (user["id"], review_id)
            )
            liked = False
        else:
            cur.execute(
                "INSERT INTO review_likes (user_id, review_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (user["id"], review_id)
            )
            liked = True
            # Grant 5 XP to the review author (not if they liked their own review)
            cur.execute("SELECT user_id FROM game_reviews WHERE id = %s", (review_id,))
            review_row = cur.fetchone()
            if review_row and review_row["user_id"] != user["id"]:
                ref = f"{review_id}:{user['id']}"
                _grant_xp(cur, review_row["user_id"], "review_like", ref, 5)
        cur.execute("SELECT COUNT(*) FROM review_likes WHERE review_id = %s", (review_id,))
        like_count = cur.fetchone()["count"]
        conn.commit()
        cur.close(); conn.close()
        return jsonify({"liked": liked, "like_count": int(like_count)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Ball Knowledge XP endpoints
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.route("/api/xp/app-open", methods=["POST"])
@login_required
def xp_app_open():
    """Grant 10 XP for opening the app. Cooldown: once per 25 hours."""
    user = current_user()
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT created_at FROM xp_events
            WHERE user_id = %s AND event_type = 'app_open'
            ORDER BY created_at DESC
            LIMIT 1
        """, (user["id"],))
        last = cur.fetchone()
        from datetime import datetime, timezone, timedelta
        now = datetime.now(timezone.utc)
        if last and (now - last["created_at"].replace(tzinfo=timezone.utc)) < timedelta(hours=25):
            cur.close(); conn.close()
            return jsonify({"granted": False, "reason": "cooldown"})
        ref = now.strftime("%Y-%m-%dT%H")
        new_xp = _grant_xp(cur, user["id"], "app_open", ref, 10)
        conn.commit()
        cur.close(); conn.close()
        return jsonify({"granted": True, "xp_gained": 10, "total_xp": new_xp,
                        "rank": get_rank_info(new_xp)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/xp/live-game-view/<game_id>", methods=["POST"])
@login_required
def xp_live_game_view(game_id):
    """Grant 10 XP for viewing a live game. Once per game_id."""
    user = current_user()
    try:
        conn = get_conn()
        cur  = conn.cursor()
        # Deny if game is already Final in our DB
        cur.execute("SELECT status FROM games WHERE game_id = %s", (game_id,))
        game_row = cur.fetchone()
        if game_row and game_row["status"] == "Final":
            cur.close(); conn.close()
            return jsonify({"granted": False, "reason": "game_over"})
        new_xp = _grant_xp(cur, user["id"], "live_game_view", game_id, 10)
        if new_xp == -1:
            cur.close(); conn.close()
            return jsonify({"granted": False, "reason": "already_earned"})
        conn.commit()
        cur.close(); conn.close()
        return jsonify({"granted": True, "xp_gained": 10, "total_xp": new_xp,
                        "rank": get_rank_info(new_xp)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GET /api/reviews/<review_id>/replies
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/reviews/<int:review_id>/replies")
def get_review_replies(review_id):
    limit  = min(int(request.args.get("limit", 3)), 100)
    offset = int(request.args.get("offset", 0))
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT rr.id, rr.reply_text, rr.created_at,
                   u.id AS user_id, u.display_name, u.avatar_url, u.favorite_team
            FROM review_replies rr
            JOIN users u ON rr.user_id = u.id
            WHERE rr.review_id = %s
            ORDER BY rr.created_at ASC
            LIMIT %s OFFSET %s
        """, (review_id, limit, offset))
        replies = [dict(r) for r in cur.fetchall()]
        cur.execute("SELECT COUNT(*) FROM review_replies WHERE review_id = %s", (review_id,))
        total = cur.fetchone()["count"]
        cur.close(); conn.close()
        for r in replies:
            r["created_at"] = str(r["created_at"])
        return jsonify({"replies": replies, "total": int(total)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# POST /api/reviews/<review_id>/replies
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/reviews/<int:review_id>/replies", methods=["POST"])
@login_required
def post_review_reply(review_id):
    user = current_user()
    body = request.get_json(silent=True) or {}
    text = (body.get("reply_text") or "").strip()
    if not text:
        return jsonify({"error": "reply_text is required"}), 400
    if len(text) > 1000:
        return jsonify({"error": "Reply must be 1000 characters or fewer"}), 400
    try:
        conn = get_conn()
        cur  = conn.cursor()
        # Verify review exists
        cur.execute("SELECT id FROM game_reviews WHERE id = %s", (review_id,))
        if not cur.fetchone():
            cur.close(); conn.close()
            return jsonify({"error": "Review not found"}), 404
        cur.execute("""
            INSERT INTO review_replies (review_id, user_id, reply_text)
            VALUES (%s, %s, %s)
            RETURNING id, created_at
        """, (review_id, user["id"], text))
        row = cur.fetchone()
        conn.commit()
        cur.close(); conn.close()
        return jsonify({"reply": {
            "id":           row["id"],
            "review_id":    review_id,
            "user_id":      user["id"],
            "display_name": user["display_name"],
            "avatar_url":   user.get("avatar_url"),
            "favorite_team": user.get("favorite_team"),
            "reply_text":   text,
            "created_at":   str(row["created_at"]),
        }}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DELETE /api/reviews/<review_id>/replies/<reply_id>
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/reviews/<int:review_id>/replies/<int:reply_id>", methods=["DELETE"])
@login_required
def delete_review_reply(review_id, reply_id):
    user = current_user()
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute(
            "DELETE FROM review_replies WHERE id = %s AND review_id = %s AND user_id = %s RETURNING id",
            (reply_id, review_id, user["id"])
        )
        deleted = cur.fetchone()
        conn.commit()
        cur.close(); conn.close()
        if not deleted:
            return jsonify({"error": "Reply not found or not yours"}), 404
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DELETE /api/admin/reviews/<review_id>  — admin deletes any review
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/admin/reviews/<int:review_id>", methods=["DELETE"])
@_admin_required
def admin_delete_review(review_id):
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("DELETE FROM game_reviews WHERE id = %s RETURNING id", (review_id,))
        deleted = cur.fetchone()
        conn.commit()
        cur.close(); conn.close()
        if not deleted:
            return jsonify({"error": "Review not found"}), 404
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GET /api/admin/reviews  — paginated list of all reviews
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/admin/reviews")
@_admin_required
def admin_list_reviews():
    limit  = min(int(request.args.get("limit", 50)), 200)
    offset = int(request.args.get("offset", 0))
    q      = request.args.get("q", "").strip()   # search review text

    filters = []
    params  = []
    if q:
        filters.append("(gr.review_text ILIKE %s OR u.display_name ILIKE %s)")
        params += [f"%{q}%", f"%{q}%"]

    where = ("WHERE " + " AND ".join(filters)) if filters else ""

    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute(f"""
            SELECT
                gr.id, gr.game_id, gr.rating, gr.review_text,
                gr.created_at,
                u.id AS user_id, u.display_name, u.email,
                g.game_date, g.home_team_abbr, g.away_team_abbr,
                g.home_score, g.away_score
            FROM game_reviews gr
            JOIN users u ON gr.user_id = u.id
            JOIN games g ON gr.game_id = g.game_id
            {where}
            ORDER BY gr.created_at DESC
            LIMIT %s OFFSET %s
        """, params + [limit, offset])
        rows = cur.fetchall()

        cur.execute(f"""
            SELECT COUNT(*) FROM game_reviews gr
            JOIN users u ON gr.user_id = u.id
            JOIN games g ON gr.game_id = g.game_id
            {where}
        """, params)
        total = cur.fetchone()["count"]

        cur.close(); conn.close()

        result = []
        for r in rows:
            d = dict(r)
            result.append({
                "id":           d["id"],
                "game_id":      d["game_id"],
                "rating":       d["rating"],
                "stars":        d["rating"] / 2,
                "review_text":  d["review_text"],
                "created_at":   str(d["created_at"]),
                "user_id":      d["user_id"],
                "display_name": d["display_name"],
                "email":        d["email"],
                "game_date":    str(d["game_date"]),
                "home_team_abbr": d["home_team_abbr"],
                "away_team_abbr": d["away_team_abbr"],
                "home_score":   d["home_score"],
                "away_score":   d["away_score"],
                "matchup":      f"{d['away_team_abbr']} @ {d['home_team_abbr']}",
            })

        return jsonify({"reviews": result, "total": total,
                        "has_more": offset + len(result) < total})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GET /api/reviews/top-games
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/reviews/top-games")
def get_top_rated_games():
    league      = request.args.get("league", "nba").lower().strip()
    _def_season = _get_wnba_season() if league == "wnba" else get_current_season()
    season      = request.args.get("season", _def_season).strip()
    season_type = request.args.get("season_type", "").strip()
    min_reviews = int(request.args.get("min_reviews", 1))
    limit       = min(int(request.args.get("limit", 25)), 100)
    days        = request.args.get("days")

    all_seasons = season.lower() in ("all", "")

    try:
        conn = get_conn()
        cur  = conn.cursor()
        s_filter  = "" if all_seasons else "AND season = %s"
        s_params  = [] if all_seasons else [season]
        st_filter = "AND season_type = %s" if season_type else ""
        st_params = [season_type] if season_type else []
        d_filter  = "AND game_date >= CURRENT_DATE - INTERVAL '%s days'" if days else ""
        d_params  = [int(days)] if days else []
        cur.execute(f"""
            SELECT *
            FROM games
            WHERE status = 'Final'
              AND league = %s
              {s_filter}
              {st_filter}
              {d_filter}
              AND review_count >= %s
            ORDER BY (rating_sum::float / NULLIF(review_count, 0)) DESC NULLS LAST,
                     review_count DESC
            LIMIT %s
        """, [league] + s_params + st_params + d_params + [min_reviews, limit])
        games = [_format_game(dict(r)) for r in cur.fetchall()]
        cur.close(); conn.close()
        return jsonify({"games": games})
    except Exception as e:
        return jsonify({"error": str(e)}), 500



# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GET /api/reviews/most-liked
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/reviews/most-liked")
def get_most_liked_reviews():
    limit   = min(int(request.args.get("limit", 20)), 100)
    offset  = int(request.args.get("offset", 0))
    days    = request.args.get("days")
    user    = current_user()
    user_id = user["id"] if user else None

    d_filter = "AND gr.created_at >= CURRENT_DATE - INTERVAL '%s days'" if days else ""
    d_params = [int(days)] if days else []

    try:
        conn = get_conn()
        cur  = conn.cursor()
        if user_id:
            cur.execute(f"""
                SELECT
                    gr.id, gr.game_id, gr.rating, gr.review_text,
                    gr.created_at, gr.updated_at,
                    COALESCE(gr.tags, '[]'::jsonb) AS tags,
                    gr.attended,
                    u.id AS user_id, u.display_name, u.avatar_url, u.favorite_team, u.is_pro, u.xp, u.equipped_ring, u.equipped_title,
                    g.game_date, g.home_team_abbr, g.away_team_abbr,
                    g.home_score, g.away_score,
                    COUNT(rl.review_id)                AS like_count,
                    BOOL_OR(rl_me.user_id IS NOT NULL) AS liked_by_me,
                    (SELECT COUNT(*) FROM review_replies rr WHERE rr.review_id = gr.id) AS reply_count
                FROM game_reviews gr
                JOIN users u  ON gr.user_id = u.id
                JOIN games g  ON gr.game_id = g.game_id
                LEFT JOIN review_likes rl    ON rl.review_id    = gr.id
                LEFT JOIN review_likes rl_me ON rl_me.review_id = gr.id
                                            AND rl_me.user_id   = %s
                WHERE 1=1 {d_filter}
                GROUP BY gr.id, u.id, g.game_id
                ORDER BY like_count DESC, gr.created_at DESC
                LIMIT %s OFFSET %s
            """, [user_id] + d_params + [limit, offset])
        else:
            cur.execute(f"""
                SELECT
                    gr.id, gr.game_id, gr.rating, gr.review_text,
                    gr.created_at, gr.updated_at,
                    COALESCE(gr.tags, '[]'::jsonb) AS tags,
                    gr.attended,
                    u.id AS user_id, u.display_name, u.avatar_url, u.favorite_team, u.is_pro, u.xp, u.equipped_ring, u.equipped_title,
                    g.game_date, g.home_team_abbr, g.away_team_abbr,
                    g.home_score, g.away_score,
                    COUNT(rl.review_id) AS like_count,
                    FALSE               AS liked_by_me,
                    (SELECT COUNT(*) FROM review_replies rr WHERE rr.review_id = gr.id) AS reply_count
                FROM game_reviews gr
                JOIN users u  ON gr.user_id = u.id
                JOIN games g  ON gr.game_id = g.game_id
                LEFT JOIN review_likes rl ON rl.review_id = gr.id
                WHERE 1=1 {d_filter}
                GROUP BY gr.id, u.id, g.game_id
                ORDER BY like_count DESC, gr.created_at DESC
                LIMIT %s OFFSET %s
            """, d_params + [limit, offset])
        rows = cur.fetchall()
        cur.execute(f"SELECT COUNT(*) FROM game_reviews gr WHERE 1=1 {d_filter}", d_params)
        total = cur.fetchone()["count"]
        cur.close(); conn.close()
        result = []
        for r in rows:
            d = dict(r)
            result.append({
                **_format_review(d),
                "game_date":      str(d["game_date"]),
                "home_team_abbr": d["home_team_abbr"],
                "away_team_abbr": d["away_team_abbr"],
                "home_score":     d["home_score"],
                "away_score":     d["away_score"],
            })
        return jsonify({"reviews": result, "total": total,
                        "has_more": offset + len(result) < total})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GET /api/performances/top-rated
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/performances/top-rated")
def get_top_rated_performances():
    limit       = min(int(request.args.get("limit", 20)), 100)
    offset      = int(request.args.get("offset", 0))
    days        = request.args.get("days")
    min_reviews = int(request.args.get("min_reviews", 1))

    d_filter = "AND pr.created_at >= CURRENT_DATE - INTERVAL '%s days'" if days else ""
    d_params = [int(days)] if days else []

    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute(f"""
            SELECT
                pr.game_id, pr.person_id,
                MAX(pr.player_name)                  AS player_name,
                AVG(pr.rating)::float                 AS avg_rating,
                COUNT(*)                              AS review_count,
                g.game_date, g.league, g.home_team_abbr, g.away_team_abbr,
                g.home_score, g.away_score,
                COALESCE(MAX(pgl.pts), MAX(wgs.pts))  AS pts,
                COALESCE(MAX(pgl.reb), MAX(wgs.reb))  AS reb,
                COALESCE(MAX(pgl.ast), MAX(wgs.ast))  AS ast
            FROM performance_reviews pr
            LEFT JOIN games g ON pr.game_id = g.game_id
            LEFT JOIN player_gamelogs pgl      ON pgl.game_id = pr.game_id AND pgl.player_id = pr.person_id
            LEFT JOIN wnba_player_game_stats wgs ON wgs.game_id = pr.game_id AND wgs.player_id = pr.person_id
            WHERE 1=1 {d_filter}
            GROUP BY pr.game_id, pr.person_id, g.game_date, g.league,
                     g.home_team_abbr, g.away_team_abbr, g.home_score, g.away_score
            HAVING COUNT(*) >= %s
            ORDER BY avg_rating DESC, review_count DESC
            LIMIT %s OFFSET %s
        """, d_params + [min_reviews, limit, offset])
        rows = cur.fetchall()
        cur.close(); conn.close()
        result = []
        for r in rows:
            d = dict(r)
            result.append({
                "game_id":        d["game_id"],
                "person_id":      d["person_id"],
                "player_name":    d.get("player_name") or "",
                "league":         d.get("league"),
                "avg_rating":     round(d["avg_rating"], 2),
                "stars":          round(d["avg_rating"] / 2, 1),
                "review_count":   d["review_count"],
                "game_date":      str(d["game_date"]) if d.get("game_date") else None,
                "home_team_abbr": d.get("home_team_abbr"),
                "away_team_abbr": d.get("away_team_abbr"),
                "home_score":     d.get("home_score"),
                "away_score":     d.get("away_score"),
                "pts":            d.get("pts"),
                "reb":            d.get("reb"),
                "ast":            d.get("ast"),
            })
        return jsonify({"performances": result, "has_more": len(result) == limit})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GET /api/reviews/recent
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/reviews/recent")
def get_recent_reviews():
    limit        = min(int(request.args.get("limit", 20)), 100)
    offset       = int(request.args.get("offset", 0))
    friends_only = request.args.get("friends") in ("1", "true")
    user         = current_user()
    user_id      = user["id"] if user else None

    if friends_only and not user_id:
        return jsonify({"reviews": [], "total": 0, "has_more": False})

    # JOIN clause that restricts to accepted friends of user_id
    friends_join   = ""
    friends_params = []
    if friends_only:
        friends_join = """
            JOIN friendships fr ON (
                (fr.sender_id = %s AND fr.receiver_id = gr.user_id)
                OR (fr.receiver_id = %s AND fr.sender_id = gr.user_id)
            ) AND fr.status = 'accepted'
        """
        friends_params = [user_id, user_id]

    # WHERE clause that excludes blocked users
    block_where  = ""
    block_params = []
    if user_id:
        block_where  = "AND gr.user_id NOT IN (SELECT blocked_id FROM user_blocks WHERE blocker_id = %s)"
        block_params = [user_id]

    try:
        conn = get_conn()
        cur  = conn.cursor()
        if user_id:
            cur.execute(f"""
                SELECT
                    gr.id, gr.game_id, gr.rating, gr.review_text,
                    gr.created_at, gr.updated_at,
                    COALESCE(gr.tags, '[]'::jsonb) AS tags,
                    gr.attended,
                    u.id AS user_id, u.display_name, u.avatar_url, u.favorite_team, u.is_pro, u.xp, u.equipped_ring, u.equipped_title,
                    g.game_date, g.home_team_abbr, g.away_team_abbr,
                    g.home_score, g.away_score,
                    COUNT(rl.review_id)                        AS like_count,
                    BOOL_OR(rl_me.user_id IS NOT NULL)         AS liked_by_me,
                    (SELECT COUNT(*) FROM review_replies rr WHERE rr.review_id = gr.id) AS reply_count
                FROM game_reviews gr
                JOIN users u  ON gr.user_id = u.id
                JOIN games g  ON gr.game_id = g.game_id
                LEFT JOIN review_likes rl    ON rl.review_id    = gr.id
                LEFT JOIN review_likes rl_me ON rl_me.review_id = gr.id
                                            AND rl_me.user_id   = %s
                {friends_join}
                WHERE 1=1 {block_where}
                GROUP BY gr.id, u.id, g.game_id
                ORDER BY gr.created_at DESC
                LIMIT %s OFFSET %s
            """, friends_params + [user_id] + block_params + [limit, offset])
        else:
            cur.execute(f"""
                SELECT
                    gr.id, gr.game_id, gr.rating, gr.review_text,
                    gr.created_at, gr.updated_at,
                    COALESCE(gr.tags, '[]'::jsonb) AS tags,
                    gr.attended,
                    u.id AS user_id, u.display_name, u.avatar_url, u.favorite_team, u.is_pro, u.xp, u.equipped_ring, u.equipped_title,
                    g.game_date, g.home_team_abbr, g.away_team_abbr,
                    g.home_score, g.away_score,
                    COUNT(rl.review_id) AS like_count,
                    FALSE               AS liked_by_me,
                    (SELECT COUNT(*) FROM review_replies rr WHERE rr.review_id = gr.id) AS reply_count
                FROM game_reviews gr
                JOIN users u  ON gr.user_id = u.id
                JOIN games g  ON gr.game_id = g.game_id
                LEFT JOIN review_likes rl ON rl.review_id = gr.id
                {friends_join}
                GROUP BY gr.id, u.id, g.game_id
                ORDER BY gr.created_at DESC
                LIMIT %s OFFSET %s
            """, friends_params + [limit, offset])
        rows = cur.fetchall()
        cur.execute(f"SELECT COUNT(*) FROM game_reviews gr {friends_join}", friends_params)
        total = cur.fetchone()["count"]
        cur.close(); conn.close()
        result = []
        for r in rows:
            d = dict(r)
            result.append({
                **_format_review(d),
                "game_date":      str(d["game_date"]),
                "home_team_abbr": d["home_team_abbr"],
                "away_team_abbr": d["away_team_abbr"],
                "home_score":     d["home_score"],
                "away_score":     d["away_score"],
            })
        return jsonify({"reviews": result, "total": total,
                        "has_more": offset + len(result) < total})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GET /api/feed  — unified stream: game reviews + performance reviews
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/feed")
def get_feed():
    limit        = min(int(request.args.get("limit", 20)), 100)
    offset       = int(request.args.get("offset", 0))
    friends_only  = request.args.get("friends") in ("1", "true")
    include_lists = request.args.get("include_lists") in ("1", "true")
    user          = current_user()
    user_id       = user["id"] if user else None

    if friends_only and not user_id:
        return jsonify({"items": [], "has_more": False})

    game_friends_join   = ""
    game_friends_params = []
    perf_friends_join   = ""
    perf_friends_params = []
    if friends_only:
        game_friends_join = """
            JOIN friendships fr ON (
                (fr.sender_id = %s AND fr.receiver_id = gr.user_id)
                OR (fr.receiver_id = %s AND fr.sender_id = gr.user_id)
            ) AND fr.status = 'accepted'
        """
        game_friends_params = [user_id, user_id]
        perf_friends_join = """
            JOIN friendships fr ON (
                (fr.sender_id = %s AND fr.receiver_id = pr.user_id)
                OR (fr.receiver_id = %s AND fr.sender_id = pr.user_id)
            ) AND fr.status = 'accepted'
        """
        perf_friends_params = [user_id, user_id]

    game_block_where  = ""
    game_block_params = []
    perf_block_where  = ""
    perf_block_params = []
    if user_id:
        game_block_where  = "AND gr.user_id NOT IN (SELECT blocked_id FROM user_blocks WHERE blocker_id = %s)"
        game_block_params = [user_id]
        perf_block_where  = "AND pr.user_id NOT IN (SELECT blocked_id FROM user_blocks WHERE blocker_id = %s)"
        perf_block_params = [user_id]

    # Lists are an opt-in feed arm: older app builds only understand game/performance
    # reviews and would mis-decode a "list" row, so only newer clients ask for them.
    list_arm            = ""
    list_friends_params = []
    list_block_params   = []
    if include_lists:
        item_count_expr = (
            "(  (SELECT COUNT(*) FROM game_list_items   WHERE list_id = gl.id)"
            " + (SELECT COUNT(*) FROM player_list_items WHERE list_id = gl.id)"
            " + (SELECT COUNT(*) FROM jersey_list_items WHERE list_id = gl.id)"
            " + (SELECT COUNT(*) FROM team_list_items   WHERE list_id = gl.id) )"
        )
        list_friends_join = ""
        if friends_only:
            list_friends_join = """
                JOIN friendships fr ON (
                    (fr.sender_id = %s AND fr.receiver_id = gl.user_id)
                    OR (fr.receiver_id = %s AND fr.sender_id = gl.user_id)
                ) AND fr.status = 'accepted'
            """
            list_friends_params = [user_id, user_id]
        list_block_where = ""
        if user_id:
            list_block_where = "AND gl.user_id NOT IN (SELECT blocked_id FROM user_blocks WHERE blocker_id = %s)"
            list_block_params = [user_id]
        list_arm = f"""
            UNION ALL

            SELECT
                'list'::text                         AS type,
                gl.id,
                NULL::text                           AS game_id,
                NULL::integer                        AS person_id,
                NULL::text                           AS player_name,
                gl.user_id,
                NULL::integer                        AS rating,
                NULL::numeric                        AS stars,
                NULL::text                           AS review_text,
                '[]'::jsonb                          AS tags,
                FALSE                                AS attended,
                gl.created_at,
                u.display_name, u.avatar_url, u.favorite_team,
                u.is_pro, u.xp, u.equipped_ring, u.equipped_title,
                NULL::date                           AS game_date,
                NULL::text                           AS home_team_abbr,
                NULL::text                           AS away_team_abbr,
                NULL::integer                        AS home_score,
                NULL::integer                        AS away_score,
                0::bigint                            AS like_count,
                FALSE                                AS liked_by_me,
                0::bigint                            AS reply_count,
                gl.title                             AS list_title,
                gl.description                       AS list_description,
                {item_count_expr}                    AS list_item_count,
                COALESCE(gl.list_type, 'games')      AS list_type,
                COALESCE(gl.is_ranked, FALSE)        AS list_is_ranked,
                NULL::integer                        AS pts,
                NULL::integer                        AS reb,
                NULL::integer                        AS ast
            FROM game_lists gl
            JOIN users u ON gl.user_id = u.id
            {list_friends_join}
            WHERE gl.is_public = TRUE {list_block_where}
              AND {item_count_expr} > 0
        """

    if user_id:
        rl_me_join      = "LEFT JOIN review_likes rl_me ON rl_me.review_id = gr.id AND rl_me.user_id = %s"
        liked_by_me_col = "BOOL_OR(rl_me.user_id IS NOT NULL) AS liked_by_me"
        rl_me_params    = [user_id]
    else:
        rl_me_join      = ""
        liked_by_me_col = "FALSE AS liked_by_me"
        rl_me_params    = []

    sql = f"""
        WITH combined AS (
            SELECT
                'game_review'::text                  AS type,
                gr.id,
                gr.game_id,
                NULL::integer                        AS person_id,
                NULL::text                           AS player_name,
                gr.user_id,
                gr.rating,
                round(gr.rating / 2.0, 1)            AS stars,
                gr.review_text,
                COALESCE(gr.tags, '[]'::jsonb)       AS tags,
                gr.attended,
                gr.created_at,
                u.display_name, u.avatar_url, u.favorite_team,
                u.is_pro, u.xp, u.equipped_ring, u.equipped_title,
                g.game_date, g.home_team_abbr, g.away_team_abbr,
                g.home_score, g.away_score,
                COUNT(rl.review_id)                  AS like_count,
                {liked_by_me_col},
                (SELECT COUNT(*) FROM review_replies rr WHERE rr.review_id = gr.id) AS reply_count,
                NULL::text     AS list_title,
                NULL::text     AS list_description,
                NULL::bigint   AS list_item_count,
                NULL::text     AS list_type,
                NULL::boolean  AS list_is_ranked,
                NULL::integer  AS pts,
                NULL::integer  AS reb,
                NULL::integer  AS ast
            FROM game_reviews gr
            JOIN users u  ON gr.user_id = u.id
            JOIN games g  ON gr.game_id = g.game_id
            LEFT JOIN review_likes rl ON rl.review_id = gr.id
            {rl_me_join}
            {game_friends_join}
            WHERE 1=1 {game_block_where}
            GROUP BY gr.id, u.id, g.game_id

            UNION ALL

            SELECT
                'performance_review'::text           AS type,
                pr.id,
                pr.game_id,
                pr.person_id,
                COALESCE(pr.player_name, '')         AS player_name,
                pr.user_id,
                pr.rating,
                round(pr.rating / 2.0, 1)            AS stars,
                pr.review_text,
                '[]'::jsonb                          AS tags,
                FALSE                                AS attended,
                pr.created_at,
                u.display_name, u.avatar_url, u.favorite_team,
                u.is_pro, u.xp, u.equipped_ring, u.equipped_title,
                g.game_date, g.home_team_abbr, g.away_team_abbr,
                g.home_score, g.away_score,
                0::bigint                            AS like_count,
                FALSE                                AS liked_by_me,
                0::bigint                            AS reply_count,
                NULL::text     AS list_title,
                NULL::text     AS list_description,
                NULL::bigint   AS list_item_count,
                NULL::text     AS list_type,
                NULL::boolean  AS list_is_ranked,
                COALESCE(pgl.pts, wgs.pts)            AS pts,
                COALESCE(pgl.reb, wgs.reb)            AS reb,
                COALESCE(pgl.ast, wgs.ast)            AS ast
            FROM performance_reviews pr
            JOIN users u ON pr.user_id = u.id
            LEFT JOIN games g ON pr.game_id = g.game_id
            LEFT JOIN player_gamelogs pgl ON pgl.game_id = pr.game_id AND pgl.player_id = pr.person_id
            LEFT JOIN wnba_player_game_stats wgs ON wgs.game_id = pr.game_id AND wgs.player_id = pr.person_id
            {perf_friends_join}
            WHERE 1=1 {perf_block_where}
            {list_arm}
        )
        SELECT * FROM combined
        ORDER BY created_at DESC
        LIMIT %s OFFSET %s
    """

    params = (
        rl_me_params +
        game_friends_params +
        game_block_params +
        perf_friends_params +
        perf_block_params +
        list_friends_params +
        list_block_params +
        [limit, offset]
    )

    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute(sql, params)
        rows = [dict(r) for r in cur.fetchall()]
        for r in rows:
            if r["type"] == "list":
                r["cover_items"] = _list_cover_items(cur, r["id"], r["list_type"], r["list_is_ranked"])
        cur.close(); conn.close()
        return jsonify({"items": _format_feed_rows(rows), "has_more": len(rows) == limit})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GET /api/lists/browse — all public lists, not just friends' (cold discovery)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/lists/browse")
def browse_lists():
    limit  = min(int(request.args.get("limit", 20)), 100)
    offset = int(request.args.get("offset", 0))
    sort   = request.args.get("sort", "popular").lower().strip()
    user      = current_user()
    user_id   = user["id"] if user else None

    item_count_expr = (
        "(  (SELECT COUNT(*) FROM game_list_items   WHERE list_id = gl.id)"
        " + (SELECT COUNT(*) FROM player_list_items WHERE list_id = gl.id)"
        " + (SELECT COUNT(*) FROM jersey_list_items WHERE list_id = gl.id)"
        " + (SELECT COUNT(*) FROM performance_list_items WHERE list_id = gl.id)"
        " + (SELECT COUNT(*) FROM team_list_items   WHERE list_id = gl.id) )"
    )
    like_count_expr = "(SELECT COUNT(*) FROM list_likes WHERE list_id = gl.id)"
    block_where  = ""
    block_params = []
    if user_id:
        block_where  = "AND gl.user_id NOT IN (SELECT blocked_id FROM user_blocks WHERE blocker_id = %s)"
        block_params = [user_id]

    # "popular" ranks by like count (most-liked first), recency as tiebreaker;
    # "recent" is the original recency-only ordering.
    order_by = ("list_like_count DESC, gl.created_at DESC" if sort == "popular"
                else "gl.created_at DESC")

    sql = f"""
        SELECT
            'list'::text                    AS type,
            gl.id, gl.user_id, gl.created_at,
            gl.title                        AS list_title,
            gl.description                  AS list_description,
            COALESCE(gl.list_type, 'games')  AS list_type,
            COALESCE(gl.is_ranked, FALSE)    AS list_is_ranked,
            u.display_name, u.avatar_url, u.favorite_team,
            u.is_pro, u.xp, u.equipped_ring, u.equipped_title,
            {item_count_expr}                AS list_item_count,
            {like_count_expr}                AS list_like_count
        FROM game_lists gl
        JOIN users u ON u.id = gl.user_id
        WHERE gl.is_public = TRUE {block_where}
          AND {item_count_expr} > 0
        ORDER BY {order_by}
        LIMIT %s OFFSET %s
    """
    params = block_params + [limit, offset]

    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute(sql, params)
        rows = [dict(r) for r in cur.fetchall()]
        for r in rows:
            r["cover_items"] = _list_cover_items(cur, r["id"], r["list_type"], r["list_is_ranked"])
        cur.close(); conn.close()
        items = [_format_list_feed_item(r) for r in rows]
        return jsonify({"items": items, "has_more": len(rows) == limit})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GET /api/users/<user_id>/activity — game + performance reviews for one user
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/users/<int:user_id>/activity")
def get_user_activity(user_id):
    limit     = min(int(request.args.get("limit", 20)), 100)
    offset    = int(request.args.get("offset", 0))
    viewer    = current_user()
    viewer_id = viewer["id"] if viewer else None

    if viewer_id:
        rl_me_join      = "LEFT JOIN review_likes rl_me ON rl_me.review_id = gr.id AND rl_me.user_id = %s"
        liked_by_me_col = "BOOL_OR(rl_me.user_id IS NOT NULL) AS liked_by_me"
        rl_me_params    = [viewer_id]
    else:
        rl_me_join      = ""
        liked_by_me_col = "FALSE AS liked_by_me"
        rl_me_params    = []

    sql = f"""
        WITH combined AS (
            SELECT
                'game_review'::text                  AS type,
                gr.id,
                gr.game_id,
                NULL::integer                        AS person_id,
                NULL::text                           AS player_name,
                gr.user_id,
                gr.rating,
                round(gr.rating / 2.0, 1)            AS stars,
                gr.review_text,
                COALESCE(gr.tags, '[]'::jsonb)       AS tags,
                gr.attended,
                gr.created_at,
                u.display_name, u.avatar_url, u.favorite_team,
                u.is_pro, u.xp, u.equipped_ring, u.equipped_title,
                g.game_date, g.home_team_abbr, g.away_team_abbr,
                g.home_score, g.away_score,
                COUNT(rl.review_id)                  AS like_count,
                {liked_by_me_col},
                (SELECT COUNT(*) FROM review_replies rr WHERE rr.review_id = gr.id) AS reply_count,
                NULL::integer                         AS pts,
                NULL::integer                         AS reb,
                NULL::integer                         AS ast
            FROM game_reviews gr
            JOIN users u  ON gr.user_id = u.id
            JOIN games g  ON gr.game_id = g.game_id
            LEFT JOIN review_likes rl ON rl.review_id = gr.id
            {rl_me_join}
            WHERE gr.user_id = %s
            GROUP BY gr.id, u.id, g.game_id

            UNION ALL

            SELECT
                'performance_review'::text           AS type,
                pr.id,
                pr.game_id,
                pr.person_id,
                COALESCE(pr.player_name, '')         AS player_name,
                pr.user_id,
                pr.rating,
                round(pr.rating / 2.0, 1)            AS stars,
                pr.review_text,
                '[]'::jsonb                          AS tags,
                FALSE                                AS attended,
                pr.created_at,
                u.display_name, u.avatar_url, u.favorite_team,
                u.is_pro, u.xp, u.equipped_ring, u.equipped_title,
                g.game_date, g.home_team_abbr, g.away_team_abbr,
                g.home_score, g.away_score,
                0::bigint                            AS like_count,
                FALSE                                AS liked_by_me,
                0::bigint                            AS reply_count,
                COALESCE(pgl.pts, wgs.pts)            AS pts,
                COALESCE(pgl.reb, wgs.reb)            AS reb,
                COALESCE(pgl.ast, wgs.ast)            AS ast
            FROM performance_reviews pr
            JOIN users u ON pr.user_id = u.id
            LEFT JOIN games g ON pr.game_id = g.game_id
            LEFT JOIN player_gamelogs pgl ON pgl.game_id = pr.game_id AND pgl.player_id = pr.person_id
            LEFT JOIN wnba_player_game_stats wgs ON wgs.game_id = pr.game_id AND wgs.player_id = pr.person_id
            WHERE pr.user_id = %s
        )
        SELECT * FROM combined
        ORDER BY COALESCE(game_date, created_at::date) DESC, created_at DESC
        LIMIT %s OFFSET %s
    """

    params = rl_me_params + [user_id, user_id, limit, offset]

    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()
        cur.close(); conn.close()
        return jsonify({"items": _format_feed_rows(rows), "has_more": len(rows) == limit})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def _format_list_feed_item(d: dict) -> dict:
    return {
        "type":                 "list",
        "id":                   d["id"],
        "user_id":              d["user_id"],
        "display_name":         d.get("display_name", ""),
        "avatar_url":           d.get("avatar_url") or "",
        "favorite_team":        d.get("favorite_team") or "",
        "is_pro":               bool(d.get("is_pro", False)),
        "xp":                   d.get("xp"),
        "equipped_ring":        d.get("equipped_ring"),
        "equipped_title":       d.get("equipped_title"),
        "created_at":           str(d["created_at"]),
        "list_title":           d.get("list_title"),
        "list_description":     d.get("list_description"),
        "list_item_count":      int(d.get("list_item_count") or 0),
        "list_like_count":      int(d.get("list_like_count") or 0),
        "list_type":            d.get("list_type") or "games",
        "list_is_ranked":       bool(d.get("list_is_ranked", False)),
        "ball_knowledge_level": _xp_to_level(int(d.get("xp") or 0)),
        "cover_items":          d.get("cover_items") or [],
    }


def _format_feed_rows(rows) -> list:
    items = []
    for r in rows:
        d = dict(r)
        if d["type"] == "list":
            items.append(_format_list_feed_item(d))
            continue
        items.append({
            "type":               d["type"],
            "id":                 d["id"],
            "game_id":            d["game_id"],
            "person_id":          d.get("person_id"),
            "player_name":        d.get("player_name"),
            "user_id":            d["user_id"],
            "display_name":       d.get("display_name", ""),
            "avatar_url":         d.get("avatar_url") or "",
            "favorite_team":      d.get("favorite_team") or "",
            "is_pro":             bool(d.get("is_pro", False)),
            "xp":                 d.get("xp"),
            "equipped_ring":      d.get("equipped_ring"),
            "equipped_title":     d.get("equipped_title"),
            "rating":             d["rating"],
            "stars":              float(d["stars"]),
            "review_text":        d.get("review_text"),
            "tags":               d.get("tags") or [],
            "attended":           bool(d.get("attended", False)),
            "created_at":         str(d["created_at"]),
            "game_date":          str(d["game_date"]) if d.get("game_date") else None,
            "home_team_abbr":     d.get("home_team_abbr"),
            "away_team_abbr":     d.get("away_team_abbr"),
            "home_score":         d.get("home_score"),
            "away_score":         d.get("away_score"),
            "like_count":         int(d.get("like_count", 0)),
            "liked_by_me":        bool(d.get("liked_by_me", False)),
            "reply_count":        int(d.get("reply_count", 0)),
            "ball_knowledge_level": _xp_to_level(int(d.get("xp") or 0)),
            "pts":                d.get("pts"),
            "reb":                d.get("reb"),
            "ast":                d.get("ast"),
        })
    return items


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GET /api/users/<user_id>/reviews
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/users/<int:user_id>/reviews")
def get_user_reviews(user_id):
    limit       = min(int(request.args.get("limit", 20)), 100)
    offset      = int(request.args.get("offset", 0))
    sort        = request.args.get("sort", "date_desc")
    team        = request.args.get("team", "").strip()
    attended    = request.args.get("attended", "")
    season      = request.args.get("season", "").strip()
    season_type = request.args.get("season_type", "").strip()

    conditions = ["gr.user_id = %s"]
    params: list = [user_id]

    if team:
        conditions.append("(g.home_team_abbr = %s OR g.away_team_abbr = %s)")
        params += [team, team]
    if attended == "true":
        conditions.append("gr.attended = TRUE")
    if season:
        conditions.append("g.season = %s")
        params.append(season)
    if season_type:
        conditions.append("g.season_type = %s")
        params.append(season_type)

    where = " AND ".join(conditions)

    order_map = {
        "date_desc":   "g.game_date DESC",
        "date_asc":    "g.game_date ASC",
        "rating_desc": "gr.rating DESC, g.game_date DESC",
        "rating_asc":  "gr.rating ASC, g.game_date DESC",
    }
    order = order_map.get(sort, "g.game_date DESC")

    viewer    = current_user()
    viewer_id = viewer["id"] if viewer else -1

    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute(f"""
            SELECT
                gr.*, u.display_name, u.avatar_url, u.favorite_team, u.is_pro, u.xp, u.equipped_ring, u.equipped_title,
                g.game_date, g.home_team_abbr, g.away_team_abbr,
                g.home_score, g.away_score, g.season, g.season_type,
                (SELECT COUNT(*) FROM review_replies rr WHERE rr.review_id = gr.id) AS reply_count,
                (SELECT COUNT(*) FROM review_likes  rl WHERE rl.review_id = gr.id) AS like_count,
                EXISTS(SELECT 1 FROM review_likes rl WHERE rl.review_id = gr.id AND rl.user_id = %s) AS liked_by_me
            FROM game_reviews gr
            JOIN users u ON gr.user_id = u.id
            JOIN games g ON gr.game_id = g.game_id
            WHERE {where}
            ORDER BY {order}
            LIMIT %s OFFSET %s
        """, [viewer_id] + params + [limit, offset])
        rows = cur.fetchall()
        cur.execute(f"""
            SELECT COUNT(*) FROM game_reviews gr
            JOIN games g ON gr.game_id = g.game_id
            WHERE {where}
        """, params)
        total = cur.fetchone()["count"]
        cur.close(); conn.close()
        result = []
        for r in rows:
            d = dict(r)
            result.append({
                **_format_review(d),
                "game_date":      str(d["game_date"]),
                "home_team_abbr": d["home_team_abbr"],
                "away_team_abbr": d["away_team_abbr"],
                "home_score":     d["home_score"],
                "away_score":     d["away_score"],
                "season":         d["season"],
                "season_type":    d["season_type"],
            })
        return jsonify({"reviews": result, "total": total})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GET /api/users/<user_id>/games  — watch-based diary log
# Every game the user marked watched, with a rating overlaid when present.
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/users/<int:user_id>/games")
def get_user_games(user_id):
    limit       = min(int(request.args.get("limit", 20)), 100)
    offset      = int(request.args.get("offset", 0))
    sort        = request.args.get("sort", "date_desc")
    log_filter  = request.args.get("filter", "all").lower().strip()
    team        = request.args.get("team", "").strip()
    season      = request.args.get("season", "").strip()
    season_type = request.args.get("season_type", "").strip()
    attended    = request.args.get("attended", "")

    conditions = ["gw.user_id = %s"]
    params: list = [user_id]

    if log_filter == "rated":
        conditions.append("gr.id IS NOT NULL")
    elif log_filter == "reviewed":
        conditions.append("gr.review_text IS NOT NULL AND gr.review_text <> ''")
    if attended == "true":
        conditions.append("gr.attended = TRUE")
    if team:
        conditions.append("(g.home_team_abbr = %s OR g.away_team_abbr = %s)")
        params += [team, team]
    if season:
        conditions.append("g.season = %s")
        params.append(season)
    if season_type:
        conditions.append("g.season_type = %s")
        params.append(season_type)

    where = " AND ".join(conditions)

    # Unrated (watched-only) games have no rating; keep them last on rating sorts.
    order_map = {
        "date_desc":   "g.game_date DESC",
        "date_asc":    "g.game_date ASC",
        "logged_desc": "gw.created_at DESC",
        "rating_desc": "gr.rating DESC NULLS LAST, g.game_date DESC",
        "rating_asc":  "gr.rating ASC NULLS LAST, g.game_date DESC",
    }
    order = order_map.get(sort, "g.game_date DESC")

    viewer    = current_user()
    viewer_id = viewer["id"] if viewer else -1

    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute(f"""
            SELECT
                gw.game_id, gw.created_at AS watched_at,
                gr.id AS review_id, gr.rating, gr.review_text, gr.attended,
                COALESCE(gr.tags, '[]'::jsonb) AS tags,
                gr.created_at AS reviewed_at, gr.updated_at,
                u.display_name, u.avatar_url, u.favorite_team, u.is_pro, u.xp,
                u.equipped_ring, u.equipped_title,
                g.game_date, g.home_team_abbr, g.away_team_abbr,
                g.home_score, g.away_score, g.season, g.season_type,
                (SELECT COUNT(*) FROM review_replies rr WHERE rr.review_id = gr.id) AS reply_count,
                (SELECT COUNT(*) FROM review_likes  rl WHERE rl.review_id = gr.id) AS like_count,
                EXISTS(SELECT 1 FROM review_likes rl WHERE rl.review_id = gr.id AND rl.user_id = %s) AS liked_by_me
            FROM game_watches gw
            JOIN users u ON u.id = gw.user_id
            JOIN games g ON g.game_id = gw.game_id
            LEFT JOIN game_reviews gr ON gr.user_id = gw.user_id AND gr.game_id = gw.game_id
            WHERE {where}
            ORDER BY {order}
            LIMIT %s OFFSET %s
        """, [viewer_id] + params + [limit, offset])
        rows = cur.fetchall()
        cur.execute(f"""
            SELECT COUNT(*)
            FROM game_watches gw
            JOIN games g ON g.game_id = gw.game_id
            LEFT JOIN game_reviews gr ON gr.user_id = gw.user_id AND gr.game_id = gw.game_id
            WHERE {where}
        """, params)
        total = cur.fetchone()["count"]
        cur.close(); conn.close()

        result = []
        for r in rows:
            d = dict(r)
            is_rated    = d["review_id"] is not None
            is_reviewed = is_rated and bool((d.get("review_text") or "").strip())
            entry = {
                "game_id":        d["game_id"],
                "watched_at":     str(d["watched_at"]),
                "game_date":      str(d["game_date"]),
                "home_team_abbr": d["home_team_abbr"],
                "away_team_abbr": d["away_team_abbr"],
                "home_score":     d["home_score"],
                "away_score":     d["away_score"],
                "season":         d["season"],
                "season_type":    d["season_type"],
                "display_name":   d.get("display_name", ""),
                "avatar_url":     d.get("avatar_url") or "",
                "favorite_team":  d.get("favorite_team") or "",
                "is_pro":         bool(d.get("is_pro", False)),
                "ball_knowledge_level": _xp_to_level(int(d.get("xp") or 0)),
                "equipped_ring":  d.get("equipped_ring"),
                "equipped_title": d.get("equipped_title"),
                "is_rated":       is_rated,
                "is_reviewed":    is_reviewed,
                "user_id":        user_id,
            }
            if is_rated:
                entry.update({
                    "review_id":   d["review_id"],
                    "rating":      d["rating"],
                    "stars":       d["rating"] / 2,
                    "review_text": d.get("review_text"),
                    "attended":    bool(d.get("attended", False)),
                    "tags":        d.get("tags") or [],
                    "created_at":  str(d.get("reviewed_at", "")),
                    "updated_at":  str(d.get("updated_at", "")),
                    "like_count":  int(d.get("like_count", 0)),
                    "liked_by_me": bool(d.get("liked_by_me", False)),
                    "reply_count": int(d.get("reply_count", 0)),
                })
            result.append(entry)
        return jsonify({"games": result, "total": total})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# get_user_profile moved to profile_routes.py


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GET /api/admin/check  — confirm admin status (for frontend)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/admin/check")
def admin_check():
    user = current_user()
    return jsonify({"is_admin": _is_admin(user)})


@app.route("/api/admin/xp/migrate-likes", methods=["POST"])
@login_required
def admin_migrate_likes_xp():
    """One-time backfill: grant 5 XP to review authors for all existing likes."""
    user = current_user()
    if not _is_admin(user):
        return jsonify({"error": "forbidden"}), 403
    try:
        conn = get_conn()
        cur  = conn.cursor()
        # Find all likes that haven't generated an xp_event yet
        cur.execute("""
            SELECT rl.user_id AS liker_id, rl.review_id, gr.user_id AS author_id, rl.created_at
            FROM review_likes rl
            JOIN game_reviews gr ON gr.id = rl.review_id
            WHERE gr.user_id != rl.user_id
              AND NOT EXISTS (
                  SELECT 1 FROM xp_events xe
                  WHERE xe.user_id    = gr.user_id
                    AND xe.event_type = 'review_like'
                    AND xe.reference_id = (rl.review_id::text || ':' || rl.user_id::text)
              )
        """)
        rows = cur.fetchall()
        granted = 0
        for row in rows:
            ref = f"{row['review_id']}:{row['liker_id']}"
            cur.execute(
                "INSERT INTO xp_events (user_id, event_type, reference_id, xp_amount, created_at) "
                "VALUES (%s, 'review_like', %s, 5, %s)",
                (row["author_id"], ref, row["created_at"])
            )
            granted += 1
        # Recompute xp for all affected authors from xp_events (source of truth)
        cur.execute("""
            UPDATE users u
            SET xp = COALESCE((SELECT SUM(xp_amount) FROM xp_events WHERE user_id = u.id), 0)
            WHERE EXISTS (SELECT 1 FROM xp_events WHERE user_id = u.id)
        """)
        conn.commit()
        cur.close(); conn.close()
        return jsonify({"granted": granted, "message": f"Backfilled {granted} like XP events."})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Page routes ───────────────────────────────────────────────────
@app.route("/reviews")
@app.route("/reviews.html")
def reviews_page():
    return app.send_static_file("reviews.html")

@app.route("/admin")
@app.route("/admin.html")
def admin_page():
    return app.send_static_file("admin.html")

# ── Run ───────────────────────────────────────────────────────


# ── RevenueCat webhook ────────────────────────────────────────
def _log_revenue_event(event: dict):
    """Best-effort append of a RevenueCat event to revenue_events.

    Never raises — revenue history must not jeopardise the webhook's 200 to
    RevenueCat (a non-200 makes RevenueCat retry/alert). Captures every event
    type (including anonymous / unconcerned ones) so MRR/churn history accrues.
    Idempotent on the RevenueCat event id.
    """
    try:
        from datetime import datetime as _dt, timezone as _tz
        try:
            uid = int(event.get("app_user_id"))
        except (ValueError, TypeError):
            uid = None
        ts_ms = event.get("event_timestamp_ms") or event.get("purchased_at_ms")
        event_at = _dt.fromtimestamp(ts_ms / 1000, tz=_tz.utc) if ts_ms else None
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS revenue_events (
                id           SERIAL PRIMARY KEY,
                event_id     TEXT UNIQUE,
                event_type   TEXT NOT NULL,
                app_user_id  TEXT,
                user_id      INTEGER,
                product_id   TEXT,
                store        TEXT,
                environment  TEXT,
                period_type  TEXT,
                price        REAL,
                currency     TEXT,
                event_at     TIMESTAMPTZ,
                payload      JSONB,
                created_at   TIMESTAMPTZ DEFAULT NOW()
            )""")
        cur.execute("""
            INSERT INTO revenue_events
                (event_id, event_type, app_user_id, user_id, product_id, store,
                 environment, period_type, price, currency, event_at, payload)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (event_id) DO NOTHING""",
            (event.get("id"), event.get("type", ""),
             str(event.get("app_user_id") or ""), uid,
             event.get("product_id"), event.get("store"),
             event.get("environment"), event.get("period_type"),
             event.get("price"), event.get("currency"), event_at,
             json.dumps(event)))
        conn.commit()
        cur.close(); conn.close()
    except Exception as e:
        print(f"[webhook] revenue_events log failed (non-fatal): {e}", flush=True)


@app.route("/api/webhooks/revenuecat", methods=["POST"])
def revenuecat_webhook():
    """
    RevenueCat sends events here when a subscription starts, renews, expires, etc.
    We use the app_user_id (our user's numeric ID) to update is_pro in the DB.
    Verify the shared secret via the Authorization header.
    """
    secret = os.getenv("REVENUECAT_WEBHOOK_SECRET", "")
    if secret and request.headers.get("Authorization") != secret:
        return jsonify({"error": "unauthorized"}), 401

    body  = request.get_json(force=True, silent=True) or {}
    event = body.get("event", {})
    event_type   = event.get("type", "")
    app_user_id  = event.get("app_user_id", "")

    print(f"[webhook] revenuecat event_type={event_type!r} app_user_id={app_user_id!r}", flush=True)

    # Log every event for revenue history (best-effort; before any early return)
    _log_revenue_event(event)

    # app_user_id is the string we passed to Purchases.logIn — our user's numeric ID
    try:
        user_id = int(app_user_id)
    except (ValueError, TypeError):
        print(f"[webhook] non-integer app_user_id, ignoring: {app_user_id!r}", flush=True)
        return jsonify({"ok": True})  # anonymous / non-integer ID, ignore

    # Events that mean the user has an active subscription
    active_events = {
        "INITIAL_PURCHASE", "RENEWAL", "REACTIVATION",
        "PRODUCT_CHANGE", "TRANSFER",
    }
    # Events that mean the subscription is no longer active
    inactive_events = {
        "CANCELLATION", "EXPIRATION", "BILLING_ISSUE",
        "SUBSCRIBER_ALIAS",
    }

    if event_type in active_events:
        is_pro = True
    elif event_type in inactive_events:
        is_pro = False
    else:
        return jsonify({"ok": True})  # unconcerned event type

    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("UPDATE users SET is_pro = %s WHERE id = %s", (is_pro, user_id))
        conn.commit()
        cur.close(); conn.close()
    except Exception as e:
        print(f"[webhook] revenuecat DB error: {e}", flush=True)
        return jsonify({"error": "db"}), 500

    return jsonify({"ok": True})


# ── Admin: founder insights dashboard ─────────────────────────
import sys as _sys_admin
_sys_admin.path.insert(0, os.path.join(os.path.dirname(__file__), "ingest"))


def _admin_health_panel() -> dict:
    """Run the data health engine (read-only) and shape it for the dashboard."""
    import health_check  # noqa: E402  — from backend/ingest
    # The engine uses positional rows, so give it a plain (non-RealDict) connection.
    hconn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
    try:
        h = health_check.collect(hconn, write_snapshot=False)
    finally:
        hconn.close()
    ov = h.overall()
    sections = []
    for r in h.results:
        if not sections or sections[-1]["name"] != r["section"]:
            if not any(s["name"] == r["section"] for s in sections):
                sections.append({"name": r["section"], "checks": []})
        for s in sections:
            if s["name"] == r["section"]:
                s["checks"].append({"status": r["status"], "name": r["name"],
                                    "detail": r["detail"]})
                break
    todos = [{"status": r["status"], "section": r["section"], "name": r["name"],
              "detail": r["detail"]}
             for r in h.results if r["status"] in ("FAIL", "WARN")]
    todos.sort(key=lambda r: 0 if r["status"] == "FAIL" else 1)
    return {"overall": ov, "todos": todos, "sections": sections,
            "n_fail": sum(r["status"] == "FAIL" for r in h.results),
            "n_warn": sum(r["status"] == "WARN" for r in h.results)}


@app.route("/api/admin/dashboard")
@_admin_required
def admin_dashboard():
    from datetime import datetime as _dt
    conn = get_conn()
    cur = conn.cursor()

    # ── Growth ──────────────────────────────────────────────
    cur.execute("SELECT COUNT(*) AS n FROM users")
    total_users = cur.fetchone()["n"]
    cur.execute("""SELECT
        COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '1 day')  AS d1,
        COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '7 days') AS d7,
        COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '30 days')AS d30
        FROM users""")
    g = cur.fetchone()
    cur.execute("""SELECT created_at::date AS d, COUNT(*) AS n
        FROM users WHERE created_at >= NOW() - INTERVAL '14 days'
        GROUP BY 1 ORDER BY 1""")
    signups_daily = [{"date": str(r["d"]), "count": r["n"]} for r in cur.fetchall()]

    # ── Engagement (xp_events + game results) ───────────────
    cur.execute("""SELECT
        COUNT(DISTINCT user_id) FILTER (WHERE created_at >= NOW() - INTERVAL '7 days')  AS active_7d,
        COUNT(DISTINCT user_id) FILTER (WHERE created_at >= NOW() - INTERVAL '1 day')   AS active_1d,
        COUNT(*) FILTER (WHERE event_type='app_open' AND created_at >= NOW() - INTERVAL '7 days') AS opens_7d
        FROM xp_events""")
    e = cur.fetchone()
    cur.execute("""SELECT event_type, COUNT(*) AS n FROM xp_events
        WHERE created_at >= NOW() - INTERVAL '30 days'
        GROUP BY 1 ORDER BY 2 DESC""")
    events_30d = [{"type": r["event_type"], "count": r["n"]} for r in cur.fetchall()]

    def _safe_count(sql):
        try:
            cur.execute(sql)
            return cur.fetchone()["n"]
        except Exception:
            conn.rollback()
            return None

    hol_7d = _safe_count("SELECT COUNT(*) AS n FROM survival_results WHERE created_at >= NOW() - INTERVAL '7 days'")
    gw_7d  = _safe_count("SELECT COUNT(*) AS n FROM poeltl_results  WHERE created_at >= NOW() - INTERVAL '7 days'")
    rev_7d = _safe_count("SELECT COUNT(*) AS n FROM game_reviews    WHERE created_at >= NOW() - INTERVAL '7 days'")

    # ── Revenue (current snapshot from is_pro + history from revenue_events) ──
    cur.execute("SELECT COUNT(*) AS n FROM users WHERE is_pro = TRUE")
    pro_users = cur.fetchone()["n"]
    price = float(os.getenv("PRO_PRICE_USD", "4.99"))

    new_subs_7d  = _safe_count("SELECT COUNT(*) AS n FROM revenue_events WHERE event_type='INITIAL_PURCHASE' AND event_at >= NOW() - INTERVAL '7 days'")
    new_subs_30d = _safe_count("SELECT COUNT(*) AS n FROM revenue_events WHERE event_type='INITIAL_PURCHASE' AND event_at >= NOW() - INTERVAL '30 days'")
    churn_30d    = _safe_count("SELECT COUNT(*) AS n FROM revenue_events WHERE event_type IN ('CANCELLATION','EXPIRATION') AND event_at >= NOW() - INTERVAL '30 days'")
    revenue_30d  = _safe_count("SELECT COALESCE(SUM(price),0) AS n FROM revenue_events WHERE event_type IN ('INITIAL_PURCHASE','RENEWAL','PRODUCT_CHANGE') AND event_at >= NOW() - INTERVAL '30 days'")
    has_history  = (_safe_count("SELECT COUNT(*) AS n FROM revenue_events") or 0) > 0

    return jsonify({
        "generated_at": _dt.now().isoformat(timespec="seconds"),
        "health": _admin_health_panel(),
        "growth": {
            "total_users": total_users,
            "new_today": g["d1"], "new_7d": g["d7"], "new_30d": g["d30"],
            "signups_daily": signups_daily,
        },
        "engagement": {
            "active_1d": e["active_1d"], "active_7d": e["active_7d"],
            "app_opens_7d": e["opens_7d"],
            "hol_plays_7d": hol_7d, "guesswho_plays_7d": gw_7d, "reviews_7d": rev_7d,
            "events_30d": events_30d,
        },
        "revenue": {
            "pro_users": pro_users,
            "price_usd": price,
            "est_mrr": round(pro_users * price, 2),
            "has_history": has_history,
            "new_subs_7d": new_subs_7d,
            "new_subs_30d": new_subs_30d,
            "churn_30d": churn_30d,
            "revenue_30d": round(revenue_30d, 2) if revenue_30d is not None else None,
            "note": ("Live from RevenueCat events." if has_history else
                     "MRR estimated from Pro count × PRO_PRICE_USD. "
                     "Real MRR/churn history accrues from now as RevenueCat events arrive."),
        },
    })


# ── Game Lists ────────────────────────────────────────────────

def _list_is_owner(list_id: int, user_id: int, cur) -> bool:
    cur.execute("SELECT user_id FROM game_lists WHERE id = %s", (list_id,))
    row = cur.fetchone()
    return row is not None and row["user_id"] == user_id


def _perf_stat_line(d: dict) -> str:
    """"73 PTS · 10 REB · 7 AST" from a row carrying pts/reb/ast (any may be None)."""
    parts = []
    for col, lbl in (("pts", "PTS"), ("reb", "REB"), ("ast", "AST")):
        v = d.get(col)
        if v is not None:
            parts.append(f"{int(round(float(v)))} {lbl}")
    return " · ".join(parts)


def _fetch_perf_items(cur, list_id: int, ranked: bool) -> list:
    """Performance list items, enriched with the box-score stat line + game context.
    Stats come from player_gamelogs (NBA) / wnba_player_game_stats (WNBA)."""
    order = "pli.sort_order ASC NULLS LAST, pli.added_at ASC" if ranked else "pli.added_at ASC"
    cur.execute(f"""
        SELECT pli.id, pli.game_id, pli.person_id, pli.player_name, pli.league,
               pli.sort_order, pli.added_at,
               g.game_date, g.home_team_abbr, g.away_team_abbr, g.home_score, g.away_score,
               COALESCE(pgl.pts, wgs.pts) AS pts,
               COALESCE(pgl.reb, wgs.reb) AS reb,
               COALESCE(pgl.ast, wgs.ast) AS ast
        FROM performance_list_items pli
        LEFT JOIN games g ON g.game_id = pli.game_id
        LEFT JOIN player_gamelogs pgl        ON pgl.game_id = pli.game_id AND pgl.player_id = pli.person_id
        LEFT JOIN wnba_player_game_stats wgs ON wgs.game_id = pli.game_id AND wgs.player_id = pli.person_id
        WHERE pli.list_id = %s ORDER BY {order}
    """, (list_id,))
    items = []
    for r in cur.fetchall():
        d = dict(r)
        items.append({
            "id":           d["id"],
            "gameId":       d["game_id"],
            "personId":     d["person_id"],
            "playerName":   d.get("player_name") or "",
            "league":       d.get("league") or "nba",
            "sortOrder":    d.get("sort_order"),
            "addedAt":      str(d["added_at"]),
            "gameDate":     str(d["game_date"]) if d.get("game_date") else None,
            "homeTeamAbbr": d.get("home_team_abbr"),
            "awayTeamAbbr": d.get("away_team_abbr"),
            "homeScore":    d.get("home_score"),
            "awayScore":    d.get("away_score"),
            "pts":          d.get("pts"),
            "reb":          d.get("reb"),
            "ast":          d.get("ast"),
            "statLine":     _perf_stat_line(d),
        })
    return items


def _list_cover_items(cur, list_id: int, list_type: str, is_ranked: bool, limit: int = 4) -> list:
    """First few items of a list, shaped for a cover-art collage (not display rows).
    Ordering mirrors _list_preview so covers and OG images agree on 'first'."""
    items = []
    if list_type == "games":
        order = "gli.sort_order ASC NULLS LAST, gli.added_at DESC" if is_ranked else "gli.added_at DESC"
        cur.execute(f"""
            SELECT g.home_team_abbr, g.away_team_abbr, g.league
            FROM game_list_items gli LEFT JOIN games g ON g.game_id = gli.game_id
            WHERE gli.list_id = %s ORDER BY {order} LIMIT %s
        """, (list_id, limit))
        for r in cur.fetchall():
            items.append({"kind": "game", "homeTeamAbbr": r.get("home_team_abbr"),
                          "awayTeamAbbr": r.get("away_team_abbr"), "league": r.get("league") or "nba"})
    elif list_type in ("players", "player_seasons"):
        order = "sort_order ASC NULLS LAST, added_at ASC" if is_ranked else "added_at ASC"
        cur.execute(f"""
            SELECT player_id, player_name, team, league FROM player_list_items
            WHERE list_id = %s ORDER BY {order} LIMIT %s
        """, (list_id, limit))
        for r in cur.fetchall():
            items.append({"kind": "player", "playerId": r.get("player_id"), "playerName": r["player_name"],
                          "team": r.get("team"), "league": r.get("league") or "nba"})
    elif list_type in ("teams", "team_seasons"):
        order = "sort_order ASC NULLS LAST, added_at ASC" if is_ranked else "added_at ASC"
        cur.execute(f"""
            SELECT team_abbr, team_name, league FROM team_list_items
            WHERE list_id = %s ORDER BY {order} LIMIT %s
        """, (list_id, limit))
        for r in cur.fetchall():
            items.append({"kind": "team", "team": r.get("team_abbr"), "teamName": r.get("team_name"),
                          "league": r.get("league") or "nba"})
    elif list_type == "jerseys":
        order = "sort_order ASC NULLS LAST, added_at ASC" if is_ranked else "added_at ASC"
        cur.execute(f"""
            SELECT image_url, label FROM jersey_list_items
            WHERE list_id = %s ORDER BY {order} LIMIT %s
        """, (list_id, limit))
        for r in cur.fetchall():
            items.append({"kind": "jersey", "imageUrl": r.get("image_url"), "label": r.get("label")})
    elif list_type == "performances":
        order = "pli.sort_order ASC NULLS LAST, pli.added_at ASC" if is_ranked else "pli.added_at ASC"
        cur.execute(f"""
            SELECT pli.person_id, pli.player_name, pli.league,
                   g.home_team_abbr, g.away_team_abbr
            FROM performance_list_items pli LEFT JOIN games g ON g.game_id = pli.game_id
            WHERE pli.list_id = %s ORDER BY {order} LIMIT %s
        """, (list_id, limit))
        for r in cur.fetchall():
            items.append({"kind": "performance", "personId": r.get("person_id"),
                          "playerName": r["player_name"], "league": r.get("league") or "nba",
                          "homeTeamAbbr": r.get("home_team_abbr"), "awayTeamAbbr": r.get("away_team_abbr")})
    return items


def _format_list(row: dict, game_count: int = 0, cover_items: list = None) -> dict:
    return {
        "id":          row["id"],
        "userId":      row["user_id"],
        "title":       row["title"],
        "description": row.get("description"),
        "isPublic":    row["is_public"],
        "isRanked":    bool(row.get("is_ranked", False)),
        "listType":    row.get("list_type", "games") or "games",
        "gameCount":   game_count,
        "createdAt":   str(row.get("created_at", "")),
        "coverItems":  cover_items or [],
    }


# Likes + comments on lists (created lazily, like review_likes).
_LIST_SOCIAL_TABLES = """
CREATE TABLE IF NOT EXISTS list_likes (
    user_id    INTEGER REFERENCES users(id)      ON DELETE CASCADE,
    list_id    INTEGER REFERENCES game_lists(id) ON DELETE CASCADE,
    created_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (user_id, list_id)
);
CREATE TABLE IF NOT EXISTS list_comments (
    id         SERIAL PRIMARY KEY,
    list_id    INTEGER REFERENCES game_lists(id) ON DELETE CASCADE,
    user_id    INTEGER REFERENCES users(id)      ON DELETE CASCADE,
    text       TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);
"""


def _format_comment(r: dict) -> dict:
    return {
        "id":          r["id"],
        "userId":      r["user_id"],
        "displayName": r.get("display_name"),
        "avatarUrl":   r.get("avatar_url"),
        "text":        r["text"],
        "createdAt":   str(r.get("created_at", "")),
    }


@app.route("/api/me/lists", methods=["GET"])
@login_required
def get_my_lists():
    user = current_user()
    conn = get_conn(); cur = conn.cursor()
    cur.execute("""
        SELECT gl.*, COUNT(gli.game_id) AS game_count
        FROM game_lists gl
        LEFT JOIN game_list_items gli ON gli.list_id = gl.id
        WHERE gl.user_id = %s
        GROUP BY gl.id
        ORDER BY gl.updated_at DESC
    """, (user["id"],))
    rows = cur.fetchall()
    lists = [_format_list(r, r["game_count"],
                           _list_cover_items(cur, r["id"], r.get("list_type") or "games", bool(r.get("is_ranked"))))
             for r in rows]
    cur.close(); conn.close()
    return jsonify({"lists": lists})


@app.route("/api/users/<int:user_id>/lists", methods=["GET"])
def get_user_lists(user_id):
    viewer = current_user()
    conn = get_conn(); cur = conn.cursor()
    # Show all lists if viewing own profile, otherwise public only
    if viewer and viewer["id"] == user_id:
        cur.execute("""
            SELECT gl.*, COUNT(gli.game_id) AS game_count
            FROM game_lists gl
            LEFT JOIN game_list_items gli ON gli.list_id = gl.id
            WHERE gl.user_id = %s
            GROUP BY gl.id ORDER BY gl.updated_at DESC
        """, (user_id,))
    else:
        cur.execute("""
            SELECT gl.*, COUNT(gli.game_id) AS game_count
            FROM game_lists gl
            LEFT JOIN game_list_items gli ON gli.list_id = gl.id
            WHERE gl.user_id = %s AND gl.is_public = TRUE
            GROUP BY gl.id ORDER BY gl.updated_at DESC
        """, (user_id,))
    rows = cur.fetchall()
    lists = [_format_list(r, r["game_count"],
                           _list_cover_items(cur, r["id"], r.get("list_type") or "games", bool(r.get("is_ranked"))))
             for r in rows]
    cur.close(); conn.close()
    return jsonify({"lists": lists})


@app.route("/api/me/lists", methods=["POST"])
@login_required
def create_list():
    user = current_user()
    # Lists are free — public, shareable lists drive growth (were Pro-gated).
    body  = request.get_json(force=True, silent=True) or {}
    title = (body.get("title") or "").strip()
    if not title:
        return jsonify({"error": "title is required"}), 400
    if len(title) > 100:
        return jsonify({"error": "title must be 100 characters or fewer"}), 400
    desc      = (body.get("description") or "").strip() or None
    is_ranked = bool(body.get("isRanked", False))
    list_type = (body.get("listType") or "games").strip()
    if list_type not in ("games", "players", "player_seasons", "jerseys", "teams", "team_seasons", "performances"):
        list_type = "games"
    conn = get_conn(); cur = conn.cursor()
    cur.execute("""
        INSERT INTO game_lists (user_id, title, description, is_ranked, list_type)
        VALUES (%s, %s, %s, %s, %s) RETURNING *
    """, (user["id"], title, desc, is_ranked, list_type))
    row = cur.fetchone()
    conn.commit(); cur.close(); conn.close()
    return jsonify({"list": _format_list(row, 0)}), 201


@app.route("/api/lists/<int:list_id>", methods=["GET"])
def get_list_detail(list_id):
    viewer = current_user()
    conn = get_conn(); cur = conn.cursor()
    cur.execute("""SELECT gl.*, u.display_name AS creator_name, u.avatar_url AS creator_avatar
                   FROM game_lists gl JOIN users u ON u.id = gl.user_id
                   WHERE gl.id = %s""", (list_id,))
    lst = cur.fetchone()
    if not lst:
        cur.close(); conn.close()
        return jsonify({"error": "not found"}), 404
    if not lst["is_public"] and (not viewer or viewer["id"] != lst["user_id"]):
        cur.close(); conn.close()
        return jsonify({"error": "not found"}), 404

    order_clause = "ORDER BY gli.sort_order ASC NULLS LAST, gli.added_at DESC" if lst.get("is_ranked") else "ORDER BY gli.added_at DESC"
    cur.execute(f"""
        SELECT gli.game_id, gli.added_at, gli.sort_order,
               g.home_team_abbr, g.away_team_abbr,
               g.home_score, g.away_score,
               g.game_date, g.league, g.season_type
        FROM game_list_items gli
        LEFT JOIN games g ON g.game_id = gli.game_id
        WHERE gli.list_id = %s
        {order_clause}
    """, (list_id,))
    games = []
    for r in cur.fetchall():
        games.append({
            "gameId":       r["game_id"],
            "addedAt":      str(r["added_at"]),
            "sortOrder":    r.get("sort_order"),
            "homeTeamAbbr": r.get("home_team_abbr"),
            "awayTeamAbbr": r.get("away_team_abbr"),
            "homeScore":    r.get("home_score"),
            "awayScore":    r.get("away_score"),
            "gameDate":     str(r["game_date"]) if r.get("game_date") else None,
            "league":       r.get("league", "nba"),
            "seasonType":   r.get("season_type"),
        })
    # Fetch player items (for player/player_seasons lists)
    player_items = []
    list_type = lst.get("list_type") or "games"
    if list_type in ("players", "player_seasons"):
        p_order = "ORDER BY sort_order ASC NULLS LAST, added_at ASC" if lst.get("is_ranked") else "ORDER BY added_at ASC"
        cur.execute(f"""
            SELECT id, player_id, player_name, team, season, sort_order, added_at, league, stats
            FROM player_list_items WHERE list_id = %s {p_order}
        """, (list_id,))
        for r in cur.fetchall():
            player_items.append({
                "id":         r["id"],
                "playerId":   r.get("player_id"),
                "playerName": r["player_name"],
                "team":       r.get("team"),
                "season":     r.get("season"),
                "sortOrder":  r.get("sort_order"),
                "addedAt":    str(r["added_at"]),
                "league":     r.get("league") or "nba",
                "stats":      r.get("stats") or [],
            })

    # Fetch jersey items (for jersey lists)
    jersey_items = []
    if list_type == "jerseys":
        j_order = "ORDER BY sort_order ASC NULLS LAST, added_at ASC" if lst.get("is_ranked") else "ORDER BY added_at ASC"
        cur.execute(f"""
            SELECT jli.id, jli.jersey_id, jli.label, jli.image_url, jli.sort_order, jli.added_at,
                   j.team_name, j.team_slug, j.variant, j.year_range
            FROM jersey_list_items jli
            LEFT JOIN jerseys j ON j.id = jli.jersey_id
            WHERE jli.list_id = %s {j_order}
        """, (list_id,))
        for r in cur.fetchall():
            jersey_items.append({
                "id":        r["id"],
                "jerseyId":  r.get("jersey_id"),
                "label":     r["label"],
                "imageUrl":  r["image_url"],
                "teamName":  r.get("team_name"),
                "teamSlug":  r.get("team_slug"),
                "variant":   r.get("variant"),
                "yearRange": r.get("year_range"),
                "sortOrder": r.get("sort_order"),
                "addedAt":   str(r["added_at"]),
            })

    # Fetch team items (for teams/team_seasons lists)
    team_items = []
    if list_type in ("teams", "team_seasons"):
        t_order = "ORDER BY sort_order ASC NULLS LAST, added_at ASC" if lst.get("is_ranked") else "ORDER BY added_at ASC"
        cur.execute(f"""
            SELECT id, team_abbr, team_name, season, wins, losses, sort_order, added_at, league
            FROM team_list_items WHERE list_id = %s {t_order}
        """, (list_id,))
        for r in cur.fetchall():
            team_items.append({
                "id":        r["id"],
                "teamAbbr":  r["team_abbr"],
                "teamName":  r["team_name"],
                "season":    r.get("season"),
                "wins":      r.get("wins"),
                "losses":    r.get("losses"),
                "sortOrder": r.get("sort_order"),
                "addedAt":   str(r["added_at"]),
                "league":    r.get("league") or "nba",
            })

    # Fetch performance items (for performances lists)
    performance_items = []
    if list_type == "performances":
        performance_items = _fetch_perf_items(cur, list_id, bool(lst.get("is_ranked")))

    # ── Social: likes + comment count ──
    cur.execute(_LIST_SOCIAL_TABLES); conn.commit()
    cur.execute("SELECT COUNT(*) AS n FROM list_likes WHERE list_id = %s", (list_id,))
    like_count = int(cur.fetchone()["n"])
    liked_by_me = False
    if viewer:
        cur.execute("SELECT 1 FROM list_likes WHERE list_id = %s AND user_id = %s", (list_id, viewer["id"]))
        liked_by_me = cur.fetchone() is not None
    cur.execute("SELECT COUNT(*) AS n FROM list_comments WHERE list_id = %s", (list_id,))
    comment_count = int(cur.fetchone()["n"])

    cur.close(); conn.close()
    result = dict(_format_list(lst, len(games)))
    result["creatorName"] = lst.get("creator_name")
    result["creatorAvatar"] = lst.get("creator_avatar")
    result["games"] = games
    result["playerItems"] = player_items
    result["jerseyItems"] = jersey_items
    result["teamItems"] = team_items
    result["performanceItems"] = performance_items
    result["likeCount"] = like_count
    result["likedByMe"] = liked_by_me
    result["commentCount"] = comment_count
    return jsonify({"list": result})


# ━━━ Likes + comments on lists ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.route("/api/lists/<int:list_id>/like", methods=["POST"])
@login_required
def toggle_list_like(list_id):
    user = current_user()
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute(_LIST_SOCIAL_TABLES); conn.commit()
        cur.execute("SELECT 1 FROM list_likes WHERE user_id = %s AND list_id = %s", (user["id"], list_id))
        if cur.fetchone():
            cur.execute("DELETE FROM list_likes WHERE user_id = %s AND list_id = %s", (user["id"], list_id))
            liked = False
        else:
            cur.execute("INSERT INTO list_likes (user_id, list_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                        (user["id"], list_id))
            liked = True
            cur.execute("SELECT user_id FROM game_lists WHERE id = %s", (list_id,))
            owner = cur.fetchone()
            if owner and owner["user_id"] != user["id"]:
                _grant_xp(cur, owner["user_id"], "list_like", f"{list_id}:{user['id']}", 5)
        cur.execute("SELECT COUNT(*) AS n FROM list_likes WHERE list_id = %s", (list_id,))
        n = int(cur.fetchone()["n"])
        conn.commit(); cur.close(); conn.close()
        return jsonify({"liked": liked, "like_count": n})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/lists/<int:list_id>/comments", methods=["GET"])
def get_list_comments(list_id):
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute(_LIST_SOCIAL_TABLES); conn.commit()
        cur.execute("""
            SELECT lc.*, u.display_name, u.avatar_url
            FROM list_comments lc JOIN users u ON u.id = lc.user_id
            WHERE lc.list_id = %s ORDER BY lc.created_at ASC
        """, (list_id,))
        comments = [_format_comment(dict(r)) for r in cur.fetchall()]
        cur.close(); conn.close()
        return jsonify({"comments": comments})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/lists/<int:list_id>/comments", methods=["POST"])
@login_required
def add_list_comment(list_id):
    user = current_user()
    body = request.get_json(force=True, silent=True) or {}
    text = (body.get("text") or "").strip()
    if not text:
        return jsonify({"error": "text is required"}), 400
    text = text[:1000]
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute(_LIST_SOCIAL_TABLES); conn.commit()
        cur.execute("SELECT is_public, user_id FROM game_lists WHERE id = %s", (list_id,))
        lst = cur.fetchone()
        if not lst or (not lst["is_public"] and lst["user_id"] != user["id"]):
            cur.close(); conn.close()
            return jsonify({"error": "not found"}), 404
        cur.execute("INSERT INTO list_comments (list_id, user_id, text) VALUES (%s, %s, %s) RETURNING id",
                    (list_id, user["id"], text))
        new_id = cur.fetchone()["id"]
        if lst["user_id"] != user["id"]:
            _grant_xp(cur, lst["user_id"], "list_comment", f"{new_id}", 3)
        conn.commit()
        cur.execute("""
            SELECT lc.*, u.display_name, u.avatar_url
            FROM list_comments lc JOIN users u ON u.id = lc.user_id
            WHERE lc.id = %s
        """, (new_id,))
        comment = _format_comment(dict(cur.fetchone()))
        cur.close(); conn.close()
        return jsonify({"comment": comment}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/lists/<int:list_id>/comments/<int:comment_id>", methods=["DELETE"])
@login_required
def delete_list_comment(list_id, comment_id):
    user = current_user()
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute(_LIST_SOCIAL_TABLES); conn.commit()
        cur.execute("SELECT user_id FROM list_comments WHERE id = %s AND list_id = %s", (comment_id, list_id))
        c = cur.fetchone()
        if not c:
            cur.close(); conn.close()
            return jsonify({"error": "not found"}), 404
        cur.execute("SELECT user_id FROM game_lists WHERE id = %s", (list_id,))
        owner = cur.fetchone()
        # Comment author OR list owner may delete
        if c["user_id"] != user["id"] and not (owner and owner["user_id"] == user["id"]):
            cur.close(); conn.close()
            return jsonify({"error": "forbidden"}), 403
        cur.execute("DELETE FROM list_comments WHERE id = %s", (comment_id,))
        conn.commit(); cur.close(); conn.close()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Public list page + share image (growth: shareable, no login) ──────────────
def _list_preview(cur, list_id: int, lst: dict):
    """Return (labels[str], total_count) for a list's items, in display order."""
    lt = lst.get("list_type") or "games"
    ranked = lst.get("is_ranked")
    labels, total = [], 0
    if lt == "games":
        cur.execute("SELECT COUNT(*) AS n FROM game_list_items WHERE list_id=%s", (list_id,))
        total = cur.fetchone()["n"]
        order = "gli.sort_order ASC NULLS LAST, gli.added_at DESC" if ranked else "gli.added_at DESC"
        cur.execute(f"""SELECT g.away_team_abbr AS a, g.home_team_abbr AS h, g.game_date AS d
                        FROM game_list_items gli LEFT JOIN games g ON g.game_id=gli.game_id
                        WHERE gli.list_id=%s ORDER BY {order} LIMIT 6""", (list_id,))
        for r in cur.fetchall():
            mk = f"{r['a'] or '?'} @ {r['h'] or '?'}"
            labels.append(f"{mk}  ·  {r['d']}" if r.get("d") else mk)
    elif lt in ("players", "player_seasons"):
        cur.execute("SELECT COUNT(*) AS n FROM player_list_items WHERE list_id=%s", (list_id,))
        total = cur.fetchone()["n"]
        order = "sort_order ASC NULLS LAST, added_at ASC" if ranked else "added_at ASC"
        cur.execute(f"""SELECT player_name, season FROM player_list_items
                        WHERE list_id=%s ORDER BY {order} LIMIT 6""", (list_id,))
        for r in cur.fetchall():
            labels.append(f"{r['player_name']} ({r['season']})" if lt == "player_seasons" and r.get("season") else r["player_name"])
    elif lt in ("teams", "team_seasons"):
        cur.execute("SELECT COUNT(*) AS n FROM team_list_items WHERE list_id=%s", (list_id,))
        total = cur.fetchone()["n"]
        order = "sort_order ASC NULLS LAST, added_at ASC" if ranked else "added_at ASC"
        cur.execute(f"""SELECT team_name, season FROM team_list_items
                        WHERE list_id=%s ORDER BY {order} LIMIT 6""", (list_id,))
        for r in cur.fetchall():
            labels.append(f"{r['team_name']} ({r['season']})" if lt == "team_seasons" and r.get("season") else r["team_name"])
    elif lt == "jerseys":
        cur.execute("SELECT COUNT(*) AS n FROM jersey_list_items WHERE list_id=%s", (list_id,))
        total = cur.fetchone()["n"]
        order = "sort_order ASC NULLS LAST, added_at ASC" if ranked else "added_at ASC"
        cur.execute(f"""SELECT label FROM jersey_list_items
                        WHERE list_id=%s ORDER BY {order} LIMIT 6""", (list_id,))
        labels = [r["label"] for r in cur.fetchall()]
    elif lt == "performances":
        cur.execute("SELECT COUNT(*) AS n FROM performance_list_items WHERE list_id=%s", (list_id,))
        total = cur.fetchone()["n"]
        order = "sort_order ASC NULLS LAST, added_at ASC" if ranked else "added_at ASC"
        cur.execute(f"""SELECT pli.player_name, g.away_team_abbr AS a, g.home_team_abbr AS h
                        FROM performance_list_items pli LEFT JOIN games g ON g.game_id=pli.game_id
                        WHERE pli.list_id=%s ORDER BY {order} LIMIT 6""", (list_id,))
        for r in cur.fetchall():
            mk = f"{r['a'] or '?'} @ {r['h'] or '?'}"
            labels.append(f"{r['player_name']}  ·  {mk}")
    return labels, total


def _fetch_public_list(list_id: int):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("""SELECT gl.*, u.display_name FROM game_lists gl
                   JOIN users u ON u.id = gl.user_id WHERE gl.id = %s""", (list_id,))
    lst = cur.fetchone()
    if not lst or not lst.get("is_public"):
        cur.close(); conn.close()
        return None, None, None, None
    labels, total = _list_preview(cur, list_id, lst)
    cur.close(); conn.close()
    return lst, labels, total, (lst.get("display_name") or "ydkball")


@app.route("/list/<int:list_id>")
@app.route("/lists/<int:list_id>")
def public_list_page(list_id):
    import html as _html
    lst, _labels, total, creator = _fetch_public_list(list_id)
    base = request.url_root.rstrip("/")
    if lst:
        title = lst["title"]
        og_title = f"{title} — ydkball"
        og_desc = (lst.get("description") or "").strip() or \
            f"{'A ranked list' if lst.get('is_ranked') else 'A list'} by {creator} · {total} on ydkball"
        og_image = f"{base}/list/{list_id}/og.png"
    else:
        og_title = "ydkball"
        og_desc = "NBA & WNBA scores, stats, game reviews, and daily games."
        og_image = f"{base}/og-image.png"

    meta = (
        f'<title>{_html.escape(og_title)}</title>\n'
        f'<meta name="description" content="{_html.escape(og_desc)}">\n'
        f'<meta property="og:title" content="{_html.escape(og_title)}">\n'
        f'<meta property="og:description" content="{_html.escape(og_desc)}">\n'
        f'<meta property="og:image" content="{_html.escape(og_image)}">\n'
        f'<meta property="og:url" content="{_html.escape(request.url)}">\n'
        f'<meta property="og:type" content="website">\n'
        f'<meta name="twitter:card" content="summary_large_image">\n'
        f'<meta name="twitter:image" content="{_html.escape(og_image)}">'
    )
    with open(os.path.join(FRONTEND_DIR, "list.html"), encoding="utf-8") as f:
        shell = f.read()
    return shell.replace("<!--OG_TAGS-->", meta)


@app.route("/list/<int:list_id>/og.png")
def list_og_image(list_id):
    import og_image
    lst, labels, total, creator = _fetch_public_list(list_id)
    if not lst:
        return "", 404
    kicker = ("RANKED LIST" if lst.get("is_ranked") else "LIST")
    noun = "ranked list" if lst.get("is_ranked") else "list"
    subtitle = f"A {noun} by {creator}  ·  {total} item{'' if total == 1 else 's'}"
    png = og_image.render_list_card(lst["title"], subtitle, labels, kicker)
    resp = Response(png, mimetype="image/png")
    resp.headers["Cache-Control"] = "public, max-age=300"
    return resp


@app.route("/mylists")
def mylists_page():
    # Client-side gated (calls /api/me/lists, which is login_required)
    return app.send_static_file("mylists.html")


@app.route("/api/lists/<int:list_id>", methods=["PATCH"])
@login_required
def update_list(list_id):
    user = current_user()
    body  = request.get_json(force=True, silent=True) or {}
    conn = get_conn(); cur = conn.cursor()
    if not _list_is_owner(list_id, user["id"], cur):
        cur.close(); conn.close()
        return jsonify({"error": "not found"}), 404
    title = (body.get("title") or "").strip()
    if title and len(title) > 100:
        return jsonify({"error": "title must be 100 characters or fewer"}), 400
    desc      = (body.get("description") or "").strip() or None
    is_public = body.get("isPublic", True)
    is_ranked = body.get("isRanked")  # None means don't change
    cur.execute("""
        UPDATE game_lists
        SET title = COALESCE(NULLIF(%s,''), title),
            description = %s,
            is_public = %s,
            is_ranked = CASE WHEN %s IS NULL THEN is_ranked ELSE %s::boolean END,
            updated_at = NOW()
        WHERE id = %s
        RETURNING *
    """, (title, desc, bool(is_public), is_ranked, is_ranked, list_id))
    row = cur.fetchone()
    conn.commit(); cur.close(); conn.close()
    return jsonify({"list": _format_list(row)})


@app.route("/api/lists/<int:list_id>", methods=["DELETE"])
@login_required
def delete_list(list_id):
    user = current_user()
    conn = get_conn(); cur = conn.cursor()
    if not _list_is_owner(list_id, user["id"], cur):
        cur.close(); conn.close()
        return jsonify({"error": "not found"}), 404
    cur.execute("DELETE FROM game_lists WHERE id = %s", (list_id,))
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True})


@app.route("/api/lists/<int:list_id>/games", methods=["POST"])
@login_required
def add_game_to_list(list_id):
    user    = current_user()
    body    = request.get_json(force=True, silent=True) or {}
    game_id = (body.get("gameId") or "").strip()
    if not game_id:
        return jsonify({"error": "gameId required"}), 400
    conn = get_conn(); cur = conn.cursor()
    if not _list_is_owner(list_id, user["id"], cur):
        cur.close(); conn.close()
        return jsonify({"error": "not found"}), 404
    cur.execute("SELECT is_ranked FROM game_lists WHERE id = %s", (list_id,))
    lst_row = cur.fetchone()
    if lst_row and lst_row["is_ranked"]:
        cur.execute("""
            SELECT COALESCE(MAX(sort_order), 0) + 1 AS next_order
            FROM game_list_items WHERE list_id = %s
        """, (list_id,))
        next_order = cur.fetchone()["next_order"]
        cur.execute("""
            INSERT INTO game_list_items (list_id, game_id, sort_order)
            VALUES (%s, %s, %s) ON CONFLICT DO NOTHING
        """, (list_id, game_id, next_order))
    else:
        cur.execute("""
            INSERT INTO game_list_items (list_id, game_id)
            VALUES (%s, %s) ON CONFLICT DO NOTHING
        """, (list_id, game_id))
    cur.execute("UPDATE game_lists SET updated_at = NOW() WHERE id = %s", (list_id,))
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True})


@app.route("/api/lists/<int:list_id>/games/<game_id>", methods=["DELETE"])
@login_required
def remove_game_from_list(list_id, game_id):
    user = current_user()
    conn = get_conn(); cur = conn.cursor()
    if not _list_is_owner(list_id, user["id"], cur):
        cur.close(); conn.close()
        return jsonify({"error": "not found"}), 404
    cur.execute("DELETE FROM game_list_items WHERE list_id = %s AND game_id = %s",
                (list_id, game_id))
    cur.execute("UPDATE game_lists SET updated_at = NOW() WHERE id = %s", (list_id,))
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True})


@app.route("/api/lists/<int:list_id>/games/reorder", methods=["PATCH"])
@login_required
def reorder_list_games(list_id):
    user    = current_user()
    body    = request.get_json(force=True, silent=True) or {}
    game_ids = body.get("gameIds", [])
    if not isinstance(game_ids, list):
        return jsonify({"error": "gameIds must be an array"}), 400
    conn = get_conn(); cur = conn.cursor()
    if not _list_is_owner(list_id, user["id"], cur):
        cur.close(); conn.close()
        return jsonify({"error": "not found"}), 404
    for i, gid in enumerate(game_ids):
        cur.execute("""
            UPDATE game_list_items SET sort_order = %s
            WHERE list_id = %s AND game_id = %s
        """, (i + 1, list_id, gid))
    cur.execute("UPDATE game_lists SET updated_at = NOW() WHERE id = %s", (list_id,))
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True})


@app.route("/api/lists/players/search")
def search_list_players():
    q         = request.args.get("q", "").strip()
    list_type = request.args.get("type", "players")
    league    = request.args.get("league", "nba")
    if len(q) < 2:
        return jsonify({"players": []})
    pattern = f"%{q}%"
    conn = get_conn(); cur = conn.cursor()

    def _nba():
        out = []
        try:
            if list_type == "player_seasons":
                cur.execute("""
                    SELECT p.player_id, p.player_name, ps.team_abbr AS team, ps.season
                    FROM players p
                    JOIN player_seasons ps ON p.player_id = ps.player_id
                    WHERE p.player_name ILIKE %s AND ps.season_type = 'Regular Season'
                    ORDER BY p.player_name, ps.season DESC LIMIT 30
                """, (pattern,))
            else:
                cur.execute("""
                    SELECT DISTINCT ON (p.player_id) p.player_id, p.player_name,
                           ps.team_abbr AS team, ps.season
                    FROM players p
                    JOIN player_seasons ps ON p.player_id = ps.player_id
                    WHERE p.player_name ILIKE %s
                    ORDER BY p.player_id, ps.season DESC LIMIT 20
                """, (pattern,))
            for r in cur.fetchall():
                item = {"playerId": r["player_id"], "playerName": r["player_name"],
                        "team": r.get("team"), "league": "nba"}
                if list_type == "player_seasons":
                    item["season"] = r.get("season")
                out.append(item)
        except Exception:
            pass
        return out

    def _wnba():
        out = []
        try:
            if list_type == "player_seasons":
                cur.execute("""
                    SELECT DISTINCT player_id, player_name, team, season
                    FROM wnba_player_seasons
                    WHERE player_name ILIKE %s AND season_type = 'Regular Season'
                    ORDER BY player_name, season DESC LIMIT 30
                """, (pattern,))
            else:
                cur.execute("""
                    SELECT DISTINCT ON (player_id) player_id, player_name, team, season
                    FROM wnba_player_seasons
                    WHERE player_name ILIKE %s
                    ORDER BY player_id, season DESC LIMIT 20
                """, (pattern,))
            for r in cur.fetchall():
                item = {"playerId": r["player_id"], "playerName": r["player_name"],
                        "team": r.get("team"), "league": "wnba"}
                if list_type == "player_seasons":
                    item["season"] = r.get("season")
                out.append(item)
        except Exception:
            pass
        return out

    if league == "all":
        nba, wnba = _nba(), _wnba()
        results = []
        for i in range(max(len(nba), len(wnba))):   # interleave so both leagues surface
            if i < len(nba):  results.append(nba[i])
            if i < len(wnba): results.append(wnba[i])
    elif league == "wnba":
        results = _wnba()
    else:
        results = _nba()

    cur.close(); conn.close()
    return jsonify({"players": results})


@app.route("/api/lists/<int:list_id>/players", methods=["POST"])
@login_required
def add_player_to_list(list_id):
    user        = current_user()
    body        = request.get_json(force=True, silent=True) or {}
    player_id   = body.get("playerId")
    player_name = (body.get("playerName") or "").strip()
    team        = body.get("team")
    season      = body.get("season")
    league      = (body.get("league") or "nba").strip().lower()
    if not player_name:
        return jsonify({"error": "playerName required"}), 400
    conn = get_conn(); cur = conn.cursor()
    if not _list_is_owner(list_id, user["id"], cur):
        cur.close(); conn.close()
        return jsonify({"error": "not found"}), 404
    sort_order = None
    cur.execute("SELECT is_ranked FROM game_lists WHERE id = %s", (list_id,))
    lst_row = cur.fetchone()
    if lst_row and lst_row["is_ranked"]:
        cur.execute("SELECT COALESCE(MAX(sort_order),0)+1 AS n FROM player_list_items WHERE list_id=%s", (list_id,))
        sort_order = cur.fetchone()["n"]
    cur.execute("""
        INSERT INTO player_list_items (list_id, player_id, player_name, team, season, sort_order, league)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING id, player_id, player_name, team, season, sort_order, added_at, league
    """, (list_id, player_id, player_name, team, season, sort_order, league))
    row = cur.fetchone()
    cur.execute("UPDATE game_lists SET updated_at = NOW() WHERE id = %s", (list_id,))
    conn.commit(); cur.close(); conn.close()
    return jsonify({"item": {
        "id": row["id"], "playerId": row.get("player_id"), "playerName": row["player_name"],
        "team": row.get("team"), "season": row.get("season"), "league": row.get("league", "nba"),
        "sortOrder": row.get("sort_order"), "addedAt": str(row["added_at"]), "stats": [],
    }}), 201


@app.route("/api/lists/<int:list_id>/players/<int:item_id>", methods=["PATCH"])
@login_required
def update_player_item(list_id, item_id):
    """Set the creator-attached stat tags on a player list item (snapshot strings).
    Stat tags need an explicit season to be unambiguous, so only player_seasons
    lists (where every item already has a season) may set them."""
    user = current_user()
    body = request.get_json(force=True, silent=True) or {}
    stats = body.get("stats")
    if not isinstance(stats, list):
        return jsonify({"error": "stats must be an array"}), 400
    stats = [str(s)[:24] for s in stats][:3]   # cap: 3 tags, short strings
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT user_id, list_type FROM game_lists WHERE id = %s", (list_id,))
    lst = cur.fetchone()
    if not lst or lst["user_id"] != user["id"]:
        cur.close(); conn.close()
        return jsonify({"error": "not found"}), 404
    if (lst.get("list_type") or "games") != "player_seasons":
        cur.close(); conn.close()
        return jsonify({"error": "stat tags require a player_seasons list"}), 400
    cur.execute("UPDATE player_list_items SET stats = %s WHERE id = %s AND list_id = %s",
                (json.dumps(stats), item_id, list_id))
    cur.execute("UPDATE game_lists SET updated_at = NOW() WHERE id = %s", (list_id,))
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True, "stats": stats})


@app.route("/api/players/<int:pid>/stat-options")
def player_stat_options(pid):
    """Return pickable stat tags for a player's season averages (NBA or WNBA).
    Used by the list builder to attach snapshot stats to a player item."""
    league = request.args.get("league", "nba").lower().strip()
    season = request.args.get("season")
    conn = get_conn(); cur = conn.cursor()
    row = None
    try:
        if league == "wnba":
            if season:
                cur.execute("SELECT * FROM wnba_player_seasons WHERE player_id=%s AND season=%s LIMIT 1", (pid, season))
            else:
                cur.execute("SELECT * FROM wnba_player_seasons WHERE player_id=%s ORDER BY season DESC LIMIT 1", (pid,))
            row = cur.fetchone()
        else:
            if season:
                cur.execute("SELECT * FROM player_seasons WHERE player_id=%s AND season=%s AND season_type='Regular Season' LIMIT 1", (pid, season))
            else:
                cur.execute("SELECT * FROM player_seasons WHERE player_id=%s AND season_type='Regular Season' ORDER BY season DESC LIMIT 1", (pid,))
            row = cur.fetchone()
    except Exception:
        row = None
    cur.close(); conn.close()
    if not row:
        return jsonify({"options": []})

    def num(col, lbl):
        v = row.get(col)
        return {"label": lbl, "display": f"{float(v):.1f} {lbl}"} if v is not None else None

    def pct(col, lbl):
        v = row.get(col)
        return {"label": lbl, "display": f"{float(v) * 100:.1f}% {lbl}"} if v is not None else None

    cands = [num("pts", "PPG"), num("reb", "RPG"), num("ast", "APG"), num("stl", "SPG"),
             num("blk", "BPG"), num("fg3m", "3PM"), pct("fg_pct", "FG"), pct("fg3_pct", "3P"),
             pct("ft_pct", "FT"), num("tov", "TOV")]
    return jsonify({"options": [c for c in cands if c], "season": row.get("season")})


@app.route("/api/lists/<int:list_id>/players/<int:item_id>", methods=["DELETE"])
@login_required
def remove_player_from_list(list_id, item_id):
    user = current_user()
    conn = get_conn(); cur = conn.cursor()
    if not _list_is_owner(list_id, user["id"], cur):
        cur.close(); conn.close()
        return jsonify({"error": "not found"}), 404
    cur.execute("DELETE FROM player_list_items WHERE id = %s AND list_id = %s", (item_id, list_id))
    cur.execute("UPDATE game_lists SET updated_at = NOW() WHERE id = %s", (list_id,))
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True})


@app.route("/api/lists/<int:list_id>/players/reorder", methods=["PATCH"])
@login_required
def reorder_player_items(list_id):
    user     = current_user()
    body     = request.get_json(force=True, silent=True) or {}
    item_ids = body.get("itemIds", [])
    if not isinstance(item_ids, list):
        return jsonify({"error": "itemIds must be an array"}), 400
    conn = get_conn(); cur = conn.cursor()
    if not _list_is_owner(list_id, user["id"], cur):
        cur.close(); conn.close()
        return jsonify({"error": "not found"}), 404
    for i, iid in enumerate(item_ids):
        cur.execute("UPDATE player_list_items SET sort_order=%s WHERE id=%s AND list_id=%s",
                    (i + 1, iid, list_id))
    cur.execute("UPDATE game_lists SET updated_at = NOW() WHERE id = %s", (list_id,))
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True})


# ━━━ Performance list items ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# A "performance" = one player's box score in one game. The pickable source is
# the current user's own rated performances (performance_reviews), mirroring how
# favorite games are chosen from games you've reviewed.

@app.route("/api/lists/performances/search")
@login_required
def search_list_performances():
    user   = current_user()
    q      = (request.args.get("q") or "").strip()
    league = (request.args.get("league") or "").strip().lower()
    conn = get_conn(); cur = conn.cursor()
    where  = "pr.user_id = %s"
    params = [user["id"]]
    if q:
        where += " AND pr.player_name ILIKE %s"; params.append(f"%{q}%")
    if league in ("nba", "wnba"):
        where += " AND g.league = %s"; params.append(league)
    cur.execute(f"""
        SELECT pr.game_id, pr.person_id,
               MAX(pr.player_name)                  AS player_name,
               MAX(pr.rating)                       AS my_rating,
               g.game_date, g.league, g.home_team_abbr, g.away_team_abbr,
               g.home_score, g.away_score,
               COALESCE(MAX(pgl.pts), MAX(wgs.pts)) AS pts,
               COALESCE(MAX(pgl.reb), MAX(wgs.reb)) AS reb,
               COALESCE(MAX(pgl.ast), MAX(wgs.ast)) AS ast
        FROM performance_reviews pr
        LEFT JOIN games g ON g.game_id = pr.game_id
        LEFT JOIN player_gamelogs pgl        ON pgl.game_id = pr.game_id AND pgl.player_id = pr.person_id
        LEFT JOIN wnba_player_game_stats wgs ON wgs.game_id = pr.game_id AND wgs.player_id = pr.person_id
        WHERE {where}
        GROUP BY pr.game_id, pr.person_id, g.game_date, g.league,
                 g.home_team_abbr, g.away_team_abbr, g.home_score, g.away_score
        ORDER BY MAX(pr.created_at) DESC
        LIMIT 40
    """, params)
    perfs = []
    for r in cur.fetchall():
        d = dict(r)
        perfs.append({
            "gameId":       d["game_id"],
            "personId":     d["person_id"],
            "playerName":   d.get("player_name") or "",
            "league":       d.get("league") or "nba",
            "gameDate":     str(d["game_date"]) if d.get("game_date") else None,
            "homeTeamAbbr": d.get("home_team_abbr"),
            "awayTeamAbbr": d.get("away_team_abbr"),
            "homeScore":    d.get("home_score"),
            "awayScore":    d.get("away_score"),
            "pts":          d.get("pts"), "reb": d.get("reb"), "ast": d.get("ast"),
            "statLine":     _perf_stat_line(d),
            "myRating":     d.get("my_rating"),
        })
    cur.close(); conn.close()
    return jsonify({"performances": perfs})


@app.route("/api/lists/<int:list_id>/performances", methods=["POST"])
@login_required
def add_performance_to_list(list_id):
    user        = current_user()
    body        = request.get_json(force=True, silent=True) or {}
    game_id     = (body.get("gameId") or "").strip()
    person_id   = body.get("personId")
    player_name = (body.get("playerName") or "").strip()
    league      = (body.get("league") or "nba").strip().lower()
    if not game_id or person_id is None or not player_name:
        return jsonify({"error": "gameId, personId and playerName are required"}), 400
    conn = get_conn(); cur = conn.cursor()
    if not _list_is_owner(list_id, user["id"], cur):
        cur.close(); conn.close()
        return jsonify({"error": "not found"}), 404
    cur.execute("SELECT is_ranked FROM game_lists WHERE id = %s", (list_id,))
    lst_row    = cur.fetchone()
    ranked     = bool(lst_row and lst_row["is_ranked"])
    sort_order = None
    if ranked:
        cur.execute("SELECT COALESCE(MAX(sort_order),0)+1 AS n FROM performance_list_items WHERE list_id=%s", (list_id,))
        sort_order = cur.fetchone()["n"]
    cur.execute("""
        INSERT INTO performance_list_items (list_id, game_id, person_id, player_name, league, sort_order)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (list_id, game_id, person_id) DO NOTHING
    """, (list_id, game_id, int(person_id), player_name, league, sort_order))
    cur.execute("UPDATE game_lists SET updated_at = NOW() WHERE id = %s", (list_id,))
    conn.commit()
    items = _fetch_perf_items(cur, list_id, ranked)
    cur.close(); conn.close()
    return jsonify({"ok": True, "items": items}), 201


@app.route("/api/lists/<int:list_id>/performances/<int:item_id>", methods=["DELETE"])
@login_required
def remove_performance_from_list(list_id, item_id):
    user = current_user()
    conn = get_conn(); cur = conn.cursor()
    if not _list_is_owner(list_id, user["id"], cur):
        cur.close(); conn.close()
        return jsonify({"error": "not found"}), 404
    cur.execute("DELETE FROM performance_list_items WHERE id = %s AND list_id = %s", (item_id, list_id))
    cur.execute("UPDATE game_lists SET updated_at = NOW() WHERE id = %s", (list_id,))
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True})


@app.route("/api/lists/<int:list_id>/performances/reorder", methods=["PATCH"])
@login_required
def reorder_performance_items(list_id):
    user     = current_user()
    body     = request.get_json(force=True, silent=True) or {}
    item_ids = body.get("itemIds", [])
    if not isinstance(item_ids, list):
        return jsonify({"error": "itemIds must be an array"}), 400
    conn = get_conn(); cur = conn.cursor()
    if not _list_is_owner(list_id, user["id"], cur):
        cur.close(); conn.close()
        return jsonify({"error": "not found"}), 404
    for i, iid in enumerate(item_ids):
        cur.execute("UPDATE performance_list_items SET sort_order=%s WHERE id=%s AND list_id=%s",
                    (i + 1, iid, list_id))
    cur.execute("UPDATE game_lists SET updated_at = NOW() WHERE id = %s", (list_id,))
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True})


@app.route("/api/teams/search")
def search_teams():
    q         = request.args.get("q", "").strip()
    list_type = request.args.get("type", "teams")
    league    = request.args.get("league", "nba").strip().lower()
    conn = get_conn(); cur = conn.cursor()
    results = []
    try:
        if list_type == "team_seasons":
            conditions = ["ts.league = %s"]
            params = [league]
            if q:
                conditions.append("(ts.team_name ILIKE %s OR ts.team_abbr ILIKE %s)")
                params.extend([f"%{q}%", f"%{q}%"])
            cur.execute(f"""
                SELECT ts.team_abbr, ts.team_name, ts.season, ts.wins, ts.losses
                FROM team_seasons ts
                WHERE {' AND '.join(conditions)}
                ORDER BY ts.team_name, ts.season DESC
                LIMIT 500
            """, params)
            for r in cur.fetchall():
                results.append({
                    "teamAbbr": r["team_abbr"],
                    "teamName": r["team_name"],
                    "season":   r["season"],
                    "wins":     r.get("wins"),
                    "losses":   r.get("losses"),
                })
        else:
            def _teams_for(lg: str) -> list:
                conditions = ["league = %s"]
                params = [lg]
                if q:
                    conditions.append("(team_name ILIKE %s OR team_abbr ILIKE %s)")
                    params.extend([f"%{q}%", f"%{q}%"])
                cur.execute(f"""
                    SELECT DISTINCT ON (team_abbr) team_abbr, team_name,
                           MAX(season) OVER (PARTITION BY team_abbr) AS latest_season
                    FROM team_seasons
                    WHERE {' AND '.join(conditions)}
                    ORDER BY team_abbr, season DESC
                    LIMIT 60
                """, params)
                return [{
                    "teamAbbr":     r["team_abbr"],
                    "teamName":     r["team_name"],
                    "latestSeason": r.get("latest_season"),
                    "league":       lg,
                } for r in cur.fetchall()]

            if league == "all":
                nba, wnba = _teams_for("nba"), _teams_for("wnba")
                for i in range(max(len(nba), len(wnba))):   # interleave so both leagues surface
                    if i < len(nba):  results.append(nba[i])
                    if i < len(wnba): results.append(wnba[i])
            else:
                results = _teams_for(league)
    except Exception as e:
        pass
    cur.close(); conn.close()
    return jsonify({"teams": results})


# WNBA abbreviation reconciliation. games + live feeds use one abbr set (LA,
# LV, NY, GS, WSH, POR); the standings feed that populates team_seasons names +
# records uses another (LAS, LVA, NYL, GSV, WAS, PDX); wnba_player_seasons is a
# mix. We canonicalize on the games abbr but read both variants everywhere.
_WNBA_STANDINGS_TO_GAMES = {"LAS": "LA", "LVA": "LV", "NYL": "NY",
                            "GSV": "GS", "WAS": "WSH", "PDX": "POR"}
_WNBA_GAMES_TO_STANDINGS = {v: k for k, v in _WNBA_STANDINGS_TO_GAMES.items()}


@app.route("/api/teams/<abbr>/profile")
def team_profile(abbr):
    """Team page: record, season list, roster (season leaders), and the
    team's schedule/results for a season (with crowd ratings). Works for
    both leagues; WNBA games are distinguished by the '10' game_id prefix."""
    abbr   = abbr.strip().upper()
    league = request.args.get("league", "nba").strip().lower()
    season = request.args.get("season", "").strip()
    if league == "wnba":
        abbr = _WNBA_STANDINGS_TO_GAMES.get(abbr, abbr)  # canonical = games abbr
        alt = _WNBA_GAMES_TO_STANDINGS.get(abbr)
        variants = [abbr] + ([alt] if alt else [])
    else:
        variants = [abbr]
    conn = get_conn(); cur = conn.cursor()
    try:
        # Seasons + record + name from team_seasons. Prefer rows with a real
        # name (WNBA has duplicate rows where team_name == team_abbr).
        cur.execute("""
            SELECT season, team_name, wins, losses, team_abbr
            FROM team_seasons
            WHERE team_abbr = ANY(%s) AND league = %s
            ORDER BY season DESC, (team_name = team_abbr) ASC
        """, (variants, league))
        rows = cur.fetchall()
        by_season = {}
        for r in rows:
            by_season.setdefault(r["season"], r)  # first = best-named

        # Only show seasons we actually have data to display — stats or games.
        # (team_seasons alone carries just a W/L record; old WNBA seasons there
        # predate our stats/games and would render as empty pages.)
        stat_seasons = []
        try:
            cur.execute("""
                SELECT DISTINCT season FROM team_season_stats
                WHERE team_abbr = ANY(%s) AND league = %s
            """, (variants, league))
            stat_seasons = [r["season"] for r in cur.fetchall()]
        except Exception:
            conn.rollback()  # table not created yet

        # Filter by the league column, not a game_id prefix: recent WNBA games
        # use ESPN-style ids (4017…) that don't start with '10', so the old
        # heuristic leaked WNBA seasons onto NBA team pages (and vice-versa).
        cur.execute("""
            SELECT DISTINCT season FROM games
            WHERE (home_team_abbr = ANY(%s) OR away_team_abbr = ANY(%s)) AND league = %s
        """, (variants, variants, league))
        game_seasons = [r["season"] for r in cur.fetchall()]

        seasons = sorted(set(stat_seasons) | set(game_seasons), reverse=True)
        if not seasons:  # nothing backfilled yet — fall back to the record list
            seasons = sorted(by_season.keys(), reverse=True)
        if not seasons:
            return jsonify({"error": "team not found"}), 404
        if not season or season not in seasons:
            season = seasons[0]

        # Name + record for the selected season (record only exists in
        # team_seasons; for stats-only seasons fall back to the team's name).
        real_names = [r["team_name"] for r in rows if r["team_name"] != r["team_abbr"]]
        fallback_name = real_names[0] if real_names else (rows[0]["team_name"] if rows else abbr)
        if season in by_season:
            srow = by_season[season]
            team_name = srow["team_name"]
            record = {"wins": srow.get("wins"), "losses": srow.get("losses")}
        else:
            team_name = fallback_name
            record = {"wins": None, "losses": None}

        # Roster / season leaders (ordered by scoring)
        roster = []
        if league == "wnba":
            cur.execute("""
                SELECT player_id, player_name, gp, min, pts, reb, ast
                FROM wnba_player_seasons
                WHERE team = ANY(%s) AND season = %s AND season_type = 'Regular Season'
                  AND COALESCE(gp, 0) > 0
                ORDER BY pts DESC NULLS LAST
            """, (variants, season))
            for r in cur.fetchall():
                roster.append({
                    "playerId": r["player_id"], "playerName": r["player_name"],
                    "gp": r.get("gp"), "mpg": r.get("min"),
                    "ppg": r.get("pts"), "rpg": r.get("reb"), "apg": r.get("ast"),
                })
        else:
            cur.execute("""
                SELECT ps.player_id, p.player_name, ps.gp, ps.min_per_game,
                       ps.pts, ps.reb, ps.ast
                FROM player_seasons ps
                JOIN players p ON p.player_id = ps.player_id
                WHERE ps.team_abbr = %s AND ps.season = %s
                  AND ps.season_type = 'Regular Season'
                  AND COALESCE(ps.gp, 0) > 0
                ORDER BY ps.pts DESC NULLS LAST
            """, (abbr, season))
            for r in cur.fetchall():
                roster.append({
                    "playerId": r["player_id"], "playerName": r["player_name"],
                    "gp": r.get("gp"), "mpg": r.get("min_per_game"),
                    "ppg": r.get("pts"), "rpg": r.get("reb"), "apg": r.get("ast"),
                })

        # Schedule / results for this season (crowd ratings included)
        cur.execute("""
            SELECT game_id, game_date, season_type,
                   home_team_abbr, away_team_abbr, home_score, away_score,
                   status, bayesian_rating, review_count
            FROM games
            WHERE season = %s
              AND (home_team_abbr = ANY(%s) OR away_team_abbr = ANY(%s))
              AND league = %s
            ORDER BY game_date
        """, (season, variants, variants, league))
        games = []
        for r in cur.fetchall():
            is_home = r["home_team_abbr"] in variants
            us   = r["home_score"] if is_home else r["away_score"]
            them = r["away_score"] if is_home else r["home_score"]
            final = (r["status"] or "").lower().startswith("final")
            result = None
            if final and us is not None and them is not None:
                result = "W" if us > them else ("L" if us < them else "T")
            games.append({
                "gameId": r["game_id"],
                "date": r["game_date"].isoformat() if r["game_date"] else None,
                "seasonType": r["season_type"],
                "isHome": is_home,
                "opponent": r["away_team_abbr"] if is_home else r["home_team_abbr"],
                "teamScore": us, "oppScore": them,
                "status": r["status"], "result": result,
                "rating": round(r["bayesian_rating"], 2) if r.get("bayesian_rating") else None,
                "reviewCount": r.get("review_count") or 0,
            })

        # ── Season stats. Single source of truth is team_season_stats (uniform
        #    across every season): PPG=pts, OPP=pts-plus_minus, DIFF=plus_minus,
        #    plus box + advanced. Only if that table has no row for the season
        #    (not yet backfilled) do we fall back to averaging the games table.
        ts = None
        try:
            cur.execute("""
                SELECT gp, wins, losses, pts, plus_minus, fg_pct, fg3_pct, ft_pct,
                       reb, ast, stl, blk, tov,
                       off_rating, def_rating, net_rating, pace
                FROM team_season_stats
                WHERE team_abbr = ANY(%s) AND league = %s AND season = %s
                  AND season_type = 'Regular Season'
                LIMIT 1
            """, (variants, league, season))
            ts = cur.fetchone()
        except Exception:
            conn.rollback()  # table not created yet

        # Fill the record from team stats when team_seasons lacks this season.
        if ts and record["wins"] is None and ts.get("wins") is not None:
            record = {"wins": ts.get("wins"), "losses": ts.get("losses")}

        stats = None
        if ts and ts.get("pts") is not None:
            pts, pm = ts.get("pts"), ts.get("plus_minus")
            stats = {
                "gp": ts.get("gp"),
                "wins": record["wins"], "losses": record["losses"],
                "ppg": round(pts, 1),
                "oppPpg": round(pts - pm, 1) if pm is not None else None,
                "diff": round(pm, 1) if pm is not None else None,
                "fgPct": ts.get("fg_pct"), "fg3Pct": ts.get("fg3_pct"), "ftPct": ts.get("ft_pct"),
                "reb": ts.get("reb"), "ast": ts.get("ast"), "stl": ts.get("stl"),
                "blk": ts.get("blk"), "tov": ts.get("tov"),
                "offRating": ts.get("off_rating"), "defRating": ts.get("def_rating"),
                "netRating": ts.get("net_rating"), "pace": ts.get("pace"),
            }
        else:
            # Fallback only when the season isn't in team_season_stats yet.
            reg = [g for g in games
                   if g["seasonType"] == "Regular Season" and g["result"] in ("W", "L", "T")
                   and g["teamScore"] is not None and g["oppScore"] is not None]
            if reg:
                gp = len(reg)
                pf = sum(g["teamScore"] for g in reg)
                pa = sum(g["oppScore"] for g in reg)
                stats = {
                    "gp": gp,
                    "wins": sum(1 for g in reg if g["result"] == "W"),
                    "losses": sum(1 for g in reg if g["result"] == "L"),
                    "ppg": round(pf / gp, 1),
                    "oppPpg": round(pa / gp, 1),
                    "diff": round((pf - pa) / gp, 1),
                }

        return jsonify({
            "teamAbbr": abbr, "teamName": team_name,
            "league": league, "season": season, "seasons": seasons,
            "record": record,
            "stats": stats,
            "roster": roster, "games": games,
        })
    finally:
        cur.close(); conn.close()


@app.route("/api/teams")
def teams_list():
    """Clean, deduped list of a league's current teams (canonical games abbr +
    proper name) for the Browse Teams surface. Unlike /api/teams/search this
    collapses the WNBA duplicate rows and drops placeholder/all-star noise."""
    league = request.args.get("league", "nba").strip().lower()
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute("SELECT MAX(season) AS m FROM team_seasons WHERE league = %s", (league,))
        row = cur.fetchone()
        latest = row["m"] if row else None
        if not latest:
            return jsonify({"teams": [], "season": None})
        cur.execute("""
            SELECT team_abbr, team_name FROM team_seasons
            WHERE league = %s AND season = %s
        """, (league, latest))
        rows = cur.fetchall()
        if league == "wnba":
            best = {}   # games_abbr -> name (prefer real names over placeholders)
            for r in rows:
                ga = _WNBA_STANDINGS_TO_GAMES.get(r["team_abbr"], r["team_abbr"])
                name = r["team_name"]
                if ga not in best or (name != ga and best[ga] == ga):
                    best[ga] = name
            teams = [{"teamAbbr": a, "teamName": n} for a, n in best.items() if n != a]
        else:
            teams = [{"teamAbbr": r["team_abbr"], "teamName": r["team_name"]} for r in rows]
        teams.sort(key=lambda t: t["teamName"])
        return jsonify({"teams": teams, "season": latest})
    finally:
        cur.close(); conn.close()


@app.route("/api/lists/<int:list_id>/teams", methods=["POST"])
@login_required
def add_team_to_list(list_id):
    user      = current_user()
    body      = request.get_json(force=True, silent=True) or {}
    team_abbr = (body.get("teamAbbr") or "").strip()
    team_name = (body.get("teamName") or "").strip()
    season    = body.get("season")
    wins      = body.get("wins")
    losses    = body.get("losses")
    league    = (body.get("league") or "nba").strip().lower()
    if not team_abbr or not team_name:
        return jsonify({"error": "teamAbbr and teamName required"}), 400
    conn = get_conn(); cur = conn.cursor()
    if not _list_is_owner(list_id, user["id"], cur):
        cur.close(); conn.close()
        return jsonify({"error": "not found"}), 404
    sort_order = None
    cur.execute("SELECT is_ranked FROM game_lists WHERE id = %s", (list_id,))
    lst_row = cur.fetchone()
    if lst_row and lst_row["is_ranked"]:
        cur.execute("SELECT COALESCE(MAX(sort_order),0)+1 AS n FROM team_list_items WHERE list_id=%s", (list_id,))
        sort_order = cur.fetchone()["n"]
    cur.execute("""
        INSERT INTO team_list_items (list_id, team_abbr, team_name, season, wins, losses, sort_order, league)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING id, team_abbr, team_name, season, wins, losses, sort_order, added_at, league
    """, (list_id, team_abbr, team_name, season, wins, losses, sort_order, league))
    row = cur.fetchone()
    cur.execute("UPDATE game_lists SET updated_at = NOW() WHERE id = %s", (list_id,))
    conn.commit(); cur.close(); conn.close()
    return jsonify({"item": {
        "id": row["id"], "teamAbbr": row["team_abbr"], "teamName": row["team_name"],
        "season": row.get("season"), "wins": row.get("wins"), "losses": row.get("losses"),
        "league": row.get("league", "nba"),
        "sortOrder": row.get("sort_order"), "addedAt": str(row["added_at"]),
    }}), 201


@app.route("/api/lists/<int:list_id>/teams/<int:item_id>", methods=["DELETE"])
@login_required
def remove_team_from_list(list_id, item_id):
    user = current_user()
    conn = get_conn(); cur = conn.cursor()
    if not _list_is_owner(list_id, user["id"], cur):
        cur.close(); conn.close()
        return jsonify({"error": "not found"}), 404
    cur.execute("DELETE FROM team_list_items WHERE id = %s AND list_id = %s", (item_id, list_id))
    cur.execute("UPDATE game_lists SET updated_at = NOW() WHERE id = %s", (list_id,))
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True})


@app.route("/api/lists/<int:list_id>/teams/reorder", methods=["PATCH"])
@login_required
def reorder_team_items(list_id):
    user     = current_user()
    body     = request.get_json(force=True, silent=True) or {}
    item_ids = body.get("itemIds", [])
    if not isinstance(item_ids, list):
        return jsonify({"error": "itemIds must be an array"}), 400
    conn = get_conn(); cur = conn.cursor()
    if not _list_is_owner(list_id, user["id"], cur):
        cur.close(); conn.close()
        return jsonify({"error": "not found"}), 404
    for i, iid in enumerate(item_ids):
        cur.execute("UPDATE team_list_items SET sort_order=%s WHERE id=%s AND list_id=%s",
                    (i + 1, iid, list_id))
    cur.execute("UPDATE game_lists SET updated_at = NOW() WHERE id = %s", (list_id,))
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True})


_EDITION_ORDER = {"Association Edition": 1, "Icon Edition": 2, "Statement Edition": 3, "City Edition": 4}


@app.route("/api/jerseys/search")
def search_jerseys():
    q      = request.args.get("q", "").strip()
    season = request.args.get("season", "").strip()
    league = request.args.get("league", "nba").strip().lower()
    conn = get_conn(); cur = conn.cursor()
    source = "lockervision_wnba" if league == "wnba" else "lockervision"
    conditions = ["source_slug = %s"]
    params = [source]
    if season:
        conditions.append("year_range = %s")
        params.append(season)
    if q:
        conditions.append("(team_name ILIKE %s OR label ILIKE %s)")
        params.extend([f"%{q}%", f"%{q}%"])
    cur.execute(f"""
        SELECT id, team_slug, team_name, year_range, label, variant, image_url
        FROM jerseys
        WHERE {' AND '.join(conditions)}
        ORDER BY team_name, year_start DESC, variant
        LIMIT 500
    """, params)
    results = []
    for r in cur.fetchall():
        results.append({
            "id":        r["id"],
            "teamSlug":  r["team_slug"],
            "teamName":  r["team_name"],
            "yearRange": r["year_range"],
            "label":     r["label"],
            "variant":   r.get("variant"),
            "imageUrl":  r["image_url"],
        })
    cur.close(); conn.close()
    return jsonify({"jerseys": results})


@app.route("/api/lists/<int:list_id>/jerseys", methods=["POST"])
@login_required
def add_jersey_to_list(list_id):
    user      = current_user()
    body      = request.get_json(force=True, silent=True) or {}
    jersey_id = body.get("jerseyId")
    label     = (body.get("label") or "").strip()
    image_url = (body.get("imageUrl") or "").strip()
    if not label or not image_url:
        return jsonify({"error": "label and imageUrl required"}), 400
    conn = get_conn(); cur = conn.cursor()
    if not _list_is_owner(list_id, user["id"], cur):
        cur.close(); conn.close()
        return jsonify({"error": "not found"}), 404
    sort_order = None
    cur.execute("SELECT is_ranked FROM game_lists WHERE id = %s", (list_id,))
    lst_row = cur.fetchone()
    if lst_row and lst_row["is_ranked"]:
        cur.execute("SELECT COALESCE(MAX(sort_order),0)+1 AS n FROM jersey_list_items WHERE list_id=%s", (list_id,))
        sort_order = cur.fetchone()["n"]
    cur.execute("""
        INSERT INTO jersey_list_items (list_id, jersey_id, label, image_url, sort_order)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id, jersey_id, label, image_url, sort_order, added_at
    """, (list_id, jersey_id, label, image_url, sort_order))
    row = cur.fetchone()
    cur.execute("UPDATE game_lists SET updated_at = NOW() WHERE id = %s", (list_id,))
    conn.commit(); cur.close(); conn.close()
    return jsonify({"item": {
        "id": row["id"], "jerseyId": row.get("jersey_id"), "label": row["label"],
        "imageUrl": row["image_url"], "sortOrder": row.get("sort_order"), "addedAt": str(row["added_at"]),
    }}), 201


@app.route("/api/lists/<int:list_id>/jerseys/<int:item_id>", methods=["DELETE"])
@login_required
def remove_jersey_from_list(list_id, item_id):
    user = current_user()
    conn = get_conn(); cur = conn.cursor()
    if not _list_is_owner(list_id, user["id"], cur):
        cur.close(); conn.close()
        return jsonify({"error": "not found"}), 404
    cur.execute("DELETE FROM jersey_list_items WHERE id = %s AND list_id = %s", (item_id, list_id))
    cur.execute("UPDATE game_lists SET updated_at = NOW() WHERE id = %s", (list_id,))
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True})


@app.route("/api/lists/<int:list_id>/jerseys/reorder", methods=["PATCH"])
@login_required
def reorder_jersey_items(list_id):
    user     = current_user()
    body     = request.get_json(force=True, silent=True) or {}
    item_ids = body.get("itemIds", [])
    if not isinstance(item_ids, list):
        return jsonify({"error": "itemIds must be an array"}), 400
    conn = get_conn(); cur = conn.cursor()
    if not _list_is_owner(list_id, user["id"], cur):
        cur.close(); conn.close()
        return jsonify({"error": "not found"}), 404
    for i, iid in enumerate(item_ids):
        cur.execute("UPDATE jersey_list_items SET sort_order=%s WHERE id=%s AND list_id=%s",
                    (i + 1, iid, list_id))
    cur.execute("UPDATE game_lists SET updated_at = NOW() WHERE id = %s", (list_id,))
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True})


# ── Profile & Friends routes ──────────────────────────────────
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PATCH /api/me/display-name
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/me/display-name", methods=["PATCH"])
@login_required
def update_display_name():
    user = current_user()
    body = request.get_json() or {}
    name = body.get("display_name", "").strip()

    if not name:
        return jsonify({"error": "display_name is required"}), 400
    if len(name) > 40:
        return jsonify({"error": "Display name must be 40 characters or fewer"}), 400
    # Basic sanity: printable chars only
    if not all(c.isprintable() for c in name):
        return jsonify({"error": "Invalid characters in display name"}), 400

    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            UPDATE users SET display_name = %s, display_name_set = TRUE, updated_at = NOW()
            WHERE id = %s
            RETURNING id, display_name, display_name_set
        """, (name, user["id"]))
        row = dict(cur.fetchone())
        conn.commit()
        cur.close(); conn.close()

        # Update session so nav shows new name immediately
        from flask import session
        if "user" in session:
            session["user"]["display_name"] = name
            session.modified = True

        return jsonify({"ok": True, "display_name": row["display_name"]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PATCH /api/me/night-mode  — toggle dark mode preference
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/me/night-mode", methods=["PATCH"])
@login_required
def set_night_mode():
    user    = current_user()
    body    = request.get_json(silent=True) or {}
    enabled = bool(body.get("enabled", False))
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("UPDATE users SET night_mode = %s WHERE id = %s", (enabled, user["id"]))
        conn.commit()
        cur.close(); conn.close()
        return jsonify({"ok": True, "night_mode": enabled})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PATCH /api/me/favorite-team  — set or clear favorite team (NBA or WNBA)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
_NBA_ABBRS = {
    "ATL","BOS","BKN","CHA","CHI","CLE","DAL","DEN","DET","GSW",
    "HOU","IND","LAC","LAL","MEM","MIA","MIL","MIN","NOP","NYK",
    "OKC","ORL","PHI","PHX","POR","SAC","SAS","TOR","UTA","WAS",
}
# WNBA teams stored with "WNBA_" prefix to avoid conflicts with NBA abbreviations
_WNBA_FAV_ABBRS = {
    "WNBA_ATL","WNBA_CHI","WNBA_CON","WNBA_DAL","WNBA_GS",
    "WNBA_IND","WNBA_LA", "WNBA_LV", "WNBA_MIN","WNBA_NY",
    "WNBA_PHX","WNBA_POR","WNBA_SEA","WNBA_TOR","WNBA_WSH",
}
_ALL_TEAM_ABBRS = _NBA_ABBRS | _WNBA_FAV_ABBRS

@app.route("/api/me/favorite-team", methods=["PATCH"])
@login_required
def update_favorite_team():
    user = current_user()
    body = request.get_json() or {}
    team = (body.get("favorite_team") or "").strip().upper() or None

    if team and team not in _ALL_TEAM_ABBRS:
        return jsonify({"error": "Invalid team abbreviation"}), 400

    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            UPDATE users SET favorite_team = %s, updated_at = NOW()
            WHERE id = %s
        """, (team, user["id"]))
        conn.commit()
        cur.close(); conn.close()

        from flask import session
        if "user" in session:
            session["user"]["favorite_team"] = team or ""
            session.modified = True

        return jsonify({"ok": True, "favorite_team": team or ""})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PATCH /api/me/ring  — set equipped avatar ring (null=rank default, 0=none, 1-10=level)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/me/ring", methods=["PATCH"])
@login_required
def update_ring():
    user = current_user()
    body = request.get_json() or {}
    ring = body.get("equipped_ring")  # null, 0, 1-10, or 11 (pro)

    if ring is not None and (not isinstance(ring, int) or ring < 0 or ring > 11):
        return jsonify({"error": "equipped_ring must be null or an integer 0–11"}), 400

    # Verify unlock: levels 2-10 require sufficient XP; level 11 (Pro) is gated client-side
    if ring and ring >= 2 and ring <= 10:
        try:
            conn = get_conn()
            cur  = conn.cursor()
            cur.execute("SELECT xp FROM users WHERE id = %s", (user["id"],))
            row = cur.fetchone()
            cur.close(); conn.close()
            if not row or _xp_to_level(int(row["xp"] or 0)) < ring:
                return jsonify({"error": "Ring level not yet unlocked"}), 403
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("UPDATE users SET equipped_ring = %s WHERE id = %s", (ring, user["id"]))
        conn.commit()
        cur.close(); conn.close()
        return jsonify({"ok": True, "equipped_ring": ring})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PATCH /api/me/title  — set equipped title (null=rank default, 0=none, 1-10=level, 11=pro)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/me/title", methods=["PATCH"])
@login_required
def update_title():
    user = current_user()
    body = request.get_json() or {}
    title = body.get("equipped_title")

    if title is not None and (not isinstance(title, int) or title < 0 or title > 11):
        return jsonify({"error": "equipped_title must be null or an integer 0–11"}), 400

    # Verify unlock: levels 2-10 require sufficient XP; level 11 (Pro) is gated client-side
    if title and title >= 2 and title <= 10:
        try:
            conn = get_conn()
            cur  = conn.cursor()
            cur.execute("SELECT xp FROM users WHERE id = %s", (user["id"],))
            row = cur.fetchone()
            cur.close(); conn.close()
            if not row or _xp_to_level(int(row["xp"] or 0)) < title:
                return jsonify({"error": "Title not yet unlocked"}), 403
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("UPDATE users SET equipped_title = %s WHERE id = %s", (title, user["id"]))
        conn.commit()
        cur.close(); conn.close()
        return jsonify({"ok": True, "equipped_title": title})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def _compress_avatar(data_url: str, max_dim: int = 200, quality: int = 80) -> str:
    """Downscale a base64 image data URL to a small JPEG thumbnail.
    Avatars render at <=90px, so storing anything larger just bloats every
    user-bearing API response (feed/profile/notifications). Returns the input
    unchanged on any failure."""
    try:
        import base64, io
        from PIL import Image
        _, _, b64 = data_url.partition(",")
        if not b64:
            return data_url
        img = Image.open(io.BytesIO(base64.b64decode(b64))).convert("RGB")
        img.thumbnail((max_dim, max_dim), Image.LANCZOS)
        out = io.BytesIO()
        img.save(out, format="JPEG", quality=quality, optimize=True)
        return "data:image/jpeg;base64," + base64.b64encode(out.getvalue()).decode("ascii")
    except Exception:
        return data_url


# POST /api/me/avatar  — upload / replace profile picture
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/me/avatar", methods=["POST"])
@login_required
def update_avatar():
    user = current_user()
    body = request.get_json() or {}
    data = body.get("avatar_data", "").strip()

    if not data:
        return jsonify({"error": "avatar_data is required"}), 400

    # Must be a data URL with an image MIME type
    if not data.startswith("data:image/"):
        return jsonify({"error": "Invalid image format"}), 400

    # Limit size: base64-encoded ~200 KB image → ~270 KB string
    if len(data) > 300_000:
        return jsonify({"error": "Image too large (max ~200 KB after resize)"}), 400

    # Downscale before storing — avatars display at <=90px; large blobs bloat
    # every feed/profile/notification response that carries this user.
    data = _compress_avatar(data)

    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            UPDATE users SET avatar_url = %s, updated_at = NOW()
            WHERE id = %s
        """, (data, user["id"]))
        conn.commit()
        cur.close(); conn.close()

        return jsonify({"ok": True, "avatar_url": data})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PUT /api/me/favorites  — set a game at a position (1–4)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/me/favorites", methods=["PUT"])
@login_required
def set_favorite():
    me   = current_user()
    body = request.get_json() or {}
    game_id  = body.get("game_id", "").strip()
    position = body.get("position")

    if not game_id:
        return jsonify({"error": "game_id is required"}), 400
    if position not in (1, 2, 3, 4):
        return jsonify({"error": "position must be 1–4"}), 400

    try:
        conn = get_conn()
        cur  = conn.cursor()

        # Verify game exists
        cur.execute("SELECT game_id FROM games WHERE game_id = %s", (game_id,))
        if not cur.fetchone():
            cur.close(); conn.close()
            return jsonify({"error": "Game not found"}), 404

        # Remove any existing entry for this game (in case it was in another slot)
        cur.execute("DELETE FROM favorite_games WHERE user_id = %s AND game_id = %s",
                    (me["id"], game_id))
        # Remove whatever was at this position
        cur.execute("DELETE FROM favorite_games WHERE user_id = %s AND position = %s",
                    (me["id"], position))
        # Insert new
        cur.execute("""
            INSERT INTO favorite_games (user_id, game_id, position)
            VALUES (%s, %s, %s)
        """, (me["id"], game_id, position))
        conn.commit()
        cur.close(); conn.close()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DELETE /api/me/favorites/<game_id>  — remove a favorite
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/me/favorites/<game_id>", methods=["DELETE"])
@login_required
def remove_favorite(game_id):
    me = current_user()
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("DELETE FROM favorite_games WHERE user_id = %s AND game_id = %s",
                    (me["id"], game_id))
        conn.commit()
        cur.close(); conn.close()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PUT /api/me/favorite-players  — pin a player at a position (1–4)
# The pinned identity element on the profile. NBA or WNBA.
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/me/favorite-players", methods=["PUT"])
@login_required
def set_favorite_player():
    me   = current_user()
    body = request.get_json() or {}
    person_id   = body.get("person_id")
    player_name = (body.get("player_name") or "").strip()
    team        = (body.get("team") or "").strip() or None
    league      = (body.get("league") or "nba").strip().lower()
    position    = body.get("position")

    if person_id is None or not player_name:
        return jsonify({"error": "person_id and player_name are required"}), 400
    if position not in (1, 2, 3, 4):
        return jsonify({"error": "position must be 1–4"}), 400

    try:
        conn = get_conn(); cur = conn.cursor()
        # Drop any existing pin for this player, and whatever sits at this slot,
        # so a player can be moved between slots without violating uniqueness.
        cur.execute("DELETE FROM favorite_players WHERE user_id = %s AND person_id = %s",
                    (me["id"], int(person_id)))
        cur.execute("DELETE FROM favorite_players WHERE user_id = %s AND position = %s",
                    (me["id"], position))
        cur.execute("""
            INSERT INTO favorite_players (user_id, person_id, player_name, team, league, position)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (me["id"], int(person_id), player_name, team, league, position))
        conn.commit(); cur.close(); conn.close()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DELETE /api/me/favorite-players/<person_id>  — unpin a player
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/me/favorite-players/<int:person_id>", methods=["DELETE"])
@login_required
def remove_favorite_player(person_id):
    me = current_user()
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("DELETE FROM favorite_players WHERE user_id = %s AND person_id = %s",
                    (me["id"], person_id))
        conn.commit(); cur.close(); conn.close()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GET /api/notifications
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/notifications")
@login_required
def get_notifications():
    user  = current_user()
    uid   = user["id"]
    limit = min(int(request.args.get("limit", 50)), 100)
    include_lists = request.args.get("include_lists") in ("1", "true")
    try:
        conn = get_conn()
        cur  = conn.cursor()
        # List-published notifications are opt-in: older app builds don't know the
        # "list_published" type and would fail to decode the notifications array.
        list_arm = ""
        list_params: list = []
        if include_lists:
            list_arm = """
                UNION ALL

                -- A friend published a public list.
                -- (list id is carried in review_id, list title in reply_text)
                SELECT
                    'list_published'        AS type,
                    gl.created_at,
                    u.id,
                    u.display_name,
                    u.avatar_url,
                    gl.id                   AS review_id,
                    NULL::text              AS game_id,
                    NULL::text              AS home_team_abbr,
                    NULL::text              AS away_team_abbr,
                    NULL::date              AS game_date,
                    gl.title                AS reply_text,
                    NULL::text              AS league
                FROM game_lists gl
                JOIN users u ON u.id = gl.user_id
                JOIN friendships f ON (
                    (f.sender_id = %s AND f.receiver_id = gl.user_id)
                    OR (f.receiver_id = %s AND f.sender_id = gl.user_id)
                ) AND f.status = 'accepted'
                WHERE gl.is_public = TRUE
                  AND (  (SELECT COUNT(*) FROM game_list_items   WHERE list_id = gl.id)
                       + (SELECT COUNT(*) FROM player_list_items WHERE list_id = gl.id)
                       + (SELECT COUNT(*) FROM jersey_list_items WHERE list_id = gl.id)
                       + (SELECT COUNT(*) FROM performance_list_items WHERE list_id = gl.id)
                       + (SELECT COUNT(*) FROM team_list_items   WHERE list_id = gl.id) ) > 0
            """
            list_params = [uid, uid]
        cur.execute(f"""
            SELECT type, created_at, actor_id, actor_name, actor_avatar,
                   review_id, game_id, home_team_abbr, away_team_abbr,
                   game_date::text, reply_text, league
            FROM (
                -- Someone liked your review
                SELECT
                    'review_like'           AS type,
                    rl.created_at,
                    u.id                    AS actor_id,
                    u.display_name          AS actor_name,
                    u.avatar_url            AS actor_avatar,
                    gr.id                   AS review_id,
                    gr.game_id,
                    g.home_team_abbr,
                    g.away_team_abbr,
                    g.game_date,
                    NULL::text              AS reply_text,
                    COALESCE(g.league,'nba') AS league
                FROM review_likes rl
                JOIN game_reviews gr ON gr.id = rl.review_id
                JOIN users        u  ON u.id  = rl.user_id
                LEFT JOIN games   g  ON g.game_id = gr.game_id
                WHERE gr.user_id = %s AND rl.user_id != %s

                UNION ALL

                -- Someone replied to your review
                SELECT
                    'review_reply'          AS type,
                    rr.created_at,
                    u.id,
                    u.display_name,
                    u.avatar_url,
                    gr.id,
                    gr.game_id,
                    g.home_team_abbr,
                    g.away_team_abbr,
                    g.game_date,
                    rr.reply_text,
                    COALESCE(g.league,'nba')
                FROM review_replies rr
                JOIN game_reviews gr ON gr.id = rr.review_id
                JOIN users        u  ON u.id  = rr.user_id
                LEFT JOIN games   g  ON g.game_id = gr.game_id
                WHERE gr.user_id = %s AND rr.user_id != %s

                UNION ALL

                -- Pending friend requests sent to you
                SELECT
                    'friend_request'        AS type,
                    f.created_at,
                    u.id,
                    u.display_name,
                    u.avatar_url,
                    NULL::int,
                    NULL::text,
                    NULL::text,
                    NULL::text,
                    NULL::date,
                    NULL::text,
                    NULL::text
                FROM friendships f
                JOIN users u ON u.id = f.sender_id
                WHERE f.receiver_id = %s AND f.status = 'pending'
                {list_arm}
            ) n
            ORDER BY created_at DESC
            LIMIT %s
        """, (uid, uid, uid, uid, uid, *list_params, limit))

        rows = cur.fetchall()
        cur.close(); conn.close()

        notifications = []
        for r in rows:
            notifications.append({
                "type":           r["type"],
                "created_at":     r["created_at"].strftime("%Y-%m-%dT%H:%M:%SZ") if r["created_at"] else None,
                "actor_id":       r["actor_id"],
                "actor_name":     r["actor_name"],
                "actor_avatar":   r["actor_avatar"],
                "review_id":      r["review_id"],
                "game_id":        r["game_id"],
                "home_team_abbr": r["home_team_abbr"],
                "away_team_abbr": r["away_team_abbr"],
                "game_date":      r["game_date"],
                "reply_text":     r["reply_text"],
                "league":         r["league"],
            })
        return jsonify({"notifications": notifications})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GET /api/users/<user_id>/profile  (public)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/users/<int:user_id>/profile")
def get_user_profile(user_id):
    viewer = current_user()  # may be None

    try:
        conn = get_conn()
        cur  = conn.cursor()

        cur.execute("""
            SELECT id, display_name, avatar_url, favorite_team, display_name_set, created_at, is_pro, xp, equipped_ring, equipped_title
            FROM users WHERE id = %s
        """, (user_id,))
        user = cur.fetchone()
        if not user:
            cur.close(); conn.close()
            return jsonify({"error": "User not found"}), 404

        cur.execute("""
            SELECT
                COUNT(*)                             AS total_reviews,
                ROUND(AVG(rating)::numeric, 2)       AS avg_rating,
                COUNT(*) FILTER (WHERE rating = 10)  AS five_star_count,
                COUNT(*) FILTER (WHERE rating <= 2)  AS half_star_count
            FROM game_reviews WHERE user_id = %s
        """, (user_id,))
        stats = dict(cur.fetchone())

        cur.execute("SELECT COUNT(*) AS n FROM game_watches WHERE user_id = %s", (user_id,))
        games_watched = int(cur.fetchone()["n"] or 0)

        # Rating distribution (1–10 buckets → displayed as ½–5 stars)
        cur.execute("""
            SELECT rating, COUNT(*) AS cnt
            FROM game_reviews WHERE user_id = %s
            GROUP BY rating ORDER BY rating
        """, (user_id,))
        dist = {r["rating"]: r["cnt"] for r in cur.fetchall()}

        # Friend status relative to viewer
        friend_status = None
        if viewer and viewer["id"] != user_id:
            cur.execute("""
                SELECT status, sender_id FROM friendships
                WHERE (sender_id = %s AND receiver_id = %s)
                   OR (sender_id = %s AND receiver_id = %s)
            """, (viewer["id"], user_id, user_id, viewer["id"]))
            fs = cur.fetchone()
            if fs:
                if fs["status"] == "accepted":
                    friend_status = "friends"
                elif fs["sender_id"] == viewer["id"]:
                    friend_status = "request_sent"
                else:
                    friend_status = "request_received"

        # Friend count
        cur.execute("""
            SELECT COUNT(*) FROM friendships
            WHERE (sender_id = %s OR receiver_id = %s) AND status = 'accepted'
        """, (user_id, user_id))
        friend_count = cur.fetchone()["count"]

        # Favorite games (up to 4, ordered by position)
        cur.execute("""
            SELECT fg.position, fg.game_id,
                   g.home_team_abbr, g.away_team_abbr,
                   g.home_score, g.away_score, g.game_date,
                   COALESCE(g.league, 'nba') AS league
            FROM favorite_games fg
            LEFT JOIN games g ON g.game_id = fg.game_id
            WHERE fg.user_id = %s
            ORDER BY fg.position
        """, (user_id,))
        favorites = [dict(r) for r in cur.fetchall()]

        # Favorite players (up to 4, ordered by position) — the pinned identity row
        cur.execute("""
            SELECT position, person_id, player_name, team,
                   COALESCE(league, 'nba') AS league
            FROM favorite_players
            WHERE user_id = %s
            ORDER BY position
        """, (user_id,))
        favorite_players = [dict(r) for r in cur.fetchall()]

        # Block status relative to viewer
        is_blocked = False
        if viewer and viewer["id"] != user_id:
            cur.execute("""
                SELECT 1 FROM user_blocks
                WHERE blocker_id = %s AND blocked_id = %s
            """, (viewer["id"], user_id))
            is_blocked = cur.fetchone() is not None

        cur.close(); conn.close()

        xp = int(user["xp"] or 0)
        return jsonify({
            "user": {
                "id":               user["id"],
                "display_name":     user["display_name"],
                "avatar_url":       user["avatar_url"],
                "favorite_team":    user["favorite_team"] or "",
                "display_name_set": user["display_name_set"],
                "member_since":     str(user["created_at"]),
                "is_pro":           bool(user["is_pro"]),
            },
            "stats": {
                "total_reviews":   int(stats["total_reviews"] or 0),
                "games_watched":   games_watched,
                "avg_rating":      round(float(stats["avg_rating"] or 0) / 2, 2),
                "five_star_count": int(stats["five_star_count"] or 0),
                "half_star_count": int(stats["half_star_count"] or 0),
                "distribution":    dist,
            },
            "ball_knowledge":  {**get_rank_info(xp), "equipped_ring": user.get("equipped_ring"), "equipped_title": user.get("equipped_title")},
            "favorites":       favorites,
            "favorite_players": favorite_players,
            "friend_count":    friend_count,
            "friend_status":   friend_status,
            "is_own":          viewer and viewer["id"] == user_id,
            "is_blocked":      is_blocked,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500





# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GET /api/users/search?q=<name>
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/users/search")
@login_required
def search_users():
    q     = request.args.get("q", "").strip()
    limit = min(int(request.args.get("limit", 10)), 20)
    me    = current_user()

    if not q:
        return jsonify({"users": []})

    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT id, display_name, avatar_url
            FROM users
            WHERE display_name ILIKE %s AND id != %s
            ORDER BY display_name
            LIMIT %s
        """, (q, me["id"], limit))
        users = [dict(r) for r in cur.fetchall()]
        cur.close(); conn.close()
        return jsonify({"users": users})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GET /api/friends  — my friends + pending requests
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/friends")
@login_required
def get_friends():
    me = current_user()
    try:
        conn = get_conn()
        cur  = conn.cursor()

        # Accepted friends
        cur.execute("""
            SELECT u.id, u.display_name, u.avatar_url,
                   f.created_at AS friends_since
            FROM friendships f
            JOIN users u ON u.id = CASE
                WHEN f.sender_id = %s THEN f.receiver_id
                ELSE f.sender_id END
            WHERE (f.sender_id = %s OR f.receiver_id = %s)
              AND f.status = 'accepted'
            ORDER BY u.display_name
        """, (me["id"], me["id"], me["id"]))
        friends = [dict(r) for r in cur.fetchall()]

        # Pending — received (I need to accept/decline)
        cur.execute("""
            SELECT u.id, u.display_name, u.avatar_url, f.id AS friendship_id
            FROM friendships f
            JOIN users u ON u.id = f.sender_id
            WHERE f.receiver_id = %s AND f.status = 'pending'
            ORDER BY f.created_at DESC
        """, (me["id"],))
        received = [dict(r) for r in cur.fetchall()]

        # Pending — sent (waiting on them)
        cur.execute("""
            SELECT u.id, u.display_name, u.avatar_url, f.id AS friendship_id
            FROM friendships f
            JOIN users u ON u.id = f.receiver_id
            WHERE f.sender_id = %s AND f.status = 'pending'
            ORDER BY f.created_at DESC
        """, (me["id"],))
        sent = [dict(r) for r in cur.fetchall()]

        cur.close(); conn.close()
        return jsonify({
            "friends":          friends,
            "requests_received": received,
            "requests_sent":    sent,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# POST /api/friends/<user_id>  — send friend request
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/friends/<int:target_id>", methods=["POST"])
@login_required
def send_friend_request(target_id):
    me = current_user()
    if me["id"] == target_id:
        return jsonify({"error": "Can't friend yourself"}), 400
    try:
        conn = get_conn()
        cur  = conn.cursor()
        # Check they exist
        cur.execute("SELECT id FROM users WHERE id = %s", (target_id,))
        if not cur.fetchone():
            return jsonify({"error": "User not found"}), 404
        # Check no existing relationship
        cur.execute("""
            SELECT id, status FROM friendships
            WHERE (sender_id = %s AND receiver_id = %s)
               OR (sender_id = %s AND receiver_id = %s)
        """, (me["id"], target_id, target_id, me["id"]))
        existing = cur.fetchone()
        if existing:
            if existing["status"] == "accepted":
                return jsonify({"error": "Already friends"}), 409
            return jsonify({"error": "Request already exists"}), 409

        cur.execute("""
            INSERT INTO friendships (sender_id, receiver_id, status)
            VALUES (%s, %s, 'pending') RETURNING id
        """, (me["id"], target_id))
        conn.commit()
        cur.close(); conn.close()
        return jsonify({"ok": True, "status": "request_sent"}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PATCH /api/friends/<user_id>  — accept friend request
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/friends/<int:target_id>", methods=["PATCH"])
@login_required
def accept_friend_request(target_id):
    me = current_user()
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            UPDATE friendships SET status = 'accepted', updated_at = NOW()
            WHERE sender_id = %s AND receiver_id = %s AND status = 'pending'
            RETURNING id
        """, (target_id, me["id"]))
        updated = cur.fetchone()
        conn.commit()
        cur.close(); conn.close()
        if not updated:
            return jsonify({"error": "No pending request found"}), 404
        return jsonify({"ok": True, "status": "friends"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DELETE /api/friends/<user_id>  — remove friend or decline/cancel request
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/friends/<int:target_id>", methods=["DELETE"])
@login_required
def remove_friend(target_id):
    me = current_user()
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            DELETE FROM friendships
            WHERE (sender_id = %s AND receiver_id = %s)
               OR (sender_id = %s AND receiver_id = %s)
            RETURNING id
        """, (me["id"], target_id, target_id, me["id"]))
        deleted = cur.fetchone()
        conn.commit()
        cur.close(); conn.close()
        if not deleted:
            return jsonify({"error": "No relationship found"}), 404
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Moderation ────────────────────────────────────────────────────

@app.route("/api/reports", methods=["POST"])
@login_required
def report_content():
    me = current_user()
    data = request.get_json(force=True) or {}
    review_id = data.get("review_id")
    if not review_id:
        return jsonify({"error": "review_id required"}), 400
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute(
            "INSERT INTO content_reports (reporter_id, review_id) VALUES (%s, %s)",
            (me["id"], review_id)
        )
        cur.execute("""
            SELECT gr.review_text, u.display_name, gr.game_id
            FROM game_reviews gr
            JOIN users u ON gr.user_id = u.id
            WHERE gr.id = %s
        """, (review_id,))
        row = cur.fetchone()
        conn.commit()
        cur.close(); conn.close()

        import os as _os, smtplib
        from email.mime.text import MIMEText
        report_email = _os.getenv("REPORT_EMAIL")
        smtp_user    = _os.getenv("SMTP_USER")
        smtp_pass    = _os.getenv("SMTP_PASS")
        if report_email and smtp_user and smtp_pass and row:
            try:
                body = (
                    f"New content report\n\n"
                    f"Reported by user ID: {me['id']} ({me.get('display_name','?')})\n"
                    f"Review ID: {review_id}\n"
                    f"Author: {row['display_name']}\n"
                    f"Game: {row['game_id']}\n"
                    f"Text: {row['review_text'] or '(no text)'}\n"
                )
                msg = MIMEText(body)
                msg["Subject"] = f"[ydkball] Content report — review #{review_id}"
                msg["From"]    = smtp_user
                msg["To"]      = report_email
                with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s:
                    s.login(smtp_user, smtp_pass)
                    s.sendmail(smtp_user, report_email, msg.as_string())
            except Exception as mail_err:
                print(f"[report] email failed: {mail_err}")

        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/users/<int:target_id>/block", methods=["POST"])
@login_required
def block_user(target_id):
    me = current_user()
    if me["id"] == target_id:
        return jsonify({"error": "Cannot block yourself"}), 400
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            INSERT INTO user_blocks (blocker_id, blocked_id)
            VALUES (%s, %s) ON CONFLICT DO NOTHING
        """, (me["id"], target_id))
        conn.commit()
        cur.close(); conn.close()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/users/<int:target_id>/block", methods=["DELETE"])
@login_required
def unblock_user(target_id):
    me = current_user()
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute(
            "DELETE FROM user_blocks WHERE blocker_id = %s AND blocked_id = %s",
            (me["id"], target_id)
        )
        conn.commit()
        cur.close(); conn.close()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Page routes ───────────────────────────────────────────────────
@app.route("/profile")
@app.route("/profile/")
def own_profile():
    return app.send_static_file("profile.html")

@app.route("/profile/<int:user_id>")
def user_profile(user_id):
    return app.send_static_file("profile.html")

@app.route("/compare")
@app.route("/compare.html")
def compare_page():
    return app.send_static_file("compare.html")


# ── Matchups API ──────────────────────────────────────────────────

_MATCHUP_SORT_LEADERS  = {'adj_delta', 'impact', 'possessions', 'min', 'avg_opp_fg_pct', 'avg_matchup_fg_pct'}
_MATCHUP_SORT_PAIRINGS = {'adj_delta', 'possessions', 'opp_season_fg_pct', 'fg_pct'}

@app.route("/api/matchups/leaders")
def matchups_leaders():
    """Top defenders ranked by opponent-adjusted FG% allowed, computed from player_matchups."""
    season      = request.args.get("season",      DEFAULT_SEASON)
    season_type = request.args.get("season_type", DEFAULT_SEASON_TYPE)
    min_poss    = max(0, int(request.args.get("min_poss", 200)))
    sort_col    = request.args.get("sort", "impact")
    sort_dir    = request.args.get("dir",  "desc").lower()
    limit       = min(int(request.args.get("limit", 150)), 300)
    pos_filter  = request.args.get("pos",  "ALL").strip().upper()
    team_filter = request.args.get("team", "ALL").strip().upper()

    if sort_col not in _MATCHUP_SORT_LEADERS:
        sort_col = "adj_delta"
    dir_sql = "ASC" if sort_dir == "asc" else "DESC"

    col_map = {
        "adj_delta":          "adj_delta",
        "impact":             "impact",
        "possessions":        "possessions",
        "min":                "ps.min",
        "avg_opp_fg_pct":     "avg_opp_fg_pct",
        "avg_matchup_fg_pct": "avg_matchup_fg_pct",
    }

    extra_where  = []
    extra_params = []
    if pos_filter  != "ALL":
        extra_where.append("p.position_group = %s")
        extra_params.append(pos_filter)
    if team_filter != "ALL":
        extra_where.append("ps.team_abbr = %s")
        extra_params.append(team_filter)
    extra_sql = ("AND " + " AND ".join(extra_where)) if extra_where else ""

    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute(f"""
            SELECT
                p.player_id,
                p.player_name,
                p.position_group,
                ps.team_abbr,
                SUM(pm.adj_delta * pm.possessions)         / NULLIF(SUM(pm.possessions), 0) AS adj_delta,
                SUM(pm.adj_delta * pm.possessions)                                           AS impact,
                SUM(pm.opp_season_fg_pct * pm.possessions) / NULLIF(SUM(pm.possessions), 0) AS avg_opp_fg_pct,
                SUM(pm.fg_pct * pm.possessions)            / NULLIF(SUM(pm.possessions), 0) AS avg_matchup_fg_pct,
                SUM(pm.possessions) AS possessions,
                ps.min
            FROM player_matchups pm
            JOIN players p ON pm.defender_id = p.player_id
            LEFT JOIN player_seasons ps ON pm.defender_id = ps.player_id
                AND pm.season = ps.season AND pm.season_type = ps.season_type
            WHERE pm.season = %s AND pm.season_type = %s
              {extra_sql}
            GROUP BY p.player_id, p.player_name, p.position_group, ps.team_abbr, ps.min
            HAVING SUM(pm.possessions) >= %s
            ORDER BY {col_map[sort_col]} {dir_sql} NULLS LAST
            LIMIT %s
        """, [season, season_type] + extra_params + [min_poss, limit])
        rows = [dict(r) for r in cur.fetchall()]
        cur.close(); conn.close()
        return jsonify({"defenders": rows, "season": season, "n": len(rows)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/matchups/pairings")
def matchups_pairings():
    """Individual defender×attacker pairing results."""
    season      = request.args.get("season",      DEFAULT_SEASON)
    season_type = request.args.get("season_type", DEFAULT_SEASON_TYPE)
    min_poss    = max(0, int(request.args.get("min_poss", 100)))
    sort_col    = request.args.get("sort", "adj_delta")
    sort_dir    = request.args.get("dir",  "desc").lower()
    limit       = min(int(request.args.get("limit", 150)), 300)
    pos_filter  = request.args.get("pos",  "ALL").strip().upper()
    team_filter = request.args.get("team", "ALL").strip().upper()

    if sort_col not in _MATCHUP_SORT_PAIRINGS:
        sort_col = "adj_delta"
    dir_sql = "ASC" if sort_dir == "asc" else "DESC"

    col_map = {
        "adj_delta":         "pm.adj_delta",
        "possessions":       "pm.possessions",
        "opp_season_fg_pct": "pm.opp_season_fg_pct",
        "fg_pct":            "pm.fg_pct",
    }

    extra_where  = []
    extra_params = []
    if pos_filter  != "ALL":
        extra_where.append("dp.position_group = %s")
        extra_params.append(pos_filter)
    if team_filter != "ALL":
        extra_where.append("dps.team_abbr = %s")
        extra_params.append(team_filter)
    extra_sql = ("AND " + " AND ".join(extra_where)) if extra_where else ""

    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute(f"""
            SELECT
                dp.player_id   AS defender_id,
                dp.player_name AS defender_name,
                dps.team_abbr  AS defender_team,
                op.player_id   AS attacker_id,
                op.player_name AS attacker_name,
                ops.team_abbr  AS attacker_team,
                pm.possessions,
                pm.fg_pct,
                pm.opp_season_fg_pct,
                pm.adj_delta,
                pm.fga,
                pm.fgm
            FROM player_matchups pm
            JOIN players dp ON pm.defender_id = dp.player_id
            JOIN players op ON pm.offensive_player_id = op.player_id
            LEFT JOIN player_seasons dps ON pm.defender_id = dps.player_id
                AND dps.season = pm.season AND dps.season_type = pm.season_type
            LEFT JOIN player_seasons ops ON pm.offensive_player_id = ops.player_id
                AND ops.season = pm.season AND ops.season_type = pm.season_type
            WHERE pm.season = %s AND pm.season_type = %s
              AND pm.possessions >= %s
              {extra_sql}
            ORDER BY {col_map[sort_col]} {dir_sql} NULLS LAST
            LIMIT %s
        """, [season, season_type, min_poss] + extra_params + [limit])
        rows = [dict(r) for r in cur.fetchall()]
        cur.close(); conn.close()
        return jsonify({"pairings": rows, "season": season, "n": len(rows)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/matchups/defender/<int:player_id>")
def matchups_defender(player_id):
    """Full matchup card for a specific defender."""
    season      = request.args.get("season",      DEFAULT_SEASON)
    season_type = request.args.get("season_type", DEFAULT_SEASON_TYPE)

    try:
        conn = get_conn()
        cur  = conn.cursor()

        cur.execute("""
            SELECT
                p.player_id,
                p.player_name,
                p.position_group,
                ps.team_abbr,
                ps.matchup_def_fg_pct_adj AS adj_delta,
                ps.matchup_poss           AS possessions,
                ps.min
            FROM player_seasons ps
            JOIN players p ON ps.player_id = p.player_id
            WHERE ps.player_id = %s AND ps.season = %s AND ps.season_type = %s
        """, (player_id, season, season_type))
        defender = cur.fetchone()

        cur.execute("""
            SELECT
                op.player_id       AS attacker_id,
                op.player_name     AS attacker_name,
                ops.team_abbr      AS attacker_team,
                op.position_group  AS attacker_pos,
                pm.possessions,
                pm.fg_pct,
                pm.opp_season_fg_pct,
                pm.adj_delta,
                pm.fga,
                pm.fgm
            FROM player_matchups pm
            JOIN players op ON pm.offensive_player_id = op.player_id
            LEFT JOIN player_seasons ops ON pm.offensive_player_id = ops.player_id
                AND ops.season = pm.season AND ops.season_type = pm.season_type
            WHERE pm.defender_id = %s AND pm.season = %s AND pm.season_type = %s
            ORDER BY pm.possessions DESC NULLS LAST
        """, (player_id, season, season_type))
        matchups = [dict(r) for r in cur.fetchall()]

        cur.close(); conn.close()
        if not defender:
            return jsonify({"error": "Player not found"}), 404
        return jsonify({"defender": dict(defender), "matchups": matchups, "season": season, "n": len(matchups)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/matchups")
@app.route("/matchups.html")
def matchups_page():
    return app.send_static_file("matchups.html")


# ── /api/trends ───────────────────────────────────────────────
# Returns risers and fallers for each of the 5 tracked stats.
# delta = avg of last N games  minus  avg of all prior games
# Only players with >= 10 mpg in their last N games are included.
# Players must have more than N games played so there are prior games to compare.

TREND_STATS = [
    {"key": "pts",    "label": "PPG"},
    {"key": "ts_pct", "label": "TS%"},
    {"key": "fg3m",   "label": "3PM"},
    {"key": "ast",    "label": "APG"},
    {"key": "reb",    "label": "RPG"},
]

def _safe(v):
    """Convert float-like values to JSON-safe Python float. NaN/Inf → None."""
    if v is None:
        return None
    try:
        f = float(v)
        return None if (math.isnan(f) or math.isinf(f)) else f
    except (TypeError, ValueError):
        return v


@app.route("/api/trends")
def get_trends():
    season    = request.args.get("season", DEFAULT_SEASON)
    n         = int(request.args.get("n", 5))
    team_days = int(request.args.get("team_days", 10))
    if n not in (5, 10, 15):
        n = 5

    try:
        conn = get_conn()
        cur  = conn.cursor()

        # Eligible players: team played within last `team_days` days AND
        # player appeared in at least one of their team's last `n` games.
        cur.execute("""
            WITH
            team_game_dates AS (
                SELECT DISTINCT
                    SUBSTRING(matchup FROM 1 FOR 3) AS team_abbr,
                    game_date
                FROM player_gamelogs
                WHERE season = %s AND matchup IS NOT NULL
            ),
            team_ranked AS (
                SELECT
                    team_abbr, game_date,
                    ROW_NUMBER() OVER (PARTITION BY team_abbr ORDER BY game_date DESC) AS team_rn,
                    MAX(game_date) OVER (PARTITION BY team_abbr) AS team_last_date
                FROM team_game_dates
            ),
            recent_team_dates AS (
                SELECT team_abbr, game_date
                FROM team_ranked
                WHERE team_rn <= %s
                  AND team_last_date >= CURRENT_DATE - (%s * INTERVAL '1 day')
            ),
            player_current_team AS (
                SELECT DISTINCT ON (player_id)
                    player_id,
                    SUBSTRING(matchup FROM 1 FOR 3) AS team_abbr
                FROM player_gamelogs
                WHERE season = %s AND matchup IS NOT NULL
                ORDER BY player_id, game_date DESC
            )
            SELECT DISTINCT pg.player_id
            FROM player_gamelogs pg
            JOIN player_current_team pct ON pct.player_id = pg.player_id
            JOIN recent_team_dates rtd
                ON rtd.team_abbr = pct.team_abbr
               AND rtd.game_date = pg.game_date
            WHERE pg.season = %s
        """, (season, n, team_days, season, season))
        eligible_ids = [r["player_id"] for r in cur.fetchall()]

        results = {}
        all_player_ids = set()

        for stat in TREND_STATS:
            col = stat["key"]
            if not eligible_ids:
                results[col] = {"label": stat["label"], "risers": [], "fallers": []}
                continue
            fga_gate = "AND fga >= 5" if col in ("ts_pct", "pts") else ""
            cur.execute(f"""
                WITH ranked AS (
                    SELECT
                        player_id,
                        player_name,
                        game_date,
                        {col},
                        min,
                        ROW_NUMBER() OVER (
                            PARTITION BY player_id
                            ORDER BY game_date DESC
                        ) AS rn,
                        COUNT(*) OVER (PARTITION BY player_id) AS total_games
                    FROM player_gamelogs
                    WHERE season = %s
                      AND {col} IS NOT NULL
                      AND min IS NOT NULL
                      AND NOT ({col} = 'NaN'::real)
                      AND player_id = ANY(%s)
                      {fga_gate}
                ),
                last_n AS (
                    SELECT
                        player_id,
                        player_name,
                        AVG({col})::numeric(7,4) AS last_n_avg,
                        AVG(min)::numeric(6,2)   AS last_n_mpg,
                        total_games
                    FROM ranked
                    WHERE rn <= %s
                    GROUP BY player_id, player_name, total_games
                    HAVING AVG(min) >= 10
                ),
                prior AS (
                    SELECT
                        player_id,
                        AVG({col})::numeric(7,4) AS prior_avg
                    FROM ranked
                    WHERE rn > %s
                    GROUP BY player_id
                    HAVING COUNT(*) >= %s
                )
                SELECT
                    l.player_id,
                    l.player_name,
                    l.last_n_avg,
                    l.last_n_mpg,
                    p.prior_avg,
                    (l.last_n_avg - p.prior_avg)::numeric(7,4) AS delta
                FROM last_n l
                JOIN prior p ON p.player_id = l.player_id
                ORDER BY delta DESC
            """, (season, eligible_ids, n, n, n))

            rows = [dict(r) for r in cur.fetchall()]
            for r in rows:
                for k in ("last_n_avg", "last_n_mpg", "prior_avg", "delta"):
                    r[k] = _safe(r[k])
                all_player_ids.add(r["player_id"])

            risers  = [r for r in rows if r["delta"] is not None and r["delta"] >= 0]
            fallers = sorted(
                [r for r in rows if r["delta"] is not None and r["delta"] < 0],
                key=lambda x: x["delta"]
            )
            results[col] = {"label": stat["label"], "risers": risers, "fallers": fallers}

        # Add most-recent team_abbr to every player row
        if all_player_ids:
            cur.execute("""
                SELECT DISTINCT ON (player_id)
                    player_id,
                    SUBSTRING(matchup FROM 1 FOR 3) AS team_abbr
                FROM player_gamelogs
                WHERE player_id = ANY(%s)
                  AND season = %s
                  AND matchup IS NOT NULL
                ORDER BY player_id, game_date DESC
            """, (list(all_player_ids), season))
            team_map = {r["player_id"]: r["team_abbr"] for r in cur.fetchall()}
            for stat_data in results.values():
                for r in stat_data["risers"] + stat_data["fallers"]:
                    r["team_abbr"] = team_map.get(r["player_id"])

        cur.close()
        conn.close()
        return jsonify({"n": n, "season": season, "team_days": team_days, "stats": results})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── /api/trends/gamelog ───────────────────────────────────────
# Returns the full game log for a single player (for the line graph).

@app.route("/api/trends/gamelog")
def get_trends_gamelog():
    player_id = request.args.get("player_id", type=int)
    season    = request.args.get("season",    DEFAULT_SEASON)

    if not player_id:
        return jsonify({"error": "player_id required"}), 400

    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT game_id, game_date, matchup, wl, min, fga, pts, ast, reb, fg3m, ts_pct
            FROM player_gamelogs
            WHERE player_id = %s
              AND season = %s
            ORDER BY game_date ASC
        """, (player_id, season))
        rows = [dict(r) for r in cur.fetchall()]
        float_cols = ("min", "fga", "pts", "ast", "reb", "fg3m", "ts_pct")
        for r in rows:
            if r["game_date"]:
                r["game_date"] = r["game_date"].strftime("%Y-%m-%d")
            for k in float_cols:
                r[k] = _safe(r[k])
            # Parse team from matchup (first 3 chars: "BOS vs. MIA" → "BOS")
            r["team_abbr"] = r["matchup"][:3] if r.get("matchup") else None
        cur.close()
        conn.close()
        return jsonify({"games": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/trends")
@app.route("/trends.html")
def trends_page():
    return app.send_static_file("trends.html")


# ══════════════════════════════════════════════════════════════════════════════
# PVA (Possession Value Added)
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/pva/leaders")
def pva_leaders():
    """
    Return the PVA leaderboard for a season.

    Query params:
      season       — e.g. "2024-25"  (defaults to current season)
      season_type  — "Regular Season" | "Playoffs"
      min_poss     — minimum offensive possessions (default 200)
      sort         — column to sort by (default "total_pva_per_100")
      dir          — "desc" | "asc" (default "desc")
      limit        — max rows (default 200)
    """
    season      = request.args.get("season",      DEFAULT_SEASON)
    season_type = request.args.get("season_type", "Regular Season")
    min_poss    = request.args.get("min_poss",    200,  type=int)
    sort        = request.args.get("sort",        "total_pva_per_100")
    direction   = request.args.get("dir",         "desc").lower()
    limit       = request.args.get("limit",       200,  type=int)

    allowed_sorts = {
        "total_pva_per_100", "off_pva_per_100", "def_pva_per_100",
        "total_pva", "off_pva", "def_pva",
        "off_possessions", "total_possessions",
        "pva_from_makes", "pva_from_misses", "pva_from_turnovers",
        "avg_actual_pts", "avg_expected_pts",
    }
    if sort not in allowed_sorts:
        sort = "total_pva_per_100"
    order = "DESC" if direction != "asc" else "ASC"

    try:
        conn = get_conn()
        cur  = conn.cursor()

        cur.execute(f"""
            SELECT
                pv.player_id,
                pv.player_name,
                pv.off_possessions,
                pv.def_possessions,
                pv.total_possessions,
                pv.off_pva,
                pv.def_pva,
                pv.total_pva,
                pv.off_pva_per_100,
                pv.def_pva_per_100,
                pv.total_pva_per_100,
                pv.pva_from_makes,
                pv.pva_from_misses,
                pv.pva_from_turnovers,
                pv.avg_expected_pts,
                pv.avg_actual_pts,
                ps.team_abbr
            FROM player_pva_season pv
            LEFT JOIN player_seasons ps
                   ON ps.player_id = pv.player_id
                  AND ps.season     = pv.season
                  AND ps.season_type = pv.season_type
            WHERE pv.season      = %s
              AND pv.season_type = %s
              AND pv.off_possessions >= %s
            ORDER BY {sort} {order}
            LIMIT %s
        """, (season, season_type, min_poss, limit))

        rows = cur.fetchall()
        float_cols = (
            "off_pva", "def_pva", "total_pva",
            "off_pva_per_100", "def_pva_per_100", "total_pva_per_100",
            "pva_from_makes", "pva_from_misses", "pva_from_turnovers",
            "avg_expected_pts", "avg_actual_pts",
        )
        result = []
        for r in rows:
            row = dict(r)
            for k in float_cols:
                row[k] = _safe(row[k])
            result.append(row)

        cur.close(); conn.close()
        return jsonify({
            "season": season,
            "season_type": season_type,
            "min_poss": min_poss,
            "players": result,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/pva/player/<int:player_id>")
def pva_player(player_id):
    """
    Return all seasons of PVA data for a single player, plus their last
    10 possessions (for game-log flavour context).
    """
    season      = request.args.get("season",      DEFAULT_SEASON)
    season_type = request.args.get("season_type", "Regular Season")

    try:
        conn = get_conn()
        cur  = conn.cursor()

        # Career PVA rows (all seasons)
        cur.execute("""
            SELECT season, season_type,
                   off_possessions, def_possessions, total_possessions,
                   off_pva, def_pva, total_pva,
                   off_pva_per_100, def_pva_per_100, total_pva_per_100,
                   pva_from_makes, pva_from_misses, pva_from_turnovers,
                   avg_expected_pts, avg_actual_pts,
                   computed_at
            FROM player_pva_season
            WHERE player_id = %s
            ORDER BY season DESC, season_type
        """, (player_id,))
        seasons = []
        float_cols = (
            "off_pva", "def_pva", "total_pva",
            "off_pva_per_100", "def_pva_per_100", "total_pva_per_100",
            "pva_from_makes", "pva_from_misses", "pva_from_turnovers",
            "avg_expected_pts", "avg_actual_pts",
        )
        for r in cur.fetchall():
            row = dict(r)
            for k in float_cols:
                row[k] = _safe(row[k])
            if row.get("computed_at"):
                row["computed_at"] = row["computed_at"].isoformat()
            seasons.append(row)

        # Per-game PVA: join possession outcomes with game log for context
        # (gives actual vs expected for games where this player was primary actor)
        cur.execute("""
            SELECT
                p.game_id,
                p.period,
                p.points_scored,
                p.expected_points,
                p.points_scored - p.expected_points AS pva,
                p.end_reason,
                p.score_margin_offense,
                p.start_clock_seconds
            FROM possessions p
            JOIN possession_events pe
                ON pe.possession_id = p.id
               AND pe.action_type IN ('2pt', '3pt', 'turnover', 'freethrow')
               AND pe.player_id = %s
            WHERE p.season      = %s
              AND p.expected_points IS NOT NULL
            ORDER BY p.game_seconds_start DESC
            LIMIT 50
        """, (player_id, season))
        recent = []
        for r in cur.fetchall():
            row = dict(r)
            row["expected_points"] = _safe(row["expected_points"])
            row["pva"]             = _safe(row["pva"])
            recent.append(row)

        cur.close(); conn.close()
        return jsonify({
            "player_id": player_id,
            "season": season,
            "season_type": season_type,
            "career": seasons,
            "recent_possessions": recent,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/pva/seasons")
def pva_seasons():
    """Return seasons that have computed PVA data."""
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT DISTINCT season, season_type,
                   COUNT(DISTINCT player_id) AS player_count
            FROM player_pva_season
            GROUP BY season, season_type
            ORDER BY season DESC
        """)
        rows = [dict(r) for r in cur.fetchall()]
        cur.close(); conn.close()
        return jsonify({"seasons": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/pva")
@app.route("/pva.html")
def pva_page():
    return app.send_static_file("pva.html")


# ── Adjusted WoWY ─────────────────────────────────────────────────────────────

@app.route("/api/adjusted-wowy/leaders")
def adjusted_wowy_leaders():
    season      = request.args.get("season", get_current_season())
    season_type = request.args.get("season_type", "Regular Season")
    min_poss    = int(request.args.get("min_poss", 500))
    sort_col    = request.args.get("sort", "adj_wowy")
    sort_dir    = request.args.get("dir", "desc")

    allowed_sorts = {"adj_wowy", "on_net_adj", "off_net_adj", "raw_wowy",
                     "on_net_raw", "off_net_raw", "on_poss"}
    if sort_col not in allowed_sorts:
        sort_col = "adj_wowy"
    order = "DESC" if sort_dir != "asc" else "ASC"

    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute(f"""
            SELECT w.player_id, w.player_name, w.team_abbr,
                   w.on_poss, w.off_poss,
                   w.on_net_adj, w.off_net_adj, w.adj_wowy,
                   w.on_net_raw, w.off_net_raw, w.raw_wowy
            FROM player_adjusted_wowy w
            WHERE w.season      = %s
              AND w.season_type = %s
              AND w.on_poss    >= %s
            ORDER BY {sort_col} {order}
        """, (season, season_type, min_poss))
        rows = [dict(r) for r in cur.fetchall()]
        cur.close(); conn.close()
        return jsonify({"season": season, "min_poss": min_poss, "players": rows})
    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500


@app.route("/api/adjusted-wowy/seasons")
def adjusted_wowy_seasons():
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT DISTINCT season, season_type, COUNT(DISTINCT player_id) AS player_count
            FROM player_adjusted_wowy
            GROUP BY season, season_type
            ORDER BY season DESC
        """)
        rows = [dict(r) for r in cur.fetchall()]
        cur.close(); conn.close()
        return jsonify({"seasons": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/adjusted-wowy/by-players")
def adjusted_wowy_by_players():
    """Return adjusted WoWY stats for specific player IDs (used by WoWY page toggle)."""
    season      = request.args.get("season", get_current_season())
    season_type = request.args.get("season_type", "Regular Season")
    players_raw = request.args.get("players", "")

    if not players_raw:
        return jsonify({"error": "players param required"}), 400

    try:
        player_ids = [int(p) for p in players_raw.split(",") if p.strip()]
    except ValueError:
        return jsonify({"error": "players must be comma-separated integers"}), 400

    if not player_ids:
        return jsonify({"error": "no valid player IDs"}), 400

    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT w.player_id, w.player_name, w.team_abbr,
                   w.on_poss, w.off_poss,
                   w.on_net_adj, w.off_net_adj, w.adj_wowy,
                   w.on_net_raw, w.off_net_raw, w.raw_wowy
            FROM player_adjusted_wowy w
            WHERE w.season      = %s
              AND w.season_type = %s
              AND w.player_id   = ANY(%s)
            ORDER BY w.adj_wowy DESC
        """, (season, season_type, player_ids))
        rows = [dict(r) for r in cur.fetchall()]
        cur.close(); conn.close()
        return jsonify({"season": season, "season_type": season_type, "players": rows})
    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500


@app.route("/impact")
@app.route("/impact.html")
def impact_page():
    return app.send_static_file("impact.html")


@app.route("/privacy")
@app.route("/privacy.html")
def privacy_page():
    return app.send_static_file("privacy.html")


# ── WoWY possession-data modes ─────────────────────────────────────────────────

def _wowy_team_id(cur, player_ids: list, season: str):
    """Return the offense_team_id most associated with these players this season."""
    cur.execute("""
        SELECT p.offense_team_id, COUNT(*) AS cnt
        FROM possessions p
        JOIN possession_lineups pl ON pl.possession_id = p.id
        WHERE pl.player_id = ANY(%s) AND pl.side = 'offense' AND p.season = %s
        GROUP BY p.offense_team_id
        ORDER BY cnt DESC LIMIT 1
    """, (player_ids, season))
    row = cur.fetchone()
    return row["offense_team_id"] if row else None


@app.route("/api/wowy/shot-profile")
def wowy_shot_profile():
    """Shot zone distribution per lineup combination, derived from possession data."""
    season     = request.args.get("season", get_current_season())
    players_raw = request.args.get("players", "")
    try:
        player_ids = [int(x) for x in players_raw.split(",") if x.strip()]
    except ValueError:
        return jsonify({"error": "Invalid player IDs"}), 400
    if not player_ids:
        return jsonify({"error": "No players specified"}), 400

    try:
        conn = get_conn()
        cur  = conn.cursor()

        team_id = _wowy_team_id(cur, player_ids, season)
        if not team_id:
            cur.close(); conn.close()
            return jsonify({"error": "No possession data for these players this season"}), 404

        # All team offensive possessions: lineup + shot zone
        cur.execute("""
            SELECT p.id AS pid, p.shot_zone,
                   array_agg(pl.player_id) AS lineup
            FROM possessions p
            JOIN possession_lineups pl
              ON pl.possession_id = p.id AND pl.side = 'offense'
            WHERE p.offense_team_id = %s AND p.season = %s
            GROUP BY p.id, p.shot_zone
        """, (team_id, season))
        rows = cur.fetchall()
        cur.close(); conn.close()

        ZONES = {1: "ra", 2: "paint", 3: "mid", 4: "c3", 5: "ab3"}
        player_set = set(player_ids)
        combos = {}  # frozenset(on_selected) → {total, ra, paint, mid, c3, ab3}

        for r in rows:
            on_sel = frozenset(set(r["lineup"]) & player_set)
            if on_sel not in combos:
                combos[on_sel] = {"total": 0, "ra": 0, "paint": 0, "mid": 0, "c3": 0, "ab3": 0}
            z = r["shot_zone"]
            if z and z > 0:
                combos[on_sel]["total"] += 1
                if z in ZONES:
                    combos[on_sel][ZONES[z]] += 1

        results = []
        for on_sel, s in combos.items():
            total = s["total"]
            pct = lambda k: round(s[k] / total * 100, 1) if total > 0 else None
            results.append({
                "on_players": sorted(on_sel),
                "fga":        total,
                "ra_pct":     pct("ra"),
                "paint_pct":  pct("paint"),
                "mid_pct":    pct("mid"),
                "c3_pct":     pct("c3"),
                "ab3_pct":    pct("ab3"),
            })

        return jsonify({"combos": results, "season": season})

    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500


@app.route("/api/wowy/shot-locations")
def wowy_shot_locations():
    """Raw FGA coordinates split by anchor player on/off court."""
    season = request.args.get("season", get_current_season())
    try:
        anchor_id = int(request.args.get("anchor", ""))
    except (ValueError, TypeError):
        return jsonify({"error": "anchor param required (player_id)"}), 400

    try:
        conn = get_conn()
        cur  = conn.cursor()

        team_id = _wowy_team_id(cur, [anchor_id], season)
        if not team_id:
            cur.close(); conn.close()
            return jsonify({"error": "No possession data for this player this season"}), 404

        cur.execute("""
            WITH anchor_on_poss AS (
                SELECT possession_id FROM possession_lineups
                WHERE player_id = %(anchor)s AND side = 'offense'
            )
            SELECT
                pe.x_legacy                         AS x,
                pe.y_legacy                         AS y,
                (pe.sub_type = 'made')              AS made,
                pe.action_type                      AS shot_type,
                (ao.possession_id IS NOT NULL)      AS anchor_on
            FROM possessions p
            JOIN possession_events pe ON pe.possession_id = p.id
            LEFT JOIN anchor_on_poss ao ON ao.possession_id = p.id
            WHERE p.offense_team_id = %(team_id)s
              AND p.season          = %(season)s
              AND pe.is_field_goal
              AND pe.x_legacy IS NOT NULL
              AND pe.y_legacy IS NOT NULL
        """, {"anchor": anchor_id, "team_id": team_id, "season": season})
        shot_rows = cur.fetchall()

        cur.execute("""
            WITH anchor_on_poss AS (
                SELECT possession_id FROM possession_lineups
                WHERE player_id = %(anchor)s AND side = 'offense'
            )
            SELECT
                COUNT(*) FILTER (WHERE ao.possession_id IS NOT NULL) AS on_poss,
                COUNT(*) FILTER (WHERE ao.possession_id IS NULL)     AS off_poss
            FROM possessions p
            LEFT JOIN anchor_on_poss ao ON ao.possession_id = p.id
            WHERE p.offense_team_id = %(team_id)s
              AND p.season          = %(season)s
        """, {"anchor": anchor_id, "team_id": team_id, "season": season})
        counts = cur.fetchone()
        cur.close(); conn.close()

        on_shots, off_shots = [], []
        for r in shot_rows:
            shot = {
                "x":    float(r["x"]),
                "y":    float(r["y"]),
                "made": bool(r["made"]),
                "is3":  r["shot_type"] == "3pt",
            }
            if r["anchor_on"]:
                on_shots.append(shot)
            else:
                off_shots.append(shot)

        return jsonify({
            "on_shots":  on_shots,
            "off_shots": off_shots,
            "on_poss":   int(counts["on_poss"]  or 0),
            "off_poss":  int(counts["off_poss"] or 0),
            "season":    season,
        })

    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500


@app.route("/api/wowy/stat-line")
def wowy_stat_line():
    """Per-teammate stat shifts for an anchor player.

    For each teammate on the same team, returns their individual per-100
    stats split by whether the anchor player is on the floor (ON) or off (OFF).
    """
    season     = request.args.get("season", get_current_season())
    try:
        anchor_id = int(request.args.get("anchor", ""))
    except (ValueError, TypeError):
        return jsonify({"error": "anchor param required (player_id)"}), 400

    try:
        conn = get_conn()
        cur  = conn.cursor()

        team_id = _wowy_team_id(cur, [anchor_id], season)
        if not team_id:
            cur.close(); conn.close()
            return jsonify({"error": "No possession data for this player this season"}), 404

        # ── Per-teammate stats split by anchor on/off ──────────────
        # CTE 1: mark every team offensive possession as anchor_on or not.
        # CTE 2: for each (teammate, possession), get their event stats.
        # CTE 3: assists where the teammate is the assister (separate field).
        # Separate CTEs avoid the N×M cross-product from joining lineups + events.
        cur.execute("""
            WITH anchor_on_poss AS (
                SELECT possession_id
                FROM possession_lineups
                WHERE player_id = %(anchor)s AND side = 'offense'
            ),
            teammate_poss AS (
                SELECT pl.player_id,
                       pl.possession_id,
                       (ao.possession_id IS NOT NULL) AS anchor_on
                FROM possession_lineups pl
                JOIN possessions p ON p.id = pl.possession_id
                LEFT JOIN anchor_on_poss ao ON ao.possession_id = pl.possession_id
                WHERE p.offense_team_id = %(team_id)s
                  AND p.season          = %(season)s
                  AND pl.side           = 'offense'
                  AND pl.player_id     != %(anchor)s
            ),
            teammate_events AS (
                SELECT tp.player_id, tp.anchor_on,
                    SUM(CASE WHEN pe.is_field_goal THEN 1 ELSE 0 END)                                          AS fga,
                    SUM(CASE WHEN pe.is_field_goal AND pe.action_type = '2pt'
                                  AND pe.sub_type = 'made' THEN 2 ELSE 0 END)
                  + SUM(CASE WHEN pe.is_field_goal AND pe.action_type = '3pt'
                                  AND pe.sub_type = 'made' THEN 3 ELSE 0 END)                                  AS fg_pts,
                    SUM(CASE WHEN pe.is_field_goal AND pe.sub_type = 'made' THEN 1 ELSE 0 END)                 AS fgm,
                    SUM(CASE WHEN pe.is_field_goal AND pe.action_type = '3pt' THEN 1 ELSE 0 END)               AS fg3a,
                    SUM(CASE WHEN pe.is_field_goal AND pe.action_type = '3pt'
                                  AND pe.sub_type = 'made' THEN 1 ELSE 0 END)                                  AS fg3m,
                    SUM(CASE WHEN pe.action_type = 'freethrow' AND pe.sub_type = 'made' THEN 1 ELSE 0 END)     AS ftm,
                    SUM(CASE WHEN pe.action_type = 'rebound' THEN 1 ELSE 0 END)                                AS reb,
                    SUM(CASE WHEN pe.action_type = 'turnover' THEN 1 ELSE 0 END)                               AS tov
                FROM teammate_poss tp
                JOIN possession_events pe
                  ON pe.possession_id = tp.possession_id AND pe.player_id = tp.player_id
                GROUP BY tp.player_id, tp.anchor_on
            ),
            teammate_ast AS (
                SELECT tp.player_id, tp.anchor_on, COUNT(*) AS ast
                FROM teammate_poss tp
                JOIN possession_events pe ON pe.possession_id = tp.possession_id
                WHERE pe.assist_player_id = tp.player_id
                  AND pe.is_field_goal AND pe.sub_type = 'made'
                GROUP BY tp.player_id, tp.anchor_on
            ),
            poss_counts AS (
                SELECT player_id, anchor_on, COUNT(*) AS poss
                FROM teammate_poss
                GROUP BY player_id, anchor_on
            )
            SELECT pc.player_id, pc.anchor_on, pc.poss,
                   COALESCE(te.fga,    0) AS fga,
                   COALESCE(te.fg_pts, 0) AS fg_pts,
                   COALESCE(te.fgm,    0) AS fgm,
                   COALESCE(te.fg3a,   0) AS fg3a,
                   COALESCE(te.fg3m,   0) AS fg3m,
                   COALESCE(te.ftm,    0) AS ftm,
                   COALESCE(te.reb,    0) AS reb,
                   COALESCE(te.tov,    0) AS tov,
                   COALESCE(ta.ast,    0) AS ast
            FROM poss_counts pc
            LEFT JOIN teammate_events te USING (player_id, anchor_on)
            LEFT JOIN teammate_ast    ta USING (player_id, anchor_on)
            ORDER BY pc.player_id, pc.anchor_on
        """, {"anchor": anchor_id, "team_id": team_id, "season": season})
        rows = cur.fetchall()

        # Resolve player names
        teammate_ids = list({r["player_id"] for r in rows})
        cur.execute(
            "SELECT player_id, player_name FROM players WHERE player_id = ANY(%s)",
            (teammate_ids,)
        )
        name_map = {r["player_id"]: r["player_name"] for r in cur.fetchall()}

        # Resolve anchor name
        cur.execute("SELECT player_name FROM players WHERE player_id = %s", (anchor_id,))
        anc = cur.fetchone()
        anchor_name = anc["player_name"] if anc else str(anchor_id)

        cur.close(); conn.close()

        # Pivot ON / OFF rows per teammate
        by_player = {}
        for r in rows:
            pid = r["player_id"]
            if pid not in by_player:
                by_player[pid] = {}
            side = "on" if r["anchor_on"] else "off"
            by_player[pid][side] = r

        def p100(n, poss):
            return round(n / poss * 100, 1) if poss > 0 else None

        def efg(fgm, fg3m, fga):
            return round((fgm + 0.5 * fg3m) / fga * 100, 1) if fga > 0 else None

        def fg3pct(fg3m, fg3a):
            return round(fg3m / fg3a * 100, 1) if fg3a > 0 else None

        def diff(a, b):
            return round(a - b, 1) if a is not None and b is not None else None

        teammates = []
        for pid, sides in by_player.items():
            on  = sides.get("on",  {})
            off = sides.get("off", {})

            on_poss  = int(on.get("poss", 0)  or 0)
            off_poss = int(off.get("poss", 0) or 0)
            if on_poss + off_poss < 50:
                continue  # skip players with almost no shared minutes

            def stat(key, poss, row):
                return p100(int(row.get(key, 0) or 0), poss)

            pts_on   = p100(int(on.get("fg_pts",0) or 0) + int(on.get("ftm",0) or 0), on_poss)
            pts_off  = p100(int(off.get("fg_pts",0) or 0) + int(off.get("ftm",0) or 0), off_poss)
            efg_on   = efg(int(on.get("fgm",0) or 0),  int(on.get("fg3m",0) or 0),  int(on.get("fga",0) or 0))
            efg_off  = efg(int(off.get("fgm",0) or 0), int(off.get("fg3m",0) or 0), int(off.get("fga",0) or 0))
            ast_on   = stat("ast",  on_poss,  on)
            ast_off  = stat("ast",  off_poss, off)
            reb_on   = stat("reb",  on_poss,  on)
            reb_off  = stat("reb",  off_poss, off)
            tov_on   = stat("tov",  on_poss,  on)
            tov_off  = stat("tov",  off_poss, off)
            fg3a_on  = stat("fg3a", on_poss,  on)
            fg3a_off = stat("fg3a", off_poss, off)
            fg3p_on  = fg3pct(int(on.get("fg3m",0) or 0),  int(on.get("fg3a",0) or 0))
            fg3p_off = fg3pct(int(off.get("fg3m",0) or 0), int(off.get("fg3a",0) or 0))

            teammates.append({
                "player_id":   pid,
                "player_name": name_map.get(pid, str(pid)),
                "on_poss":     on_poss,
                "off_poss":    off_poss,
                "pts_on":  pts_on,  "pts_off":  pts_off,  "pts_diff":  diff(pts_on,  pts_off),
                "efg_on":  efg_on,  "efg_off":  efg_off,  "efg_diff":  diff(efg_on,  efg_off),
                "ast_on":  ast_on,  "ast_off":  ast_off,  "ast_diff":  diff(ast_on,  ast_off),
                "reb_on":  reb_on,  "reb_off":  reb_off,  "reb_diff":  diff(reb_on,  reb_off),
                "tov_on":  tov_on,  "tov_off":  tov_off,  "tov_diff":  diff(tov_on,  tov_off),
                "fg3a_on": fg3a_on, "fg3a_off": fg3a_off, "fg3a_diff": diff(fg3a_on, fg3a_off),
                "fg3p_on": fg3p_on, "fg3p_off": fg3p_off, "fg3p_diff": diff(fg3p_on, fg3p_off),
            })

        # Sort by on_poss descending (most shared minutes first)
        teammates.sort(key=lambda t: t["on_poss"], reverse=True)

        return jsonify({
            "anchor_id":   anchor_id,
            "anchor_name": anchor_name,
            "teammates":   teammates,
            "season":      season,
        })

    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500


start_sb_poller()


# ── WNBA ──────────────────────────────────────────────────────────────────────

_wnba_past_sb_cache:   dict = {}
_wnba_future_sb_cache: dict = {}
_wnba_today_sb_cache:  dict = {}

_ESPN_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept":     "application/json",
}


def _get_wnba_season() -> str:
    """Current WNBA season year, e.g. '2025'. Season runs May–October."""
    today = date.today()
    return str(today.year) if today.month >= 5 else str(today.year - 1)


# CDN tricode → app abbreviation (WNBA CDN uses different tricodes than our app)
_WNBA_CDN_ABBR_MAP = {"LVA": "LV", "LAS": "LA", "NYL": "NY", "GSV": "GS", "WAS": "WSH", "PDX": "POR"}

# Static schedule cache (2026 season, refreshed every 2h)
_wnba_cdn_schedule_cache: dict = {}   # {"dates": {dateStr: [game, ...]}, "ts": float}
_WNBA_CDN_SCHEDULE_TTL = 7200

# Final game poster cache — result never changes once a game is final
_wnba_game_posters_cache: dict = {}  # gameId -> {"away": int|None, "home": int|None}

# Live CDN scoreboard cache (today only, 2-minute TTL)
_wnba_cdn_today_cache: dict = {}


def _wnba_cdn_abbr(tricode: str) -> str:
    """Map WNBA CDN tricode to our app abbreviation."""
    return _WNBA_CDN_ABBR_MAP.get(tricode.upper(), tricode.upper())


def _wnba_cdn_game_dict(g: dict, game_date: str = "") -> dict:
    """Convert a WNBA CDN game entry into our standard game dict format."""
    away = g.get("awayTeam", {})
    home = g.get("homeTeam", {})
    raw_utc = g.get("gameTimeUTC", "")
    # CDN schedule uses "1900-01-01T..." as a placeholder for upcoming games.
    # Resolve to real UTC using gameStatusText + the known game date.
    if raw_utc.startswith("1900-") and game_date:
        raw_utc = _resolve_1900_game_time(g.get("gameStatusText", ""), game_date) or raw_utc
    return {
        "gameId":         g.get("gameId", ""),
        "gameStatus":     g.get("gameStatus", 1),
        "gameStatusText": g.get("gameStatusText", ""),
        "period":         g.get("period", 0),
        "gameClock":      g.get("gameClock", ""),
        "gameTimeUTC":    raw_utc,
        "away": {"abbr": _wnba_cdn_abbr(away.get("teamTricode", "")),
                 "score": int(away.get("score", 0) or 0)},
        "home": {"abbr": _wnba_cdn_abbr(home.get("teamTricode", "")),
                 "score": int(home.get("score", 0) or 0)},
    }


def _wnba_cdn_schedule() -> dict:
    """Return the 2026 WNBA CDN schedule as {dateStr: [game_dict, ...]}. Cached 2h."""
    cached = _wnba_cdn_schedule_cache
    if cached.get("dates") and _time.time() - cached.get("ts", 0) < _WNBA_CDN_SCHEDULE_TTL:
        return cached["dates"]
    try:
        resp = _cdn_get(
            "https://cdn.wnba.com/static/json/staticData/scheduleLeagueV2_1.json",
            headers=_WNBA_CDN_HEADERS, timeout=15)
        resp.raise_for_status()
        dates: dict[str, list] = {}
        for entry in resp.json().get("leagueSchedule", {}).get("gameDates", []):
            raw_date = entry.get("gameDate", "")         # "04/25/2026 00:00:00"
            try:
                from datetime import datetime as _dt2
                date_key = _dt2.strptime(raw_date, "%m/%d/%Y %H:%M:%S").strftime("%Y-%m-%d")
            except Exception:
                continue
            dates[date_key] = [_wnba_cdn_game_dict(g, date_key) for g in entry.get("games", [])]
        cached["dates"] = dates
        cached["ts"]    = _time.time()
        return dates
    except Exception as e:
        print(f"[wnba-cdn] schedule fetch error: {e}", flush=True)
        return cached.get("dates", {})


def _wnba_cdn_scoreboard_today(game_today: str) -> list | None:
    """Fetch today's WNBA games from the live CDN. Returns game-list or None."""
    c = _wnba_cdn_today_cache
    if c.get("date") == game_today and _time.time() - c.get("ts", 0) < 30:
        return c["games"]
    try:
        resp = _cdn_get(
            "https://cdn.wnba.com/static/json/liveData/scoreboard/todaysScoreboard_10.json",
            headers=_WNBA_CDN_HEADERS, timeout=8)
        resp.raise_for_status()
        cdn_games = resp.json().get("scoreboard", {}).get("games", [])
        cdn_date  = resp.json().get("scoreboard", {}).get("gameDate", "")
        if cdn_date != game_today:
            return None  # CDN still on prior date
        games = [_wnba_cdn_game_dict(g) for g in cdn_games]
        _crosscheck_tipoff(games, nba=False)
        for g in games:
            if g["gameStatus"] == 3 and g["gameId"]:
                _upsert_wnba_game(g["gameId"], game_today,
                                  g["home"]["abbr"], g["away"]["abbr"],
                                  g["home"]["score"], g["away"]["score"])
        c.update({"games": games, "date": game_today, "ts": _time.time()})
        return games
    except Exception as e:
        print(f"[wnba-cdn] live scoreboard error: {e}", flush=True)
        return None


def _espn_wnba_scoreboard(date_str: str) -> list | None:
    """Fetch WNBA scoreboard from ESPN for YYYY-MM-DD (fallback for pre-2026 dates).
    Returns list of game dicts (same shape as CDN) or None on failure."""
    date_compact = date_str.replace("-", "")
    url = (f"https://site.api.espn.com/apis/site/v2/sports/basketball/wnba"
           f"/scoreboard?dates={date_compact}")
    try:
        resp = _requests.get(url, headers=_ESPN_HEADERS, timeout=10)
        resp.raise_for_status()
        events = resp.json().get("events", [])
        games  = []
        for event in events:
            competitions = event.get("competitions", [])
            if not competitions:
                continue
            comp        = competitions[0]
            status_obj  = comp.get("status", {})
            status_name = status_obj.get("type", {}).get("name", "")
            if status_name == "STATUS_FINAL":
                game_status, game_status_text = 3, "Final"
            elif status_name == "STATUS_IN_PROGRESS":
                game_status = 2
                game_status_text = status_obj.get("displayClock", "")
            else:
                game_status, game_status_text = 1, "Scheduled"
            home_d, away_d = {}, {}
            for ct in comp.get("competitors", []):
                abbr  = ct.get("team", {}).get("abbreviation", "")
                score = int(ct.get("score", 0) or 0)
                if ct.get("homeAway") == "home":
                    home_d = {"abbr": abbr, "score": score}
                else:
                    away_d = {"abbr": abbr, "score": score}
            game_id = event.get("id", "")
            games.append({
                "gameId":         game_id,
                "gameStatus":     game_status,
                "gameStatusText": game_status_text,
                "period":         status_obj.get("period", 0),
                "gameClock":      status_obj.get("displayClock", ""),
                "gameTimeUTC":    comp.get("date", ""),
                "away": away_d,
                "home": home_d,
            })
            if game_status == 3 and game_id:
                _upsert_wnba_game(game_id, date_str,
                                  home_d.get("abbr", ""), away_d.get("abbr", ""),
                                  home_d.get("score", 0), away_d.get("score", 0))
        return games
    except Exception as e:
        print(f"[wnba] ESPN fetch error: {e}", flush=True)
        return None


# Game IDs we've already triggered a CDN boxscore ingest for
_wnba_ingested_game_ids: set = set()


def _wnba_cdn_ingest_game_bg(game_id: str, home_abbr: str, away_abbr: str):
    """
    Background thread: fetch WNBA CDN boxscore (uses WNBA personId = cdn.wnba.com IDs)
    and upsert each player's game stats into wnba_player_game_stats.
    Only runs for WNBA CDN game IDs (starts with '10').
    """
    if not str(game_id).startswith("10"):
        return  # ESPN game IDs — CDN boxscore not available
    try:
        # Cross-process dedup: the in-memory set is per gunicorn worker, so
        # check the DB before paying for a CDN fetch another worker already did.
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("SELECT 1 FROM wnba_player_game_stats WHERE game_id = %s LIMIT 1", (game_id,))
        already = cur.fetchone() is not None
        cur.close(); conn.close()
        if already:
            return

        url  = f"https://cdn.wnba.com/static/json/liveData/boxscore/boxscore_{game_id}.json"
        resp = _cdn_get(url, headers=_WNBA_CDN_HEADERS, timeout=12)
        resp.raise_for_status()
        game_data = resp.json().get("game", {})
        season    = _get_wnba_season()

        def _si(v):
            try:
                return int(float(str(v)))
            except Exception:
                return 0

        rows: list = []
        for side in ["awayTeam", "homeTeam"]:
            team_data = game_data.get(side, {})
            tricode   = team_data.get("teamTricode", "")
            abbr      = _wnba_cdn_abbr(tricode)
            for player in team_data.get("players", []):
                if not player.get("played"):
                    continue
                pid   = player.get("personId")
                if not pid:
                    continue
                stats = player.get("statistics", {})
                rows.append((
                    int(pid),
                    player.get("name", ""),
                    abbr,
                    game_id,
                    season,
                    _si(stats.get("points", 0)),
                    _si(stats.get("reboundsTotal", 0)),
                    _si(stats.get("assists", 0)),
                    _si(stats.get("turnovers", 0)),
                    _si(stats.get("fieldGoalsMade", 0)),
                    _si(stats.get("fieldGoalsAttempted", 0)),
                    _si(stats.get("threePointersMade", 0)),
                    _si(stats.get("threePointersAttempted", 0)),
                ))

        if not rows:
            return

        conn = get_conn()
        cur  = conn.cursor()
        for row in rows:
            cur.execute("""
                INSERT INTO wnba_player_game_stats
                    (player_id, player_name, team, game_id, season,
                     pts, reb, ast, tov, fgm, fga, fg3m, fg3a)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (player_id, game_id) DO UPDATE SET
                    pts  = EXCLUDED.pts,  reb  = EXCLUDED.reb,
                    ast  = EXCLUDED.ast,  tov  = EXCLUDED.tov,
                    fgm  = EXCLUDED.fgm,  fga  = EXCLUDED.fga,
                    fg3m = EXCLUDED.fg3m, fg3a = EXCLUDED.fg3a
            """, row)
        conn.commit()
        cur.close(); conn.close()

        for abbr in [home_abbr.upper(), away_abbr.upper()]:
            _wnba_team_star_cache.pop(abbr, None)

        print(f"[wnba-cdn] boxscore ingested: {game_id} ({len(rows)} players)", flush=True)
    except Exception as e:
        print(f"[wnba-cdn] boxscore ingest error {game_id}: {e}", flush=True)
        _wnba_ingested_game_ids.discard(game_id)  # allow retry


def _upsert_wnba_game(game_id, game_date, home_abbr, away_abbr, home_score, away_score):
    """Upsert a completed WNBA game into the shared games table."""
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            INSERT INTO games (
                game_id, season, season_type, game_date,
                home_team_abbr, away_team_abbr,
                home_score, away_score, status, league
            ) VALUES (%s, %s, 'Regular Season', %s, %s, %s, %s, %s, 'Final', 'wnba')
            ON CONFLICT (game_id) DO UPDATE SET
                home_score = EXCLUDED.home_score,
                away_score = EXCLUDED.away_score,
                status     = 'Final',
                updated_at = NOW()
            WHERE games.status != 'Final' OR games.home_score IS NULL
        """, (game_id, _get_wnba_season(), game_date,
              home_abbr, away_abbr, home_score, away_score))
        conn.commit()
        cur.close(); conn.close()
    except Exception as e:
        print(f"[wnba] upsert error: {e}", flush=True)
        return

    # Trigger a background CDN boxscore ingest the first time we see this game as Final
    if game_id not in _wnba_ingested_game_ids:
        _wnba_ingested_game_ids.add(game_id)
        _threading.Thread(
            target=_wnba_cdn_ingest_game_bg,
            args=(game_id, home_abbr, away_abbr),
            daemon=True
        ).start()


def _enrich_wnba_games(games: list):
    """Attach review stats to WNBA game dicts in-place."""
    if not games:
        return
    game_ids = [str(g.get("gameId", "")) for g in games if g.get("gameId")]
    if not game_ids:
        return
    review_stats: dict = {}
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT game_id,
                   COUNT(*) AS review_count,
                   ROUND((AVG(rating) / 2.0)::numeric, 2)::float AS avg_stars
            FROM game_reviews
            WHERE game_id = ANY(%s)
            GROUP BY game_id
        """, (game_ids,))
        for r in cur.fetchall():
            review_stats[r["game_id"]] = {
                "avg_stars":    r["avg_stars"],
                "review_count": int(r["review_count"] or 0),
            }
        cur.close(); conn.close()
    except Exception:
        pass
    for g in games:
        rs = review_stats.get(str(g.get("gameId", "")), {})
        g["avg_stars"]    = rs.get("avg_stars")
        g["review_count"] = rs.get("review_count", 0)
        g["is_playoffs"]  = False


# ── /api/wnba/scoreboard?date=YYYY-MM-DD ─────────────────────────────────────
@app.route("/api/wnba/scoreboard")
def get_wnba_scoreboard():
    date_str    = request.args.get("date", "").strip()
    _game_today = _compute_game_today()
    if not date_str:
        date_str = _game_today

    is_past  = date_str < _game_today
    is_today = date_str == _game_today

    if is_past and date_str in _wnba_past_sb_cache:
        return jsonify(_wnba_past_sb_cache[date_str]["payload"])

    cdn_season = _get_wnba_season()  # e.g. "2026"
    is_cdn_season = date_str >= f"{cdn_season}-01-01"  # CDN only has current season

    # Past — DB first, but only trust it if the count matches the CDN schedule.
    # If the server only saw some games finish before midnight (and the rest were
    # upserted later / never), the DB can have fewer rows than actually played.
    if is_past:
        try:
            conn = get_conn()
            cur  = conn.cursor()
            cur.execute("""
                SELECT game_id, home_team_abbr, away_team_abbr, home_score, away_score
                FROM games
                WHERE game_date = %s AND league = 'wnba' AND status = 'Final'
                ORDER BY game_id
            """, (date_str,))
            db_rows = cur.fetchall()
            cur.close(); conn.close()
            if db_rows:
                # For CDN-season dates, verify DB count against schedule so we
                # don't return a partial list when only some games were upserted.
                cdn_game_count = 0
                if is_cdn_season:
                    try:
                        cdn_game_count = len(_wnba_cdn_schedule().get(date_str, []))
                    except Exception:
                        pass
                if cdn_game_count == 0 or len(db_rows) >= cdn_game_count:
                    games = [{
                        "gameId": r["game_id"], "gameStatus": 3,
                        "gameStatusText": "Final", "period": 0,
                        "gameClock": "", "gameTimeUTC": "",
                        "away": {"abbr": r["away_team_abbr"], "score": int(r["away_score"] or 0)},
                        "home": {"abbr": r["home_team_abbr"], "score": int(r["home_score"] or 0)},
                    } for r in db_rows]
                    _enrich_wnba_games(games)
                    payload = {"games": games, "date": date_str}
                    _wnba_past_sb_cache[date_str] = {"payload": payload, "ts": _time.time()}
                    return jsonify(payload)
                # else: DB is incomplete — fall through to CDN schedule path
        except Exception:
            pass

    # Today — try WNBA CDN live scoreboard first (2-min cache), then ESPN fallback
    if is_today:
        c = _wnba_cdn_today_cache
        if c.get("date") == _game_today and _time.time() - c.get("ts", 0) < 30:
            return jsonify({"games": c["games"], "date": date_str})
        games = _wnba_cdn_scoreboard_today(_game_today)
        if games is not None:
            _enrich_wnba_games(games)
            payload = {"games": games, "date": date_str}
            _wnba_cdn_today_cache.update({"games": games, "date": _game_today, "ts": _time.time()})
            return jsonify(payload)

    # CDN season — try static schedule (covers both today-when-live-fails and future dates)
    if is_cdn_season:
        schedule = _wnba_cdn_schedule()
        cdn_games = schedule.get(date_str)
        if cdn_games is not None:
            if is_past and date_str in _wnba_past_sb_cache:
                return jsonify(_wnba_past_sb_cache[date_str]["payload"])
            # Upsert any Final games from the schedule
            for g in cdn_games:
                if g["gameStatus"] == 3 and g["gameId"]:
                    _upsert_wnba_game(g["gameId"], date_str,
                                      g["home"]["abbr"], g["away"]["abbr"],
                                      g["home"]["score"], g["away"]["score"])
            _enrich_wnba_games(cdn_games)
            payload = {"games": cdn_games, "date": date_str}
            if is_past:
                _wnba_past_sb_cache[date_str] = {"payload": payload, "ts": _time.time()}
            elif not is_today and date_str in _wnba_future_sb_cache:
                pass  # let it fall through each time for schedule accuracy
            return jsonify(payload)

    # Future non-CDN — check cache (TTL 60 min)
    if not is_past and not is_today and date_str in _wnba_future_sb_cache:
        entry = _wnba_future_sb_cache[date_str]
        if _time.time() - entry["ts"] < 3600:
            return jsonify(entry["payload"])

    # ESPN fallback (pre-2026 dates and CDN failures)
    games = _espn_wnba_scoreboard(date_str) or []
    _enrich_wnba_games(games)
    payload = {"games": games, "date": date_str}
    if is_past:
        _wnba_past_sb_cache[date_str] = {"payload": payload, "ts": _time.time()}
    elif not is_today:
        _wnba_future_sb_cache[date_str] = {"payload": payload, "ts": _time.time()}
    return jsonify(payload)


# ── /api/wnba/top-performers?date=YYYY-MM-DD ─────────────────────────────────
@app.route("/api/wnba/top-performers")
def get_wnba_top_performers():
    date_str    = request.args.get("date", "").strip()
    _game_today = _compute_game_today()
    if not date_str:
        date_str = _game_today

    is_today      = date_str == _game_today
    cdn_season    = _get_wnba_season()
    is_cdn_season = date_str >= f"{cdn_season}-01-01"

    # ── Get game list ─────────────────────────────────────────────
    games = None
    if is_today:
        games = _wnba_cdn_scoreboard_today(_game_today)
    if games is None and is_cdn_season:
        schedule = _wnba_cdn_schedule()
        cdn_games = schedule.get(date_str)
        if cdn_games is not None:
            games = cdn_games
    if games is None:
        games = _espn_wnba_scoreboard(date_str) or []

    if not games:
        return jsonify({"players": [], "date": date_str})

    all_players = []

    # ── CDN games: parallel boxscore fetch ────────────────────────
    cdn_game_ids = [g.get("gameId", "") for g in games
                    if str(g.get("gameId", "")).startswith("10")]
    espn_games   = [g for g in games if not str(g.get("gameId", "")).startswith("10")]

    if cdn_game_ids:
        boxscores = _wnba_fetch_cdn_boxscores_parallel(cdn_game_ids)
        for g in games:
            gid = g.get("gameId", "")
            if not str(gid).startswith("10"):
                continue
            box = boxscores.get(gid)
            if not box:
                continue
            away_abbr = g.get("away", {}).get("abbr", "")
            home_abbr = g.get("home", {}).get("abbr", "")
            matchup   = f"{away_abbr} @ {home_abbr}" if away_abbr else gid
            is_live   = g.get("gameStatus") == 2
            for side in ["awayTeam", "homeTeam"]:
                team_data = box.get(side, {})
                team_abbr = _wnba_cdn_abbr(team_data.get("teamTricode", ""))
                for player in team_data.get("players", []):
                    if not player.get("played"):
                        continue
                    pid   = player.get("personId")
                    if not pid:
                        continue
                    stats = player.get("statistics", {})
                    min_str = stats.get("minutes", "PT0M0.00S") or "PT0M0.00S"
                    try:
                        mins = float(min_str.replace("PT", "").replace("S", "").split("M")[0])
                    except Exception:
                        mins = 0
                    if mins < 1:
                        continue
                    def _si(v):
                        try: return int(float(str(v)))
                        except Exception: return 0
                    pts   = _si(stats.get("points", 0))
                    reb   = _si(stats.get("reboundsTotal", 0))
                    ast   = _si(stats.get("assists", 0))
                    all_players.append({
                        "player_id": pid,
                        "name":      player.get("name", ""),
                        "team":      team_abbr,
                        "matchup":   matchup,
                        "game_id":   gid,
                        "is_live":   is_live,
                        "pts":       pts,
                        "reb":       reb,
                        "ast":       ast,
                        "total":     pts + reb + ast,
                        "league":    "wnba",
                    })

    # ── ESPN games (pre-2026 IDs): legacy ESPN boxscore ──────────
    for g in espn_games:
        gid = g.get("gameId", "")
        if not gid:
            continue
        away_abbr = g.get("away", {}).get("abbr", "")
        home_abbr = g.get("home", {}).get("abbr", "")
        matchup   = f"{away_abbr} @ {home_abbr}" if away_abbr else gid
        is_live   = g.get("gameStatus") == 2
        try:
            url  = (f"https://site.api.espn.com/apis/site/v2/sports/basketball"
                    f"/wnba/summary?event={gid}")
            resp = _requests.get(url, headers=_ESPN_HEADERS, timeout=10)
            resp.raise_for_status()
            bs_players = resp.json().get("boxscore", {}).get("players", [])
            for team_entry in bs_players:
                team_abbr = team_entry.get("team", {}).get("abbreviation", "")
                for section in team_entry.get("statistics", []):
                    labels = section.get("names") or section.get("labels") or []
                    for ae in section.get("athletes", []):
                        stats_list = ae.get("stats", [])
                        if not stats_list:
                            continue
                        stat_map = {labels[i]: stats_list[i]
                                    for i in range(min(len(labels), len(stats_list)))}
                        min_val = stat_map.get("MIN", "0:00")
                        if not min_val or min_val in ("0:00", "--", ""):
                            continue
                        def _si(v):
                            try:
                                s = str(v)
                                return int(s.split("-")[0]) if "-" in s else int(float(s))
                            except Exception:
                                return 0
                        pts = _si(stat_map.get("PTS", 0))
                        reb = _si(stat_map.get("REB", 0))
                        ast = _si(stat_map.get("AST", 0))
                        athlete = ae.get("athlete", {})
                        all_players.append({
                            "player_id": athlete.get("id"),
                            "name":      athlete.get("displayName", ""),
                            "team":      team_abbr,
                            "matchup":   matchup,
                            "game_id":   gid,
                            "is_live":   is_live,
                            "pts":       pts,
                            "reb":       reb,
                            "ast":       ast,
                            "total":     pts + reb + ast,
                            "league":    "wnba",
                        })
        except Exception as e:
            print(f"[wnba] boxscore error {gid}: {e}", flush=True)

    all_players.sort(key=lambda x: x["total"], reverse=True)
    top5 = all_players[:5]

    # Attach the crowd's avg performance rating (0–5) to each, if any exist.
    try:
        pconn = get_conn(); pcur = pconn.cursor()
        for pl in top5:
            pcur.execute("""
                SELECT AVG(rating::float) AS a, COUNT(*) AS n
                FROM performance_reviews
                WHERE game_id = %s AND person_id = %s
            """, (str(pl["game_id"]), pl["player_id"]))
            r = pcur.fetchone()
            n = int(r["n"]) if r and r["n"] else 0
            pl["fan_stars"] = round(r["a"] / 2, 1) if n > 0 else None
        pcur.close()
    except Exception:
        for pl in top5:
            pl.setdefault("fan_stars", None)

    return jsonify({"players": top5, "date": date_str})


# ── ESPN WNBA team ID cache ───────────────────────────────────────────────────
_wnba_team_ids: dict = {}   # abbr → espn_team_id
_wnba_team_ids_ts: float = 0.0

def _get_wnba_team_ids() -> dict:
    """Fetch ESPN WNBA team list and return abbr→id map. Cached 24h."""
    global _wnba_team_ids, _wnba_team_ids_ts
    if _wnba_team_ids and _time.time() - _wnba_team_ids_ts < 86400:
        return _wnba_team_ids
    try:
        url  = "https://site.api.espn.com/apis/site/v2/sports/basketball/wnba/teams"
        resp = _requests.get(url, headers=_ESPN_HEADERS, timeout=10)
        resp.raise_for_status()
        result = {}
        for entry in resp.json().get("sports", [{}])[0].get("leagues", [{}])[0].get("teams", []):
            team = entry.get("team", {})
            abbr = team.get("abbreviation", "").upper()
            tid  = team.get("id", "")
            if abbr and tid:
                result[abbr] = str(tid)
        if result:
            _wnba_team_ids = result
            _wnba_team_ids_ts = _time.time()
        return result
    except Exception as e:
        print(f"[wnba] team IDs fetch error: {e}", flush=True)
        return _wnba_team_ids  # return stale cache


def _wnba_box_star(event_id: str) -> tuple[str | None, str | None]:
    """Fetch ESPN WNBA boxscore, return (away_player_id, home_player_id) top P+R+A each."""
    try:
        url  = (f"https://site.api.espn.com/apis/site/v2/sports/basketball"
                f"/wnba/summary?event={event_id}")
        resp = _requests.get(url, headers=_ESPN_HEADERS, timeout=10)
        resp.raise_for_status()
        bs_teams = resp.json().get("boxscore", {}).get("players", [])
        # bs_teams order: [away, home]
        results = []
        for team_entry in bs_teams:
            best_id, best_total = None, -1
            for section in team_entry.get("statistics", []):
                labels = section.get("names") or section.get("labels") or []
                for ae in section.get("athletes", []):
                    stats_list = ae.get("stats", [])
                    if not stats_list:
                        continue
                    stat_map = {labels[i]: stats_list[i]
                                for i in range(min(len(labels), len(stats_list)))}
                    min_val = stat_map.get("MIN", "0:00")
                    if not min_val or min_val in ("0:00", "--", ""):
                        continue
                    def _si(v):
                        try:
                            s = str(v)
                            return int(s.split("-")[0]) if "-" in s else int(float(s))
                        except Exception:
                            return 0
                    total = _si(stat_map.get("PTS", 0)) + _si(stat_map.get("REB", 0)) + _si(stat_map.get("AST", 0))
                    if total > best_total:
                        best_total = total
                        best_id = ae.get("athlete", {}).get("id")
            results.append(best_id)
        # pad to 2 slots
        while len(results) < 2:
            results.append(None)
        return results[0], results[1]
    except Exception as e:
        print(f"[wnba] box_star error {event_id}: {e}", flush=True)
        return None, None


_wnba_team_star_cache: dict = {}  # abbr → {"id": int|None, "ts": float}
_WNBA_TEAM_STAR_TTL = 6 * 3600


def _wnba_get_team_star(abbr: str) -> int | None:
    """
    Return the WNBA CDN player ID (cdn.wnba.com personId) of the top P+R+A player
    for a WNBA team. Checks current-season game stats first (auto-ingested from CDN
    boxscores), then falls back to wnba_player_seasons (historical ingest).
    Cached 6 hours per team.
    """
    abbr = abbr.upper()
    cached = _wnba_team_star_cache.get(abbr)
    if cached and _time.time() - cached["ts"] < _WNBA_TEAM_STAR_TTL:
        return cached["id"]

    player_id = None
    try:
        conn = get_conn()
        cur  = conn.cursor()
        season = _get_wnba_season()
        # 1. Try current-season game stats (CDN personIds, updates after each game)
        cur.execute("""
            SELECT player_id
            FROM   wnba_player_game_stats
            WHERE  team = %s AND season = %s
            GROUP  BY player_id
            ORDER  BY SUM(pts + reb + ast) DESC NULLS LAST
            LIMIT  1
        """, (abbr, season))
        row = cur.fetchone()
        if row:
            player_id = int(row["player_id"])
        else:
            # 2. Fall back to wnba_player_seasons (manually ingested historical data)
            cur.execute("""
                SELECT player_id
                FROM   wnba_player_seasons
                WHERE  team = %s AND season_type = 'Regular Season'
                ORDER  BY season DESC, (pts + reb + ast) DESC NULLS LAST
                LIMIT  1
            """, (abbr,))
            row = cur.fetchone()
            if row:
                player_id = int(row["player_id"])
        cur.close(); conn.close()
    except Exception as e:
        print(f"[wnba] team_star error {abbr}: {e}", flush=True)

    _wnba_team_star_cache[abbr] = {"id": player_id, "ts": _time.time()}
    return player_id


def _wnba_fetch_cdn_boxscores_parallel(game_ids, timeout=8):
    """Fetch WNBA CDN boxscores for multiple game IDs in parallel.
    Returns a dict mapping game_id -> game dict (or None on failure)."""
    def _fetch_one(gid):
        try:
            url  = f"https://cdn.wnba.com/static/json/liveData/boxscore/boxscore_{gid}.json"
            resp = _cdn_get(url, headers=_WNBA_CDN_HEADERS, timeout=timeout)
            if resp.status_code == 200:
                return gid, resp.json().get("game", {})
        except Exception:
            pass
        return gid, None

    results = {}
    with ThreadPoolExecutor(max_workers=min(len(game_ids), 12)) as pool:
        futures = {pool.submit(_fetch_one, gid): gid for gid in game_ids}
        for fut in as_completed(futures):
            gid, box = fut.result()
            results[gid] = box
    return results


@app.route("/api/wnba/game-posters", methods=["POST"])
def get_wnba_game_posters():
    """
    Returns WNBA CDN player IDs (Integer) for scoreboard poster headers.

    Final CDN games (gameId starts with '10') → actual P+R+A leader from CDN boxscore.
    All others (pre-2026 ESPN IDs, upcoming/live) → top player from wnba_player_game_stats
    or wnba_player_seasons DB.

    Body:    {"games": [{"gameId":"...","away":"NY","home":"IND","status":3}, ...]}
    Returns: {"posters": {"<gameId>": {"away": <int|null>, "home": <int|null>}}}
    """
    body  = request.get_json(force=True, silent=True) or {}
    games = body.get("games", [])
    if not games:
        return jsonify({"posters": {}})

    posters: dict = {}

    # ── Final CDN games: fetch actual boxscore leaders ────────────
    cdn_final   = [g for g in games if str(g.get("gameId", "")).startswith("10")
                   and int(g.get("status", 1) or 1) == 3]
    other_games = [g for g in games if g not in cdn_final]

    if cdn_final:
        uncached_final = [g for g in cdn_final if g.get("gameId") not in _wnba_game_posters_cache]
        if uncached_final:
            boxscores = _wnba_fetch_cdn_boxscores_parallel([g["gameId"] for g in uncached_final])
            for g in uncached_final:
                gid = g.get("gameId", "")
                box = boxscores.get(gid)
                if box:
                    _wnba_game_posters_cache[gid] = {
                        "away": _box_star(box.get("awayTeam", {})),
                        "home": _box_star(box.get("homeTeam", {})),
                    }
        for g in cdn_final:
            gid = g.get("gameId", "")
            if gid in _wnba_game_posters_cache:
                posters[gid] = _wnba_game_posters_cache[gid]
            else:
                other_games.append(g)  # CDN miss → fall back to DB

    # ── Upcoming / live / pre-2026: team star from DB ─────────────
    if other_games:
        now       = _time.time()
        all_abbrs = {g.get("away", "").upper() for g in other_games} | \
                    {g.get("home", "").upper() for g in other_games}
        uncached  = [a for a in all_abbrs if a and
                     not (a in _wnba_team_star_cache and
                          now - _wnba_team_star_cache[a]["ts"] < _WNBA_TEAM_STAR_TTL)]

        if uncached:
            season = _get_wnba_season()
            try:
                conn = get_conn()
                cur  = conn.cursor()
                # Check current-season game stats first (CDN ingest)
                cur.execute("""
                    SELECT DISTINCT ON (team) team, player_id
                    FROM (
                        SELECT team, player_id, SUM(pts + reb + ast) AS total
                        FROM   wnba_player_game_stats
                        WHERE  team = ANY(%s) AND season = %s
                        GROUP  BY team, player_id
                    ) agg
                    ORDER  BY team, total DESC NULLS LAST
                """, (uncached, season))
                found = set()
                for row in cur.fetchall():
                    abbr = row["team"]
                    _wnba_team_star_cache[abbr] = {"id": int(row["player_id"]), "ts": now}
                    found.add(abbr)
                # Fall back to historical seasons for any still uncached
                still_missing = [a for a in uncached if a not in found]
                if still_missing:
                    cur.execute("""
                        SELECT DISTINCT ON (team) team, player_id
                        FROM   wnba_player_seasons
                        WHERE  team = ANY(%s) AND season_type = 'Regular Season'
                        ORDER  BY team, season DESC, (pts + reb + ast) DESC NULLS LAST
                    """, (still_missing,))
                    for row in cur.fetchall():
                        abbr = row["team"]
                        _wnba_team_star_cache[abbr] = {"id": int(row["player_id"]), "ts": now}
                # Mark any abbrs with no data as None
                for abbr in uncached:
                    if abbr not in _wnba_team_star_cache or _wnba_team_star_cache[abbr]["ts"] != now:
                        _wnba_team_star_cache[abbr] = {"id": None, "ts": now}
                cur.close(); conn.close()
            except Exception as e:
                print(f"[wnba] game-posters bulk lookup error: {e}", flush=True)

        for g in other_games:
            gid     = g.get("gameId", "")
            away_cd = _wnba_team_star_cache.get(g.get("away", "").upper(), {})
            home_cd = _wnba_team_star_cache.get(g.get("home", "").upper(), {})
            posters[gid] = {"away": away_cd.get("id"), "home": home_cd.get("id")}

    return jsonify({"posters": posters})


# ── /api/wnba/leaderboard ────────────────────────────────────────────────────

@app.route("/api/wnba/leaderboard")
def get_wnba_leaderboard():
    """
    Returns all WNBA players for a season sorted by a stat.
    Query params: season (default most recent), season_type, sort (default pts), limit (default 50)
    """
    season      = request.args.get("season")
    season_type = request.args.get("season_type", "Regular Season")
    sort_col    = request.args.get("sort", "pts")
    limit       = min(int(request.args.get("limit", 50)), 200)

    allowed_sorts = {"pts", "reb", "ast", "stl", "blk", "tov", "min", "fg_pct",
                     "fg3_pct", "ft_pct", "fgm", "fga", "fg3m", "fg3a", "eff"}
    if sort_col not in allowed_sorts:
        sort_col = "pts"

    try:
        conn = get_conn()
        cur  = conn.cursor()
        if not season:
            cur.execute("SELECT MAX(season) FROM wnba_player_seasons WHERE season_type = %s", (season_type,))
            row = cur.fetchone()
            season = row["max"] if row else "2025"
        cur.execute(f"""
            SELECT player_id, player_name, team, season, season_type,
                   gp, min, pts, reb, ast, stl, blk, tov,
                   fgm, fga, fg_pct, fg3m, fg3a, fg3_pct,
                   ftm, fta, ft_pct, oreb, dreb, eff
            FROM   wnba_player_seasons
            WHERE  season = %s AND season_type = %s
            ORDER  BY {sort_col} DESC NULLS LAST
            LIMIT  %s
        """, (season, season_type, limit))
        players = [dict(r) for r in cur.fetchall()]
        cur.close(); conn.close()
        return jsonify({"players": players, "season": season, "season_type": season_type})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/wnba/player-stats")
def get_wnba_player_stats():
    """Single-player WNBA stat row."""
    player_id   = request.args.get("player_id")
    season      = request.args.get("season")
    season_type = request.args.get("season_type", "Regular Season")
    if not player_id:
        return jsonify({"error": "player_id required"}), 400
    try:
        conn = get_conn()
        cur  = conn.cursor()
        if not season:
            cur.execute("SELECT MAX(season) FROM wnba_player_seasons WHERE season_type = %s", (season_type,))
            row = cur.fetchone()
            season = row["max"] if row else "2025"
        cur.execute("""
            SELECT * FROM wnba_player_seasons
            WHERE player_id = %s AND season = %s AND season_type = %s
        """, (player_id, season, season_type))
        row = cur.fetchone()
        cur.close(); conn.close()
        if not row:
            return jsonify({"error": "Not found"}), 404
        return jsonify({"stats": dict(row)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── /api/wnba/team-stats/<abbr> ───────────────────────────────────────────────
_wnba_team_stats_cache: dict = {}   # abbr → {"data": dict, "ts": float}

@app.route("/api/wnba/team-stats/<abbr>")
def get_wnba_team_stats(abbr):
    """
    WNBA team season averages, mirroring the NBA preview/team-stats approach.

    Priority:
      1. wnba_player_seasons — most recent season with ≥5 qualifying players
         (shows current season once fetch_wnba_player_stats.py has run,
          otherwise falls back to previous season automatically)
      2. wnba_player_game_stats — current-season CDN ingest (pts/reb/ast only),
         for expansion teams (TOR, POR) that have no historical season rows yet
    """
    abbr = abbr.upper()
    cached = _wnba_team_stats_cache.get(abbr)
    if cached and _time.time() - cached["ts"] < 3600:
        return jsonify(cached["data"])

    result = None
    season = _get_wnba_season()

    # 1. wnba_player_game_stats — current-season CDN auto-ingest (full stat line)
    #    Becomes available after the first completed game; always reflects live season.
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT
                COUNT(DISTINCT game_id)              AS gp,
                ROUND(AVG(team_pts)::numeric,  1)    AS ppg,
                ROUND(AVG(team_reb)::numeric,  1)    AS rpg,
                ROUND(AVG(team_ast)::numeric,  1)    AS apg,
                ROUND(AVG(team_tov)::numeric,  1)    AS topg,
                SUM(team_fgm)                        AS tot_fgm,
                SUM(team_fga)                        AS tot_fga,
                SUM(team_fg3m)                       AS tot_fg3m,
                SUM(team_fg3a)                       AS tot_fg3a
            FROM (
                SELECT game_id,
                       SUM(pts)  AS team_pts, SUM(reb)  AS team_reb,
                       SUM(ast)  AS team_ast, SUM(tov)  AS team_tov,
                       SUM(fgm)  AS team_fgm, SUM(fga)  AS team_fga,
                       SUM(fg3m) AS team_fg3m, SUM(fg3a) AS team_fg3a
                FROM   wnba_player_game_stats
                WHERE  team = %s AND season = %s
                GROUP  BY game_id
            ) game_totals
        """, (abbr, season))
        row = cur.fetchone()
        cur.close(); conn.close()
        if row and row["gp"] and int(row["gp"]) >= 1:
            def safe_div(a, b): return round(a / b, 4) if b else None
            result = {
                "abbr":    abbr,
                "ppg":     float(row["ppg"])  if row["ppg"]  else None,
                "rpg":     float(row["rpg"])  if row["rpg"]  else None,
                "apg":     float(row["apg"])  if row["apg"]  else None,
                "topg":    float(row["topg"]) if row["topg"] else None,
                "fg_pct":  safe_div(row["tot_fgm"],  row["tot_fga"]),
                "fg3_pct": safe_div(row["tot_fg3m"], row["tot_fg3a"]),
            }
    except Exception as e:
        print(f"[wnba] team-stats game_stats error {abbr}: {e}", flush=True)

    # 2. wnba_player_seasons — most recent season (pre-season fallback, full stats)
    if not result:
        try:
            conn = get_conn()
            cur  = conn.cursor()
            cur.execute("""
                SELECT
                    SUM(pts  * gp) AS tot_pts,  SUM(reb  * gp) AS tot_reb,
                    SUM(ast  * gp) AS tot_ast,  SUM(tov  * gp) AS tot_tov,
                    SUM(fgm  * gp) AS tot_fgm,  SUM(fga  * gp) AS tot_fga,
                    SUM(fg3m * gp) AS tot_fg3m, SUM(fg3a * gp) AS tot_fg3a,
                    MAX(gp)        AS max_gp
                FROM   wnba_player_seasons
                WHERE  team = %s AND season_type = 'Regular Season' AND gp >= 5
                  AND  season = (
                      SELECT MAX(season) FROM wnba_player_seasons
                      WHERE  team = %s AND season_type = 'Regular Season' AND gp >= 5
                  )
            """, (abbr, abbr))
            row = cur.fetchone()
            cur.close(); conn.close()
            if row and row["max_gp"]:
                max_gp = float(row["max_gp"])
                def safe_div(a, b): return round(a / b, 4) if b else None
                result = {
                    "abbr":    abbr,
                    "ppg":     round(row["tot_pts"]  / max_gp, 1) if row["tot_pts"]  else None,
                    "rpg":     round(row["tot_reb"]  / max_gp, 1) if row["tot_reb"]  else None,
                    "apg":     round(row["tot_ast"]  / max_gp, 1) if row["tot_ast"]  else None,
                    "topg":    round(row["tot_tov"]  / max_gp, 1) if row["tot_tov"]  else None,
                    "fg_pct":  safe_div(row["tot_fgm"], row["tot_fga"]),
                    "fg3_pct": safe_div(row["tot_fg3m"], row["tot_fg3a"]),
                }
        except Exception as e:
            print(f"[wnba] team-stats seasons error {abbr}: {e}", flush=True)

    if not result:
        result = {"abbr": abbr, "ppg": None, "rpg": None, "apg": None,
                  "topg": None, "fg_pct": None, "fg3_pct": None}

    # Ensure all numeric values are JSON-serializable (guard against Decimal)
    safe = {k: float(v) if v is not None and not isinstance(v, (int, float, str, bool)) else v
            for k, v in result.items()}

    try:
        _wnba_team_stats_cache[abbr] = {"data": safe, "ts": _time.time()}
        return jsonify(safe)
    except Exception as e:
        print(f"[wnba] team-stats jsonify error {abbr}: {e}", flush=True)
        return jsonify({"abbr": abbr, "ppg": None, "rpg": None, "apg": None,
                        "topg": None, "fg_pct": None, "fg3_pct": None})


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Survival trivia — procedural NBA stat game (offseason engagement)
#   Free: one shared daily run.   Pro: unlimited (replayable 10-question) runs.
#   Answer validation is client-side (the autocomplete picker yields a real
#   player_id checked against each question's answer_ids); the client submits
#   its final score.
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _survival_today():
    """The Survival daily rolls over at **midnight ET** (NBA's timezone) — not the server's
    UTC midnight. Returns today's calendar date in America/New_York."""
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        from backports.zoneinfo import ZoneInfo
    from datetime import datetime as _dt
    return _dt.now(ZoneInfo("America/New_York")).date()


def _survival_streak_best(cur, user_id):
    """Best score + current consecutive-day streak across this user's daily results."""
    cur.execute("SELECT date, score FROM survival_results "
                "WHERE user_id = %s AND mode = 'daily' ORDER BY date DESC", (user_id,))
    rows = cur.fetchall()
    best = max((r["score"] for r in rows), default=0)
    streak, expected = 0, _survival_today()
    for r in rows:
        if r["date"] == expected:
            streak += 1
            expected = expected - timedelta(days=1)
        elif r["date"] < expected:
            break
    return best, streak


@app.route("/api/survival/daily")
@login_required
def survival_daily():
    """Today's shared daily run (questions + answer sets), plus the player's prior result.
    Login required — the one-run-a-day lock needs an identity (otherwise you could log out
    and replay forever)."""
    user  = current_user()
    today = _survival_today().isoformat()
    conn  = get_conn(); cur = conn.cursor()
    run   = survival_api.ensure_daily(conn, today)
    cur.execute("SELECT score FROM survival_results "
                "WHERE user_id = %s AND mode = 'daily' AND date = %s", (user["id"], today))
    r = cur.fetchone()
    best, streak = _survival_streak_best(cur, user["id"])
    return jsonify({
        "date": today, "lives": survival_api.LIVES, "questions": run,
        "your_result": (r["score"] if r else None), "best": best, "streak": streak,
    })


@app.route("/api/survival/daily/result", methods=["POST"])
@login_required
def survival_daily_result():
    """Record the daily result (first attempt counts) and grant Ball Knowledge XP.

    The client submits `picks` — the player_id it chose for each question, in order — and
    the **server** scores them against the stored daily (so the score isn't a raw number we
    blindly trust). XP: +10 for playing + 5 per correct answer (no perfect bonus — a 10/10
    is +60). Granted once per ET day (idempotent). Unlimited runs grant no XP. (Falls back to
    a trusted `score` for older clients during a deploy.)"""
    user  = current_user()
    data  = request.get_json(force=True, silent=True) or {}
    today = _survival_today().isoformat()
    conn  = get_conn(); cur = conn.cursor()

    # score server-side against the stored daily
    cur.execute("SELECT payload FROM survival_daily WHERE date = %s", (today,))
    row = cur.fetchone()
    questions = row["payload"] if row else []
    total = len(questions)
    picks = data.get("picks")
    if isinstance(picks, list) and total:
        correct = sum(1 for i, q in enumerate(questions)
                      if i < len(picks) and isinstance(picks[i], int)
                      and picks[i] in (q.get("answer_ids") or []))
    else:                                              # legacy client → trust the score
        correct = max(0, int(data.get("score", 0)))
    perfect = total > 0 and correct == total
    score = correct

    cur.execute("""INSERT INTO survival_results (user_id, mode, date, score)
                   VALUES (%s, 'daily', %s, %s)
                   ON CONFLICT (user_id, mode, date) DO NOTHING""", (user["id"], today, score))

    # Ball Knowledge XP — once per day: +10 for playing + 5 per correct (no perfect bonus)
    play_xp, per_correct = 10, 5
    xp_amount = play_xp + per_correct * correct
    new_total = _grant_xp(cur, user["id"], "survival_daily", today, xp_amount)
    conn.commit()

    granted   = new_total != -1
    xp_gained = xp_amount if granted else 0
    if granted:
        total_xp = new_total
    else:
        cur.execute("SELECT xp FROM users WHERE id = %s", (user["id"],))
        total_xp = (cur.fetchone() or {}).get("xp") or 0

    cur.execute("SELECT score FROM survival_results "
                "WHERE user_id = %s AND mode = 'daily' AND date = %s", (user["id"], today))
    official = cur.fetchone()["score"]
    best, streak = _survival_streak_best(cur, user["id"])
    return jsonify({
        "recorded": official, "best": best, "streak": streak, "total": total,
        "correct": correct, "perfect": perfect,
        "xp_gained": xp_gained, "total_xp": total_xp, "rank": get_rank_info(total_xp),
        # breakdown for the result screen's "Ball Knowledge earned" panel
        "xp_breakdown": {"play": play_xp, "per_correct": per_correct,
                         "correct": correct, "correct_xp": per_correct * correct,
                         "total": xp_amount},
    })


@app.route("/api/survival/unlimited")
@login_required
def survival_unlimited():
    """One on-demand question for an Unlimited run — Pro only. `pos` = 1-based position;
    `exclude` = comma-separated answer player_ids already used (so we don't repeat one).
    The client fetches these one at a time (capped at a 10-question run) and prefetches
    the next while you answer."""
    user = current_user()
    conn = get_conn(); cur = conn.cursor()
    # is_pro lives in the DB, not the session cookie — look it up fresh (also handles
    # users who upgraded mid-session).
    cur.execute("SELECT is_pro FROM users WHERE id = %s", (user["id"],))
    row = cur.fetchone()
    if not (row and row["is_pro"]):
        return jsonify({"error": "pro_required",
                        "message": "Unlimited runs are a Pro feature."}), 403
    pos = max(1, int(request.args.get("pos", 1)))
    exclude = [int(x) for x in request.args.get("exclude", "").split(",") if x.strip().isdigit()]
    q = survival_api.next_unlimited(conn, pos, exclude=exclude)
    return jsonify({"lives": survival_api.LIVES, "question": q})


@app.route("/api/survival/players")
def survival_players():
    """All players [{id, name}] for the client's autocomplete picker (cached)."""
    return jsonify({"players": survival_api.player_list(get_conn())})


_LB_LIMIT = 50  # daily leaderboards: how many ranked rows we return (`you` is always included separately)


@app.route("/api/survival/leaderboard")
@login_required
def survival_leaderboard():
    """Today's global daily leaderboard, ranked by score (ties share a rank)."""
    user  = current_user()
    today = _survival_today().isoformat()
    conn  = get_conn(); cur = conn.cursor()
    cur.execute("""SELECT sr.user_id, sr.score, u.display_name, u.avatar_url
                   FROM survival_results sr JOIN users u ON u.id = sr.user_id
                   WHERE sr.mode = 'daily' AND sr.date = %s
                   ORDER BY sr.score DESC, sr.created_at ASC""", (today,))
    rows = cur.fetchall()
    entries, you, prev_score, rank = [], None, None, 0
    for i, r in enumerate(rows, start=1):
        if r["score"] != prev_score:
            rank, prev_score = i, r["score"]
        entry = {"rank": rank, "user_id": r["user_id"], "display_name": r["display_name"],
                  "avatar_url": r["avatar_url"], "score": r["score"], "is_you": r["user_id"] == user["id"]}
        entries.append(entry)
        if entry["is_you"]:
            you = entry
    return jsonify({"date": today, "total_players": len(rows), "entries": entries[:_LB_LIMIT], "you": you})


# ── Poeltl: "guess the performance" ──────────────────────────────────────────
def _poeltl_streak(cur, user_id):
    """Current consecutive-day SOLVED streak (a missed/failed day ends it)."""
    cur.execute("SELECT date, solved FROM poeltl_results "
                "WHERE user_id = %s AND mode = 'daily' ORDER BY date DESC", (user_id,))
    rows = cur.fetchall()
    streak, expected = 0, _survival_today()
    for r in rows:
        if r["date"] == expected and r["solved"]:
            streak += 1
            expected = expected - timedelta(days=1)
        elif r["date"] == expected:        # played today/expected but didn't solve
            break
        elif r["date"] < expected:
            break
    return streak


@app.route("/api/poeltl/daily")
@login_required
def poeltl_daily():
    """Today's shared performance puzzle (box score only — the answer stays server-side),
    the autocomplete player bank, and the player's prior result. Login-gated, once/ET-day."""
    user  = current_user()
    today = _survival_today().isoformat()
    conn  = get_conn(); cur = conn.cursor()
    daily = poeltl_api.ensure_daily(conn, today)
    if not daily:
        return jsonify({"error": "unavailable", "message": "No puzzle for today yet."}), 503

    cur.execute("SELECT solved, guesses FROM poeltl_results "
                "WHERE user_id = %s AND mode = 'daily' AND date = %s", (user["id"], today))
    r = cur.fetchone()
    body = {
        "date": today,
        **poeltl_api.puzzle_view(daily),
        "players": survival_api.player_list(conn),
        "your_result": ({"solved": r["solved"], "guesses": r["guesses"]} if r else None),
        "streak": _poeltl_streak(cur, user["id"]),
    }
    if r:                                  # already played → reveal everything (clues/answer/opp/date)
        body.update(poeltl_api.end_reveal(daily))
    return jsonify(body)


@app.route("/api/poeltl/guess", methods=["POST"])
@login_required
def poeltl_guess():
    """Score the player's ordered guesses against today's stored answer; reveal one clue per
    wrong guess. When the round is done (solved or out of guesses) record the result + grant
    Ball Knowledge (once/ET-day, idempotent) and reveal the answer."""
    user  = current_user()
    data  = request.get_json(force=True, silent=True) or {}
    today = _survival_today().isoformat()
    conn  = get_conn(); cur = conn.cursor()

    daily = poeltl_api.ensure_daily(conn, today)
    if not daily:
        return jsonify({"error": "unavailable"}), 503

    guesses = [g for g in (data.get("guesses") or []) if isinstance(g, int)]
    res = poeltl_api.score_guesses(daily, guesses)

    out = dict(res)
    if res["done"]:
        solved = res["solved"]
        used   = res["guesses_used"]
        cur.execute("""INSERT INTO poeltl_results (user_id, mode, date, solved, guesses)
                       VALUES (%s, 'daily', %s, %s, %s)
                       ON CONFLICT (user_id, mode, date) DO NOTHING""",
                    (user["id"], today, solved, used))
        # XP: +10 for playing, +10 more for solving.
        xp_amount = 20 if solved else 10
        new_total = _grant_xp(cur, user["id"], "poeltl_daily", today, xp_amount)
        conn.commit()
        granted = new_total != -1
        if granted:
            total_xp = new_total
        else:
            cur.execute("SELECT xp FROM users WHERE id = %s", (user["id"],))
            total_xp = (cur.fetchone() or {}).get("xp") or 0
        out["xp_gained"] = xp_amount if granted else 0
        out["total_xp"] = total_xp
        out["rank"] = get_rank_info(total_xp)
        out["streak"] = _poeltl_streak(cur, user["id"])
    return jsonify(out)


@app.route("/api/poeltl/unlimited")
@login_required
def poeltl_unlimited():
    """A random practice round — Pro only. Ships the answer + full clue ladder for local play
    (unlimited is solo practice, no streak/XP). Also returns the autocomplete player bank."""
    user = current_user()
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT is_pro FROM users WHERE id = %s", (user["id"],))
    row = cur.fetchone()
    if not (row and row["is_pro"]):
        return jsonify({"error": "pro_required", "message": "Unlimited is a Pro feature."}), 403
    r = poeltl_api.unlimited_round(conn)
    if not r:
        return jsonify({"error": "unavailable"}), 503
    r["players"] = survival_api.player_list(conn)
    return jsonify(r)


@app.route("/api/poeltl/leaderboard")
@login_required
def poeltl_leaderboard():
    """Today's global daily leaderboard: solved beats unsolved, then fewest guesses (ties share a rank)."""
    user  = current_user()
    today = _survival_today().isoformat()
    conn  = get_conn(); cur = conn.cursor()
    cur.execute("""SELECT pr.user_id, pr.solved, pr.guesses, u.display_name, u.avatar_url
                   FROM poeltl_results pr JOIN users u ON u.id = pr.user_id
                   WHERE pr.mode = 'daily' AND pr.date = %s
                   ORDER BY pr.solved DESC, pr.guesses ASC, pr.created_at ASC""", (today,))
    rows = cur.fetchall()
    entries, you, prev_key, rank = [], None, None, 0
    for i, r in enumerate(rows, start=1):
        key = (r["solved"], r["guesses"])
        if key != prev_key:
            rank, prev_key = i, key
        entry = {"rank": rank, "user_id": r["user_id"], "display_name": r["display_name"],
                  "avatar_url": r["avatar_url"], "solved": r["solved"], "guesses": r["guesses"],
                  "is_you": r["user_id"] == user["id"]}
        entries.append(entry)
        if entry["is_you"]:
            you = entry
    return jsonify({"date": today, "total_players": len(rows), "entries": entries[:_LB_LIMIT], "you": you})


@app.route("/games")
@app.route("/games.html")
def games_page():
    return app.send_static_file("games.html")


@app.route("/survival")
@app.route("/survival.html")
def survival_page():
    return app.send_static_file("survival.html")


@app.route("/guesswho")
@app.route("/guesswho.html")
def guesswho_page():
    return app.send_static_file("guesswho.html")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GET /api/players/browse  — paginated player list with filters
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.route("/api/players/browse")
def browse_players():
    limit    = min(int(request.args.get("limit", 30)), 50)
    offset   = int(request.args.get("offset", 0))
    q        = request.args.get("q", "").strip()
    league   = request.args.get("league", "nba").lower()
    season   = request.args.get("season", "").strip()
    team     = request.args.get("team", "").strip()
    position = request.args.get("position", "").strip()  # NBA only: G / F / C
    sort     = request.args.get("sort", "name")
    is_wnba  = league == "wnba"

    _safe_sort = {
        "name": "player_name ASC",
        "pts":  "pts  DESC NULLS LAST",
        "reb":  "reb  DESC NULLS LAST",
        "ast":  "ast  DESC NULLS LAST",
        "stl":  "stl  DESC NULLS LAST",
        "blk":  "blk  DESC NULLS LAST",
        "gp":   "gp   DESC NULLS LAST",
    }
    order_sql = _safe_sort.get(sort, "player_name ASC")

    def _f(v): return round(float(v), 1) if v is not None else None

    try:
        conn = get_conn(); cur = conn.cursor()

        if is_wnba:
            cur.execute("SELECT MAX(season) FROM wnba_player_seasons WHERE season_type = 'Regular Season'")
            latest = (cur.fetchone() or {}).get("max") or "2025"
            if not season: season = latest
            conds  = ["season_type = 'Regular Season'", "season = %s"]
            params = [season]
            if q:    conds.append("player_name ILIKE %s"); params.append(f"%{q}%")
            if team: conds.append("team ILIKE %s");        params.append(team)
            inner_where = " AND ".join(conds)
            cur.execute(f"""
                SELECT * FROM (
                    SELECT DISTINCT ON (player_id)
                           player_id AS person_id, player_name,
                           NULL::text    AS position,
                           team          AS team_abbr,
                           pts, reb, ast, stl, blk, gp
                    FROM wnba_player_seasons
                    WHERE {inner_where}
                    ORDER BY player_id
                ) t
                ORDER BY {order_sql}
                LIMIT %s OFFSET %s
            """, params + [limit + 1, offset])
        else:
            cur.execute("SELECT MAX(season) FROM player_seasons WHERE season_type = 'Regular Season'")
            latest = (cur.fetchone() or {}).get("max") or "2024-25"
            if not season: season = latest
            conds  = ["ps.season = %s", "ps.season_type = 'Regular Season'"]
            params = [season]
            if q:        conds.append("p.player_name ILIKE %s");  params.append(f"%{q}%")
            if team:     conds.append("ps.team_abbr ILIKE %s");   params.append(team)
            if position: conds.append("p.position ILIKE %s");     params.append(f"{position}%")
            inner_where = " AND ".join(conds)
            cur.execute(f"""
                SELECT * FROM (
                    SELECT DISTINCT ON (p.player_id)
                           p.player_id AS person_id, p.player_name, p.position,
                           ps.team_abbr, ps.pts, ps.reb, ps.ast, ps.stl, ps.blk, ps.gp
                    FROM players p
                    JOIN player_seasons ps ON p.player_id = ps.player_id
                    WHERE {inner_where}
                    ORDER BY p.player_id
                ) t
                ORDER BY {order_sql}
                LIMIT %s OFFSET %s
            """, params + [limit + 1, offset])

        rows     = cur.fetchall()
        has_more = len(rows) > limit
        players  = [
            {
                "person_id":   r["person_id"],
                "player_name": r["player_name"],
                "position":    r.get("position"),
                "team_abbr":   r.get("team_abbr"),
                "pts": _f(r.get("pts")), "reb": _f(r.get("reb")), "ast": _f(r.get("ast")),
                "stl": _f(r.get("stl")), "blk": _f(r.get("blk")),
                "gp":  int(r["gp"]) if r.get("gp") is not None else None,
            }
            for r in rows[:limit]
        ]
        cur.close(); conn.close()
        return jsonify({"players": players, "has_more": has_more})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# Player profile  GET /api/players/<person_id>/profile
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_PLAYER_FOLLOWS_DDL = """
    CREATE TABLE IF NOT EXISTS player_follows (
        user_id    INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        person_id  INTEGER NOT NULL,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (user_id, person_id)
    )
"""


@app.route("/api/players/<int:person_id>/profile")
def get_player_profile(person_id):
    season  = request.args.get("season") or None
    league  = request.args.get("league", "nba").lower()
    is_wnba = league == "wnba"
    user    = current_user()
    user_id = user["id"] if user else None

    def _pct(v): return round(float(v), 3) if v is not None else None
    def _stat(v): return round(float(v), 1) if v is not None else None
    def _int(v):  return int(round(float(v))) if v is not None else None

    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute(_PLAYER_FOLLOWS_DDL)
        conn.commit()

        # ── Bio + seasons ─────────────────────────────────────────
        if is_wnba:
            # Try wnba_player_seasons first (historical); fall back to wnba_player_game_stats
            # (current-season CDN ingest) for new/expansion players not yet in the history table.
            cur.execute("""
                SELECT DISTINCT ON (player_id) player_id, player_name, team AS team_abbr, season
                FROM wnba_player_seasons
                WHERE player_id = %s
                ORDER BY player_id, season DESC
            """, (person_id,))
            bio_row = cur.fetchone()
            if not bio_row:
                cur.execute("""
                    SELECT DISTINCT ON (player_id) player_id, player_name, team AS team_abbr, season
                    FROM wnba_player_game_stats
                    WHERE player_id = %s AND player_name IS NOT NULL
                    ORDER BY player_id, season DESC NULLS LAST
                """, (person_id,))
                bio_row = cur.fetchone()
            if not bio_row:
                cur.close(); conn.close()
                return jsonify({"error": "Player not found"}), 404
            bio_row = dict(bio_row)
            player_info = {
                "personId": bio_row["player_id"],
                "name":     bio_row["player_name"],
                "position": None, "teamAbbr": bio_row.get("team_abbr"),
                "heightInches": None, "draftYear": None, "draftNumber": None, "college": None,
            }

            # Union both tables so current-season (game_stats only) always appears in the list
            cur.execute("""
                SELECT season FROM (
                    SELECT DISTINCT season FROM wnba_player_seasons WHERE player_id = %s
                    UNION
                    SELECT DISTINCT season FROM wnba_player_game_stats
                    WHERE player_id = %s AND season IS NOT NULL
                ) s ORDER BY season DESC
            """, (person_id, person_id))
            unique_seasons = [r["season"] for r in cur.fetchall()]
            team_abbr = bio_row.get("team_abbr")

        else:
            cur.execute("""
                SELECT player_id, player_name, position, position_group,
                       height_inches, draft_year, draft_number, college
                FROM players WHERE player_id = %s
            """, (person_id,))
            bio_row = cur.fetchone()
            if not bio_row:
                cur.close(); conn.close()
                return jsonify({"error": "Player not found"}), 404
            bio_row = dict(bio_row)
            player_info = {
                "personId":     bio_row["player_id"],
                "name":         bio_row["player_name"],
                "position":     bio_row.get("position"),
                "teamAbbr":     None,
                "heightInches": bio_row.get("height_inches"),
                "draftYear":    bio_row.get("draft_year"),
                "draftNumber":  bio_row.get("draft_number"),
                "college":      bio_row.get("college"),
            }

            # UNION with gamelogs so current season always appears even if
            # player_seasons hasn't been ingested yet for this season
            cur.execute("""
                SELECT season FROM (
                    SELECT DISTINCT season FROM player_seasons WHERE player_id = %s
                    UNION
                    SELECT DISTINCT season FROM player_gamelogs
                    WHERE player_id = %s AND season IS NOT NULL
                ) s ORDER BY season DESC
            """, (person_id, person_id))
            unique_seasons = [r["season"] for r in cur.fetchall()]
            # Get team_abbr from the most recent entry (prefer player_seasons, fall back to gamelogs)
            cur.execute("""
                SELECT team_abbr FROM player_seasons WHERE player_id = %s
                ORDER BY season DESC LIMIT 1
            """, (person_id,))
            ta_row = cur.fetchone()
            if not ta_row:
                cur.execute("""
                    SELECT SUBSTRING(matchup, 1, 3) AS team_abbr FROM player_gamelogs
                    WHERE player_id = %s AND matchup IS NOT NULL ORDER BY game_date DESC LIMIT 1
                """, (person_id,))
                ta_row = cur.fetchone()
            team_abbr = ta_row["team_abbr"] if ta_row else None

        wnba_default = _get_wnba_season()
        fallback_season = wnba_default if is_wnba else DEFAULT_SEASON
        active_season = season if season in unique_seasons else (unique_seasons[0] if unique_seasons else fallback_season)

        # ── Season averages ───────────────────────────────────────
        if is_wnba:
            # Primary: wnba_player_seasons (historical ingest, per-game averages)
            cur.execute("""
                SELECT gp, min, pts, reb, ast, stl, blk, tov, pf,
                       fgm, fga, fg_pct, fg3m, fg3a, fg3_pct, ftm, fta, ft_pct,
                       plus_minus, team
                FROM wnba_player_seasons
                WHERE player_id = %s AND season = %s AND COALESCE(gp, 0) > 0
                ORDER BY CASE WHEN season_type = 'Regular Season' THEN 0 ELSE 1 END
                LIMIT 1
            """, (person_id, active_season))
            avg_row = cur.fetchone()
            if not avg_row:
                # Fallback: compute from current-season CDN game stats (also covers empty rows)
                # wnba_player_game_stats has: pts, reb, ast, tov, fgm, fga, fg3m, fg3a
                cur.execute("""
                    SELECT COUNT(DISTINCT game_id)                              AS gp,
                           AVG(pts::float)                                      AS pts,
                           AVG(reb::float)                                      AS reb,
                           AVG(ast::float)                                      AS ast,
                           AVG(tov::float)                                      AS tov,
                           AVG(fg3m::float)                                     AS fg3m,
                           AVG(fg3a::float)                                     AS fg3a,
                           CASE WHEN SUM(fga) > 0
                                THEN ROUND(SUM(fgm)::numeric / SUM(fga), 3) END AS fg_pct,
                           CASE WHEN SUM(fg3a) > 0
                                THEN ROUND(SUM(fg3m)::numeric / SUM(fg3a), 3) END AS fg3_pct,
                           MAX(team)                                            AS team
                    FROM wnba_player_game_stats
                    WHERE player_id = %s AND season = %s
                """, (person_id, active_season))
                avg_row = cur.fetchone()
            season_avgs = dict(avg_row) if avg_row else None
            if season_avgs and (season_avgs.get("gp") or 0) > 0:
                # Always use the team from the selected season, not the most recent
                team_abbr = season_avgs.get("team") or team_abbr
                # Compute TS% from per-game averages (pts / (2*(fga + 0.44*fta)))
                _pts = season_avgs.get("pts") or 0
                _fga = season_avgs.get("fga") or 0
                _fta = season_avgs.get("fta") or 0
                _ts  = round(_pts / (2 * (_fga + 0.44 * _fta)), 3) if (_fga + _fta) > 0 else None
                avgs_out = {
                    "gp":        season_avgs.get("gp"),
                    "min":       _stat(season_avgs.get("min")),
                    "pts":       _stat(season_avgs.get("pts")),
                    "reb":       _stat(season_avgs.get("reb")),
                    "ast":       _stat(season_avgs.get("ast")),
                    "stl":       _stat(season_avgs.get("stl")),
                    "blk":       _stat(season_avgs.get("blk")),
                    "tov":       _stat(season_avgs.get("tov")),
                    "pf":        _stat(season_avgs.get("pf")),
                    "fg3m":      _stat(season_avgs.get("fg3m")),
                    "fg3a":      _stat(season_avgs.get("fg3a")),
                    "ftm":       _stat(season_avgs.get("ftm")),
                    "fta":       _stat(season_avgs.get("fta")),
                    "fgPct":     _pct(season_avgs.get("fg_pct")),
                    "fg3Pct":    _pct(season_avgs.get("fg3_pct")),
                    "ftPct":     _pct(season_avgs.get("ft_pct")),
                    "tsPct":     _ts,
                    "usgPct":    None,
                    "plusMinus": _stat(season_avgs.get("plus_minus")),
                }
            else:
                avgs_out = None
        else:
            # Primary: player_seasons (ingested season averages)
            cur.execute("""
                SELECT gp, min_per_game AS min, pts, reb, ast, stl, blk, tov, pf,
                       fgm, fga, fg_pct, fg3m, fg3a, fg3_pct, ftm, fta, ft_pct,
                       ts_pct, usg_pct, plus_minus, team_abbr
                FROM player_seasons
                WHERE player_id = %s AND season = %s AND COALESCE(gp, 0) > 0
                ORDER BY CASE WHEN season_type = 'Regular Season' THEN 0 ELSE 1 END
                LIMIT 1
            """, (person_id, active_season))
            avg_row = cur.fetchone()
            if not avg_row:
                # Fallback: compute from player_gamelogs (also covers empty placeholder rows)
                # gamelogs has: pts, reb, ast, fg3m, fgm, fga, ftm, fta, ts_pct
                # (no fg3a, stl, blk, tov, pf, plus_minus)
                cur.execute("""
                    SELECT COUNT(DISTINCT game_id)                              AS gp,
                           AVG(pts::float)                                      AS pts,
                           AVG(reb::float)                                      AS reb,
                           AVG(ast::float)                                      AS ast,
                           AVG(min::float)                                      AS min,
                           AVG(fg3m::float)                                     AS fg3m,
                           CASE WHEN SUM(fga) > 0
                                THEN ROUND(SUM(fgm)::numeric / SUM(fga), 3) END AS fg_pct,
                           CASE WHEN SUM(fta) > 0
                                THEN ROUND(SUM(ftm)::numeric / SUM(fta), 3) END AS ft_pct,
                           AVG(ftm::float)                                      AS ftm,
                           AVG(fta::float)                                      AS fta,
                           AVG(ts_pct::float)                                   AS ts_pct,
                           SUBSTRING(MAX(matchup), 1, 3)                        AS team_abbr
                    FROM player_gamelogs
                    WHERE player_id = %s AND season = %s AND season_type = 'Regular Season'
                """, (person_id, active_season))
                avg_row = cur.fetchone()
            season_avgs = dict(avg_row) if avg_row else None
            if season_avgs and (season_avgs.get("gp") or 0) > 0:
                # Always use the team from the selected season
                team_abbr = season_avgs.get("team_abbr") or team_abbr
                avgs_out = {
                    "gp":        season_avgs.get("gp"),
                    "min":       _stat(season_avgs.get("min")),
                    "pts":       _stat(season_avgs.get("pts")),
                    "reb":       _stat(season_avgs.get("reb")),
                    "ast":       _stat(season_avgs.get("ast")),
                    "stl":       _stat(season_avgs.get("stl")),
                    "blk":       _stat(season_avgs.get("blk")),
                    "tov":       _stat(season_avgs.get("tov")),
                    "pf":        _stat(season_avgs.get("pf")),
                    "fg3m":      _stat(season_avgs.get("fg3m")),
                    "fg3a":      _stat(season_avgs.get("fg3a")),
                    "ftm":       _stat(season_avgs.get("ftm")),
                    "fta":       _stat(season_avgs.get("fta")),
                    "fgPct":     _pct(season_avgs.get("fg_pct")),
                    "fg3Pct":    _pct(season_avgs.get("fg3_pct")),
                    "ftPct":     _pct(season_avgs.get("ft_pct")),
                    "tsPct":     _pct(season_avgs.get("ts_pct")),
                    "usgPct":    _pct(season_avgs.get("usg_pct")),
                    "plusMinus": _stat(season_avgs.get("plus_minus")),
                }
            else:
                avgs_out = None

        player_info["teamAbbr"] = team_abbr

        # ── All-time community rating ──────────────────────────────
        cur.execute("""
            SELECT COUNT(*) AS cnt, COALESCE(AVG(rating::float), 0) AS avg_r
            FROM performance_reviews WHERE person_id = %s
        """, (person_id,))
        at = dict(cur.fetchone())
        at_count = int(at["cnt"])
        at_stars  = round(at["avg_r"] / 2, 2) if at_count > 0 else None

        # ── Season community rating ────────────────────────────────
        if is_wnba:
            cur.execute("""
                SELECT COUNT(pr.id) AS cnt, COALESCE(AVG(pr.rating::float), 0) AS avg_r
                FROM performance_reviews pr
                JOIN wnba_player_game_stats g ON g.game_id = pr.game_id AND g.player_id = pr.person_id
                WHERE pr.person_id = %s AND g.season = %s
            """, (person_id, active_season))
        else:
            cur.execute("""
                SELECT COUNT(pr.id) AS cnt, COALESCE(AVG(pr.rating::float), 0) AS avg_r
                FROM performance_reviews pr
                JOIN player_gamelogs g ON g.game_id = pr.game_id AND g.player_id = pr.person_id
                WHERE pr.person_id = %s AND g.season = %s
            """, (person_id, active_season))
        sa = dict(cur.fetchone())
        sa_count = int(sa["cnt"])
        sa_stars  = round(sa["avg_r"] / 2, 2) if sa_count > 0 else None

        # ── Rating trend (last 10 rated games this season) ────────
        if is_wnba:
            cur.execute("""
                SELECT g.game_id, gm.game_date,
                       CASE WHEN gm.home_team_abbr = g.team THEN g.team || ' vs. ' || gm.away_team_abbr
                            ELSE g.team || ' @ ' || gm.home_team_abbr END AS matchup,
                       CASE WHEN gm.home_team_abbr = g.team THEN
                                CASE WHEN gm.home_score > gm.away_score THEN 'W' ELSE 'L' END
                            ELSE
                                CASE WHEN gm.away_score > gm.home_score THEN 'W' ELSE 'L' END
                       END AS wl,
                       g.pts, g.reb, g.ast,
                       COUNT(pr.id) AS rating_count, AVG(pr.rating::float) AS avg_r
                FROM wnba_player_game_stats g
                JOIN performance_reviews pr ON pr.game_id = g.game_id AND pr.person_id = g.player_id
                LEFT JOIN games gm ON gm.game_id = g.game_id
                WHERE g.player_id = %s AND g.season = %s
                GROUP BY g.game_id, gm.game_date, g.team, gm.home_team_abbr, gm.away_team_abbr,
                         gm.home_score, gm.away_score, g.pts, g.reb, g.ast
                ORDER BY gm.game_date DESC NULLS LAST
                LIMIT 10
            """, (person_id, active_season))
        else:
            cur.execute("""
                SELECT g.game_id, g.game_date, g.matchup, g.wl,
                       g.pts, g.reb, g.ast, g.fg3m, g.min,
                       COUNT(pr.id) AS rating_count, AVG(pr.rating::float) AS avg_r
                FROM player_gamelogs g
                JOIN performance_reviews pr ON pr.game_id = g.game_id AND pr.person_id = g.player_id
                WHERE g.player_id = %s AND g.season = %s
                GROUP BY g.game_id, g.game_date, g.matchup, g.wl,
                         g.pts, g.reb, g.ast, g.fg3m, g.min
                ORDER BY g.game_date DESC
                LIMIT 10
            """, (person_id, active_season))
        trend = [
            {
                "gameId":      r["game_id"],
                "gameDate":    str(r["game_date"])[:10] if r["game_date"] else None,
                "matchup":     r["matchup"],
                "wl":          r["wl"],
                "avgStars":    round(r["avg_r"] / 2, 2),
                "ratingCount": int(r["rating_count"]),
                "pts": _int(r["pts"]), "reb": _int(r["reb"]), "ast": _int(r["ast"]),
            }
            for r in cur.fetchall()
        ]

        # ── Best performance all-time (min 2 ratings) ─────────────
        if is_wnba:
            cur.execute("""
                SELECT g.game_id, gm.game_date,
                       CASE WHEN gm.home_team_abbr = g.team THEN g.team || ' vs. ' || gm.away_team_abbr
                            ELSE g.team || ' @ ' || gm.home_team_abbr END AS matchup,
                       g.season, g.pts, g.reb, g.ast,
                       CASE WHEN gm.home_team_abbr = g.team THEN
                                CASE WHEN gm.home_score > gm.away_score THEN 'W' ELSE 'L' END
                            ELSE
                                CASE WHEN gm.away_score > gm.home_score THEN 'W' ELSE 'L' END
                       END AS wl,
                       COUNT(pr.id) AS rating_count, AVG(pr.rating::float) AS avg_r
                FROM wnba_player_game_stats g
                JOIN performance_reviews pr ON pr.game_id = g.game_id AND pr.person_id = g.player_id
                LEFT JOIN games gm ON gm.game_id = g.game_id
                WHERE g.player_id = %s
                GROUP BY g.game_id, gm.game_date, g.team, gm.home_team_abbr, gm.away_team_abbr,
                         gm.home_score, gm.away_score, g.season, g.pts, g.reb, g.ast
                HAVING COUNT(pr.id) >= 2
                ORDER BY AVG(pr.rating::float) DESC, COUNT(pr.id) DESC
                LIMIT 1
            """, (person_id,))
        else:
            cur.execute("""
                SELECT g.game_id, g.game_date, g.matchup, g.season, g.wl,
                       g.pts, g.reb, g.ast, g.fg3m,
                       COUNT(pr.id) AS rating_count, AVG(pr.rating::float) AS avg_r
                FROM player_gamelogs g
                JOIN performance_reviews pr ON pr.game_id = g.game_id AND pr.person_id = g.player_id
                WHERE g.player_id = %s
                GROUP BY g.game_id, g.game_date, g.matchup, g.season, g.wl,
                         g.pts, g.reb, g.ast, g.fg3m
                HAVING COUNT(pr.id) >= 2
                ORDER BY AVG(pr.rating::float) DESC, COUNT(pr.id) DESC
                LIMIT 1
            """, (person_id,))
        best_row = cur.fetchone()
        best_perf = None
        if best_row:
            b = dict(best_row)
            best_perf = {
                "gameId":      b["game_id"],
                "gameDate":    str(b["game_date"])[:10] if b["game_date"] else None,
                "matchup":     b.get("matchup"),
                "season":      b.get("season"),
                "wl":          b.get("wl"),
                "avgStars":    round(b["avg_r"] / 2, 2),
                "ratingCount": int(b["rating_count"]),
                "pts": _int(b["pts"]), "reb": _int(b["reb"]), "ast": _int(b["ast"]),
            }

        # ── Recent performances this season ───────────────────────
        if is_wnba:
            cur.execute("""
                SELECT g.game_id, gm.game_date,
                       CASE WHEN gm.home_team_abbr = g.team THEN g.team || ' vs. ' || gm.away_team_abbr
                            ELSE g.team || ' @ ' || gm.home_team_abbr END AS matchup,
                       CASE WHEN gm.home_team_abbr = g.team THEN
                                CASE WHEN gm.home_score > gm.away_score THEN 'W' ELSE 'L' END
                            ELSE
                                CASE WHEN gm.away_score > gm.home_score THEN 'W' ELSE 'L' END
                       END AS wl,
                       g.pts, g.reb, g.ast, g.fg3m, g.fgm, g.fga,
                       COUNT(pr.id) AS rating_count,
                       COALESCE(AVG(pr.rating::float), 0) AS avg_r
                FROM wnba_player_game_stats g
                LEFT JOIN performance_reviews pr ON pr.game_id = g.game_id AND pr.person_id = g.player_id
                LEFT JOIN games gm ON gm.game_id = g.game_id
                WHERE g.player_id = %s AND g.season = %s
                GROUP BY g.game_id, gm.game_date, g.team, gm.home_team_abbr, gm.away_team_abbr,
                         gm.home_score, gm.away_score, g.pts, g.reb, g.ast, g.fg3m, g.fgm, g.fga
                ORDER BY gm.game_date DESC NULLS LAST
                LIMIT 10
            """, (person_id, active_season))
        else:
            cur.execute("""
                SELECT g.game_id, g.game_date, g.matchup, g.wl,
                       g.pts, g.reb, g.ast, g.fg3m, g.fgm, g.fga, g.min,
                       COUNT(pr.id) AS rating_count,
                       COALESCE(AVG(pr.rating::float), 0) AS avg_r
                FROM player_gamelogs g
                LEFT JOIN performance_reviews pr ON pr.game_id = g.game_id AND pr.person_id = g.player_id
                WHERE g.player_id = %s AND g.season = %s
                GROUP BY g.game_id, g.game_date, g.matchup, g.wl,
                         g.pts, g.reb, g.ast, g.fg3m, g.fgm, g.fga, g.min
                ORDER BY g.game_date DESC
                LIMIT 10
            """, (person_id, active_season))
        rc = cur.fetchall()
        recent_perfs = [
            {
                "gameId":      r["game_id"],
                "gameDate":    str(r["game_date"])[:10] if r["game_date"] else None,
                "matchup":     r["matchup"],
                "wl":          r.get("wl"),
                "pts":  _int(r["pts"]),  "reb":  _int(r["reb"]),  "ast":  _int(r["ast"]),
                "fg3m": _int(r.get("fg3m")), "fgm": _int(r.get("fgm")), "fga": _int(r.get("fga")),
                "min":  _int(r.get("min")),
                "ratingCount": int(r["rating_count"]),
                "avgStars":    round(r["avg_r"] / 2, 2) if int(r["rating_count"]) > 0 else None,
            }
            for r in rc
        ]

        # ── Recent community reviews ───────────────────────────────
        if is_wnba:
            cur.execute("""
                SELECT pr.id, pr.game_id, pr.rating, pr.review_text, pr.created_at,
                       u.id AS user_id, u.display_name, u.avatar_url,
                       gm.game_date,
                       CASE WHEN gm.home_team_abbr = g.team THEN g.team || ' vs. ' || gm.away_team_abbr
                            ELSE g.team || ' @ ' || gm.home_team_abbr END AS matchup
                FROM performance_reviews pr
                JOIN users u ON u.id = pr.user_id
                LEFT JOIN wnba_player_game_stats g ON g.game_id = pr.game_id AND g.player_id = pr.person_id
                LEFT JOIN games gm ON gm.game_id = pr.game_id
                WHERE pr.person_id = %s
                ORDER BY pr.created_at DESC LIMIT 10
            """, (person_id,))
        else:
            cur.execute("""
                SELECT pr.id, pr.game_id, pr.rating, pr.review_text, pr.created_at,
                       u.id AS user_id, u.display_name, u.avatar_url,
                       g.game_date, g.matchup
                FROM performance_reviews pr
                JOIN users u ON u.id = pr.user_id
                LEFT JOIN player_gamelogs g ON g.game_id = pr.game_id AND g.player_id = pr.person_id
                WHERE pr.person_id = %s
                ORDER BY pr.created_at DESC LIMIT 10
            """, (person_id,))
        recent_reviews = [
            {
                "id":          r["id"],
                "gameId":      r["game_id"],
                "gameDate":    str(r["game_date"])[:10] if r["game_date"] else None,
                "matchup":     r.get("matchup"),
                "userId":      r["user_id"],
                "displayName": r["display_name"],
                "avatarUrl":   r["avatar_url"] or "",
                "rating":      r["rating"],
                "stars":       round(r["rating"] / 2, 1),
                "reviewText":  r["review_text"],
                "createdAt":   str(r["created_at"]),
            }
            for r in cur.fetchall()
        ]

        # ── Follow status ──────────────────────────────────────────
        cur.execute("SELECT COUNT(*) AS cnt FROM player_follows WHERE person_id = %s", (person_id,))
        follower_count = int(cur.fetchone()["cnt"])
        is_following = False
        if user_id:
            cur.execute("SELECT 1 FROM player_follows WHERE user_id = %s AND person_id = %s",
                        (user_id, person_id))
            is_following = cur.fetchone() is not None

        cur.close(); conn.close()
        return jsonify({
            "player":           player_info,
            "seasons":          unique_seasons,
            "currentSeason":    active_season,
            "seasonAverages":   avgs_out,
            "ratingSummary": {
                "allTimeAvgStars":    at_stars,
                "allTimeReviewCount": at_count,
                "seasonAvgStars":     sa_stars,
                "seasonReviewCount":  sa_count,
            },
            "trend":              trend,
            "bestPerformance":    best_perf,
            "recentPerformances": recent_perfs,
            "recentReviews":      recent_reviews,
            "followerCount":      follower_count,
            "isFollowing":        is_following,
        })
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error": str(e)}), 500


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Full-season gamelogs  GET /api/players/<person_id>/gamelogs
#   ?season=<s>&league=<nba|wnba>&sort=<key>
# Powers the "See All" screen on a player profile: every game the
# player played in one season, sorted. One player-season is bounded
# (~82 reg + playoffs), so this is a small, single-table read.
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Whitelist sort keys → ORDER BY clause. Never interpolate raw input.
_GAMELOG_SORTS = {
    "recent":   "game_date DESC",
    "earliest": "game_date ASC",
    "opponent": "opp_abbr ASC, game_date DESC",
    "pts":      "pts DESC NULLS LAST, game_date DESC",
    "reb":      "reb DESC NULLS LAST, game_date DESC",
    "ast":      "ast DESC NULLS LAST, game_date DESC",
    "fg3m":     "fg3m DESC NULLS LAST, game_date DESC",
    "min":      "min DESC NULLS LAST, game_date DESC",
}


@app.route("/api/players/<int:person_id>/gamelogs")
def get_player_gamelogs(person_id):
    season  = request.args.get("season") or None
    league  = request.args.get("league", "nba").lower()
    is_wnba = league == "wnba"
    sort    = request.args.get("sort", "recent").lower()
    order_by = _GAMELOG_SORTS.get(sort, _GAMELOG_SORTS["recent"])

    def _int(v): return int(round(float(v))) if v is not None else None

    try:
        conn = get_conn()
        cur  = conn.cursor()

        # Resolve season: use given one if the player has games there, else newest.
        if is_wnba:
            cur.execute("""
                SELECT DISTINCT season FROM wnba_player_game_stats
                WHERE player_id = %s AND season IS NOT NULL ORDER BY season DESC
            """, (person_id,))
        else:
            cur.execute("""
                SELECT DISTINCT season FROM player_gamelogs
                WHERE player_id = %s AND season IS NOT NULL ORDER BY season DESC
            """, (person_id,))
        seasons = [r["season"] for r in cur.fetchall()]
        active_season = season if season in seasons else (seasons[0] if seasons else season)

        if is_wnba:
            cur.execute(f"""
                SELECT g.game_id, gm.game_date,
                       CASE WHEN gm.home_team_abbr = g.team THEN g.team || ' vs. ' || gm.away_team_abbr
                            ELSE g.team || ' @ ' || gm.home_team_abbr END AS matchup,
                       CASE WHEN gm.home_team_abbr = g.team THEN gm.away_team_abbr
                            ELSE gm.home_team_abbr END AS opp_abbr,
                       CASE WHEN gm.home_team_abbr = g.team THEN
                                CASE WHEN gm.home_score > gm.away_score THEN 'W' ELSE 'L' END
                            ELSE
                                CASE WHEN gm.away_score > gm.home_score THEN 'W' ELSE 'L' END
                       END AS wl,
                       g.pts, g.reb, g.ast, g.fg3m, g.fgm, g.fga, NULL::int AS min,
                       COUNT(pr.id) AS rating_count,
                       COALESCE(AVG(pr.rating::float), 0) AS avg_r
                FROM wnba_player_game_stats g
                LEFT JOIN performance_reviews pr ON pr.game_id = g.game_id AND pr.person_id = g.player_id
                LEFT JOIN games gm ON gm.game_id = g.game_id
                WHERE g.player_id = %s AND g.season = %s
                GROUP BY g.game_id, gm.game_date, g.team, gm.home_team_abbr, gm.away_team_abbr,
                         gm.home_score, gm.away_score, g.pts, g.reb, g.ast, g.fg3m, g.fgm, g.fga
                ORDER BY {order_by} NULLS LAST
            """, (person_id, active_season))
        else:
            cur.execute(f"""
                SELECT g.game_id, g.game_date, g.matchup, RIGHT(g.matchup, 3) AS opp_abbr, g.wl,
                       g.pts, g.reb, g.ast, g.fg3m, g.fgm, g.fga, g.min,
                       COUNT(pr.id) AS rating_count,
                       COALESCE(AVG(pr.rating::float), 0) AS avg_r
                FROM player_gamelogs g
                LEFT JOIN performance_reviews pr ON pr.game_id = g.game_id AND pr.person_id = g.player_id
                WHERE g.player_id = %s AND g.season = %s
                GROUP BY g.game_id, g.game_date, g.matchup, g.wl,
                         g.pts, g.reb, g.ast, g.fg3m, g.fgm, g.fga, g.min
                ORDER BY {order_by}
            """, (person_id, active_season))

        games = [
            {
                "gameId":      r["game_id"],
                "gameDate":    str(r["game_date"])[:10] if r["game_date"] else None,
                "matchup":     r["matchup"],
                "wl":          r.get("wl"),
                "pts":  _int(r["pts"]),  "reb":  _int(r["reb"]),  "ast":  _int(r["ast"]),
                "fg3m": _int(r.get("fg3m")), "fgm": _int(r.get("fgm")), "fga": _int(r.get("fga")),
                "min":  _int(r.get("min")),
                "ratingCount": int(r["rating_count"]),
                "avgStars":    round(r["avg_r"] / 2, 2) if int(r["rating_count"]) > 0 else None,
            }
            for r in cur.fetchall()
        ]

        cur.close(); conn.close()
        return jsonify({
            "season":     active_season,
            "seasons":    seasons,
            "sort":       sort if sort in _GAMELOG_SORTS else "recent",
            "count":      len(games),
            "games":      games,
        })
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/players/<int:person_id>/follow", methods=["POST"])
@login_required
def follow_player(person_id):
    user = current_user()
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute(_PLAYER_FOLLOWS_DDL)
        cur.execute(
            "INSERT INTO player_follows (user_id, person_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            (user["id"], person_id)
        )
        conn.commit()
        cur.execute("SELECT COUNT(*) AS cnt FROM player_follows WHERE person_id = %s", (person_id,))
        count = int(cur.fetchone()["cnt"])
        cur.close(); conn.close()
        return jsonify({"isFollowing": True, "followerCount": count})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/players/<int:person_id>/follow", methods=["DELETE"])
@login_required
def unfollow_player(person_id):
    user = current_user()
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute(
            "DELETE FROM player_follows WHERE user_id = %s AND person_id = %s",
            (user["id"], person_id)
        )
        conn.commit()
        cur.execute("SELECT COUNT(*) AS cnt FROM player_follows WHERE person_id = %s", (person_id,))
        count = int(cur.fetchone()["cnt"])
        cur.close(); conn.close()
        return jsonify({"isFollowing": False, "followerCount": count})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=False)