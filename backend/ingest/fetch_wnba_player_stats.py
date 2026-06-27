"""
Fetch per-game season averages for ALL WNBA players from stats.wnba.com
and upsert them into the wnba_player_seasons PostgreSQL table.

Uses leaguedashplayerstats (not leagueLeaders) so all players are returned,
including those with limited games due to injury or roster call-ups.

Usage:
    python backend/ingest/fetch_wnba_player_stats.py               # current WNBA season
    python backend/ingest/fetch_wnba_player_stats.py --season 2025
    python backend/ingest/fetch_wnba_player_stats.py --season all  # 2018–present
    python backend/ingest/fetch_wnba_player_stats.py --season 2025 --season-type "Playoffs"
"""

import os
import sys
import time
import argparse
from datetime import date

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '../../.env'))

import requests
import psycopg2
import psycopg2.extras

DATABASE_URL = os.getenv('DATABASE_URL')
if not DATABASE_URL:
    print("❌ DATABASE_URL not set.")
    sys.exit(1)

WNBA_STATS_URL = 'https://stats.wnba.com/stats/leaguedashplayerstats'

WNBA_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
    'Referer':    'https://stats.wnba.com',
    'Accept':     'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9',
}

# stats.wnba.com abbreviation → app abbreviation
ABBR_MAP = {
    'LAS': 'LA',   # Los Angeles Sparks
    'LVA': 'LV',   # Las Vegas Aces
    'NYL': 'NY',   # New York Liberty
    'GSV': 'GS',   # Golden State Valkyries
    'WAS': 'WSH',  # Washington Mystics
}

def _current_wnba_year() -> str:
    today = date.today()
    return str(today.year)

ALL_SEASONS = [str(y) for y in range(2018, date.today().year + 1)]


# ── DB helpers ────────────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)


def ensure_table(conn):
    cur = conn.cursor()
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
            pf          REAL,
            plus_minus  REAL,
            updated_at  TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (player_id, season, season_type)
        )
    """)
    # Add new columns to existing tables (safe on first run, idempotent)
    cur.execute("ALTER TABLE wnba_player_seasons ADD COLUMN IF NOT EXISTS pf REAL")
    cur.execute("ALTER TABLE wnba_player_seasons ADD COLUMN IF NOT EXISTS plus_minus REAL")
    conn.commit()
    cur.close()


# ── Fetch ─────────────────────────────────────────────────────────────────────

def fetch_players(season: str, season_type: str) -> list[dict]:
    """
    Fetch leaguedashplayerstats from stats.wnba.com.
    Returns all players regardless of games-played threshold.
    """
    params = {
        'LeagueID':        '10',
        'PerMode':         'PerGame',
        'Season':          season,
        'SeasonType':      season_type,
        'MeasureType':     'Base',
        'PlayerExperience': '',
        'PlayerPosition':  '',
        'StarterBench':    '',
        'LastNGames':      0,
        'Month':           0,
        'OpponentTeamID':  0,
        'PaceAdjust':      'N',
        'PlusMinus':       'N',
        'Rank':            'N',
        'DateFrom':        '',
        'DateTo':          '',
        'GameScope':       '',
        'GameSegment':     '',
        'Location':        '',
        'Outcome':         '',
        'Period':          0,
        'SeasonSegment':   '',
        'ShotClockRange':  '',
        'VsConference':    '',
        'VsDivision':      '',
    }

    for attempt in range(3):
        try:
            resp = requests.get(WNBA_STATS_URL, params=params,
                                headers=WNBA_HEADERS, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            result_set = data.get('resultSets', [{}])[0]
            headers = result_set.get('headers', [])
            rows    = result_set.get('rowSet', [])
            if not headers or not rows:
                print(f"  ⚠️  {season} {season_type}: empty response")
                return []
            return [dict(zip(headers, row)) for row in rows]
        except Exception as e:
            print(f"  ⚠️  Attempt {attempt + 1}/3 failed for {season} {season_type}: {e}")
            if attempt < 2:
                time.sleep(3)

    return []


# ── Upsert ────────────────────────────────────────────────────────────────────

def upsert_players(players: list[dict], season: str, season_type: str) -> int:
    if not players:
        return 0

    conn = get_conn()
    ensure_table(conn)
    cur = conn.cursor()
    count = 0

    for p in players:
        raw_team = p.get('TEAM_ABBREVIATION', '') or ''
        team = ABBR_MAP.get(raw_team, raw_team) or None

        try:
            cur.execute("""
                INSERT INTO wnba_player_seasons (
                    player_id, player_name, season, season_type,
                    team, gp, min, pts, reb, ast, stl, blk, tov,
                    fgm, fga, fg_pct, fg3m, fg3a, fg3_pct,
                    ftm, fta, ft_pct, oreb, dreb, eff,
                    pf, plus_minus,
                    updated_at
                ) VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    %s, %s,
                    NOW()
                )
                ON CONFLICT (player_id, season, season_type) DO UPDATE SET
                    player_name = EXCLUDED.player_name,
                    team        = EXCLUDED.team,
                    gp          = EXCLUDED.gp,
                    min         = EXCLUDED.min,
                    pts         = EXCLUDED.pts,
                    reb         = EXCLUDED.reb,
                    ast         = EXCLUDED.ast,
                    stl         = EXCLUDED.stl,
                    blk         = EXCLUDED.blk,
                    tov         = EXCLUDED.tov,
                    fgm         = EXCLUDED.fgm,
                    fga         = EXCLUDED.fga,
                    fg_pct      = EXCLUDED.fg_pct,
                    fg3m        = EXCLUDED.fg3m,
                    fg3a        = EXCLUDED.fg3a,
                    fg3_pct     = EXCLUDED.fg3_pct,
                    ftm         = EXCLUDED.ftm,
                    fta         = EXCLUDED.fta,
                    ft_pct      = EXCLUDED.ft_pct,
                    oreb        = EXCLUDED.oreb,
                    dreb        = EXCLUDED.dreb,
                    eff         = EXCLUDED.eff,
                    pf          = EXCLUDED.pf,
                    plus_minus  = EXCLUDED.plus_minus,
                    updated_at  = NOW()
            """, (
                p.get('PLAYER_ID'),   p.get('PLAYER_NAME'), season, season_type,
                team,
                p.get('GP'),    p.get('MIN'),     p.get('PTS'),    p.get('REB'),
                p.get('AST'),   p.get('STL'),     p.get('BLK'),    p.get('TOV'),
                p.get('FGM'),   p.get('FGA'),     p.get('FG_PCT'),
                p.get('FG3M'),  p.get('FG3A'),    p.get('FG3_PCT'),
                p.get('FTM'),   p.get('FTA'),     p.get('FT_PCT'),
                p.get('OREB'),  p.get('DREB'),
                None,  # eff not available in leaguedashplayerstats
                p.get('PF'),    p.get('PLUS_MINUS'),
            ))
            count += 1
        except Exception as e:
            print(f"  [!] DB error for player {p.get('PLAYER_ID')}: {e}")

    conn.commit()
    cur.close()
    conn.close()
    return count


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='Fetch WNBA per-game season averages and upsert into DB.'
    )
    parser.add_argument(
        '--season',
        default=_current_wnba_year(),
        help='Season year (e.g. "2025") or "all" to backfill 2018–present.',
    )
    parser.add_argument(
        '--season-type',
        default='Regular Season',
        help='Season type (default: "Regular Season").',
    )
    args = parser.parse_args()

    seasons = ALL_SEASONS if args.season.lower() == 'all' else [args.season]
    season_type = args.season_type

    for i, season in enumerate(seasons):
        players = fetch_players(season, season_type)
        if not players:
            print(f"⚠️  {season} {season_type}: no data returned, skipping.")
        else:
            n = upsert_players(players, season, season_type)
            print(f"✅ {season} {season_type}: {n} players upserted")

        if i < len(seasons) - 1:
            time.sleep(1)


if __name__ == '__main__':
    main()
