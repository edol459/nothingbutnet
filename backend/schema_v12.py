"""
ydkball — Schema v12: Current team on players
=============================================================
python backend/schema_v12.py

`players` is already an identity table — name, position, height, draft, college,
country, no stats — which is why a drafted rookie can exist in it months before
he plays. What it lacks is the one attribute that actually changes: who he plays
for right now.

Until now "what team is this player on?" was answered from `player_seasons`, the
stats table, via the most recent season's `team_abbr`. That answer is correct
during a season and wrong all summer: a player traded in July keeps his old team
until he plays a game in October, because a stats row is the only thing that
moves him. Everything downstream inherits it — the profile, Browse Players and
its team filter, list building, the ballot pickers.

So team membership gets its own column, fed from the league's roster feed rather
than from box scores. Two rules keep this from becoming a second source of truth
for things it shouldn't own:

  * It answers "who is this player NOW". Anything historical — a box score, a
    game review, the team clue in Guess Who — keeps reading the row from when it
    happened. A player traded today still played that 2023 game for his old team.

  * Reads COALESCE onto the old path, so a NULL here is exactly today's
    behaviour rather than a blank.

`current_team_season` records which season the assignment came from, so a stale
sync is visible rather than silent.

Safe to run multiple times.
"""
import os, sys
from dotenv import load_dotenv
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("DATABASE_URL not found."); sys.exit(1)

SQL = """
ALTER TABLE players ADD COLUMN IF NOT EXISTS current_team        TEXT;
ALTER TABLE players ADD COLUMN IF NOT EXISTS current_team_season TEXT;
ALTER TABLE players ADD COLUMN IF NOT EXISTS current_team_at     TIMESTAMPTZ;

-- Browse Players filters by team, so that lookup wants an index once it reads
-- this column instead of player_seasons.
CREATE INDEX IF NOT EXISTS idx_players_current_team
    ON players (current_team) WHERE current_team IS NOT NULL;
"""


def main():
    conn = psycopg2.connect(DATABASE_URL)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute(SQL)
    print("schema v12 applied — players.current_team ready.")
    cur.close(); conn.close()


if __name__ == "__main__":
    main()
