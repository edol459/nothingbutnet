"""
test_pbpstats_fetch.py
----------------------
Fetches one NBA game via pbpstats and prints the full richness of
possession-level data available.

Strategy: fetch raw PBP JSON ourselves (with proper NBA headers + cookie
warming), save to disk, then load via pbpstats file loader so pbpstats
handles all the possession parsing without making its own HTTP requests.

Usage:
    pip install pbpstats requests
    python test_pbpstats_fetch.py
"""

import json
import os
import time
import tempfile
import requests

from pbpstats.data_loader import (
    StatsNbaPossessionFileLoader,
    StatsNbaPossessionLoader,
)
from pbpstats.resources.enhanced_pbp import FieldGoal, FreeThrow, Rebound, Turnover

# ── Config ────────────────────────────────────────────────────────────────────
GAME_ID = "0022400547"   # swap for any 10-digit NBA game ID

NBA_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.nba.com/",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token": "true",
    "Origin": "https://www.nba.com",
    "Connection": "keep-alive",
}

PBP_URL = "https://stats.nba.com/stats/playbyplayv2"
PBP_PARAMS = {
    "GameId": GAME_ID,
    "StartPeriod": 0,
    "EndPeriod": 10,
    "RangeType": 2,
    "StartRange": 0,
    "EndRange": 55800,
}

# ── Step 1: Fetch raw PBP JSON with proper headers ────────────────────────────
print("Warming NBA.com cookie...")
session = requests.Session()
session.get("https://www.nba.com", headers=NBA_HEADERS, timeout=15)
time.sleep(1)

print(f"Fetching PBP for game {GAME_ID}...")
response = session.get(PBP_URL, params=PBP_PARAMS, headers=NBA_HEADERS, timeout=30)
response.raise_for_status()
pbp_data = response.json()

# Verify we got real data
result_sets = pbp_data.get("resultSets", [])
if not result_sets:
    raise ValueError(f"No resultSets in response. Got keys: {list(pbp_data.keys())}")

rows = result_sets[0].get("rowSet", [])
print(f"  Raw PBP events fetched: {len(rows)}")

# ── Step 2: Save to disk in the format pbpstats file loader expects ───────────
# pbpstats looks for files at: {data_dir}/pbp/stats_{game_id}.json
data_dir = tempfile.mkdtemp()
pbp_dir = os.path.join(data_dir, "pbp")
os.makedirs(pbp_dir, exist_ok=True)

pbp_file = os.path.join(pbp_dir, f"stats_{GAME_ID}.json")
with open(pbp_file, "w") as f:
    json.dump(pbp_data, f)
print(f"  Saved to {pbp_file}")

# ── Step 3: Load possessions via pbpstats file loader ─────────────────────────
print("\nParsing possessions via pbpstats...")
source_loader = StatsNbaPossessionFileLoader(data_dir)
possession_loader = StatsNbaPossessionLoader(GAME_ID, source_loader)
possessions = possession_loader.items
print(f"Total possessions parsed: {len(possessions)}")
print("=" * 70)

# ── Step 4: Print detailed possession data ────────────────────────────────────
for i, poss in enumerate(possessions[:8]):
    print(f"\n{'=' * 70}")
    print(f"POSSESSION {i + 1}")
    print(f"{'=' * 70}")

    print(f"  Period:              {poss.period}")
    print(f"  Start time:          {poss.start_time}")
    print(f"  End time:            {poss.end_time}")
    print(f"  Offense team ID:     {poss.offense_team_id}")
    print(f"  Start score margin:  {poss.start_score_margin}")
    print(f"  Start type:          {poss.possession_start_type}")

    print(f"\n  -- Causal chain from previous possession --")
    print(f"  Prev shooter:        {poss.previous_possession_end_shooter_player_id}")
    print(f"  Prev rebounder:      {poss.previous_possession_end_rebound_player_id}")
    print(f"  Prev turnover by:    {poss.previous_possession_end_turnover_player_id}")
    print(f"  Prev steal by:       {poss.previous_possession_end_steal_player_id}")

    print(f"\n  -- Events ({len(poss.events)} total) --")
    for ev in poss.events:
        ev_type = type(ev).__name__
        player = getattr(ev, 'player1_id', None)
        clock = getattr(ev, 'clock', '?')
        line = f"    [{clock}] {ev_type:<22} player={player}"

        if isinstance(ev, FieldGoal):
            line += f"  made={ev.is_made}  {ev.shot_value}pt"
            line += f"  assisted={ev.is_assisted}"
            if ev.is_assisted:
                line += f"(by {ev.player2_id})"
            line += f"  blocked={ev.is_blocked}"
            if ev.is_blocked:
                line += f"(by {ev.player3_id})"
            line += f"  corner3={ev.is_corner_3}"
            line += f"  dist={ev.distance}ft"
            shot_type = getattr(ev, 'shot_type', None)
            if shot_type:
                line += f"  zone={shot_type}"
        elif isinstance(ev, Turnover):
            line += f"  steal={ev.is_steal}"
            if ev.is_steal:
                line += f"(by {ev.player3_id})"
        elif isinstance(ev, Rebound):
            line += f"  oreb={ev.oreb}  real={ev.is_real_rebound}"

        print(line)

        # THE KEY CHECK: lineups on floor for every event
        if hasattr(ev, 'current_players'):
            cp = ev.current_players
            print(f"      On floor: {cp}")
        else:
            print(f"      On floor: [NO current_players ATTRIBUTE]")

    # Outcome stats
    stats = poss.possession_stats
    print(f"\n  -- Outcome stats ({len(stats)} rows) --")
    by_player = {}
    for row in stats:
        pid = row['player_id']
        if pid not in by_player:
            by_player[pid] = {}
        by_player[pid][row['stat_key']] = row['stat_value']

    for pid, stat_dict in list(by_player.items())[:6]:
        interesting = {k: v for k, v in stat_dict.items()
                      if k in ('PlusMinus', 'OffPoss', 'DefPoss', 'Pts',
                               'FGM', 'FGA', 'Ast', 'Tov', 'Oreb', 'Dreb')}
        if interesting:
            print(f"    Player {pid}: {interesting}")

print("\n" + "=" * 70)
print("Done.")