# ydkball Database Map
*Generated 2026-06-27. Re-audit with the query scripts in `backend/ingest/` when adding new features.*

---

## Daily Update Pipelines

Two scripts run automatically every day and keep the DB fresh.

### `daily_update.py` — Railway Cron (cloud, runs at server time)
Runs steps that work from a cloud/datacenter IP:

| Step | Script | Writes To |
|------|--------|-----------|
| NBA team W-L records | `fetch_team_seasons.py` | `team_seasons` |
| WNBA team W-L records | `fetch_wnba_team_seasons.py` | `team_seasons` |
| Players table sync | `fetch_players.py` | `players` |
| DARKO DPM | `fetch_darko.py` | `player_seasons` |
| LEBRON / O-LEBRON / WAR | `fetch_lebron.py` | `player_seasons` |
| Net Points per 100 | `fetch_net_pts.py` | `player_seasons` |

### `daily_update_local.py` — Windows Task Scheduler (residential IP required)
Runs steps that stats.nba.com blocks from cloud IPs:

| Step | Script | Writes To |
|------|--------|-----------|
| Season aggregate stats | `fetch_season.py` | `player_seasons` |
| PBP stats (bad pass + lost ball TOV) | `fetch_new_pbp_stats.py` | `player_seasons` |
| Closest defender shots | `fetch_closest_defender.py` | `player_seasons` |
| Matchup defense | `fetch_matchups.py` | `player_matchups` |
| Gravity / shot quality / leverage | `fetch_nba_stats.py` | `player_seasons` |
| Per-game logs | `fetch_gamelogs.py` | `player_gamelogs` |
| Roster data | `fetch_roster.py` | `team_rosters` |
| WoWY lineups | `fetch_wowy_lineups.py` | `wowy_lineups` |
| Recompute percentiles | `compute_pctiles.py` | `player_pctiles`, `precomputed_data` |

> Season and season_type are **auto-detected** from today's date — no manual changes needed when playoffs start. Both scripts detect "Playoffs" from ~April 20 through June.

---

## NBA Data Layer

### `players` — 2,860 rows
Bio table for all NBA players ever. Joined into almost everything.

| Field | Coverage |
|-------|----------|
| player_id, player_name | 100% |
| height_inches | 98% (2,800/2,860) |
| draft_year, draft_number | 70% (1,992/2,860) — undrafted players have NULL |
| position | 48% (1,372/2,860) ⚠️ |
| college | 100% (stored as empty string for international) |

> **Gap:** Only ~half of players have `position` filled in. The `players` table is synced by `fetch_players.py` which pulls from the NBA CDN — position data comes from a different endpoint and may not have been backfilled for historical players.

---

### `player_seasons` — NBA season averages + advanced stats
Primary source for the **Stats tab / Builder** on the web. Updated daily by `daily_update_local.py`.

| Season | Reg Season | Playoffs | Notes |
|--------|-----------|---------|-------|
| 2025-26 | ✅ 582 players | ✅ 230 players | Current |
| 2024-25 | ✅ 569 players | ✅ 219 players | |
| 2023-24 | ✅ 572 players | ✅ 214 players | |
| 2022-23 | ✅ 539 players | ✅ 217 players | |
| 2021-22 | ✅ 605 players | ✅ 217 players | |
| 2020-21 | ✅ 540 players | ✅ 239 players | |
| 2019-20 | ✅ 529 players | — | No playoffs col (bubble format) |
| 2018-19 | ✅ 530 players | — | |
| 2017-18 | ✅ 540 players | — | |
| 2016-17 | ✅ 486 players | — | |
| … | Historical | — | Goes back to 1996-97 |

> **Note:** `player_seasons` is the canonical per-season average table. The iOS player profile uses this as primary source, with a `player_gamelogs` fallback for any player/season combination missing here.

---

### `player_gamelogs` — game-by-game logs
Used by the **Guess Who** game and as fallback for player profile current-season stats.

| Season | Reg Season | Playoffs |
|--------|-----------|---------|
| 2025-26 | ✅ 26,650 rows / 581 players | ✅ 1,921 rows / 230 players |
| 2024-25 | ✅ 26,306 rows / 569 players | ✅ 1,804 rows / 219 players |
| 2023-24 | ✅ 26,401 rows / 572 players | ✅ 1,685 rows / 214 players |
| 2022-23 | ✅ 25,894 rows / 539 players | ✅ 1,728 rows / 217 players |
| 2021-22 | ✅ 26,039 rows / 605 players | ✅ 1,891 rows / 217 players |
| 2020-21 | ✅ 23,054 rows / 540 players | ✅ 1,864 rows / 239 players |
| 2019-20 | ✅ 22,393 rows / 529 players | ✅ 1,694 rows / 217 players |
| … | Historical | Goes back to 1996-97 |

---

### `player_pctiles` — percentile rankings (Builder tool)
One row per stat_key per season/type. Recomputed by `compute_pctiles.py` (runs in local daily update).

| Season | Reg Season | Playoffs | Notes |
|--------|-----------|---------|-------|
| 2025-26 | ✅ 166 stat keys | ✅ 152 stat keys | |
| 2024-25 | ✅ 139 stat keys | ❌ Missing | ⚠️ Never computed |
| 2023-24 | ❌ Missing | ❌ Missing | ⚠️ |
| 2022-23 | ✅ 130 stat keys | ✅ 130 stat keys | |
| 2021-22 | ✅ 130 stat keys | ✅ 129 stat keys | |

> **Gap:** 2024-25 Playoffs and all of 2023-24 are missing percentiles. This means the Builder tool only works correctly for seasons that have pctiles. Fix: run `compute_pctiles.py --season 2024-25 --season-type Playoffs` and `--season 2023-24` from the local machine.

---

### `games` — NBA game records
Shared with WNBA (league column). Powers game reviews, predictions, feed.

| Season | Reg Season | Playoffs | Play-In |
|--------|-----------|---------|---------|
| 2025-26 | ✅ 1,226 games | ✅ 85 games | ✅ 5 games |
| 2024-25 | ✅ 1,225 games | ✅ 84 games | — |
| 2023-24 | ✅ 1,230 games | ✅ 82 games | — |
| 2022-23 | ✅ 1,230 games | ✅ 84 games | — |
| … | Complete | Goes back to 2010-11 | |

---

### `team_seasons` — NBA team W-L records
Updated daily by Railway cron. 30 teams every season.

**Coverage:** 1996-97 through 2025-26 (with gaps at 2001-02 through 2004-05, 2006-07 through 2008-09, 2010-11 present). Recent seasons (2020-21 → 2025-26) are complete.

---

### `team_rosters` — active rosters (for WoWY tool)

| Season | Teams | Players |
|--------|-------|---------|
| 2025-26 | 30 | 545 |
| 2024-25 | 30 | 534 |
| 2023-24 | 29 | 515 |
| 2022-23 | 30 | 508 |
| 2021-22 | 30 | 506 |
| 2020-21 | 30 | 503 |

---

### `wowy_lineups` — 5-man lineup data

| Season | Teams | Lineups |
|--------|-------|---------|
| 2025-26 | 30 | 14,073 |
| 2024-25 | 30 | 12,769 |
| 2023-24 | 30 | 12,046 |
| 2022-23 | 30 | 12,859 |
| 2021-22 | 30 | 13,119 |
| 2020-21 | 27 | 10,484 |

---

### `player_matchups` — opponent-adjusted matchup defense

| Season | Defenders | Rows | Notes |
|--------|-----------|------|-------|
| 2025-26 | 473 | 14,046 | Regular Season only |
| 2024-25 | 266 | 2,052 | ⚠️ Low row count vs current season |
| 2023-24 | 284 | 2,385 | Regular Season only |
| 2022-23 | 283 | 2,447 | |
| 2021-22 | 297 | 2,118 | |
| 2020-21 | 281 | 1,915 | |

> **Note:** 2025-26 has ~7x more rows than prior seasons — this likely reflects a script change (min-possession threshold lowered) rather than a data error.

---

### `player_pva_season` — Possession Value Added

| Season | Coverage |
|--------|----------|
| 2024-25 Regular Season | ✅ 570 players |
| 2025-26 | ❌ Missing ⚠️ |

> **Gap:** PVA has not been recomputed for 2025-26. Run `compute_pva.py` from the local machine (it is NOT in either daily update pipeline).

---

### `player_adjusted_wowy` — Adjusted WoWY (on/off splits)

| Season | Coverage |
|--------|----------|
| 2024-25 Regular Season | ✅ 495 players |
| 2025-26 | ❌ Missing ⚠️ |

> **Gap:** Same as PVA — not in the daily pipeline. Needs a manual run of its compute script.

---

### `possessions` + `possession_events` + `possession_lineups` — Play-by-Play
Collected by `collection_progress` tracking. 1.9 GB total.

| Season | Games Collected | Notes |
|--------|-----------------|-------|
| 2025-26 | 1,230 (complete) | ✅ |
| 2024-25 | 1,230 (complete) | ✅ |
| 2023-24 | 1,178 / ~1,312 | Partial |
| 2022-23 | 19 games only | ⚠️ Almost nothing |
| 2021-22 | 1,217 (nearly complete) | ✅ |
| 2020-21 | 26 games only | ⚠️ Almost nothing |

**Collection status:** 4,951 games done, 50 failed (tracked in `collection_progress`).

> **Gap:** 2022-23 and 2020-21 PBP data is essentially missing. These seasons won't work correctly in any feature that relies on possessions.

---

## WNBA Data Layer

### `wnba_player_seasons` — season averages (manually ingested)
Run `fetch_wnba_player_stats.py` to update. Does NOT auto-run.

| Season | Players | Notes |
|--------|---------|-------|
| 2025 | ✅ 115 players | Ingested 2026-06-27 |
| 2024 | ✅ 95 players | |
| 2023 | ✅ 105 players | |
| 2022 | ✅ 98 players | |
| 2021 | ✅ 101 players | |
| 2020 | ✅ 104 players | |
| 2019 | ✅ 106 players | |
| 2018 | ✅ 117 players | |
| 2026 | ❌ Not here | Covered by `wnba_player_game_stats` fallback |

> **Note:** This table only holds Regular Season averages (no Playoffs split). The iOS profile falls back to `wnba_player_game_stats` for any season not found here.

---

### `wnba_player_game_stats` — current-season CDN ingest
Auto-updated via Railway. Columns: player_id, player_name, team, game_id, season, pts, reb, ast, tov, fgm, fga, fg3m, fg3a.

| Season | Players | Games | Rows |
|--------|---------|-------|------|
| 2026 | 271 | 139 | 3,816 |

> **Note:** 271 players is high (active roster would be ~144). This is normal — the CDN returns all players who appeared in any game, including DNPs, and the season is still in progress.

---

### `games` (WNBA) — game records

| Season | Games | Date Range |
|--------|-------|-----------|
| 2026 | 139 (in-progress) | 2026-04-29 → 2026-06-26 |
| 2025 | 312 | 2025-05-16 → 2025-10-10 |
| 2024 | 264 | 2024-05-14 → 2024-10-20 |

> **Gap:** Only 3 seasons of WNBA game records. The `games` table WNBA data was not backfilled before 2024.

---

### `team_seasons` (WNBA) — team W-L records
Updated daily by Railway cron. Has seasons 2021–2026 and a long historical tail (1997–2018).

> **Gap:** 2019 and 2020 are missing. (2019 was a normal season; 2020 was the COVID bubble.) These years exist in `wnba_player_seasons` but the team W-L was never ingested.

---

## App & User Data Layer

### `users` — 264 registered users
219 have avatars set (83%).

### `game_reviews` — 1,214 reviews
Active since April 2026. Growing daily.

### `performance_reviews` — 0 rows
New table for player performance ratings (iOS feature just built). No user data yet — will populate as the feature is used.

### `player_follows` — 1 row
New table (IF NOT EXISTS, created lazily). Will grow as players use the Follow button on player profiles.

### `friendships` — 102 total
77 accepted, 25 pending.

### `review_likes` — 570 likes
### `review_replies` — 11 replies

### `xp_events` — engagement tracking

| Event Type | Count |
|-----------|-------|
| review_like | 542 |
| app_open | 449 |
| live_game_view | 274 |
| survival_daily (Higher or Lower) | 4 |
| poeltl_daily (Guess Who) | 4 |

---

### `survival_daily` — Higher or Lower daily puzzles
8 puzzles generated, covering 2026-06-19 → 2026-06-26.

### `survival_results` — Higher or Lower play history
7 results from 2 users.

### `poeltl_daily` — Guess Who daily puzzles
5 puzzles, covering 2026-06-22 → 2026-06-26.

### `poeltl_results` — Guess Who play history
4 results from 3 users.

### `game_lists` — user-created lists
4 lists from 2 users. 21 player list items.

### `jerseys` — 1,116 jersey records
No league column — NBA only.

### `game_odds` — 1 row (stale, June 14)
### `game_predictions` — 0 rows
The odds/predictions feature appears to be dormant.

### `precomputed_data`
2 keys: `player_percentiles` and `win_correlations`, both for 2025-26 Regular Season.

---

## Known Gaps Summary

| Priority | Gap | Fix |
|----------|-----|-----|
| 🔴 High | `player_pctiles` missing 2024-25 Playoffs | Run `compute_pctiles.py --season 2024-25 --season-type Playoffs` from local machine |
| 🔴 High | `player_pva_season` missing 2025-26 | Run `compute_pva.py` — not in daily pipeline |
| 🔴 High | `player_adjusted_wowy` missing 2025-26 | Same compute script as above |
| 🟡 Medium | `players.position` only 48% filled | Backfill from NBA roster/bio endpoint |
| 🟡 Medium | PBP nearly empty for 2022-23 and 2020-21 | Would require retroactive collection run |
| 🟡 Medium | `wnba_player_seasons` never auto-updates | Add `fetch_wnba_player_stats.py` to Railway cron at season end |
| 🟡 Medium | WNBA `games` only back to 2024 | Run `fetch_wnba_season.py 2025` (2025 games missing) |
| 🟢 Low | `team_seasons` WNBA missing 2019 + 2020 | Run `fetch_wnba_team_seasons.py` for those years |
| 🟢 Low | `player_pctiles` missing 2023-24 | Run `compute_pctiles.py --season 2023-24` |
| 🟢 Low | `game_odds` / `game_predictions` stale | Feature appears inactive — ignore or remove |
| 🟢 Low | `wnba_player_seasons` `ALL_SEASONS` cap at 2025 | Update script to include 2026 for future `--season all` runs |
