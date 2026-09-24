# Local daily pipeline — what it feeds, and what's paused

Audited 2026-09-24, before the 2026-27 NBA season.

## Why this exists

`daily_update_local.py` runs on the Windows PC because stats.nba.com blocks
Railway. It was 14 steps; several existed only to feed the website's stats
section, which is no longer the product's focus. This records which is which, so
re-enabling one is a decision rather than an excavation.

## The split

Verified by checking whether any **app-facing** endpoint reads the specific
columns each step writes — not merely the table, since five steps all write into
`player_seasons`.

**The iOS app calls none of** `/api/builder`, `/api/stats`, `/api/stat-keys`,
`/api/matchups`, `/api/wowy`, `/api/trends`, `/api/pva`.

| Step | Writes | Read by | Status |
|---|---|---|---|
| `fetch_season.py` | `player_seasons` core, `players` | app + web | **runs** |
| `fetch_new_pbp_stats.py` | `bad_pass_tov`, `lost_ball_tov` | Builder only | runs |
| `fetch_closest_defender.py` | 20 × `cd_*` | Builder only | runs |
| `fetch_nba_stats.py` | gravity, shot quality, leverage | Stats, WoWY | runs |
| `fetch_gamelogs.py` | `player_gamelogs` | **app** + Trends | **runs** |
| `fetch_wnba_player_stats.py` ×2 | `wnba_player_seasons` | **app** | **runs** |
| `sync_player_teams.py` | `players.current_team` | **app** identity | **runs** |
| `grade_awards.py` | award results | **app** Ballots | **runs** |
| `fetch_team_season_stats.py` | `team_season_stats` | **app** team profile | **runs** |
| `compute_pctiles.py` | `player_pctiles` | Builder only | runs |
| `fetch_matchups.py` | `player_matchups`, `matchup_*` | Matchups, Compare | **PAUSED** |
| `fetch_roster.py` | `team_rosters` | WoWY only | **PAUSED** |
| `fetch_wowy_lineups.py` | `wowy_lineups` | WoWY only | **PAUSED** |
| shot zones (inside `fetch_season.py`) | `player_shot_zones` | **nothing** | **PAUSED** |

`fetch_roster.py` looked load-bearing because `team_rosters` appears in
`/api/awards/template` — but that is a code **comment** explaining why the route
uses `players.current_team` instead. No query reads it there.

`player_shot_zones` is read by no API route at all. Its only consumer is
`compute_metrics.py`, which is in neither pipeline.

## What paused means

The pages stay up. They serve last season's data and go stale rather than
breaking — no 500s, no empty states. Existing rows are untouched.

## Re-enabling

1. Uncomment the step in `backend/ingest/daily_update_local.py` (each keeps its
   arguments and a `# → table` note).
2. Shot zones only: set `FETCH_SHOT_ZONES = True` in `fetch_season.py`. It is a
   flag rather than a commented step because the fetch is inline in a script the
   app still needs.
3. Set `enforce` back to `self.nba_in_season()` for that table in
   `health_check.py`. **Do not skip this** — it is what keeps a paused feed from
   reporting FAIL every day, and equally what makes a resumed feed monitored
   again.

## Season rollover — automatic, with one gap

`season_util._max_played_season()` reads the newest season having a **Final
Regular Season** game in `games`, per league, with a LIKE mask so an NBA label
(`2026-27`) and a WNBA one (`2026`) can never cross.

- **NBA** flips to `2026-27` when the first regular-season game goes final —
  the night of **Oct 20**. Preseason does not count (`season_type` filter), so
  Oct 3–19 still resolves to `2025-26`.
- **WNBA** flips to `2027` when the first 2027 regular-season game goes final,
  around **May 2027**. Until then `2026` is correct — that season is complete.

**The gap:** from Oct 3 to Oct 20, the pipeline refetches 2025-26 nightly
because no 2026-27 game has been played yet. Harmless but wasted work, and it is
the same root cause behind health_check's calendar-based season warning in that
window.

Jerseys are the exception to all of this: they lead the season, so they key off
`roster_season()` instead. See `jersey-ingest.md`.
