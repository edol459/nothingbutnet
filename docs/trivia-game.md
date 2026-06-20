# ydkball Trivia — Design & Status

A procedurally-generated NBA trivia game built to keep users engaged year-round (esp. the
offseason). The base game is **Survival**. Wired into the app (web): `/api/survival/*`
endpoints + a `/survival` play page. A console prototype (`survival.py`) is still the
tuning harness.

> The Wordle-style "one shared puzzle a day" format was deliberately **not** used for trivia
> (binary recall doesn't suit a daily deduction ritual) — it's reserved for a future,
> separate **performance-guessing** game. Trivia's natural rhythm is "how far can you get?",
> i.e. Survival.

## Concept
Combine **stat × filter × season** into single-answer questions: "Who led the NBA in assists
in 2015-16?" Infinitely generatable from existing data, deterministic answers, on-brand.

## Files
- `backend/games/question_engine.py` — generation engine, difficulty model, difficulty-targeted generation.
- `backend/games/survival_api.py` — **server-facing wrapper**: serializes runs (questions + `answer_ids`), `ensure_daily()` (generate-and-store w/ cross-day dedup), `build_endless()`, `player_list()` (autocomplete bank). Imported by `server.py`.
- `backend/games/generate_daily.py` — **cron entrypoint**: pre-generates the *shared* daily run into `survival_daily` (`--date`, `--days N`, `--force --fresh`). The daily must be pre-generated because it's shared + seeded by date; endless is **not** pre-generated (see below).
- `backend/games/survival.py` — console prototype: daily + endless survival (`--show N`, `--endless`, `--pro`, `--date`, `--reset`). The tuning harness.
- `backend/games/count_pool.py` — counts distinct questions per difficulty.
- `frontend/games.html` — the **Games hub** (`/games`), styled in the site's editorial theme; one card (Survival) + a "more coming" placeholder. Linked from the **Games** nav tab added to every page.
- `frontend/survival.html` — web play page (autocomplete picker, ydkball-logo lives, score, share, endless button). Keeps a focused topbar (no full nav) since it's the immersive play screen.
- `backend/ingest/fetch_awards.py` — backfills the `awards[]` column (resumable). **Done: 242 players.**
- `backend/ingest/fetch_shot_distance.py` — backfills `fgm_25ft_pg`.

The console prototype runs standalone via `DATABASE_URL`; the live game runs through `server.py`.

## API (live)
- `GET /api/survival/daily` — today's shared run (`questions` w/ `answer_ids` + reveal names, `lives`), plus the signed-in user's `your_result` / `best` / `streak`. Reads the cron-generated row (`ensure_daily` inline-generates only as a fallback).
- `POST /api/survival/daily/result` `{score}` — records the daily score (first attempt counts, `ON CONFLICT DO NOTHING`); returns `best` + consecutive-day `streak`. Login required.
- `GET /api/survival/endless` — a fresh batch. **Pro only** (`is_pro`; 403 otherwise, 401 if logged out).
- `GET /api/survival/players` — `[{id,name}]` for the client autocomplete (cached in-process).
- `GET /survival` — the play page.

**Answer validation is client-side:** the autocomplete picker yields a real `player_id`, the
client checks it against the question's `answer_ids`, and submits its final score. Standard for
casual daily games (Wordle ships the answer too); harden to server-side per-guess later if needed.

## DB tables (created in `_ensure_tables`)
- `survival_daily(date PK, payload JSONB)` — the stored daily run, shared by all users.
- `survival_used(text PK, used_on)` — every daily question text, for cross-day dedup (the generator avoids the last ~400).
- `survival_results(user_id, mode, date, score, UNIQUE(user_id,mode,date))` — per-user scores; best/streak derived.

## Data
`player_seasons`: 30 seasons (1996-97 → 2025-26), regular + playoffs, ~190 columns, plus:
- `awards TEXT[]` — MVP / ROTY / DPOY / 6MOY / MIP / Finals MVP / All-Star (from nba_api PlayerAwards).
- `fgm_25ft_pg` — made FGs from 25+ ft (only ~recent seasons backfilled; run `--all` to extend).

Classic + standard-advanced stats span all 30 years; tracking/hustle/play-type + playoffs are
**2020-21 → present only**.

## Stat pool (~27, three niche tiers)
- **Tier 0 (common):** PPG, RPG, APG, SPG, BPG, 3PM, TOV, P+R+A
- **Tier 1 (star-led):** TS%, 3P%, FG%, FT%, +/-, 25-ft makes, drives, passes, assist-pts-created, post-up pts, potential assists
- **Tier 2 (obscure → auto-hard):** ISO/PnR/roll/post PPP, rim defense, charges, contested shots, deflections

## Question formats (single-answer only — Survival is one-guess)
- **superlative** — "Who led the NBA in X in Y?"
- **team leader** — "Who led the [team] in X in Y?" (pts/reb/ast/P+R+A memorable; stl/blk/3PM are deep cuts)
- **award** — "Who won MVP/ROTY/DPOY/6MOY/MIP/Finals MVP in Y?"; "Name an All-Star from Y?"
- **this-or-that** — "Who averaged more X in Y?" with **two headshot buttons** (the 2 players carried
  in the question's `options`, not the text); tap one. (2 from the top of a leaderboard.)
- **name-any** — "Name a player who averaged 25+ PPG in Y" (milestones); club variants 20/10, 20/5/5, 25/5/5
- **club** — "Who had a 50/40/90 / triple-double-average season in Y?"
- Regular season **and** playoffs.

(top-N / threshold are multi-answer — retired from rotation, still callable via `--op` for testing.)

## Difficulty — random, no ramp, no labels (player-facing)
There is **no escalating ramp** and **no easy/medium/hard label shown**. After a **gentle opener**
(`SAFE_OPENING` = 2 guaranteed gimmes), every question is a **random draw** weighted to the natural
pool mix (`DIFF_WEIGHTS` easy .42 / med .40 / hard .18) — so a run is sometimes easier, sometimes
harder. The structural difficulty score (below) still exists **internally** — only to seed gimme
openers and keep pool variety — but is never displayed.

Internal tiers (still computed by `score_difficulty` / `generate_targeted`):
- **Easy:** tier-0 leaders, recent team pts/reb/ast leaders, this-or-that, name-any, recent awards, All-Star.
- **Medium:** tier-1 90s leaders, old team pts/reb/ast, recent team-secondary, mid-era awards, single clubs.
- **Hard:** tier-2 stats; old/MIP awards; old-team-secondary deep cuts; brutal hustle-stat leaders.

## Survival (two modes)
**3 lives**, one guess per question, wrong = lose a life. Difficulty is a random weighted draw.

- **Daily** (free, the headline): a **fixed 10-question run**, shared + seeded by date so scores are
  comparable. First 2 are **gimme openers** (gentle onboarding for free users). Clear all 10 (≤2 misses) →
  the **"you know ball"** shareable (X/10). One official attempt (first counts), then **locked** for the
  day. Tracks streak + best. Shows "Question N / 10".
- **Unlimited** (Pro): **open-ended** — keep answering until you lose 3 lives; **score = how many you got
  right** (can go forever). **Totally random from question 1 — no gimme opener** (Pro players opted in).
  Replayable as many times as you want. Questions are generated **one at a time, on demand**
  (`next_unlimited`): `GET /api/survival/unlimited?pos=N&exclude=<used player_ids>`, no cap — the client
  prefetches the next while you answer, so transitions feel instant. No question counter (the score *is*
  the progress); best tracked in localStorage; no streak; "Play again" on game over. No pre-generated pool
  — on-demand generation is a few ms co-located with the DB; local lag is just laptop→Railway.

- **Dedup:** within a run — no repeated question, (operator+stat), answer player, or team. Across days —
  `survival_used` (the daily generator avoids the last ~400 question texts).
- Share = today's date + a 🏀 (correct) / ❌ (wrong or unreached) grid.

## Question pool (per `count_pool.py`)
~7,500 concrete questions — **easy 2,761 · medium 3,317 · hard 1,437** (37/44/19) — plus tens of
thousands of this-or-that pairings. Team leaders dominate the medium/hard variety.

## Name matching
Tolerant resolver: nicknames (shaq, KD, CP3), accents (Jokić), misspellings, iconic first names,
career-games prominence tiebreaker. Becomes an autocomplete dropdown in the real UI.

## Known limitations
- Recency skew (modern stats + playoffs are recent; 90s ≈ a small slice).
- 25-ft makes only partially backfilled (`fetch_shot_distance.py --all` extends it).
- Daily survival uses independent per-date seeding in the prototype; **production should generate
  the daily schedule sequentially with a used-signature list** so it never repeats across days.
- Console latency is remote-DB round-trips; production pre-generates the daily run and co-locates the DB.

## Roadmap (productionizing)
1. ~~**Endpoints:** `/api/survival/daily` + `/api/survival/endless` (Pro).~~ ✅ done
2. ~~**DB tables:** daily store + used-text dedup; per-user scores / streak / best.~~ ✅ done
3. **Play UI:** ✅ web (`/survival`); **iOS next** (reuse the same endpoints + an autocomplete picker).
4. **Schedule the cron:** run `generate_daily.py --days N` daily (e.g. Railway cron) so the daily
   row is always pre-warmed and `/api/survival/daily` never blocks on generation.
5. Polish: a leaderboard (we already store `survival_results`), richer share card, value reveals.
6. Later: the **performance-guessing** game as the separate daily Wordle.

## Ops note
The first request for a date inline-generates if the cron hasn't run — fine in production
(co-located DB ≈ seconds) but ~3 min over the Railway proxy from a laptop. For local testing,
pre-warm with `python backend/games/generate_daily.py` before hitting the endpoint.
