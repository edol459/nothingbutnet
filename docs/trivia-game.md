# ydkball Trivia — Design & Status

Two things live here. **Higher or Lower** is the live game: a true chain
(`backend/games/chain_api.py`, `/api/survival/chain/*`, `/survival`). The older
ten-unrelated-questions quiz (`survival_api.py`, `/api/survival/daily`) is **still
served** because the shipped iOS app talks to it against production — see
"The legacy quiz" at the bottom. Do not change those endpoints' shapes until the
app is ported.

## Higher or Lower (the chain game)

A run is **one board** — a single stat axis — and a chain of players drawn from it.
Round N shows the anchor with its number visible and a challenger with its number
hidden; you call HIGHER or LOWER, the hidden number counts up, and the revealed
card slides into the anchor slot as a new challenger slides in.

Two rules carry the design:

- **The revealed card becomes the next anchor**, never "the higher one stays". If
  the higher player survived, the anchor would ratchet monotonically toward the top
  of the board and "LOWER" would be correct nearly every time within five rounds.
- **The board is fixed for the whole run.** The anchor's number stays on screen
  across rounds, so the axis cannot change under it. Variety is across days.

### Modes

- **Daily (free, login required)** — a date-seeded 10-round chain, identical for
  everyone, **sudden death**: one wrong call ends the day and your score is rounds
  survived (0–10). Everyone plays the same chain, so the leaderboard is a straight
  comparison. Resumable mid-run via `survival_chain_progress` (quitting can't be
  used to reroll a bad start); one official attempt per ET day.
- **Pro** — unlimited replays (`/chain/run`, a fresh random chain) and no round
  limit: past the pre-built chain the client pulls one link at a time from
  `/chain/next`, which is stateless (board key + anchor id + exclusions).

### Boards

Five families, keyed self-describingly (`career:pts`, `avg:reb`, `peak:ast`,
`high:fg3m`, `season:pts:2024-25`):

| kind | what it is | scope shown |
|---|---|---|
| `career` | career totals (`SUM(stat·gp)`) | All-time |
| `avg` | career per-game average — rewards peak where totals reward longevity | Career average |
| `peak` | each player's best qualified single season, captioned with that season | Career-best season |
| `high` | best single game ever, from `player_gamelogs` | Single game |
| `season` | one season's per-game leaderboard, recent seasons weighted up | *the season* |

Only **points, rebounds, assists and made threes** are boards. Steals, blocks and
the percentage stats are excluded: a chain commits a whole run to one axis, so an
axis nobody has a feel for (1.4 vs 1.6 SPG) doesn't cost one question like it did
in the quiz — it ruins the day. `high` boards additionally *can't* offer blocks or
steals, because `player_gamelogs` doesn't carry those columns at all.

### The pool, and why it isn't ranked by the stat

**Boards draw from `_pool_ids()`, gated on career minutes — not on the stat.**
Ranking the pool by the stat made every board a leaderboard of specialists: career
rebounds was literally the 90 best rebounders, so it filled with long-tenured
centres and could never ask LeBron vs Curry. Gating on minutes instead
(`_MIN_MINUTES`, 15,000) lets each board run its natural full range, down to a
guard whose career high is 4 rebounds. It roughly doubles the distinct values on
every board.

Pool membership and pick probability are **separate dials**. The pool is wide so
the low end is real; `_FAME_W` (All-Star count) then biases *which* of several
equally-valid candidates is shown, so stars surface more often than journeymen
without ever being the only option. A 333-man pool drifts deep-cut without it.

**Career completeness** is `_COMPLETE_CAREER`: drafted 1996+, or undrafted and
first seen 1997-98+. A player's first season *in our data* is not their debut —
everyone already in the league in 1996-97 looks like a 1996-97 rookie, which is how
John Stockton landed on a rebounding board with the 9 he grabbed after our window
opens. `draft_year` is the real test, and it still admits Kobe, Iverson and Nash.
Undrafted players all have `draft_year = NULL` (VanVleet, Haslem, Calderon), hence
the second branch.

### Ties

When both players share a value, **either call clears the round** — the server
scores it that way and both clients must agree, or they'd disagree about where the
run ended. Ties are opt-in rather than accidental: `_TIE_CHANCE` sets the rate and
they never come two in a row (the endless endpoint takes a `tie=1` flag, since it
can't tell from an anchor id alone). Measured ~3.8% of rounds, zero consecutive.
Integer boards — single-game bests — depend on this; without it half their usable
partners get discarded.

### Difficulty — rank distance, not value gap

Difficulty is **how far apart the two players sit in the board's order**, ramping
from `_RANK_EASY` (~a third of the board away) to `_RANK_HARD` (~1%) by round 10,
then creeping toward a `_DIFF_CAP` of 0.97 in an endless run.

It was originally the value gap as a fraction of the board's spread, which looked
right and was wrong: **every board is 3–6× skewed** (LeBron on career points, Curry
on threes), so a single outlier stretched the scale and even the "hardest" target
stayed wider than a fifth of the field. A player ran 46 rounds deep on career threes
per game, where everyone below Curry is packed between 0 and 1.5. Rank distance has
no such failure mode — N places apart is N places apart whatever the distribution
does. Measured median relative gap now runs ~40% at round 1 down to ~6% at round 10.

### Values are rounded to what the card shows

`_round()` collapses each value to its displayed precision before the board is
sorted. The card shows one decimal, so 1.54 and 1.49 both read "1.5" — comparing
the raw floats marked one of them lower and scored the player wrong on two numbers
that were **identical on screen**. Rounding first makes ordering, tie detection and
scoring all agree with what the player can actually see.

### Players with no portrait

The CDN answers **200 for every player id**, serving a shared silhouette when it has
no photo — the same trap LockerVision sets for jerseys. `backend/ingest/sync_headshots.py`
probes each player once and records `players.has_headshot`, identifying the
placeholder by *sharing* (a real portrait is unique to its player) so it survives the
CDN swapping its silhouette.

Those players are **not filtered out**. Headshot coverage is an era boundary, not a
popularity one — 24% of players who last played 2010-14 have art versus 96% of
2015-19 — so filtering would quietly delete Jason Kidd, Shawn Marion and Jermaine
O'Neal from the game. Each link carries `art: false` instead and the clients draw a
monogram in the art column, deliberately **unmasked**: the left-edge fade exists to
blend a photo into the type column and on a letterform it just erases half the mark.

Tune and inspect it with the module's own CLI:

```bash
python backend/games/chain_api.py                      # print a chain with answers
python backend/games/chain_api.py --board high:reb
python backend/games/chain_api.py --simulate 400       # gap profile per round
```

To play a specific board rather than wait for it, a Pro run accepts a board key:
`/survival?mode=unlimited&board=high:reb`.

### API

- `GET  /api/survival/chain/daily` — today's chain + your standing (`your_result`,
  `progress`, `best`, `streak`, `is_pro`). Login required.
- `POST /api/survival/chain/daily/progress` `{picks}` — save calls so far.
- `POST /api/survival/chain/daily/result` `{picks}` — **server scores it**: walks
  the calls under sudden death and stops at the first miss, so padding picks past a
  miss scores 0, not 10. Grants Ball Knowledge (+10 played, +5 per round survived).
- `GET  /api/survival/chain/run` — a fresh random chain. Pro only (403).
- `GET  /api/survival/chain/next?board=&anchor=&exclude=&round=` — one more link. Pro only.
- `GET  /api/survival/chain/leaderboard` — today's ranking (mode `chain`).
- `GET  /api/survival/chain/share.png?date=&score=&mode=&label=` — the result card
  (Pillow, `og_image.render_chain_card`). Unauthenticated by design: the client
  fetches it for the native share sheet and scrapers have no session.

**XP uses the `survival_daily` event type on purpose** — Higher or Lower pays once
per day whichever version you played, so the chain and the legacy quiz can coexist
during the iOS port without double-dipping.

### Tables

- `survival_chain_daily(date PK, payload JSONB)` — the shared chain.
- `survival_chain_progress(user_id, date, picks)` — in-progress calls (anti-replay).
- `survival_results` with **`mode='chain'`** — scores. The legacy quiz uses
  `mode='daily'` in the same table; they're a different scoring scale and must not mix.

### Known limitation

The daily ships the whole chain, values included, so devtools can read ahead — the
same posture as the old quiz and as Wordle shipping its answer. What the server
prevents is a *fabricated* score: the leaderboard only shows what it re-derived from
the submitted calls.

### Not yet done

- **iOS is still on the legacy quiz** (`Survival.swift`, `SurvivalGameView.swift`).
  Porting it means the chain endpoints above plus the two-panel board.
- No `/hol/<date>` landing page, so a shared link previews the generic site card
  rather than the day's result (the PNG endpoint exists and is ready for it).

---

## The legacy quiz (still served, iOS only)

## Concept
**Every question is a this-or-that:** "Who [did more X] — A or B?" — two players, tap one
(headshot buttons). Chosen over superlatives ("who led the NBA in…") because it's a *judgment*
not *recall*, has vastly more variety (pairs × axes × seasons vs. a handful of memorable leaders),
and is a faster, catchier loop. Generated by `qe.generate_thisorthat` across **five axes**:
- **Season stat** (~34%) — "Who averaged more rebounds in 2018-19?" (top-15 of a leaderboard;
  intuitive box stats + TS%/3P% only — `_TOOT_SEASON`; ~30% of these ask about the **playoffs**,
  which only has data 2020-21+).
- **Career total** (~18%) — "Who has more career points?" (`SUM(stat·gp)`, nearby ranks).
- **Single-season high** (~16%) — "Who had a higher single-season scoring average?" (each player's
  career-best qualified season).
- **Accolade count** (~16%) — "Who won more MVPs?" / "made more All-Star teams?" (counts from `awards[]`).
- **Award decoy** (~16%) — "Who won Finals MVP in 2016?" → the **real winner vs. their top-scoring
  teammate** that season (works for any award: Finals MVP, MVP, DPOY, ROTY, 6MOY, MIP).

**Correctness guard:** career / single-season / accolade / award axes only use players who **debuted
in 1997-98+** (our data starts 1996-97, so earlier players have truncated totals → wrong answers).
The season axis is per-season so it's unaffected.

The older single-answer formats (superlative, team leader, award, name-any, club) still exist in
`question_engine.py` for reference/tooling but are **no longer used** by Survival.

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

## Survival (two modes — the legacy quiz)
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
