# Live Logging — spec

**Status:** proposed, not started. Written 2026-08-20.

## The change

Logging a game stops being a post-game form and becomes a **notebook you keep during the
game** — the digital version of the fan who brings a scorebook to the ballpark.

You open a game as it tips off, and for the next two and a half hours you rate, grade and
write as it happens, revising freely. Nothing is public. When the final buzzer goes, the
**Submit** button unlocks and your notebook becomes a log.

### Why this is worth the pre-season window

- **It changes the engagement class.** A 2-minute retrospective form becomes a 2.5-hour
  session. That is the difference between an app you visit and an app you have open.
- **It fixes player grading at the root.** Only **8 users have ever graded a performance**.
  We treated that as a UI problem and rebuilt the control three times. It is substantially a
  *timing* problem: grading 26 players from memory afterwards is homework; grading someone
  right after they do something is just reacting.
- **It gives push and the alarm clock a payoff.** Those currently deliver you to a
  scoreboard. Now the alarm's promise is "your notebook is open", and the notification and
  the session finally point at the same thing.
- **It is ownable.** Scores, stats and reviews exist elsewhere. A scorekeeper's notebook for
  the modern fan does not.

### The rule that makes it safe

**You can write and rate freely while the game is live. Nothing becomes public until you
submit, and you cannot submit until the game is final.**

This is a data-integrity rule, not a UX preference. Player grades roll into community
averages and GPAs; game ratings roll into `games.rating_sum` and the score every other
surface reads. A half-formed third-quarter opinion must never reach those numbers.

---

## 1. Data model

Drafts get **their own table**. They are deliberately NOT a `status` column on
`game_reviews` / `performance_reviews`.

> Rationale: with a status column, every existing aggregate query — the game score, the
> community average, the GPA, the Feed rails, the leaderboards — would need
> `WHERE status = 'published'`. Missing one silently publishes someone's draft, which is the
> exact failure this feature exists to prevent. A separate table is *physically incapable*
> of leaking into an aggregate.

> **Constraint that shapes this table:** the `games` table only ever holds **finished**
> games — `/api/games/<id>` 404s while a game is live. So a draft cannot have a foreign key
> to `games`, and the unfinished-logs list cannot join to it for display (that join would
> silently drop exactly the live-game drafts this feature is about). The draft therefore
> carries its own minimal game context, denormalised on purpose.

```sql
CREATE TABLE IF NOT EXISTS game_log_drafts (
    user_id     INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    -- NO foreign key: a live game has no row in `games` yet.
    game_id     TEXT    NOT NULL,
    -- Denormalised game context so a draft is renderable before the game is persisted.
    league      TEXT    NOT NULL DEFAULT 'nba',
    home_abbr   TEXT,
    away_abbr   TEXT,
    -- Nullable and CHECK-free on purpose: a draft is allowed to be incomplete or briefly
    -- invalid. Validation happens at publish time, against the published scale.
    rating      INTEGER,
    review_text TEXT,
    tags        JSONB   DEFAULT '[]'::jsonb,
    attended    BOOLEAN DEFAULT FALSE,
    -- {"2544": 9, "203076": 7}. A blob, not rows: a draft is edited as a unit and nothing
    -- ever aggregates across drafts, so 26 rows per user per game would be churn for no
    -- query benefit.
    grades      JSONB   DEFAULT '{}'::jsonb,
    updated_at  TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (user_id, game_id)
);
CREATE INDEX IF NOT EXISTS idx_drafts_user_updated
    ON game_log_drafts(user_id, updated_at DESC);
```

**Conflict policy:** last write wins on `updated_at`. One user editing the same game from two
devices simultaneously is rare enough not to warrant merge logic; the client sends whole
drafts, so a late write simply overwrites.

**Retention:** a draft is deleted when its log is published. Drafts for games that went final
more than 30 days ago are swept by the daily pipeline (they represent notebooks the user
abandoned, and they'd otherwise accumulate forever).

---

## 2. API

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/api/games/<id>/draft` | Load a draft (or `null`) |
| `PUT` | `/api/games/<id>/draft` | Upsert the whole draft |
| `DELETE` | `/api/games/<id>/draft` | Discard (this is what **Clear** does once drafts are server-side) |
| `GET` | `/api/me/drafts` | Unfinished logs, newest first, with game + status |
| `POST` | `/api/games/<id>/log` | **Publish.** Already exists. Extend to delete the draft in the same transaction. |

Two constraints on publish:

1. It must **reject when the game is not final** — server-side, not just a disabled button.
   The button is a courtesy; the endpoint is the guarantee.
2. Deleting the draft must be **inside the existing transaction**, so a publish can never
   half-commit and leave a stale draft that would resurrect old values on next open.

**Write frequency:** the client debounces `PUT` to ~2s after the last edit, and flushes on
background/close. During a live game a user might touch the notebook a hundred times; we do
not need a hundred round trips. (We already learned this locally — the grade wheel emits a
value per snap, which was writing `UserDefaults` dozens of times per flick.)

---

## 3. Sheet state machine

| Game status | Draft | Live stats | Submit |
|---|---|---|---|
| `scheduled` | Editable (rating, review, attendance) | — no box score yet, grading section hidden | Disabled — "Starts at 7:30" |
| `live` | Fully editable, incl. grading | Polled | **Disabled** — "Submit when the game ends" |
| `final` | Loaded into the sheet | Static final line | **Enabled** |

Notes:

- The disabled-submit state must **explain itself**, not just grey out. A disabled primary
  button with no reason reads as a bug.
- A game going final **while the sheet is open** should enable Submit live, with a visible
  transition ("Final — ready to submit"). That moment is the payoff of the whole session and
  should not require closing and reopening.
- Everything below the game rating already tolerates a missing rating (grades-only logs are
  valid), so no publish-path changes are needed for partial notebooks.

---

## 4. Live stats

This is the main engineering cost, and it is smaller than it looks because the fetch already
exists.

**Today:** `/api/players/today` deliberately serves *season averages* for live games —
`"Live and Scheduled use season avg minutes/points (no live polling needed — cheap and
stable while a game is in progress)"`. A notebook whose stat line does not move while you
watch would feel broken.

**Already available:** `_fetch_live_boxscore_data()` (CDN-first, cached, NBA + WNBA) and
`GET /api/live/boxscore/<game_id>`.

**Work:** give `/api/players/today` a live branch that reads the live box score for
`status == "live"`, returning real `pts / reb / ast / min` instead of season averages.
Cache per game for the poll interval so ten users watching one game do not mean ten CDN
fetches.

**Polling policy:**
- Every **25s**, only while the log sheet is foreground and visible.
- **Stop on backgrounding.** Polling for a screen nobody is looking at is the one way this
  becomes a battery complaint.
- Back off to 60s if the score has not changed in several polls (timeouts, halftime).
- Never poll a `final` game — its box score is cached indefinitely already.

---

## 5. Log sheet redesign

Ships **with** this work rather than after it — the sheet has to change anyway to carry live
state, and the current layout is accreted (things were added below the star picker as they
were invented, so the heading still says "WRITE A REVIEW" and the sheet never says which
game you are logging).

```
┌─ Log this game ──────────────────── ✕ ─┐
│  TOR 112 – 108 WSH        ● LIVE Q3 7:42│  ← identity + live state
│  Sat Aug 16 · Scotiabank Arena          │
├─────────────────────────────────────────┤
│  HOW WAS THE GAME?                      │
│      ★  ★  ★  ★  ☆         4.0          │  ← primary, unmistakable
│                                         │
│  ☐  I was at this game                  │  ← labelled, not a bare ticket icon
├─────────────────────────────────────────┤
│  GRADE THE PLAYERS        3 of 24   ⌄   │
│  ▓▓▓░░░░░░░░░░░                         │
│  [TOR] [WSH]                            │
│  … rows with inline grade wheels …      │
├─────────────────────────────────────────┤
│  ADD A NOTE                    optional │  ← collapsed to one line until tapped
│  ＋ Write something about this game      │
├─────────────────────────────────────────┤
│  Saved · syncing                        │  ← draft state, honest about what's stored
│  [ Clear ]   [ Submit when game ends ]  │
└─────────────────────────────────────────┘
```

Changes from today, each with a reason:

1. **Game identity header.** The sheet currently never says which game you are logging.
   Also the natural home for the arena name, which sets up the arena diary (#9).
2. **Rating promoted.** Bigger, with an actual question and the numeric readout. It is the
   one thing most logs will contain, and today it is visually weaker than the empty text box
   below it.
3. **Attendance becomes a labelled row.** It is currently an unlabelled ticket icon next to
   an unlabelled `+`. Nobody will find it, and it is the seed of the arena diary.
4. **Review collapses to one line** until tapped. Reclaims ~120pt and stops the sheet
   reading as a writing assignment for the majority who want to rate and grade.
5. **Grades above the review.** *Open question — see below.*
6. **Draft state is stated.** "Saved", "Syncing", "Ready to submit". If the notebook is the
   product, its persistence must be visible, not implied.

---

## 6. Unfinished logs + the completion nudge

Server-side drafts create a surface that does not exist today: **games you started and did
not finish**.

- **Where:** a rail or row on the Feed tab (which is already the social/activity surface),
  and/or a section on the profile. Not a new tab.
- **Push:** when a game with an unsubmitted draft goes final, send *"Your log for TOR/WSH is
  ready to submit."* This is the first push with a genuinely useful payload rather than "a
  game happened", and it closes the one real hole in the submit gate — the user who drafts
  through a whole game and never comes back.
- **Depends on** the APNs work (#2), which is not yet set up on the Apple Developer side.

---

## 7. Build order

Roughly nine weeks to opening night (~late Oct 2026). Live logging is worthless in the
offseason, so it competes for the same window as push and the alarm clock.

**Phase 1 — drafts are real** *(the foundation; everything else assumes it)*
- `game_log_drafts` table + the four endpoints
- Publish deletes the draft in-transaction; publish rejects non-final games
- Client: replace `UserDefaults` drafts with server sync, debounced
- Ship the **sheet redesign** here

**Phase 2 — the live session** *(the actual feature)*
- Live branch in `/api/players/today` + per-game cache
- Polling policy in the client
- Live/final state machine + the "game just went final" transition

**Phase 3 — closing the loop**
- Unfinished logs surface
- "Ready to submit" push *(needs APNs from #2)*

**Deliberately deferred:** the C- scale migration, the web pass, and the leaderboards all
stay parked. None of them are season-critical.

**The trade to be explicit about:** this likely outranks the team alarm clock (#1a). The
alarm is a delivery mechanism for a session that does not exist yet; building the session
first makes the alarm obviously worth building second. If both fit, order is
live logging → push → alarm clock.

---

## 8. Open questions

1. ~~Grades above or below the review?~~ **DECIDED 2026-08-20: grades above.** Accepting the
   risk that demoting reviews could reduce them, on the basis that position drives behaviour
   and the review collapses to a single line anyway (so it loses prominence, not presence).
   Worth watching review volume after launch.
2. **Is any of this Pro?** Recommendation: **no** — live logging is the loop that makes the
   app worth using, and gating it hurts adoption. Gate something adjacent instead (notebook
   history, export, season recaps).
3. **Scheduled games — draftable?** Letting someone write pre-game notes is cheap and fits
   the notebook idea, but there is no roster to grade until tip-off. Suggest: allow rating
   and notes, hide the grading section.
4. **What happens to an abandoned draft when its game is >30 days final?** Suggest sweeping
   it in the daily pipeline, but that silently destroys user work — possibly better to keep
   it forever and let the unfinished-logs surface carry the weight.
