# ydkball (nothingbutnet)

NBA & WNBA scores, stats, game reviews, and daily games. Flask backend + static
frontend, Postgres (Railway), plus an iOS app that shares the same API.
Public site: ydkball.net. Code/repo name is "nothingbutnet"; product is "ydkball".

## Run it

```bash
# backend (serves the frontend too)
python backend/server.py            # local dev
# prod: Procfile → gunicorn server:app   (Railway)
```
`backend/server.py` is a Flask app whose static_folder is `frontend/`, so `/`
serves `frontend/index.html` and every page is a static HTML file in `frontend/`.

Config comes from `.env` (DATABASE_URL, Google/Apple OAuth, RevenueCat,
ADMIN_GOOGLE_IDS, GMAIL_* for the health-report email). `.venv/` is the local
virtualenv.

## Naming gotchas (important)

- **"Higher or Lower"** is the user-facing name; the code says **`survival`**
  (`backend/games/survival_api.py`, `survival_daily` table). See `docs/trivia-game.md`.
- **"Guess Who"** is user-facing; the code/routes say **`poeltl`**
  (`backend/games/poeltl_api.py`, `poeltl_daily` table).
- Repo is "nothingbutnet", product is "ydkball".

## Layout

- `backend/server.py` — the monolith: all API routes, page routes, scoreboards,
  games endpoints, admin, RevenueCat webhook. Large; grep by route.
- `backend/auth.py` — Google + Apple sign-in, sessions, mobile Bearer tokens
  (`current_user()`, `login_required`). Admin gate is in server.py:
  `_is_admin` / `_admin_required`, allowlisted by `ADMIN_GOOGLE_IDS` env.
- `backend/games/` — daily games (survival/poeltl) + generators.
- `backend/ingest/` — all data fetch/compute scripts + the daily pipelines.
- `backend/schema*.py` — table DDL (schema.py, schema_additions.py, schema_v3..v5).
- `frontend/` — one static HTML file per page; `/admin` is `admin.html`.

## Frontend conventions

- **Canonical header**: every page uses the same `<header class="site-nav">` markup +
  `/site-nav.css` + `/site-nav.js`. Never add a per-page nav.
- **Design tokens** (the "paper" theme) live in each page's `:root`: `--paper`,
  `--bg-card`, `--ink`/`--ink2..4`, `--orange`, `--gold`, `--green`, `--red`,
  fonts `--ff-serif` (DM Serif Display), `--ff-sans` (Instrument Sans),
  `--ff-mono` (DM Mono).
- **Night mode**: `html.night-mode` overrides the tokens; boot script at the top of
  `<head>` reads `localStorage.ydkball_night`. New pages should support it by using
  the tokens (not hardcoded colors).

## iOS app (separate repo, shared backend)

Native **SwiftUI** app (iOS 17+; bundle id `net.ydkball.ydkball`, team `RHB7DB5Q97`)
living in its **own git repo at `/Users/ethandolder/projects/ydkball/`** — *not* in
this repo. This Flask server is its backend, and the app hits the **production** API
directly (`API.swift` base = `https://ydkball.net`, even in dev). So changing shared
endpoints, auth, or JSON shapes can silently break the app — keep the contract below stable.

- **App layout:** flat — all Swift in `ydkball/ydkball/`. Entry `ydkballApp.swift` (@main).
  Networking in `API.swift`; auth in `AuthManager.swift`; Pro/RevenueCat in `ProManager.swift`.
  Views mirror the web: `ScoresView`, `FeedView` (reviews), `GamesView`/`BrowseGamesView`,
  `DiscoverView`, `ProfileView`, `FriendsView`, plus games `Survival*`/`Poeltl*`, `ProPaywallView`.
- **Auth = Bearer token, not cookies.** Token is kept in `UserDefaults` (key `ydkball.mobileToken`),
  sent as `Authorization: Bearer <token>`; backend `current_user()` (auth.py) accepts a web
  session cookie *or* that token (resolved against `users.mobile_token`).
  - Google: app opens `/auth/google/login?mobile=1` in a web auth session and catches the
    `ydkball://auth-complete?token=…` redirect.
  - Apple (native Sign in with Apple): `POST /auth/apple {identity_token, full_name}` → `{token}`.
- **Pro / subscriptions = RevenueCat** (entitlement id `"pro"`). App calls `Purchases.logIn("<users.id>")`,
  so RevenueCat's `app_user_id` **is** `users.id`; it also trusts backend `is_pro` from `/auth/me`.
  Webhook `POST /api/webhooks/revenuecat` (authed via `REVENUECAT_WEBHOOK_SECRET` header) flips
  `users.is_pro` by event type and logs to `revenue_events`.
- **Deep links:** custom scheme `ydkball://` (auth callback); Universal Links via
  `/.well-known/apple-app-site-association` (appID `RHB7DB5Q97.net.ydkball.ydkball`, only `/profile/*`).

## Data pipelines (keep the DB fresh)

Three jobs, see `DATABASE_MAP.md` for the full table-by-table map.
- `backend/ingest/daily_update.py` — **Railway cron** (`cloud_daily`): steps that
  work from a datacenter IP (team records, players sync, DARKO/LEBRON/Net-Pts).
- `backend/ingest/daily_update_local.py` — **Windows Task Scheduler** (`local_daily`):
  steps that need a residential IP because stats.nba.com blocks Railway
  (season stats, PBP, gamelogs, lineups, pctiles, **WNBA season averages** —
  stats.wnba.com is behind the same gate). Writes straight to Railway PG.
- `backend/games/generate_daily.py` — **Railway cron** (`puzzle_gen`): pre-generates
  the Higher-or-Lower daily. Guess Who is generated lazily on first request (no cron).

All NBA/WNBA CDN calls go through `_cdn_get()` with curl_cffi Chrome impersonation
to defeat Akamai TLS fingerprinting on Railway IPs — see `docs/cdn-akamai-bot-manager.md`.

## Awards ballots ("Ballots")

A preseason prediction sheet: one per user, per league, per season. Code says
`list_type = 'awards'` on `game_lists`; user-facing name is **Ballot**.

**Why it reuses `game_lists`:** ballots inherit likes, comments, the public
`/list/<id>` page and its OG card for free. What they don't inherit is item
shape — a ballot is fixed slots, not an ordered bag — so picks live in
`award_ballot_items`, keyed `(list_id, award_code)`. That primary key *is* the
"one pick per award" rule.

- **Slots** — `_AWARD_TEMPLATES` in server.py. NBA has 8 (CHAMPION, MVP, ROTY,
  DPOY, 6MOY, MIP, CPOY, COY); WNBA has 6 (no Clutch Player award exists, and we
  hold no WNBA coach data). Each slot has an `entity` — `player`, `team` or
  `coach` — which decides what the picker offers, what counts as eligible, and
  how a pick is graded. It's a property of the award, so adding the team and
  coach slots needed no schema change.
- **One per season** — partial unique index (schema_v10). Not cosmetic: ballots
  pay XP, so being able to make ten and cover every MVP candidate is an exploit.
- **Open/closed is automatic.** `_award_window()` reads `scheduled_games` at
  request time — never `get_current_season()`, which returns the season that just
  *ended* all summer. `locked_at` is stamped at creation from the first scheduled
  tipoff. No cron, nothing to toggle.
- **Ball Knowledge** — 50 for a complete locked ballot, 100 per correct pick,
  granted lazily when the owner next opens it (`_grant_ballot_xp`). `_grant_xp`
  dedupes on `(user, type, reference_id)`, so re-reads are free.
- **Not copyable, not rankable, not renamable** — enforced in `update_list` as
  well as the client, because the generic edit sheet posts every field back.

### Grading (once a year, mostly automatic)

`backend/ingest/grade_awards.py` runs in `daily_update_local.py` and gates
itself: it does nothing unless locked ballots exist for a season whose winners
aren't recorded. When they are announced it runs `fetch_awards.py
--refresh-season` (needed because that script's progress file otherwise skips
everyone) then `record_award_results.py`.

That covers 7 of 8 NBA awards. **Coach of the Year has no upstream feed** and is
entered by hand — as are all WNBA player awards:

```bash
python backend/ingest/record_award_results.py --season 2026-27 --set COY="Name"
```

The champion needs no feed at all: it's the winner of the season's last playoff
game. `health_check.py` warns whenever locked ballots have no answer key, so the
annual step nags you rather than the reverse.

### Testing it

Every state is date-driven and so unreachable by waiting.
`backend/dev_ballot_scenarios.py you@example.com --state graded` stages any of
`empty · partial · complete · locked · graded · claimed` (plus `--league wnba`,
otherwise unreachable until spring) and prints what to expect in the app. Always
`--reset` after: it plants fabricated winners that would grade real ballots.

### Player teams in the offseason

`players.current_team` (schema_v12) answers "who does this player play for now",
which the stats tables can't — `player_seasons.team_abbr` only moves when a
player takes the floor, so every summer trade was invisible until October.
Filled daily by `sync_player_teams.py` from one CommonAllPlayers call.
`season_util.roster_season()` is the rule: **stats resolve from played games,
membership resolves from the schedule.**

Identity surfaces (search, pickers, profile header) read `current_team`; stat
rows keep the team the stats were earned with. Browse Players sends both, so a
row can read "LAL → PHI".

## Monitoring (admin + health)

- `/admin` (admin-gated) → **Insights** dashboard + **Moderation**. Insights are
  served by `/api/admin/dashboard`.
- `backend/ingest/health_check.py` — verifies pipeline freshness/run-tracking,
  today's daily puzzles, row-count anomalies, and structural gaps. Standalone it
  prints/saves a report (exit 1 on FAIL); `--email` sends it via Gmail SMTP.
  Importable via `collect(conn, ...)` (needs a PLAIN psycopg2 conn).
- `backend/ingest/pipeline_status.py` + `pipeline_runs` table — the three pipelines
  record start/finish/per-step status so the health check knows "did it run today?".

## Conventions

- DB access: `get_conn()` in server.py returns a thread-local `RealDictCursor`
  connection (dict rows). `health_check.py` uses positional rows — give it its own
  plain connection.
- `datetime` the class is NOT imported at module scope in server.py (only `date`,
  `timedelta`); import it locally as needed.
- Commit/push only when asked; branch off `master`. Railway auto-deploys `master`.
